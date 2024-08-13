import requests
import psycopg2
import json
from dotenv import load_dotenv
import os
from tqdm import tqdm
import pandas as pd

# Load credentials from credentials.env
load_dotenv('/credentials.env')

# Set default values to get from API
default_min_age = 18
default_max_age = 19
default_min_year = 1990
default_max_year = 1991
default_delete_old_data = False
default_latitude = 38.00
default_longitude = 57.50

# Function to get valid input with specified ranges
def get_valid_input(prompt, default_value, min_value=None, max_value=None, input_type=int):
    while True:
        try:
            value = input(f"{prompt} (default: {default_value}): ").strip()
            if value == "":
                value = default_value
            elif input_type == bool:
                if value.lower() in ['true', 't', 'yes', 'y', '1']:
                    value = True
                elif value.lower() in ['false', 'f', 'no', 'n', '0']:
                    value = False
                else:
                    raise ValueError("Invalid boolean value")
            else:
                value = input_type(value)
            if min_value is not None and value < min_value:
                print(f"Please enter a value bigger than {min_value}.")
            elif max_value is not None and value > max_value:
                print(f"Please enter a value smaller than {max_value}.")
            else:
                return value
        except ValueError:
            print("Please enter a valid value.")

# Request valid input from the user
print(f"")
delete_old_data = get_valid_input("    \033[1;31mDo you want to delete the previous database data?\033[0m", default_delete_old_data, input_type=bool)
print(f"")
min_age = get_valid_input("    \033[1;33mEnter minimum age\033[0m", default_min_age, min_value=0)
max_age = get_valid_input("    \033[1;33mEnter maximum age\033[0m", default_max_age, min_value=min_age)
min_year = get_valid_input("    \033[1;33mEnter start year\033[0m", default_min_year, min_value=1900)
max_year = get_valid_input("    \033[1;33mEnter end year\033[0m", default_max_year, min_value=min_year)
latitude = get_valid_input("    \033[1;33mEnter latitude to analysis\033[0m", default_latitude, min_value=-90, max_value=90)
longitude = get_valid_input("    \033[1;33mEnter longitude to analysis\033[0m", default_longitude, min_value=-180, max_value=180)

# Here you can continue with the rest of your script, using min_age, max_age, start_year, and end_year
print(f"")
print(f"    \033[1;32mAge range: {min_age} - {max_age}\033[0m")
print(f"    \033[1;32mYear range: {min_year} - {max_year}\033[0m")
print(f"")

# Configuration for the database connection
db_config = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'dbname': os.getenv('DB_DATABASE'),
    'user': os.getenv('DB_USERNAME'),
    'password': os.getenv('DB_PWD')
}

def get_population_data(year, age):
    try:
        url = f'https://d6wn6bmjj722w.population.io:443/1.0/population/{year}/aged/{age}/'
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"    \033[1;31mError fetching population data:\033[0m")
        print(f"{e}")
        return None

def get_weather_data(year):
    try:
        url = f'https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}&start_date={year}-01-01&end_date={year}-12-31&daily=apparent_temperature_max,apparent_temperature_min,daylight_duration,precipitation_sum,wind_speed_10m_max'
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"    \033[1;31mError fetching weather data:\033[0m")
        print(f"{e}")
        return None

def transform_population_data(data):
    # Create the DataFrame
    flattened_data = sum(data, []) if data else []
    df = pd.DataFrame(flattened_data)
    return df

def transform_wheather_data(data):
    if not data or 'daily' not in data:
        raise ValueError("Data must contain 'daily' key")
    
    # Extract the daily data
    daily_data = data['daily']
    
    # Create a DataFrame from the 'daily' data
    df = pd.DataFrame({
        'date': daily_data['time'],
        'apparent_temperature_max': daily_data['apparent_temperature_max'],
        'apparent_temperature_min': daily_data['apparent_temperature_min'],
        'daylight_duration': daily_data.get('daylight_duration', [None]*len(daily_data['time'])),
        'precipitation_sum': daily_data.get('precipitation_sum', [None]*len(daily_data['time'])),
        'wind_speed_10m_max': daily_data.get('wind_speed_10m_max', [None]*len(daily_data['time'])),
    })
    
    # Convert date column to datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Fill missing values if needed, here filling with NaN as default
    df.fillna(value=pd.NA, inplace=True)
    
    return df
    

def create_tables(conn):
    create_population_table_query = '''
    CREATE TABLE IF NOT EXISTS population_data (
        id_event INT IDENTITY(1,1) PRIMARY KEY,
        females INT,
        country VARCHAR(250),
        age SMALLINT,
        males INT,
        year SMALLINT,
        total INT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    '''
    create_weather_table_query = '''
    CREATE TABLE IF NOT EXISTS weather_data (
        id_event INT IDENTITY(1,1) PRIMARY KEY,
        date DATE,
        latitude FLOAT,
        longitude FLOAT,
        wind_speed_10m_max FLOAT,
        daylight_duration FLOAT,
        apparent_temperature_max FLOAT,
        apparent_temperature_min FLOAT,
        precipitation_sum FLOAT
    );
    '''
    create_relation_table_query = '''
    CREATE TABLE IF NOT EXISTS population_weather_relation (
        id_event INT PRIMARY KEY,
        population_change FLOAT,
        average_temperature_change FLOAT,
        location VARCHAR(100),
        year INT
    );
    '''
    with conn.cursor() as cursor:
        cursor.execute(create_population_table_query)
        cursor.execute(create_weather_table_query)
        cursor.execute(create_relation_table_query)
        conn.commit()

def drop_old_data(conn):
    drop_population_table_query = '''
    DELETE FROM population_data;
    '''
    drop_weather_table_query = '''
    DELETE FROM weather_data;
    '''
    drop_relation_table_query = '''
    DELETE FROM population_weather_relation;
    '''
    with conn.cursor() as cursor:
        cursor.execute(drop_population_table_query)
        cursor.execute(drop_weather_table_query)
        cursor.execute(drop_relation_table_query)
        conn.commit()

def insert_population_data(conn, population_df):
    # Show in console the first rows of the dataframe
    print("")
    print(f"    Population DataFrame shape: {population_df.shape}")
    print(f"    First few rows of the DataFrame:")
    print("")
    print(population_df.head())
    print("")
    # Insert data in database showing progress bar in shell
    insert_query = '''
    INSERT INTO population_data (females, country, age, males, year, total)
    VALUES (%s, %s, %s, %s, %s, %s);
    '''
    with conn.cursor() as cursor:
        for idx, row in tqdm(population_df.iterrows(), total=population_df.shape[0], desc=f"    \033[1;32mProgress\033[0m"):
            try:
                cursor.execute(insert_query, (row['females'], row['country'], row['age'], row['males'], row['year'], row['total']))
            except psycopg2.Error as e:
                print(f"\r    \033[1;31mError inserting row {idx + 1}: {e}\033[0m", end="")
        conn.commit()
    print("")
    print(f"    \033[1;32mAll rows inserted successfully.\033[0m")

def insert_weather_data(conn, weather_data):
    insert_query = '''
    INSERT INTO weather_data (event_data)
    VALUES (%s);
    '''
    with conn.cursor() as cursor:
        for entry in weather_data:
            event_data = json.dumps(entry)
            cursor.execute(insert_query, (event_data,))
        conn.commit()

def main():
    # Fetch data from the APIs
    population_data = []
    weather_data = []
    print(f"    \033[1;34mExtracting APIs data\033[0m")
    print(f"")
    for x in tqdm(range(min_year, max_year), desc=f"    \033[1;32mProgress\033[0m", leave=True):
        for y in tqdm(range(min_age, max_age), desc=f"    Fetching data for ages in year {x}", leave=True):
            data = get_population_data(x, y)
            if data:
                population_data.append(data)
        data = get_weather_data(x)
        if data:
            weather_data.append(data)
    
    # Transform data to dataframe
    print(f"    \033[1;34mTransforming APIs data to dataframe\033[0m")
    print(f"")
    population_df = transform_population_data(population_data)

    # Connect to the database
    try:
        conn = psycopg2.connect(**db_config)
    except psycopg2.Error as e:
        print(f"    \033[1;31mError connecting to the database:\033[0m")
        print(f"{e}")
        print(f"")
        return
    
    try:
        # Empty the tables if desired
        if delete_old_data:
            print(f"    \033[1;31mDeleting old data\033[0m")
            print(f"")
            drop_old_data(conn)
        
        # Create the tables
        print(f"")
        print(f"    \033[1;34mCreating tables...\033[0m")
        create_tables(conn)
        
        # Insert data into the tables
        print(f"")
        print(f"    \033[1;34mInserting population data...\033[0m")
        insert_population_data(conn, population_df)
        
        print(f"")
        print(f"    \033[1;34mInserting weather data...\033[0m")
        insert_weather_data(conn, weather_data)
        
    finally:
        # Ensure the connection is closed
        conn.close()
        print(f"")
        print(f"    \033[1;32mWell done!! Great job!\033[0m")


if __name__ == '__main__':
    main()
