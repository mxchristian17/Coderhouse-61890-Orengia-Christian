import psycopg2
from tqdm import tqdm

# Exception for duplicated rows
class DuplicateRowError(Exception):
    pass

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

    check_query = '''
    SELECT 1 FROM population_data 
    WHERE females=%s AND country=%s AND age=%s AND males=%s AND year=%s AND total=%s;
    '''

    with conn.cursor() as cursor:
        for idx, row in tqdm(population_df.iterrows(), total=population_df.shape[0], desc=f"    \033[1;32mProgress\033[0m"):
            cursor.execute(check_query, (row['females'], row['country'], row['age'], row['males'], row['year'], row['total']))
            try:
                if cursor.fetchone() is None:
                    cursor.execute(insert_query, (row['females'], row['country'], row['age'], row['males'], row['year'], row['total']))
                else:
                    raise DuplicateRowError(f"\r    \033[1;33mRow {idx + 1} already exists, skipping...\033[0m")
            except DuplicateRowError as e:
                print(f"\033[1;33m{e}\033[0m")
            except psycopg2.Error as e:
                print(f"\033[1;31mError: {e}\033[0m")
        conn.commit()

    print("")
    print(f"    \033[1;32mAll rows inserted successfully.\033[0m")

def insert_weather_data(conn, weather_df):
    # Mostrar en consola las primeras filas del dataframe
    print("")
    print(f"    Weather DataFrame shape: {weather_df.shape}")
    print(f"    First few rows of the DataFrame:")
    print("")
    print(weather_df.head())
    print("")
    
    # Consulta de inserción
    insert_query = '''
    INSERT INTO weather_data (
        date, latitude, longitude, elevation, 
        wind_speed_10m_max, daylight_duration, 
        apparent_temperature_max, apparent_temperature_min, 
        precipitation_sum
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    '''

    check_query = '''
    SELECT 1 FROM weather_data 
    WHERE date=%s AND latitude=%s AND longitude=%s AND elevation=%s AND 
          wind_speed_10m_max=%s AND daylight_duration=%s AND 
          apparent_temperature_max=%s AND apparent_temperature_min=%s AND 
          precipitation_sum=%s;
    '''
    
    with conn.cursor() as cursor:
        for idx, row in tqdm(weather_df.iterrows(), total=weather_df.shape[0], desc=f"    \033[1;32mProgress\033[0m"):
            cursor.execute(check_query, (
                row['date'], row['latitude'], row['longitude'], row['elevation'], 
                row['wind_speed_10m_max'], row['daylight_duration'], 
                row['apparent_temperature_max'], row['apparent_temperature_min'], 
                row['precipitation_sum']
            ))
            try:
                if cursor.fetchone() is None:
                    cursor.execute(insert_query, (
                        row['date'], row['latitude'], row['longitude'], row['elevation'], 
                        row['wind_speed_10m_max'], row['daylight_duration'], 
                        row['apparent_temperature_max'], row['apparent_temperature_min'], 
                        row['precipitation_sum']
                    ))
                else:
                    raise DuplicateRowError(f"\r    \033[1;33mRow {idx + 1} already exists, skipping...\033[0m")
            except DuplicateRowError as e:
                print(f"\033[1;33m{e}\033[0m")
            except psycopg2.Error as e:
                print(f"\033[1;31mError: {e}\033[0m")
        conn.commit()
    
    print("")
    print(f"    \033[1;32mAll rows inserted successfully.\033[0m")