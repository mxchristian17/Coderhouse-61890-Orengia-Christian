from dotenv import load_dotenv
from tqdm import tqdm
from extract import get_population_data, get_weather_data
from transform import transform_population_data, transform_weather_data
from load import insert_population_data, insert_weather_data
from table_management import create_tables, drop_old_data, Session
from auxiliary_functions import get_valid_input
from sqlalchemy.exc import SQLAlchemyError

# Load credentials from credentials.env
load_dotenv('/credentials.env')

# Set default values to get from API
default_min_age = 18
default_max_age = 19
default_min_year = 1990
default_max_year = 1991
default_delete_old_data = False
default_latitude = -38.00042
default_longitude = -57.5562

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

print(f"")
print(f"    \033[1;32mAge range: {min_age} - {max_age}\033[0m")
print(f"    \033[1;32mYear range: {min_year} - {max_year}\033[0m")
print(f"")


def main():
    # Extract data from the APIs
    population_data = []
    weather_data = []
    print(f"    \033[1;34mExtracting APIs data\033[0m")
    print(f"")
    for x in tqdm(range(min_year, max_year), desc=f"    \033[1;32mProgress\033[0m", leave=True):
        for y in tqdm(range(min_age, max_age), desc=f"    Fetching data for ages in year {x}", leave=True):
            data = get_population_data(x, y)
            if data:
                population_data.append(data)
        data = get_weather_data(x, latitude, longitude)
        if data:
            weather_data.append(data)
    
    # Transform and adapt data to dataframe
    print(f"    \033[1;34mTransforming `Population Data` data to dataframe\033[0m")
    print(f"")
    population_df = transform_population_data(population_data)

    print(f"    \033[1;34mTransforming `Weather Data` data to dataframe\033[0m")
    print(f"")
    weather_df = transform_weather_data(weather_data)
    
    # Create database session
    try:
        session = Session()
    except SQLAlchemyError as e:
        print(f"\033[1;31mError: Unable to create a session with the database.\033[0m")
        print(f"\033[1;31mDetails: {e}\033[0m")
        return

    try:
        # Empty the tables if desired
        if delete_old_data:
            print(f"    \033[1;31mDeleting old data\033[0m")
            print(f"")
            drop_old_data()
        
        # Create the tables
        print(f"")
        print(f"    \033[1;34mCreating tables...\033[0m")
        create_tables()
        
        # Insert data into the tables
        print(f"")
        print(f"    \033[1;34mInserting population data...\033[0m")
        insert_population_data(session, population_df)
        
        print(f"")
        print(f"    \033[1;34mInserting weather data...\033[0m")
        insert_weather_data(session, weather_df)

        print(f"")
        print(f"    \033[1;32mWell done!! Great job!\033[0m")
    
    except SQLAlchemyError as e:
        print(f"\033[1;31mError: An error occurred during the database operations.\033[0m")
        print(f"\033[1;31mDetails: {e}\033[0m")
        session.rollback()  # Revert changes if fails
        
    finally:
        # Ensure the connection is closed
        session.close()


if __name__ == '__main__':
    main()
