from dotenv import load_dotenv
from tqdm import tqdm
from dags_modules.extract import get_population_data, get_weather_data
from dags_modules.transform import transform_population_data, transform_weather_data
from dags_modules.load import insert_population_data, insert_weather_data
from dags_modules.table_management import create_tables, drop_old_data, Session
from dags_modules.auxiliary_functions import get_max_year_population
from sqlalchemy.exc import SQLAlchemyError

# Load credentials from credentials.env
load_dotenv('/.env')

# Set default values to get from API
default_min_age = 18
default_max_age = 19
default_delete_old_data = False
default_latitude = -38.00042
default_longitude = -57.5562

def etl_process():

    # Create database session
    try:
        session = Session()
    except SQLAlchemyError as e:
        print(f"\033[1;31mError: Unable to create a session with the database.\033[0m")
        print(f"\033[1;31mDetails: {e}\033[0m")
        return
    
    try:
        # Empty the tables if desired
        delete_old_data = default_delete_old_data

        if delete_old_data:
            print(f"\033[1;31mDeleting old data\033[0m")
            drop_old_data()
        
        # Create the tables
        print(f"\033[1;34mCreating tables...\033[0m")
        create_tables()
    except SQLAlchemyError as e:
        print(f"\033[1;31mError: Unable to manage database tables.\033[0m")
        print(f"\033[1;31mDetails: {e}\033[0m")
        return
    
    try:
        max_database_year = get_max_year_population(session)
    except Exception as e:
        error_message = f"Error obtaining max year from database: {e}"
        print(error_message)
        raise RuntimeError(error_message) from e

    # Search values for ETL
    min_age = default_min_age
    max_age = default_max_age
    min_year = max_database_year+1
    max_year = max_database_year+2
    latitude = default_latitude
    longitude = default_longitude

    # Extract data from the APIs
    try:

        population_data = []
        weather_data = []
        print(f"\033[1;34mExtracting APIs data\033[0m")
        for x in range(min_year, max_year):
            print(f"\033[1;32mProgress: Year {x}\033[0m")
            for y in range(min_age, max_age):
                print(f"Fetching data for ages in year {x}")
                data = get_population_data(x, y)
                if data:
                    population_data.append(data)
            data = get_weather_data(x, latitude, longitude)
            if data:
                weather_data.append(data)

    except Exception as e:
        error_message = f"Error obtaining APIs data: {e}"
        print(error_message)
        raise RuntimeError(error_message) from e
    
    # Transform and adapt data to dataframe
    try:
        print(f"\033[1;34mTransforming 'Population Data' to dataframe\033[0m")
        population_df = transform_population_data(population_data)

        print(f"\033[1;34mTransforming 'Weather Data' to dataframe\033[0m")
        weather_df = transform_weather_data(weather_data)

    except Exception as e:
        error_message = f"Error during data transformation: {e}"
        print(error_message)
        raise RuntimeError(error_message) from e
    
    

    try:
        
        # Insert data into the tables
        print(f"\033[1;34mInserting population data...\033[0m")
        insert_population_data(session, population_df)
        
        print(f"\033[1;34mInserting weather data...\033[0m")
        insert_weather_data(session, weather_df)

        print(f"\033[1;32mWell done!! Great job!\033[0m")
    
    except SQLAlchemyError as e:
        print(f"\033[1;31mError: An error occurred during the database operations.\033[0m")
        print(f"\033[1;31mDetails: {e}\033[0m")
        session.rollback()  # Revert changes if fails
        
    finally:
        # Ensure the connection is closed
        session.close()