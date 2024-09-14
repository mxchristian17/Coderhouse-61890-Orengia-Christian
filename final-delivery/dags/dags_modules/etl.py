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

# Extract phase
def extract_data():
    try:
        session = Session()
        max_database_year = get_max_year_population(session)
    except SQLAlchemyError as e:
        print(f"Error: Unable to create a session with the database. Details: {e}")
        raise RuntimeError(e)

    min_age = default_min_age
    max_age = default_max_age
    min_year = max_database_year + 1
    max_year = max_database_year + 2
    latitude = default_latitude
    longitude = default_longitude

    try:
        population_data = []
        weather_data = []
        print("Extracting APIs data")
        for x in range(min_year, max_year):
            for y in range(min_age, max_age):
                data = get_population_data(x, y)
                if data:
                    population_data.append(data)
            weather = get_weather_data(x, latitude, longitude)
            if weather:
                weather_data.append(weather)

        return population_data, weather_data

    except Exception as e:
        print(f"Error obtaining API data: {e}")
        raise RuntimeError(e)

# Transform phase
def transform_data(population_data, weather_data):
    try:
        print("Transforming Population Data to dataframe")
        population_df = transform_population_data(population_data)

        print("Transforming Weather Data to dataframe")
        weather_df = transform_weather_data(weather_data)

        return population_df, weather_df

    except Exception as e:
        print(f"Error during data transformation: {e}")
        raise RuntimeError(e)

# Load phase
def load_data(population_df, weather_df):
    try:
        session = Session()

        # Insert data into tables
        print("Inserting population data...")
        insert_population_data(session, population_df)

        print("Inserting weather data...")
        insert_weather_data(session, weather_df)

        print("Data successfully loaded.")

    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        session.rollback()
        raise RuntimeError(e)
    
    finally:
        session.close()

# Create and drop tables
def setup_database():
    try:
        delete_old_data = default_delete_old_data
        if delete_old_data:
            print("Deleting old data")
            drop_old_data()

        print("Creating tables")
        create_tables()

    except SQLAlchemyError as e:
        print(f"Error managing database tables: {e}")
        raise RuntimeError(e)