from tqdm import tqdm
from sqlalchemy import func, and_
from sqlalchemy.orm import Session
from table_management import PopulationData, WeatherData

# Exception for duplicated rows
class DuplicateRowError(Exception):
    pass

def insert_population_data(session: Session, population_df):
    # Show in console the first rows of the dataframe
    print("")
    print(f"    Population DataFrame shape: {population_df.shape}")
    print(f"    First few rows of the DataFrame:")
    print("")
    print(population_df.head())
    print("")

    max_id = session.query(func.max(PopulationData.id_event)).scalar()
    if max_id is None:
        max_id = 0


    for idx, row in tqdm(population_df.iterrows(), total=population_df.shape[0], desc=f"    \033[1;32mProgress\033[0m"):
        # Check if the row already exists in the database
        existing_row = session.query(PopulationData).filter(
            and_(
                PopulationData.females == row['females'],
                PopulationData.country == row['country'],
                PopulationData.age == row['age'],
                PopulationData.males == row['males'],
                PopulationData.year == row['year'],
                PopulationData.total == row['total']
            )
        ).first()

        if existing_row:
            print(f"\r    \033[1;33mRow {idx + 1} already exists, skipping...\033[0m")
            continue

        population_entry = PopulationData(
            id_event=max_id + 1,
            females=row['females'],
            country=row['country'],
            age=row['age'],
            males=row['males'],
            year=row['year'],
            total=row['total']
        )
        max_id = max_id + 1

        try:
            # Try to add the new entry
            session.add(population_entry)
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"\r    \033[1;31mError inserting row {idx + 1}: {e}, skipping...\033[0m")
    
    print("")
    print(f"    \033[1;32mAll rows inserted successfully.\033[0m")

def insert_weather_data(session: Session, weather_df):
    # Show in console the first rows of the dataframe
    print("")
    print(f"    Weather DataFrame shape: {weather_df.shape}")
    print(f"    First few rows of the DataFrame:")
    print("")
    print(weather_df.head())
    print("")

    max_id = session.query(func.max(WeatherData.id_event)).scalar()
    if max_id is None:
        max_id = 0
    
    for idx, row in tqdm(weather_df.iterrows(), total=weather_df.shape[0], desc=f"    \033[1;32mProgress\033[0m"):
        # Check if the row already exists in the database
        existing_row = session.query(WeatherData).filter(
            and_(
                WeatherData.date == row['date'],
                WeatherData.latitude == row['latitude'],
                WeatherData.longitude == row['longitude'],
                WeatherData.elevation == row['elevation'],
                WeatherData.wind_speed_10m_max == row['wind_speed_10m_max'],
                WeatherData.daylight_duration == row['daylight_duration'],
                WeatherData.apparent_temperature_max == row['apparent_temperature_max'],
                WeatherData.apparent_temperature_min == row['apparent_temperature_min'],
                WeatherData.precipitation_sum == row['precipitation_sum']
            )
        ).first()

        if existing_row:
            print(f"\r    \033[1;33mRow {idx + 1} already exists, skipping...\033[0m")
            continue

        weather_entry = WeatherData(
            id_event=max_id + 1,
            date=row['date'],
            latitude=row['latitude'],
            longitude=row['longitude'],
            elevation=row['elevation'],
            wind_speed_10m_max=row['wind_speed_10m_max'],
            daylight_duration=row['daylight_duration'],
            apparent_temperature_max=row['apparent_temperature_max'],
            apparent_temperature_min=row['apparent_temperature_min'],
            precipitation_sum=row['precipitation_sum']
        )
        max_id = max_id + 1
        
        try:
            # Try to add the new entry
            session.add(weather_entry)
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"\r    \033[1;31mError inserting row {idx + 1}: {e}, skipping...\033[0m")
    
    print("")
    print(f"    \033[1;32mAll rows inserted successfully.\033[0m")