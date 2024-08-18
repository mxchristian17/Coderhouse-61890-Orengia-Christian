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
        elevation FLOAT,
        wind_speed_10m_max FLOAT,
        daylight_duration FLOAT,
        apparent_temperature_max FLOAT,
        apparent_temperature_min FLOAT,
        precipitation_sum FLOAT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    DROP TABLE IF EXISTS population_data;
    '''
    drop_weather_table_query = '''
    DROP TABLE IF EXISTS weather_data;
    '''
    drop_relation_table_query = '''
    DROP TABLE IF EXISTS population_weather_relation;
    '''
    with conn.cursor() as cursor:
        cursor.execute(drop_population_table_query)
        cursor.execute(drop_weather_table_query)
        cursor.execute(drop_relation_table_query)
        conn.commit()