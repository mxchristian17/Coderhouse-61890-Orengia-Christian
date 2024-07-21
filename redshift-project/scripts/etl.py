import requests
import psycopg2
import json
from datetime import datetime

min_age = 18
max_age = 20
min_year = 1990
max_year = 1992

# Configuración de la conexión a la base de datos
db_config = {
    'host': 'db',
    'port': 5432,
    'dbname': 'etl',
    'user': 'postgres',
    'password': 'dataEngineering'
}

def get_population_data(year, age):
    url = f'https://d6wn6bmjj722w.population.io:443/1.0/population/{year}/aged/{age}/'
    response = requests.get(url)
    data = response.json()
    return data

def get_weather_data(year):
    url = f'https://archive-api.open-meteo.com/v1/archive?latitude=52.52,37.22&longitude=13.41,33.41&start_date={year}-01-01&end_date={year}-12-31&daily=apparent_temperature_max,apparent_temperature_min,daylight_duration,precipitation_sum,wind_speed_10m_max'
    response = requests.get(url)
    data = response.json()
    return data

def create_tables(conn):
    create_population_table_query = '''
    CREATE TABLE IF NOT EXISTS stage.population_data (
        id_evento SERIAL PRIMARY KEY,
        data_evento TEXT
    );
    '''
    create_weather_table_query = '''
    CREATE TABLE IF NOT EXISTS stage.weather_data (
        id_evento SERIAL PRIMARY KEY,
        data_evento TEXT
    );
    '''
    create_relation_table_query = '''
    CREATE TABLE IF NOT EXISTS prod.population_weather_relation (
        id_evento INT PRIMARY KEY,
        cambio_poblacion FLOAT,
        cambio_temperatura_promedio FLOAT,
        ubicacion VARCHAR(100),
        anio INT
    );
    '''
    with conn.cursor() as cursor:
        cursor.execute(create_population_table_query)
        cursor.execute(create_weather_table_query)
        cursor.execute(create_relation_table_query)
        conn.commit()

def insert_population_data(conn, population_data):
    insert_query = '''
    INSERT INTO stage.population_data (data_evento)
    VALUES (%s);
    '''
    with conn.cursor() as cursor:
        for entry in population_data:
            data_evento = json.dumps(entry)
            cursor.execute(insert_query, (data_evento,))
        conn.commit()

def insert_weather_data(conn, weather_data):
    insert_query = '''
    INSERT INTO stage.weather_data (data_evento)
    VALUES (%s);
    '''
    with conn.cursor() as cursor:
        for entry in weather_data:
            data_evento = json.dumps(entry)
            cursor.execute(insert_query, (data_evento,))
        conn.commit()

def main():
    # Obtener datos de las APIs
    population_data = []
    weather_data = []
    for x in range(min_year, max_year):
        for y in range(min_age, max_age):
            data = get_population_data(x, y)
            population_data.append(data)
            #weather_data = get_weather_data()
        data = get_weather_data(x)
        weather_data.append(data)
    # Conectar a la base de datos
    conn = psycopg2.connect(**db_config)
    
    # Crear las tablas
    create_tables(conn)
    
    # Insertar datos en las tablas
    insert_population_data(conn, population_data)
    insert_weather_data(conn, weather_data)
    
    # Cerrar la conexión
    conn.close()

if __name__ == '__main__':
    main()
