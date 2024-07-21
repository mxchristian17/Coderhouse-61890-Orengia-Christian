import requests
import psycopg2
import json
from datetime import datetime

# Configuración de la conexión a la base de datos
db_config = {
    'host': 'db',
    'port': 5432,
    'dbname': 'data_lake_db',
    'user': 'postgres',
    'password': 'dataEngineering'
}

def get_population_data():
    url = 'https://api.population.io/1.0/population/World/today-and-tomorrow/'
    response = requests.get(url)
    data = response.json()
    return data

def get_weather_data():
    url = 'https://api.open-meteo.com/v1/forecast?latitude=35&longitude=139&hourly=temperature_2m'
    response = requests.get(url)
    data = response.json()
    return data

def create_tables(conn):
    create_population_table_query = '''
    CREATE TABLE IF NOT EXISTS stage.population_data (
        id_evento SERIAL PRIMARY KEY,
        data_evento TEXT,
        fecha_evento DATE
    );
    '''
    create_weather_table_query = '''
    CREATE TABLE IF NOT EXISTS stage.weather_data (
        id_evento SERIAL PRIMARY KEY,
        data_evento TEXT,
        fecha_evento DATE
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
    INSERT INTO stage.population_data (data_evento, fecha_evento)
    VALUES (%s, %s);
    '''
    with conn.cursor() as cursor:
        for entry in population_data['total_population']:
            data_evento = json.dumps(entry)
            fecha_evento = datetime.strptime(entry['date'], '%Y-%m-%d').date()
            cursor.execute(insert_query, (data_evento, fecha_evento))
        conn.commit()

def insert_weather_data(conn, weather_data):
    insert_query = '''
    INSERT INTO stage.weather_data (data_evento, fecha_evento)
    VALUES (%s, %s);
    '''
    with conn.cursor() as cursor:
        for i, temp in enumerate(weather_data['hourly']['temperature_2m']):
            data_evento = json.dumps({'hour': i, 'temperature': temp})
            fecha_evento = datetime.now().date()  # Suponiendo que la fecha es la actual
            cursor.execute(insert_query, (data_evento, fecha_evento))
        conn.commit()

def main():
    # Obtener datos de las APIs
    population_data = get_population_data()
    weather_data = get_weather_data()
    
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
