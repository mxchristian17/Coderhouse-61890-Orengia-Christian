DROP DATABASE IF EXISTS data_lake_db;
CREATE DATABASE data_lake_db;
\c data_lake_db ;

CREATE SCHEMA stage ;
CREATE SCHEMA prod ;


CREATE TABLE stage.population_data (
    id_evento SERIAL PRIMARY KEY,
    data_evento TEXT,
    fecha_evento DATE
);

CREATE TABLE stage.weather_data (
    id_evento SERIAL PRIMARY KEY,
    data_evento TEXT,
    fecha_evento DATE
);

SELECT 
  column_name, 
  data_type, 
  character_maximum_length, 
  is_nullable, 
  column_default 
FROM 
  information_schema.columns 
WHERE 
  table_name = 'population_data';

SELECT 
  column_name, 
  data_type, 
  character_maximum_length, 
  is_nullable, 
  column_default 
FROM 
  information_schema.columns 
WHERE 
  table_name = 'weather_data';

CREATE TABLE prod.population_weather_relation (
    id_evento INT PRIMARY KEY,
    cambio_poblacion FLOAT,
    cambio_temperatura_promedio FLOAT
    ubicacion VARCHAR(100),
    anio INT
);