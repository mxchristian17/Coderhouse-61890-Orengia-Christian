DROP DATABASE IF EXISTS etl;
CREATE DATABASE etl;
\c etl ;

CREATE SCHEMA stage ;
CREATE SCHEMA prod ;

DROP TABLE IF EXISTS stage.population_data;
CREATE TABLE stage.population_data (
    id_evento SERIAL PRIMARY KEY,
    data_evento TEXT
);

DROP TABLE IF EXISTS stage.weather_data;
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

DROP TABLE IF EXISTS prod.population_weather_relation;
CREATE TABLE prod.population_weather_relation (
    id_evento INT PRIMARY KEY,
    cambio_poblacion FLOAT,
    cambio_temperatura_promedio FLOAT,
    ubicacion VARCHAR(100),
    anio INT
);