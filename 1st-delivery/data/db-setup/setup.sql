DROP DATABASE IF EXISTS etl;
CREATE DATABASE etl;
\c etl;

CREATE SCHEMA data_extraction;
CREATE SCHEMA data_loaded;

DROP TABLE IF EXISTS data_extraction.population_data;
CREATE TABLE data_extraction.population_data (
    id_event SERIAL PRIMARY KEY,
    event_data JSON
);

DROP TABLE IF EXISTS data_extraction.weather_data;
CREATE TABLE data_extraction.weather_data (
    id_event SERIAL PRIMARY KEY,
    event_data JSON,
    event_date DATE
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

DROP TABLE IF EXISTS data_loaded.population_weather_relation;
CREATE TABLE data_loaded.population_weather_relation (
    id_event INT PRIMARY KEY,
    population_change FLOAT,
    average_temperature_change FLOAT,
    location VARCHAR(100),
    year INT
);
