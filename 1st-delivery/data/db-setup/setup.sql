DROP TABLE IF EXISTS population_data;
CREATE TABLE population_data (
    id_event INT IDENTITY(1,1) PRIMARY KEY,
    event_data VARCHAR(MAX)
);

DROP TABLE IF EXISTS weather_data;
CREATE TABLE weather_data (
    id_event INT IDENTITY(1,1) PRIMARY KEY,
    event_data VARCHAR(MAX)
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

DROP TABLE IF EXISTS population_weather_relation;
CREATE TABLE population_weather_relation (
    id_event INT IDENTITY(1,1) PRIMARY KEY,
    population_change FLOAT,
    average_temperature_change FLOAT,
    location VARCHAR(100),
    year INT
);
