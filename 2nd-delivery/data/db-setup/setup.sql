DROP TABLE IF EXISTS population_data;
CREATE TABLE population_data (
    id_event INT PRIMARY KEY,
    females INT,
    country VARCHAR(250),
    age SMALLINT,
    males INT,
    year SMALLINT,
    total INT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS weather_data;
CREATE TABLE weather_data (
    id_event INT PRIMARY KEY,
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

DROP TABLE IF EXISTS population_weather_relation;
CREATE TABLE population_weather_relation (
    id_event INT PRIMARY KEY,
    population_change FLOAT,
    average_temperature_change FLOAT,
    location VARCHAR(100),
    year INT
);
