import requests

def get_population_data(year, age):
    try:
        url = f'https://d6wn6bmjj722w.population.io:443/1.0/population/{year}/aged/{age}/'
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"    \033[1;31mError fetching population data:\033[0m")
        print(f"{e}")
        return None

def get_weather_data(year, latitude, longitude):
    try:
        url = f'https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}&start_date={year}-01-01&end_date={year}-12-31&daily=apparent_temperature_max,apparent_temperature_min,daylight_duration,precipitation_sum,wind_speed_10m_max'
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"    \033[1;31mError fetching weather data:\033[0m")
        print(f"{e}")
        return None