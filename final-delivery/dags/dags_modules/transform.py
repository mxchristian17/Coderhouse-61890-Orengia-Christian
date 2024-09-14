import pandas as pd
# Si se desea correr una versión con menos registros a los fines evaluativos reemplazar el código
# que no está comentado (Version full) por el código comentado de este script (Version light).
# De ese modo se limitará a la inserción de 10 registros por ejecución de la ETL

# Inicio Version light
# ----------------------------------------------------------------
# def transform_population_data(data):
#     # Create the DataFrame
#     flattened_data = sum(data, []) if data else []
    
#     # Limit to the first 5 records
#     flattened_data = flattened_data[:5]
    
#     df = pd.DataFrame(flattened_data)
#     return df

# def transform_weather_data(data):
#     weather_data = []
#     for year_data in data:
#         if 'daily' not in year_data:
#             raise ValueError("The year data must contain 'daily' key")
        
#         date = year_data['daily']['time']
#         apparent_temperature_max = year_data['daily']['apparent_temperature_max']
#         apparent_temperature_min = year_data['daily']['apparent_temperature_min']
#         daylight_duration = year_data['daily']['daylight_duration']
#         precipitation_sum = year_data['daily']['precipitation_sum']
#         wind_speed_10m_max = year_data['daily']['wind_speed_10m_max']

#         # Limit to the first 5 records
#         for index, date_data in enumerate(date[:5]):
#             weather_data.append({
#                 'date': date_data,
#                 'latitude': year_data['latitude'],
#                 'longitude': year_data['longitude'],
#                 'elevation': year_data['elevation'],
#                 'apparent_temperature_max': apparent_temperature_max[index],
#                 'apparent_temperature_min': apparent_temperature_min[index],
#                 'daylight_duration': daylight_duration[index],
#                 'precipitation_sum': precipitation_sum[index],
#                 'wind_speed_10m_max': wind_speed_10m_max[index]
#             })

#     df = pd.DataFrame(weather_data)
#     return df
# Fin Version light
# ----------------------------------------------------------------

# Inicio Version full
# ----------------------------------------------------------------
def transform_population_data(data):
    # Create the DataFrame
    flattened_data = sum(data, []) if data else []
    df = pd.DataFrame(flattened_data)
    return df

def transform_weather_data(data):
    weather_data = []
    for year_data in data:
        if 'daily' not in year_data:
            raise ValueError("The year data must contain 'daily' key")
        
        date = year_data['daily']['time']
        apparent_temperature_max = year_data['daily']['apparent_temperature_max']
        apparent_temperature_min = year_data['daily']['apparent_temperature_min']
        daylight_duration = year_data['daily']['daylight_duration']
        precipitation_sum = year_data['daily']['precipitation_sum']
        wind_speed_10m_max = year_data['daily']['wind_speed_10m_max']

        for index, date_data in enumerate(date):
            weather_data.append({
                'date': date_data,
                'latitude': year_data['latitude'],
                'longitude': year_data['longitude'],
                'elevation': year_data['elevation'],
                'apparent_temperature_max': apparent_temperature_max[index],
                'apparent_temperature_min': apparent_temperature_min[index],
                'daylight_duration': daylight_duration[index],
                'precipitation_sum': precipitation_sum[index],
                'wind_speed_10m_max': wind_speed_10m_max[index]
            })

    df = pd.DataFrame(weather_data)
    return df
# Fin Version full
# ----------------------------------------------------------------