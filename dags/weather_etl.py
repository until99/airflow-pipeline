import requests, json, sqlite3, json
import pandas as pd

import sqlalchemy
from sqlalchemy.orm import sessionmaker

API_TOKEN = '384bef246265439fa8a213046222606'
DATABASE_LOCATION = 'sqlite:///APP_WEATHER.db'
DATABASE_NAME = 'APP_WEATHER.db'

url = f"http://api.weatherapi.com/v1/forecast.json?key={API_TOKEN}&q=joinville&days=1&aqi=yes&alerts=yes?API_KEY=?key=384bef246265439fa8a213046222606"

def run_weather_etl():
    r = requests.get(url)
    data = r.json()

    location_name = data['location']['name']
    location_region = data['location']['region']
    location_country = data['location']['country']
    
    forecast_time = []
    forecast_temp_c = []
    forecast_is_day = []
    forecast_condition = []
    forecast_condition_icon = []
    forecast_wind_kph = []
    forecast_wind_degree = []
    forecast_wind_dir = []
    forecast_pressure_mb = []
    forecast_precip_mm = []
    forecast_humidity = []
    forecast_cloud = []
    forecast_feelslike_c = []
    forecast_windchill_c = []
    forecast_heatindex_c = []
    forecast_dewpoint_c = []
    forecast_will_it_rain = []
    forecast_chance_of_rain = []
    forecast_vis_km = []
    forecast_gust_kph = []
    forecast_uv = []

    for weather in data['forecast']['forecastday'][0]['hour']:
        forecast_time.append(weather['time'])
        forecast_temp_c.append(weather['temp_c'])
        forecast_is_day.append(weather['is_day'])
        forecast_condition.append(weather['condition']['text'])
        forecast_condition_icon.append(weather['condition']['icon'])
        forecast_wind_kph.append(weather['wind_kph'])
        forecast_wind_degree.append(weather['wind_degree'])
        forecast_wind_dir.append(weather['wind_dir'])
        forecast_pressure_mb.append(weather['pressure_mb'])
        forecast_precip_mm.append(weather['precip_mm'])
        forecast_humidity.append(weather['humidity'])
        forecast_cloud.append(weather['cloud'])
        forecast_feelslike_c.append(weather['feelslike_c'])
        forecast_windchill_c.append(weather['windchill_c'])
        forecast_heatindex_c.append(weather['heatindex_c'])
        forecast_dewpoint_c.append(weather['dewpoint_c'])
        forecast_will_it_rain.append(weather['will_it_rain'])
        forecast_chance_of_rain.append(weather['chance_of_rain'])
        forecast_vis_km.append(weather['vis_km'])
        forecast_gust_kph.append(weather['gust_kph'])
        forecast_uv.append(weather['uv'])

    weather_dict = {
        "location_name": location_name,
        "location_region": location_region,
        "location_country": location_country,
        "forecast_time": forecast_time,
        "forecast_temp_c": forecast_temp_c,
        "forecast_is_day": forecast_is_day,
        "forecast_condition": forecast_condition,
        "forecast_condition_icon": forecast_condition_icon,
        "forecast_wind_kph": forecast_wind_kph,
        "forecast_wind_degree": forecast_wind_degree,
        "forecast_wind_dir": forecast_wind_dir,
        "forecast_pressure_mb": forecast_pressure_mb,
        "forecast_precip_mm": forecast_precip_mm,
        "forecast_humidity": forecast_humidity,
        "forecast_cloud": forecast_cloud,
        "forecast_feelslike_c": forecast_feelslike_c,
        "forecast_windchill_c": forecast_windchill_c,
        "forecast_heatindex_c": forecast_heatindex_c,
        "forecast_dewpoint_c": forecast_dewpoint_c,
        "forecast_will_it_rain": forecast_will_it_rain,
        "forecast_chance_of_rain": forecast_chance_of_rain,
        "forecast_vis_km": forecast_vis_km,
        "forecast_gust_kph": forecast_gust_kph,
        "forecast_uv": forecast_uv
    }

    weather_df = pd.DataFrame(weather_dict, columns=[
        "location_name",
        "location_region",
        "location_country",
        "forecast_time",
        "forecast_temp_c",
        "forecast_is_day",
        "forecast_condition",
        "forecast_condition_icon",
        "forecast_wind_kph",
        "forecast_wind_degree",
        "forecast_wind_dir",
        "forecast_pressure_mb",
        "forecast_precip_mm",
        "forecast_humidity",
        "forecast_cloud",
        "forecast_feelslike_c",
        "forecast_windchill_c",
        "forecast_heatindex_c",
        "forecast_dewpoint_c",
        "forecast_will_it_rain",
        "forecast_chance_of_rain",
        "forecast_vis_km",
        "forecast_gust_kph",
        "forecast_uv"
    ])

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS TB_WEATHER_FORECAST (
        location_name VARCHAR(255),
        location_region VARCHAR(255),
        location_country VARCHAR(255),
        forecast_time VARCHAR(255),
        forecast_temp_c DECIMAL(5, 2),
        forecast_is_day BOOLEAN,
        forecast_condition VARCHAR(255),
        forecast_condition_icon VARCHAR(255),
        forecast_wind_kph DECIMAL(5, 2),
        forecast_wind_degree INT,
        forecast_wind_dir VARCHAR(50),
        forecast_pressure_mb DECIMAL(6, 2),
        forecast_precip_mm DECIMAL(5, 2),
        forecast_humidity INT,
        forecast_cloud INT,
        forecast_feelslike_c DECIMAL(5, 2),
        forecast_windchill_c DECIMAL(5, 2),
        forecast_heatindex_c DECIMAL(5, 2),
        forecast_dewpoint_c DECIMAL(5, 2),
        forecast_will_it_rain BOOLEAN,
        forecast_chance_of_rain DECIMAL(5, 2),
        forecast_vis_km DECIMAL(5, 2),
        forecast_gust_kph DECIMAL(5, 2),
        forecast_uv DECIMAL(3, 2)
    );
    """

    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        weather_df.to_sql("TB_WEATHER_FORECAST", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close database successfully")