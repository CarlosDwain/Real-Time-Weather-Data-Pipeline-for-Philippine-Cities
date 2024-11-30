import requests
import pandas as pd
import pyodbc
from sqlalchemy import create_engine, text, MetaData, Table
import logging
import time
from datetime import datetime, timedelta

# Set up logging configuration
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# OpenWeatherMap API Key
API_KEY = 'e1ab73f82915b93ab18119a8a583aed2'  # Replace with your actual API key

# Database connection parameters
DB_DRIVER = 'ODBC+Driver+18+for+SQL+Server'  # Adjust driver version if needed
DB_SERVER = 'CARLOSDWAIN\SQLEXPRESS'        # Replace with your server name
DB_DATABASE = 'OpenWeather'                   # Replace with your database name
DB_TRUSTED_CONNECTION = 'yes'                 # Use 'yes' for Windows Authentication
DB_TRUST_SERVER_CERTIFICATE = 'yes'

# Set the API endpoints
PSGC_CITIES_URL = 'https://psgc.gitlab.io/api/cities.json'
PSGC_PROVINCES_URL = 'https://psgc.gitlab.io/api/provinces.json'
WEATHER_API_URL = 'https://api.openweathermap.org/data/2.5/weather'

# ---------------------------- Functions ---------------------------- #

def fetch_psgc_data(url, remove_prefix=None):
    """
    Fetches and processes data from the PSGC API.

    :param url: API endpoint URL.
    :param remove_prefix: String prefix to remove from the 'name' field.
    :return: DataFrame containing the processed data.
    """
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)
        if remove_prefix:
            df['name'] = df['name'].str.replace(remove_prefix, '', regex=False)
        return df
    else:
        logging.error(f"Failed to retrieve data from {url}. Status code: {response.status_code}")
        return pd.DataFrame()

def create_database_connection():
    """
    Creates a database engine using SQLAlchemy.

    :return: SQLAlchemy engine object.
    """
    connection_string = (
        f"mssql+pyodbc://{DB_SERVER}/{DB_DATABASE}?"
        f"driver={DB_DRIVER}&Trusted_Connection={DB_TRUSTED_CONNECTION}&TrustServerCertificate={DB_TRUST_SERVER_CERTIFICATE}"
    )
    logging.debug("Connecting to the database with the following connection string: %s", connection_string)
    try:
        engine = create_engine(connection_string, echo=False)
        logging.info("Database connection created successfully.")
        return engine
    except Exception as e:
        logging.error("Failed to create database connection: %s", e)
        raise

def create_tables(engine):
    """
    Creates necessary tables in the database if they do not exist.

    :param engine: SQLAlchemy engine object.
    """
    with engine.connect() as conn:
        try:
            with conn.begin():
                conn.execute(text(""" 
                    IF OBJECT_ID('dbo.WeatherData', 'U') IS NULL 
                    CREATE TABLE dbo.WeatherData ( 
                        weather_id INT IDENTITY(1,1) PRIMARY KEY, 
                        location_id INT FOREIGN KEY REFERENCES dbo.Locations(location_id), 
                        weather_main VARCHAR(50), 
                        weather_description VARCHAR(255), 
                        temperature_c FLOAT, 
                        feels_like_c FLOAT, 
                        temp_min_c FLOAT, 
                        temp_max_c FLOAT, 
                        pressure_hpa INT, 
                        humidity_percent INT, 
                        wind_speed_mps FLOAT, 
                        wind_direction_deg INT, 
                        visibility_m INT, 
                        rain_1h_mm FLOAT, 
                        cloudiness_percent INT, 
                        sunrise DATETIME, 
                        sunset DATETIME, 
                        data_datetime DATETIME, 
                        inserted_at DATETIME DEFAULT GETDATE() 
                    ) 
                """))
                logging.info("WeatherData table checked/created.")
        except Exception as e:
            logging.error("An error occurred while creating tables: %s", e)
            raise

def fetch_weather_data(location_name):
    """
    Fetches weather data for a given location from OpenWeatherMap API.

    :param location_name: Name of the city or province.
    :return: Dictionary containing weather data.
    """
    params = {
        'q': location_name,
        'appid': API_KEY,
        'units': 'metric'
    }
    response = requests.get(WEATHER_API_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        logging.error(f"Failed to retrieve weather data for {location_name}. Status code: {response.status_code}")
        return None

def insert_weather_data(engine, location_id, weather_data):
    """
    Inserts weather data into the WeatherData table.

    :param engine: SQLAlchemy engine object.
    :param location_id: ID of the location in the database.
    :param weather_data: Weather data to be inserted.
    """
    with engine.connect() as conn:
        try:
            with conn.begin():
                conn.execute(text("""
                    INSERT INTO WeatherData (location_id, weather_main, weather_description, temperature_c, feels_like_c, 
                                             temp_min_c, temp_max_c, pressure_hpa, humidity_percent, wind_speed_mps, 
                                             wind_direction_deg, visibility_m, rain_1h_mm, cloudiness_percent, 
                                             sunrise, sunset, data_datetime) 
                    VALUES (:location_id, :weather_main, :weather_description, :temperature_c, :feels_like_c, 
                            :temp_min_c, :temp_max_c, :pressure_hpa, :humidity_percent, :wind_speed_mps, 
                            :wind_direction_deg, :visibility_m, :rain_1h_mm, :cloudiness_percent, 
                            :sunrise, :sunset, :data_datetime)
                """), {
                    'location_id': location_id,
                    'weather_main': weather_data['weather'][0]['main'],
                    'weather_description': weather_data['weather'][0]['description'],
                    'temperature_c': weather_data['main']['temp'],
                    'feels_like_c': weather_data['main']['feels_like'],
                    'temp_min_c': weather_data['main']['temp_min'],
                    'temp_max_c': weather_data['main']['temp_max'],
                    'pressure_hpa': weather_data['main']['pressure'],
                    'humidity_percent': weather_data['main']['humidity'],
                    'wind_speed_mps': weather_data['wind']['speed'],
                    'wind_direction_deg': weather_data['wind']['deg'],
                    'visibility_m': weather_data.get('visibility', None),
                    'rain_1h_mm': weather_data.get('rain', {}).get('1h', 0),
                    'cloudiness_percent': weather_data['clouds']['all'],
                    'sunrise': datetime.fromtimestamp(weather_data['sys']['sunrise']),
                    'sunset': datetime.fromtimestamp(weather_data['sys']['sunset']),
                    'data_datetime': datetime.fromtimestamp(weather_data['dt'])
                })
            logging.info(f"Inserted weather data for location ID {location_id}")
        except Exception as e:
            logging.error("Error inserting weather data: %s", e)
            raise

def upsert_locations(engine, df):
    """
    Inserts or updates locations in the database.

    :param engine: SQLAlchemy engine object.
    :param df: DataFrame containing the locations.
    """
    meta = MetaData()
    locations_table = Table('Locations', meta, autoload_with=engine)

    try:
        with engine.connect() as conn:
            with conn.begin():
                for index, row in df.iterrows():
                    merge_statement = text(f"""
                        MERGE INTO Locations AS target
                        USING (SELECT :location_name AS location_name, :location_type AS location_type) AS source
                        ON target.location_name = source.location_name
                        WHEN MATCHED THEN
                            UPDATE SET location_type = source.location_type
                        WHEN NOT MATCHED THEN
                            INSERT (location_name, location_type)
                            VALUES (source.location_name, source.location_type);
                    """)
                    conn.execute(merge_statement, {
                        'location_name': row['name'],
                        'location_type': row['location_type']
                    })
        logging.info("Upsert operation completed for all locations.")
    except Exception as e:
        logging.error("Error during upsert operation: %s", e)
        raise

def schedule_fetch_and_insert(engine, all_locations_df):
    """
    Fetches weather data and performs upserts 3-4 times a day.

    :param engine: SQLAlchemy engine object.
    :param all_locations_df: DataFrame containing all locations.
    """
    logging.info("Fetching and inserting weather data...")
    for index, row in all_locations_df.iterrows():
        location_name = row['name']
        location_id = index + 1  # Assuming sequential IDs
        weather_data = fetch_weather_data(location_name)

        if weather_data:
            try:
                insert_weather_data(engine, location_id, weather_data)
            except Exception as e:
                logging.error("Failed to insert weather data for location %s: %s", location_name, e)

def main():
    engine = create_database_connection()
    create_tables(engine)

    # Fetch locations from PSGC API and perform upsert
    cities_df = fetch_psgc_data(PSGC_CITIES_URL, remove_prefix="City of ")
    provinces_df = fetch_psgc_data(PSGC_PROVINCES_URL)
    all_locations_df = pd.concat([cities_df, provinces_df])
    all_locations_df['location_type'] = ['city'] * len(cities_df) + ['province'] * len(provinces_df)

    # Perform the fetch and insert operations 3-4 times a day
    while True:
        upsert_locations(engine, all_locations_df)
        schedule_fetch_and_insert(engine, all_locations_df)

        # Wait for 6 hours (21600 seconds) before the next run
        logging.info("Waiting for the next schedule in 6 hours...")
        time.sleep(60 * 60 * 6)  # Sleep for 6 hours

if __name__ == "__main__":
    main()
