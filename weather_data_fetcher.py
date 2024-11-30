import requests
import pandas as pd
import pyodbc
from sqlalchemy import create_engine, text, MetaData, Table
import logging
import time
from datetime import datetime, timedelta
import hashlib
import os
import numpy as np
from dotenv import load_dotenv

# Set up logging configuration
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env file
load_dotenv()

# Fetch the API key
API_KEY = os.getenv('API_KEY')
if API_KEY is None:
    raise ValueError("API_KEY is not set. Please set the API_KEY environment variable.")

# Database connection parameters
DB_DRIVER = os.getenv('DB_DRIVER')
DB_SERVER = os.getenv('DB_SERVER')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_TRUSTED_CONNECTION = os.getenv('DB_TRUSTED_CONNECTION')
DB_TRUST_SERVER_CERTIFICATE = os.getenv('DB_TRUST_SERVER_CERTIFICATE')

# Set the API endpoints
PSGC_CITIES_URL = 'https://psgc.gitlab.io/api/cities.json'
PSGC_PROVINCES_URL = 'https://psgc.gitlab.io/api/provinces.json'
WEATHER_API_URL = 'https://api.openweathermap.org/data/2.5/weather'

EXCEL_FILE = 'all_locations.xlsx'

# ---------------------------- Functions ---------------------------- #

def fetch_psgc_data(url):
    """Fetches and processes data from the PSGC API."""
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return pd.DataFrame(data)
    else:
        logging.error(f"Failed to retrieve data from {url}. Status code: {response.status_code}")
        return pd.DataFrame()
    
def create_tables(engine):
    """
    Creates necessary tables in the database if they do not exist.

    :param engine: SQLAlchemy engine object.
    """
    with engine.connect() as conn:
        try:
            with conn.begin():
                # Create Locations table if it does not exist
                conn.execute(text("""
                    IF OBJECT_ID('dbo.Locations', 'U') IS NULL 
                    CREATE TABLE dbo.Locations (
                        location_id INT IDENTITY(1,1) PRIMARY KEY,
                        location_name VARCHAR(100) NOT NULL,
                        province_name VARCHAR(100) NOT NULL,
                        latitude FLOAT,
                        longitude FLOAT,
                        inserted_at DATETIME DEFAULT GETDATE()
                    )
                """))
                
                # Create WeatherData table if it does not exist
                conn.execute(text(""" 
                    IF OBJECT_ID('dbo.WeatherData', 'U') IS NULL 
                    CREATE TABLE dbo.WeatherData ( 
                        weather_id INT IDENTITY(1,1) PRIMARY KEY, 
                        location_id INT FOREIGN KEY REFERENCES dbo.Locations(location_id),
                        location_name VARCHAR(100),
                        province_name VARCHAR(100),  
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
                logging.info("Tables checked/created successfully.")
        except Exception as e:
            logging.error("An error occurred while creating tables: %s", e)
            raise

def get_geocode(city_name, province_name, country_code='PH'):
    """Fetch latitude and longitude using OpenWeatherMap Geocoding API."""
    
    # Normalize the city name by removing common prefixes
    normalized_city_name = normalize_city_name(city_name)
    
    # Try to fetch coordinates using the normalized city name
    coords = fetch_coordinates(normalized_city_name, province_name, country_code)
    if coords:
        return coords
    
    # If no coordinates found, try the original city name
    coords = fetch_coordinates(city_name, province_name, country_code)
    if coords:
        return coords
    
    # If the original city name fails and it ends with " City", try without it
    if city_name.endswith(" City"):
        stripped_city_name = city_name[:-5].strip()  # Remove " City"
        logging.info(f"Trying with stripped city name: '{stripped_city_name}'")
        coords = fetch_coordinates(stripped_city_name, province_name, country_code)
        if coords:
            return coords
            
    # Log the case where the geocode was unsuccessful
    logging.warning(f"Geocode request failed for {city_name} in {province_name}.")
    return None

def fetch_coordinates(city_name, province_name, country_code):
    """Fetch coordinates for a given city name, province name, and country code."""
    base_url = "http://api.openweathermap.org/geo/1.0/direct"
    url = f"{base_url}?q={city_name},{country_code}&limit=5&appid={API_KEY}"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        coords = match_province(data, province_name, normalized_city_name=city_name)
        return coords
    
    return None

def normalize_city_name(city_name):
    """Normalize the city name to improve geocoding results."""
    common_prefixes = ['City of ', 'Municipality of ', 'Barangay ', 'Town of ']
    
    # Remove any common prefixes
    for prefix in common_prefixes:
        if city_name.startswith(prefix):
            normalized = city_name.replace(prefix, '', 1).strip()
            logging.debug(f"Normalized city name from '{city_name}' to '{normalized}'")
            return normalized
    
    logging.debug(f"No normalization needed for city name: '{city_name}'")
    return city_name

def match_province(data_json, province_name, normalized_city_name=None, original_city_name=None):
    """Match the province/state in the API response with the given province name."""
    
    # Check if normalized_city_name is None
    if normalized_city_name is None:
        logging.warning("Normalized city name is None, skipping province match.")
        return None

    # If the data_json has no entries, log and return None
    if not data_json:
        logging.warning("No location data found in the geocode response.")
        return None

    for location in data_json:
        state = location.get('state')

        # Handle case where state is NaN or None
        if state is None or (isinstance(state, float) and np.isnan(state)):
            lat = location.get('lat')
            lon = location.get('lon')
            logging.info(f"Using coordinates without province match: lat={lat}, lon={lon} for {original_city_name}")
            return lat, lon
        
        # Check if normalized_city_name is "Isabela" and state is "Basilan"
        if normalized_city_name.lower() == "isabela" and  "basilan" in state.lower():
            lat = location.get('lat')
            lon = location.get('lon')
            logging.info(f"Using coordinates for Isabela in Basilan: lat={lat}, lon={lon}")
            return lat, lon

        # Ensure province_name is a string and handle NaN or None
        if isinstance(province_name, str):
            if isinstance(state, str):
                # Check for direct match
                if state.lower() == province_name.lower():
                    lat = location.get('lat')
                    lon = location.get('lon')
                    return lat, lon
                
                # Check if province_name is part of the state
                if province_name.lower() in state.lower():
                    lat = location.get('lat')
                    lon = location.get('lon')
                    logging.info(f"Partial match found: province_name='{province_name}' in state='{state}'")
                    return lat, lon

            # Special case for 'Naga'
            if normalized_city_name.lower() == 'naga' and (state == 'nan' or state == '') and province_name.lower() == 'camarines sur':
                lat = location.get('lat')
                lon = location.get('lon')
                logging.info(f"Using Naga's coordinates: lat={lat}, lon={lon}")
                return lat, lon
        else:
            logging.warning(f"Province name is not a valid string: {province_name}. Using coordinates without match.")

    # Log if no matching province found
    if original_city_name:
        logging.warning(f"No matching province found for {original_city_name}.")
        
    return None

def upsert_locations(cities_province_merged, engine):
    """
    Deletes existing locations from the database and inserts new locations.

    Parameters:
        cities_province_merged (pd.DataFrame): DataFrame containing location data.
        engine (SQLAlchemy engine): SQLAlchemy engine to connect to the database.
    """
    with engine.connect() as conn:
        with conn.begin():
            # Delete all existing locations
            logging.info("Deleting existing locations from the database.")
            delete_stmt = text("DELETE FROM dbo.Locations")
            conn.execute(delete_stmt)

            # Reset the identity counter
            reset_identity_stmt = text("DBCC CHECKIDENT ('dbo.Locations', RESEED, 0)")
            conn.execute(reset_identity_stmt)

            # Insert new locations
            for index, row in cities_province_merged.iterrows():
                location_name = row['name']
                province_name = row['province_name']
                latitude = row['latitude']
                longitude = row['longitude']

                logging.info(f"Inserting new location: {location_name}")
                insert_stmt = text("""
                    INSERT INTO dbo.Locations (location_name, province_name, latitude, longitude)
                    VALUES (:location_name, :province_name, :latitude, :longitude)
                """)
                conn.execute(insert_stmt, {
                    'location_name': location_name,
                    'province_name': province_name,
                    'latitude': latitude,
                    'longitude': longitude
                })

def insert_weather_data(lat, lon, city_name, province_name, engine):
    """Fetch and insert weather data for a city based on its latitude and longitude."""
    url = f"{WEATHER_API_URL}?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    
    if response.status_code == 200:
        weather_data = response.json()
        
        # Extract weather information
        main_weather = weather_data['weather'][0]['main']
        description = weather_data['weather'][0]['description']
        temp = weather_data['main']['temp']
        feels_like = weather_data['main']['feels_like']
        temp_min = weather_data['main']['temp_min']
        temp_max = weather_data['main']['temp_max']
        pressure = weather_data['main']['pressure']
        humidity = weather_data['main']['humidity']
        wind_speed = weather_data['wind']['speed']
        wind_direction = weather_data['wind'].get('deg', None)
        visibility = weather_data.get('visibility', None)
        rain = weather_data.get('rain', {}).get('1h', 0)
        cloudiness = weather_data['clouds'].get('all', None)
        sunrise = datetime.fromtimestamp(weather_data['sys']['sunrise'])
        sunset = datetime.fromtimestamp(weather_data['sys']['sunset'])
        timestamp = datetime.now()
        
        # Fetch the location_id first
        with engine.connect() as conn:
            location_id_query = text("SELECT location_id FROM dbo.Locations WHERE location_name = :city_name AND province_name = :province_name")
            location_id_result = conn.execute(location_id_query, {'city_name': city_name, 'province_name': province_name}).fetchone()

            if location_id_result:
                location_id = location_id_result[0]  # Extract the location_id from the result
            else:
                logging.error(f"No location_id found for city: {city_name}")
                return  # Exit the function if the location_id is not found

        # Now insert the weather data using a separate connection
        with engine.connect() as conn:
            insert_stmt = text(""" 
                INSERT INTO WeatherData (location_id, location_name, province_name, weather_main, weather_description, 
                                         temperature_c, feels_like_c, temp_min_c, temp_max_c, 
                                         pressure_hpa, humidity_percent, wind_speed_mps, 
                                         wind_direction_deg, visibility_m, rain_1h_mm, 
                                         cloudiness_percent, sunrise, sunset, data_datetime)
                VALUES (:location_id, :city, :province, :weather, :description, :temp, :feels_like, :temp_min, :temp_max, 
                        :pressure, :humidity, :wind_speed, :wind_direction, :visibility, :rain, 
                        :cloudiness, :sunrise, :sunset, :timestamp)
            """)

            with conn.begin():
                # Execute the insert statement
                conn.execute(insert_stmt, {
                    'location_id': location_id,
                    'city': city_name, 
                    'province': province_name,
                    'weather': main_weather, 
                    'description': description, 
                    'temp': temp, 
                    'feels_like': feels_like, 
                    'temp_min': temp_min, 
                    'temp_max': temp_max, 
                    'pressure': pressure, 
                    'humidity': humidity, 
                    'wind_speed': wind_speed, 
                    'wind_direction': wind_direction, 
                    'visibility': visibility, 
                    'rain': rain, 
                    'cloudiness': cloudiness, 
                    'sunrise': sunrise, 
                    'sunset': sunset, 
                    'timestamp': timestamp
                })
        
        logging.info(f"Weather data inserted for {city_name} with location_id {location_id}")
    else:
        logging.error(f"Failed to fetch weather data for {city_name}. Status code: {response.status_code}")

# ---------------------------- Main ---------------------------- #

def main():

    conn_string = (
        f"mssql+pyodbc://{DB_SERVER}/{DB_DATABASE}?"
        f"driver={DB_DRIVER}&Trusted_Connection={DB_TRUSTED_CONNECTION}&TrustServerCertificate={DB_TRUST_SERVER_CERTIFICATE}"
    )
    engine = create_engine(conn_string)

    # Ensure tables exist in the database
    create_tables(engine)
        
    # Fetch locations from PSGC API and perform upsert
    cities_df = fetch_psgc_data(PSGC_CITIES_URL)
    provinces_df = fetch_psgc_data(PSGC_PROVINCES_URL)
    provinces_df = provinces_df.rename(columns={'name': 'province_name'})

    # Merge cities with their province names
    cities_province_merged = pd.merge(cities_df, provinces_df[['code', 'province_name']],
                                      left_on='provinceCode', right_on='code', how='left', suffixes=('_city', '_province'))

    # Drop the 'code_province' column that comes from provinces_df after merging
    cities_province_merged = cities_province_merged.drop(columns=['code_province'])

    # Convert the 'province_name' column to string type
    cities_province_merged['province_name'] = cities_province_merged['province_name'].astype(str)

    # Save the new fectched locations data into an excel file for comparison
    cities_province_merged.to_excel("new fetched locations.xlsx", index=False)
    new_locations_df = pd.read_excel("new fetched locations.xlsx")

    print(new_locations_df.info())

    # Load the existing existing locations data for comparison
    try:
        existing_locations_df = pd.read_excel(EXCEL_FILE)
    except FileNotFoundError:
        existing_locations_df = pd.DataFrame()  # If file doesn't exist, create an empty DataFrame

    # Columns to compare between the new fetched locations data and the existing locations data
    compare_columns = [
        'code_city', 'name', 'oldName', 'isCapital', 'provinceCode', 'districtCode',
        'regionCode', 'islandGroupCode', 'psgc10DigitCode', 'province_name'
    ]

    # Check if there are any changes in the key columns
    if not existing_locations_df.empty:
        merged_diff = new_locations_df.merge(existing_locations_df[compare_columns], how='outer', indicator=True)

        # Find rows that are different (not in both DataFrames)
        changes = merged_diff[merged_diff['_merge'] != 'both']

        if changes.empty:
            logging.info("No changes detected in location data. Skipping geocode and upsert.")
            
    else:
        # If the file doesn't exist, treat it as all changes
        changes = cities_province_merged.copy()

    print(changes)

    # Only proceed if there are changes
    if not changes.empty:
        # Initialize lists for latitudes and longitudes
        latitudes = []
        longitudes = []

        # Iterate through the changes and perform geocoding
        for index, row in cities_province_merged.iterrows():
            city_name = row['name']
            province_name = row['province_name']

            coords = get_geocode(city_name, province_name)
            if coords:
                lat, lon = coords
                latitudes.append(lat)
                longitudes.append(lon)
            else:
                latitudes.append(None)
                longitudes.append(None)

            # Sleep between requests to avoid hitting API rate limits
            time.sleep(1)

        # Add the latitudes and longitudes to the DataFrame
        cities_province_merged['latitude'] = latitudes
        cities_province_merged['longitude'] = longitudes

        # Save the updated data to the Excel file
        cities_province_merged.to_excel(EXCEL_FILE, index=False)

        # Upsert the changes into the database
        upsert_locations(cities_province_merged, engine)

        logging.info("Geocode performed and database updated with new changes.")
    else:
        logging.info("No new changes to process for geocoding or database update.")

    # Load the Excel file and fetch weather data for each city
    loaded_locations_data = pd.read_excel(EXCEL_FILE)
    # Convert the 'province_name' column to string type
    loaded_locations_data['province_name'] = loaded_locations_data['province_name'].astype(str)

    print(loaded_locations_data)

    for index, row in loaded_locations_data.iterrows():
        lat = row['latitude']
        lon = row['longitude']
        city_name = row['name']
        province_name_row = row['province_name']

        # print(f"lat:{lat}\nlon:{lon}\ncity:{city_name}\nprovince{province_name_row}")

        if pd.notna(lat) and pd.notna(lon):
            insert_weather_data(lat, lon, city_name, province_name_row, engine)
        else:
            logging.warning(f"Missing latitude or longitude for {city_name} in {province_name_row}. Skipping...")

if __name__ == "__main__":
    main()