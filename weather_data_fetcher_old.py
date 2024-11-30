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

HASH_FILE = 'last_hash.txt'
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

def calculate_md5(file_path):
    """Calculate MD5 hash of a file."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def check_for_updates(file_path):
    """Check if the Excel file has been updated by comparing its MD5 hash."""
    current_hash = calculate_md5(file_path)
    logging.info(f"Current hash: {current_hash}")

    if os.path.exists(HASH_FILE):
        with open(HASH_FILE, 'r') as f:
            last_hash = f.read().strip()
            logging.info(f"Last hash: {last_hash}")
        
        if current_hash == last_hash:
            logging.info("No changes detected in the city data.")
            return False  # No changes detected

    # Update the hash file
    with open(HASH_FILE, 'w') as f:
        f.write(current_hash)
    
    return True  # Changes detected

def upsert_locations(cities_province_merged, engine):
    """
    Upserts locations into the database.
    
    Parameters:
        cities_province_merged (pd.DataFrame): DataFrame containing location data.
        engine (SQLAlchemy engine): SQLAlchemy engine to connect to the database.
    """
    with engine.connect() as conn:
        with conn.begin():
            for index, row in cities_province_merged.iterrows():
                location_name = row['name']
                province_name = row['province_name']
                latitude = row['latitude']
                longitude = row['longitude']

                # Check if the location already exists
                existing_location = conn.execute(
                    text("SELECT location_id FROM dbo.Locations WHERE location_name = :location_name"),
                    {"location_name": location_name}
                ).fetchone()

                if existing_location:
                    # Update existing location
                    logging.info(f"Updating existing location: {location_name}")
                    update_stmt = text("""
                        UPDATE dbo.Locations
                        SET province_name = :province_name,
                            latitude = :latitude,
                            longitude = :longitude
                        WHERE location_name = :location_name
                    """)
                    conn.execute(update_stmt, {
                        'province_name': province_name,
                        'latitude': latitude,
                        'longitude': longitude,
                        'location_name': location_name
                    })
                else:
                    # Insert new location
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
            location_id_result = conn.execute(location_id_query, {'city_name': city_name}).fetchone()

            if location_id_result:
                location_id = location_id_result[0]  # Extract the location_id from the result
            else:
                logging.error(f"No location_id found for city: {city_name}")
                return  # Exit the function if the location_id is not found

        # Now insert the weather data using a separate connection
        with engine.connect() as conn:
            insert_stmt = text(""" 
                INSERT INTO WeatherData (location_id, location_name, weather_main, weather_description, 
                                         temperature_c, feels_like_c, temp_min_c, temp_max_c, 
                                         pressure_hpa, humidity_percent, wind_speed_mps, 
                                         wind_direction_deg, visibility_m, rain_1h_mm, 
                                         cloudiness_percent, sunrise, sunset, data_datetime)
                VALUES (:location_id, :city, :weather, :description, :temp, :feels_like, :temp_min, :temp_max, 
                        :pressure, :humidity, :wind_speed, :wind_direction, :visibility, :rain, 
                        :cloudiness, :sunrise, :sunset, :timestamp)
            """)

            with conn.begin():
                # Execute the insert statement
                conn.execute(insert_stmt, {
                    'location_id': location_id,
                    'city': city_name, 
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

    # Check if the Excel file exists; if not, create an empty one
    if not os.path.exists(EXCEL_FILE):
        logging.info(f"{EXCEL_FILE} does not exist. Creating an empty Excel file...")
        # Define the new columns
        empty_df = pd.DataFrame(columns=[
            'code_city', 'name', 'oldName', 'isCapital',
            'provinceCode', 'districtCode', 'regionCode',
            'islandGroupCode', 'psgc10DigitCode', 'province_name'
        ])  # Define your initial columns
        empty_df.to_excel(EXCEL_FILE, index=False)

    # Check for changes in the Excel file
    if check_for_updates(EXCEL_FILE):
        logging.info("Changes detected. Processing geocode and weather data...")
        
        # Fetch locations from PSGC API and perform upsert
        cities_df = fetch_psgc_data(PSGC_CITIES_URL)
        provinces_df = fetch_psgc_data(PSGC_PROVINCES_URL)
        provinces_df = provinces_df.rename(columns={'name': 'province_name'})

        cities_province_merged = pd.merge(cities_df, provinces_df[['code', 'province_name']],
                                          left_on='provinceCode', right_on='code', how='left', suffixes=('_city', '_province'))

        # Drop the 'code_province' column that comes from provinces_df after merging
        cities_province_merged = cities_province_merged.drop(columns=['code_province'])

        # Convert the 'province_name' column to string type
        cities_province_merged['province_name'] = cities_province_merged['province_name'].astype(str)

        # Iterate through the merged DataFrame to get latitude and longitude
        latitudes = []
        longitudes = []

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

        # Add latitudes and longitudes to the DataFrame
        cities_province_merged['latitude'] = latitudes
        cities_province_merged['longitude'] = longitudes

        # Save the updated data to Excel
        cities_province_merged.to_excel(EXCEL_FILE, index=False)

        # Upsert locations into the database
        upsert_locations(cities_province_merged, engine)

    else:
        logging.info("No changes detected in city data. Proceeding to insert weather data...")
    
    # Load the Excel file and fetch weather data for each city
    cities_province_merged = pd.read_excel(EXCEL_FILE)
    # Convert the 'province_name' column to string type
    cities_province_merged['province_name'] = cities_province_merged['province_name'].astype(str)

    for index, row in cities_province_merged.iterrows():
        lat = row['latitude']
        lon = row['longitude']
        city_name = row['name']
        province_name_row = row['province_name']

        if pd.notna(lat) and pd.notna(lon):
            insert_weather_data(lat, lon, city_name, province_name_row, engine)
        else:
            logging.warning(f"Missing latitude or longitude for {city_name} in {province_name_row}. Skipping...")

if __name__ == "__main__":
    main()