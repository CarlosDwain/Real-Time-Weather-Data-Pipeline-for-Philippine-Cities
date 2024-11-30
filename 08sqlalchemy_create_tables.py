import requests
import pandas as pd
import pyodbc
from sqlalchemy import create_engine, text
import logging

# Set up logging configuration
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection parameters
DB_DRIVER = 'ODBC+Driver+18+for+SQL+Server'  # Adjust driver version if needed
DB_SERVER = 'CARLOSDWAIN\\SQLEXPRESS'        # Replace with your server name
DB_DATABASE = 'OpenWeather'                   # Replace with your database name
DB_TRUSTED_CONNECTION = 'yes'                 # Use 'yes' for Windows Authentication
DB_TRUST_SERVER_CERTIFICATE = 'yes'

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
        engine = create_engine(connection_string, echo=True)
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
            # Start a transaction
            with conn.begin():
                logging.debug("Checking if the Locations table exists...")
                conn.execute(text(""" 
                    IF OBJECT_ID('dbo.Locations', 'U') IS NULL 
                    CREATE TABLE dbo.Locations ( 
                        location_id INT IDENTITY(1,1) PRIMARY KEY, 
                        location_name VARCHAR(255), 
                        location_type VARCHAR(50), 
                        latitude FLOAT, 
                        longitude FLOAT 
                    ) 
                """))
                logging.info("Locations table checked/created.")

                logging.debug("Checking if the WeatherData table exists...")
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

def main():
    # Create database connection
    logging.info("Creating database connection...")
    try:
        engine = create_database_connection()
    except Exception as e:
        logging.critical("Exiting due to database connection error: %s", e)
        return

    # Create tables if they don't exist
    logging.info("Creating tables...")
    try:
        create_tables(engine)
    except Exception as e:
        logging.critical("Database setup failed: %s", e)
        return

    logging.info("Database setup complete.")

# Run the script
if __name__ == "__main__":
    main()
