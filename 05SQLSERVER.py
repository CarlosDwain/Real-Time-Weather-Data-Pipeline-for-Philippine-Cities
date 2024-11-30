import pyodbc
import pandas as pd

# Connection to SQL Server using Windows Authentication
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=CARLOSDWAIN\SQLEXPRESS;'
    'DATABASE=OpenWeather;'            
    'Trusted_Connection=yes;'          
    'TrustServerCertificate=yes;'      
)

cursor = conn.cursor()

# Insert weather data into the database
insert_query = """
INSERT INTO WeatherData 
(City, Country, Latitude, Longitude, Weather, WeatherDescription, 
Temperature_K, FeelsLike_K, MinTemperature_K, MaxTemperature_K, 
Pressure_hPa, Humidity_Percent, WindSpeed_ms, WindDirection_degrees, 
Visibility_m, RainVolume_1h, Cloudiness_Percent, Sunrise, Sunset, Datetime, InsertedAt)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
"""

# Load the weather data from Excel
weather_data = pd.read_excel(r'D:\CARLOS\py_prac\02rest_api\api-openweather\philippine_weather_data.xlsx')

# Iterate over the DataFrame rows and insert data into SQL
for _, row in weather_data.iterrows():
    cursor.execute(insert_query, (
        row['City'], row['Country'], row['Latitude'], row['Longitude'],
        row['Weather'], row['Weather Description'], row['Temperature (K)'],
        row['Feels Like (K)'], row['Min Temperature (K)'], row['Max Temperature (K)'],
        row['Pressure (hPa)'], row['Humidity (%)'], row['Wind Speed (m/s)'],
        row['Wind Direction (degrees)'], row['Visibility (m)'], row['Rain Volume (1h)'],
        row['Cloudiness (%)'], row['Sunrise'], row['Sunset'], row['Datetime']
    ))

# Commit the transaction
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()
