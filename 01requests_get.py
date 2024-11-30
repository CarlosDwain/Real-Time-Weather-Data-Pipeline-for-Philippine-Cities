import requests
import pandas as pd

# Your OpenWeatherMap API key
api_key = "e1ab73f82915b93ab18119a8a583aed2"

# Load the Excel file with city names
cities_df = pd.read_excel(r"D:\CARLOS\py_prac\02rest_api\api-openweather\psgc_cities.xlsx")

# Extract the 'name' column which contains the city names
cities = cities_df['name'].tolist()

# Create an empty list to store weather data for each city
weather_data_list = []

# Loop through each city to fetch weather data
for city in cities:
    # URL for Current Weather Data API
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city},PH&appid={api_key}"
    
    # Make the GET request
    response = requests.get(url)
    data = response.json()
    
    # Check if the response is successful
    if response.status_code == 200:
        # Extract relevant fields
        weather_data = {
            'City': data['name'],
            'Country': data['sys']['country'],
            'Latitude': data['coord']['lat'],
            'Longitude': data['coord']['lon'],
            'Weather': data['weather'][0]['main'],
            'Weather Description': data['weather'][0]['description'],
            'Temperature (K)': data['main']['temp'],
            'Feels Like (K)': data['main']['feels_like'],
            'Min Temperature (K)': data['main']['temp_min'],
            'Max Temperature (K)': data['main']['temp_max'],
            'Pressure (hPa)': data['main']['pressure'],
            'Humidity (%)': data['main']['humidity'],
            'Wind Speed (m/s)': data['wind']['speed'],
            'Wind Direction (degrees)': data['wind']['deg'],
            'Visibility (m)': data.get('visibility', 'N/A'),  # Use .get to avoid KeyError if field is missing
            'Rain Volume (1h)': data.get('rain', {}).get('1h', 0),  # Handle rain key
            'Cloudiness (%)': data['clouds']['all'],
            'Sunrise': pd.to_datetime(data['sys']['sunrise'], unit='s'),
            'Sunset': pd.to_datetime(data['sys']['sunset'], unit='s'),
            'Datetime': pd.to_datetime(data['dt'], unit='s'),
        }
        # Append the data to the list
        weather_data_list.append(weather_data)
    else:
        print(f"Failed to retrieve data for {city}. Error: {data['message']}")

# Convert the list of dictionaries to a DataFrame
df_weather = pd.DataFrame(weather_data_list)

# Display the DataFrame
print(df_weather)

# Optionally, save the data to an Excel file
df_weather.to_excel("philippine_weather_data.xlsx", index=False)
