import requests
import pandas as pd

# URL for the PSGC cities API
url = "https://psgc.gitlab.io/api/cities.json"

# Make the GET request to fetch the data
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Parse the JSON data
    cities_data = response.json()
    
    # Convert the JSON data to a DataFrame
    df_cities = pd.DataFrame(cities_data)
    
    # Remove the phrase "City of" from the 'name' column
    # df_cities['name'] = df_cities['name'].str.replace('City of ', '', regex=False)
    
    # Display the first few rows of the modified DataFrame
    print(df_cities)
    
    # Optionally, save the modified data to an Excel file
    df_cities.to_excel("psgc_cities.xlsx", index=False)
    
else:
    print(f"Failed to retrieve data. Status code: {response.status_code}")
