import requests
import pandas as pd

API_key = 'e1ab73f82915b93ab18119a8a583aed2'
location_name = 'City of San Fernando'
country_name = 'PH'

# URL for the PSGC cities API
url = f"http://api.openweathermap.org/geo/1.0/direct?q={location_name},{country_name}&limit=5&appid={API_key}"

# Make the GET request to fetch the data
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Parse the JSON data
    data_json = response.json()
    df = pd.DataFrame(data_json)
    print(df)
    
else:
    print(f"Failed to retrieve data. Status code: {response.status_code}")
