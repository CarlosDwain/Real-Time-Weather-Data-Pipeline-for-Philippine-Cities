import requests
import pandas as pd

# URL for the PSGC provinces API
url = "https://psgc.gitlab.io/api/provinces.json"

# Make the GET request to fetch the data
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Parse the JSON data
    provinces_data = response.json()
    
    # Convert the JSON data to a DataFrame for better visualization
    df_provinces = pd.DataFrame(provinces_data)
    
    # Display the first few rows of the DataFrame
    print(df_provinces)
    
    # Optionally, save the data to an Excel file
    df_provinces.to_excel("psgc_provinces.xlsx", index=False)
    
else:
    print(f"Failed to retrieve data. Status code: {response.status_code}")
