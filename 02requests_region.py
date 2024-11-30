import requests
import pandas as pd

# URL for the PSGC regions API
url = "https://psgc.gitlab.io/api/regions.json"

# Make the GET request to fetch the data
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Parse the JSON data
    regions_data = response.json()
    
    # Convert the JSON data to a DataFrame for better visualization
    df_regions = pd.DataFrame(regions_data)
    
    # Display the first few rows of the DataFrame
    print(df_regions)
    
    # Optionally, save the data to an Excel file
    df_regions.to_excel("psgc_regions.xlsx", index=False)
    
else:
    print(f"Failed to retrieve data. Status code: {response.status_code}")