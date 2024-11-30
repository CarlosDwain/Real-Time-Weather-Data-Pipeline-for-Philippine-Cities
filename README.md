# Real-Time-Weather-Data-Pipeline-for-Philippine-Cities

## Overview
To design, implement, and demonstrate a complete data engineering pipeline that fetches weather data in real-time for cities, municipalities, and provinces in the Philippines, processes it, stores it in a database, and provides insightful analytics and visualizations.

## Table of Contents

- [Project highlights](#section1)
- [Technology Used](#section2)
- [Requirements](#section3)

<a name="section1"></a>
## Project highlights
1. Data Extraction
   - Fetch cities and provinces from the Philippine Standard Geographic Code (PSGC) API.
   - Merge city data with province names for completeness.
2. Data Processing
   - Compare new location data with existing records for changes.
   - Perform geocoding to fetch latitude and longitude for cities.
   - Add real-time weather data for each location using a weather API.
3. Data Storage
   - Upsert cleaned and enriched data into a database.
   - Maintain historical and real-time weather data for analytics.
4. Automation
   - For periodic ingestion, I used task scheduler in Windows. 

<a name="section2"></a>
## Technology Used
- Python
  - pandas: Data manipulation and cleaning.
  - openpyxl: Handling Excel files.
  - sqlalchemy: Database interaction.
  - requests: API calls for data fetching.
  - Database: Microsoft SQL Server.
- APIs
  - PSGC API: Fetch location data.
  - Geocoding API: Retrieve latitude and longitude.
  - Weather API: Fetch real-time weather data.


## Requirements
- A virtual environment (recommended)
- Python 3.x (I used 3.11.1)
- Dependencies listed in `requirement_pyqt5.txt`
