import pyodbc

conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=CARLOSDWAIN\\SQLEXPRESS;"
    "DATABASE=OpenWeather;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)
try:
    conn = pyodbc.connect(conn_str)
    print("Connection successful!")
except Exception as e:
    print(f"Connection failed: {e}")