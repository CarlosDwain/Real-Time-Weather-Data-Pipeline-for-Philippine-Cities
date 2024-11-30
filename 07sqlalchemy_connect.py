from sqlalchemy import create_engine, text

connection_string = (
    "mssql+pyodbc://CARLOSDWAIN\SQLEXPRESS/OpenWeather?"
    "driver=ODBC+Driver+18+for+SQL+Server&Trusted_Connection=yes&TrustServerCertificate=yes"
)
engine = create_engine(connection_string)

# Test the connection
try:
    with engine.connect() as connection:
        result = connection.execute(text("SELECT @@VERSION;"))
        for row in result:
            print(row)
except Exception as e:
    print("Error:", e)
