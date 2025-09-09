import urllib.parse
from sqlalchemy import create_engine, text

odbc_str = (
    "Driver=ODBC Driver 18 for SQL Server;"
    "Server=tcp:militaryaircraft25755.database.windows.net,1433;"
    "Database=aircraftdb;"
    "Uid=sqladmin;"
    "Pwd=H@vezathen@llee1981;"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)
params = urllib.parse.quote_plus(odbc_str)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

with engine.begin() as conn:
    print(conn.execute(text("SELECT TOP (1) name FROM sys.databases")).scalar())
