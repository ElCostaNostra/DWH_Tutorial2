
#imports
import pyodbc
import pandas as pd

#connect,read and use SQL server
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=DESKTOP-VFOTSRD\\SQLEXPRESS;DATABASE=DWH; Trusted_Connection=yes')
conn.autocommit = True
cursor = conn.cursor()

#read data from SQL server
df =pd.read_sql_query("SELECT * FROM Ingestion.CRM_LOC_A101", conn)

df["CID"] = df["CID"].str.replace("-", "", regex=False)
df["CNTRY"] = df["CNTRY"].apply(
        lambda value: value.strip() if isinstance(value, str) else value)
df["CNTRY"] = df["CNTRY"].replace({"DE": "Germany", "US": "USA", "": pd.NA}).fillna("N/A")
df.to_csv("clean_CRM_LOC_A101.csv", index=False)

#Ensure schema exists
cursor.execute("""/* Ensure schema exists */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Transformation')
    EXEC('CREATE SCHEMA Transformation');
""")

#Create table
cursor.execute("""
/* clean_CRM_LOC_A101.csv -> Transformation.CRM_LOC_A101 */
IF OBJECT_ID('Transformation.CRM_LOC_A101','U') IS NULL
CREATE TABLE Transformation.CRM_LOC_A101 (    
    [CID]   VARCHAR(20)   NULL,
    [CNTRY] VARCHAR(100)  NULL
);
""")

#truncate table to avoid overwriting data
cursor.execute(f"TRUNCATE TABLE Transformation.CRM_LOC_A101")

#set up bulk insert data from cleaned CSV file
bulk_sql = f"""
BULK INSERT Transformation.CRM_LOC_A101
FROM '{r"C:\Users\Alienware\Desktop\t2\clean_CRM_LOC_A101.csv"}'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
)
"""

#execute bulk insert & close SQL connection
cursor.execute(bulk_sql)
cursor.close()
conn.close()