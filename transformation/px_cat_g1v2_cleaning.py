#imports
import pyodbc
import pandas as pd

#connect,read and use SQL server
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=DESKTOP-VFOTSRD\\SQLEXPRESS;DATABASE=DWH; Trusted_Connection=yes')
conn.autocommit = True
cursor = conn.cursor()

''' Cleaning part'''
df =pd.read_sql_query("SELECT * FROM Ingestion.CRM_PX_CAT_G1V2", conn)

#no cleaning required for this table, just parsing through to transformation layer
df.to_csv("clean_CRM_PX_CAT_G1V2.csv", index=False)

#Ensure schema exists
cursor.execute("""/* Ensure schema exists */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Transformation')
    EXEC('CREATE SCHEMA Transformation');
""")

#Create table
cursor.execute("""
/* clean_CRM_PX_CAT_G1V2.csv -> Transformation.CRM_PX_CAT_G1V2 */
IF OBJECT_ID('Transformation.CRM_PX_CAT_G1V2','U') IS NULL
CREATE TABLE Transformation.CRM_PX_CAT_G1V2 (
    [ID]          VARCHAR(20)   NULL,
    [CAT]         VARCHAR(100)  NULL,
    [SUBCAT]      VARCHAR(100)  NULL,
    [MAINTENANCE] VARCHAR(10)   NULL   -- values like Yes/No
);
""")

#truncate table to avoid overwriting data
cursor.execute(f"TRUNCATE TABLE Transformation.CRM_PX_CAT_G1V2")

#set up bulk insert data from cleaned CSV file
bulk_sql = f"""
BULK INSERT Transformation.CRM_PX_CAT_G1V2
FROM '{r"C:\Users\Alienware\Desktop\t2\clean_CRM_PX_CAT_G1V2.csv"}'
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