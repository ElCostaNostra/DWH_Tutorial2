import pyodbc
import pandas as pd

conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=DESKTOP-VFOTSRD\\SQLEXPRESS;DATABASE=DWH; Trusted_Connection=yes')
conn.autocommit = True

cursor = conn.cursor()

df =pd.read_sql_query("SELECT * FROM Ingestion.CRM_cust_info", conn)

df = df.dropna(subset=["cst_id"])
df = df.drop_duplicates(subset=["cst_id"], keep="last")
df['cst_id'] = df['cst_id'].astype(int)
df['cst_gndr'] = df['cst_gndr'].fillna('N/A')
df['cst_gndr'] = df['cst_gndr'].replace({'F': 'Female', 'M': 'Male'})
df['cst_marital_status'] = df['cst_marital_status'].fillna('N/A')
df['cst_marital_status'] = df['cst_marital_status'].replace({'S': 'Single', 'M': 'Married'})
df['cst_firstname'] = df['cst_firstname'].str.strip()
df['cst_lastname'] = df['cst_lastname'].str.strip()
df.to_csv("clean_CRM_cust_info.csv", index=False)


cursor.execute(f"TRUNCATE TABLE Transformation.CRM_cust_info")
cursor.execute("""
/* Ensure schema exists */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Transformation')
    EXEC('CREATE SCHEMA Transformation');

/* clean_CRM_cust_info.csv -> Transformation.CRM_cust_info */
IF OBJECT_ID('Transformation.CRM_cust_info','U') IS NULL 
CREATE TABLE Transformation.CRM_cust_info (
    [cst_id]             INT           NULL,
    [cst_key]            VARCHAR(20)   NULL,
    [cst_firstname]      NVARCHAR(100) NULL,
    [cst_lastname]       NVARCHAR(100) NULL,
    [cst_marital_status] VARCHAR(10)   NULL,
    [cst_gndr]           VARCHAR(10)   NULL,
    [cst_create_date]    DATE          NULL
);

""")

bulk_sql = f"""
BULK INSERT Transformation.CRM_cust_info
FROM '{r"C:\Users\Alienware\Desktop\t2\clean_CRM_cust_info.csv"}'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
)
"""

cursor.execute(bulk_sql)
cursor.close()
conn.close()
