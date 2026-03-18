
#imports
import pyodbc
import pandas as pd

#connect,read and use SQL server
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=DESKTOP-VFOTSRD\\SQLEXPRESS;DATABASE=DWH; Trusted_Connection=yes')
conn.autocommit = True
cursor = conn.cursor()

#read data from SQL server
df =pd.read_sql_query("SELECT * FROM Ingestion.CRM_prd_info", conn)

#clean data
df['prd_cost'] = df['prd_cost'].fillna(0)
df['prd_sub_category']=df['prd_key'].str[6:]
df['prd_key'] = df['prd_key'].str[:5].str.replace('-', '_')
df['prd_line'] = df['prd_line'].fillna('Other')
df['prd_end_dt'] = (
    df.groupby('prd_nm')['prd_start_dt'].shift(-1) - pd.Timedelta(days=1)
)
df['prd_line'] = df['prd_line'].str.strip()
df['prd_line'] = df['prd_line'].replace({'S': 'Sport', 'M': 'Mountain', 'R': 'Road', 'T': 'Touring'})
df.to_csv("clean_CRM_prd_info.csv", index=False)

#Ensure schema exists
cursor.execute("""/* Ensure schema exists */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Transformation')
    EXEC('CREATE SCHEMA Transformation');
""")

#Create table
cursor.execute("""
/* clean_CRM_prd_info.csv -> Transformation.CRM_prd_info */
IF OBJECT_ID('Transformation.CRM_prd_info','U') IS NULL
CREATE TABLE Transformation.CRM_prd_info (
    [prd_id]            INT             NOT NULL,
    [prd_key]           VARCHAR(10)     NOT NULL,
    [prd_nm]            NVARCHAR(100)   NOT NULL,
    [prd_cost]          DECIMAL(10,2)   NOT NULL,
    [prd_line]          VARCHAR(20)     NOT NULL,
    [prd_start_dt]      DATE            NOT NULL,
    [prd_end_dt]        DATE            NULL,
    [prd_sub_category]  VARCHAR(20)     NOT NULL
);

""")

#truncate table to avoid overwriting data
cursor.execute(f"TRUNCATE TABLE Transformation.CRM_prd_info")

#set up bulk insert data from cleaned CSV file
bulk_sql = f"""
BULK INSERT Transformation.CRM_prd_info
FROM '{r"C:\Users\Alienware\Desktop\t2\clean_CRM_prd_info.csv"}'
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

