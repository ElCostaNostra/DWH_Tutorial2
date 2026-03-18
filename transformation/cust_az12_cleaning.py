# for erp customer table
# fix id column to maintain consistency
# fix gender column for ocnisstency
# for ones born in the future, replace with null

#imports
import pyodbc
import pandas as pd

#connect,read and use SQL server
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=DESKTOP-VFOTSRD\\SQLEXPRESS;DATABASE=DWH; Trusted_Connection=yes')
conn.autocommit = True
cursor = conn.cursor()

#read data from SQL server
df =pd.read_sql_query("SELECT * FROM Ingestion.CRM_CUST_AZ12", conn)

#cleaning
df["CID"] = df["CID"].str.replace(r"^NAS", "", regex=True)
df["BDATE"] = pd.to_datetime(df["BDATE"], errors="coerce")
df.loc[
        df["BDATE"] > pd.Timestamp.today().normalize(),
        "BDATE",
    ] = pd.NaT
gen_codes = df["GEN"].apply(
        lambda value: value.strip().upper() if isinstance(value, str) else None
    )
df["GEN"] = gen_codes.map(
        {
            "MALE": "Male",
            "M": "Male",
            "FEMALE": "Female",
            "F": "Female",
        }
    ).fillna("N/A")
df["BDATE"] = df["BDATE"].dt.date

df.to_csv("clean_CRM_CUST_AZ12.csv", index=False)

#Ensure schema exists
cursor.execute("""/* Ensure schema exists */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Transformation')
    EXEC('CREATE SCHEMA Transformation');
""")

#Create table
cursor.execute("""
/* clean_CRM_CUST_AZ12.csv -> Transformation.CRM_CUST_AZ12 */
IF OBJECT_ID('Transformation.CRM_CUST_AZ12','U') IS NULL
CREATE TABLE Transformation.CRM_CUST_AZ12 (
    [CID]   VARCHAR(20)  NULL,
    [BDATE] DATE         NULL,
    [GEN]   VARCHAR(20)  NULL
);
""")

#truncate table to avoid overwriting data
cursor.execute(f"TRUNCATE TABLE Transformation.CRM_CUST_AZ12")

#set up bulk insert data from cleaned CSV file
bulk_sql = f"""
BULK INSERT Transformation.CRM_CUST_AZ12
FROM '{r"C:\Users\Alienware\Desktop\t2\clean_CRM_CUST_AZ12.csv"}'
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

