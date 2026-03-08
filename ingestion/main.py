import pyodbc

conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=DESKTOP-VFOTSRD\\SQLEXPRESS;DATABASE=master; Trusted_Connection=yes')

conn.autocommit = True

cursor = conn.cursor()
cursor.execute(" IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DWH') CREATE DATABASE DWH")


conn.close()

conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=DESKTOP-VFOTSRD\\SQLEXPRESS;DATABASE=DWH; Trusted_Connection=yes')
conn.autocommit = True

cursor = conn.cursor()

cursor.execute("""
/* Ensure schema exists */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Ingestion')
    EXEC('CREATE SCHEMA Ingestion');

/* LOC_A101.csv -> Ingestion.CRM_LOC_A101 */
IF OBJECT_ID('Ingestion.CRM_LOC_A101','U') IS NULL
CREATE TABLE Ingestion.CRM_LOC_A101 (
    [CID]   VARCHAR(20)   NULL,
    [CNTRY] VARCHAR(100)  NULL
);

/* PX_CAT_G1V2.csv -> Ingestion.CRM_PX_CAT_G1V2 */
IF OBJECT_ID('Ingestion.CRM_PX_CAT_G1V2','U') IS NULL
CREATE TABLE Ingestion.CRM_PX_CAT_G1V2 (
    [ID]          VARCHAR(20)   NULL,
    [CAT]         VARCHAR(100)  NULL,
    [SUBCAT]      VARCHAR(100)  NULL,
    [MAINTENANCE] VARCHAR(10)   NULL   -- values like Yes/No
);

/* CUST_AZ12.csv -> Ingestion.CRM_CUST_AZ12 */
IF OBJECT_ID('Ingestion.CRM_CUST_AZ12','U') IS NULL
CREATE TABLE Ingestion.CRM_CUST_AZ12 (
    [CID]   VARCHAR(20)  NULL,
    [BDATE] DATE         NULL,
    [GEN]   VARCHAR(20)  NULL
);

/* prd_info.csv -> Ingestion.CRM_prd_info */
IF OBJECT_ID('Ingestion.CRM_prd_info','U') IS NULL
CREATE TABLE Ingestion.CRM_prd_info (
    [prd_id]        INT            NULL,
    [prd_key]       VARCHAR(50)    NULL,
    [prd_nm]        NVARCHAR(255)  NULL,
    [prd_cost]      DECIMAL(18,2)  NULL,
    [prd_line]      VARCHAR(10)    NULL,
    [prd_start_dt]  DATE           NULL,
    [prd_end_dt]    DATE           NULL
);

/* sales_details.csv -> Ingestion.CRM_sales_details */
IF OBJECT_ID('Ingestion.CRM_sales_details','U') IS NULL
CREATE TABLE Ingestion.CRM_sales_details (
    [sls_ord_num]   VARCHAR(30)    NULL,
    [sls_prd_key]   VARCHAR(50)    NULL,
    [sls_cust_id]   INT            NULL,
    [sls_order_dt]  INT            NULL,  -- appears as YYYYMMDD in the file
    [sls_ship_dt]   INT            NULL,
    [sls_due_dt]    INT            NULL,
    [sls_sales]     DECIMAL(18,2)  NULL,
    [sls_quantity]  INT            NULL,
    [sls_price]     DECIMAL(18,2)  NULL
);

/* cust_info.csv -> Ingestion.CRM_cust_info */
IF OBJECT_ID('Ingestion.CRM_cust_info','U') IS NULL
CREATE TABLE Ingestion.CRM_cust_info (
    [cst_id]             INT           NULL,
    [cst_key]            VARCHAR(20)   NULL,
    [cst_firstname]      NVARCHAR(100) NULL,
    [cst_lastname]       NVARCHAR(100) NULL,
    [cst_marital_status] VARCHAR(10)   NULL,
    [cst_gndr]           VARCHAR(10)   NULL,
    [cst_create_date]    DATE          NULL
);
""")
cursor.close()
conn.close()

SERVER = r"DESKTOP-VFOTSRD\SQLEXPRESS"
DATABASE = "DWH"

# IMPORTANT: This path must be readable by SQL Server (service account)

BASE_DIR = r"C:\Users\Alienware\Desktop\t2\Data"  # or r"\\DESKTOP-VFOTSRD\csvshare"

FILES_TO_TABLES = [
    ("LOC_A101.csv",     "Ingestion.CRM_LOC_A101"),
    ("PX_CAT_G1V2.csv",  "Ingestion.CRM_PX_CAT_G1V2"),
    ("CUST_AZ12.csv",    "Ingestion.CRM_CUST_AZ12"),
    ("prd_info.csv",     "Ingestion.CRM_prd_info"),
    ("sales_details.csv","Ingestion.CRM_sales_details"),
    ("cust_info.csv",    "Ingestion.CRM_cust_info"),
]

conn = pyodbc.connect(
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    rf"SERVER={SERVER};"
    rf"DATABASE={DATABASE};"
    r"Trusted_Connection=yes;"
)
conn.autocommit = True
cur = conn.cursor()

for filename, table in FILES_TO_TABLES:
    fullpath = f"{BASE_DIR}\\{filename}"  # ok for local path
    # If using UNC, BASE_DIR already looks like \\DESKTOP...\share and this still works

    # Optional but recommended for staging to avoid duplicates:
    cur.execute(f"TRUNCATE TABLE {table}")

    sql = f"""
    BULK INSERT {table}
    FROM '{fullpath}'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0d0a',
        TABLOCK,
        CODEPAGE = '65001'
    );
    """

    try:
        cur.execute(sql)
        print(f"BULK INSERT OK -> {table} ({filename})")
    except pyodbc.Error as e:
        # Try alternative row terminator if needed (some files are \n only)
        sql_alt = sql.replace("0x0d0a", "0x0a")
        cur.execute(sql_alt)
        print(f"BULK INSERT OK (alt row terminator) -> {table} ({filename})")

cur.close()
conn.close()
print("Done ✅")