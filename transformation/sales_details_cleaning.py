#imports
import pyodbc
import pandas as pd

#connect,read and use SQL server
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=DESKTOP-VFOTSRD\\SQLEXPRESS;DATABASE=DWH; Trusted_Connection=yes')
conn.autocommit = True
cursor = conn.cursor()

''' Cleaning part'''
df =pd.read_sql_query("SELECT * FROM Ingestion.CRM_sales_details", conn)
df["sls_order_dt"] = pd.to_datetime(
        df["sls_order_dt"].astype("string"), format="%Y%m%d", errors="coerce"
    )
df["sls_ship_dt"] = pd.to_datetime(
        df["sls_ship_dt"].astype("string"), format="%Y%m%d", errors="coerce"
    )
df["sls_due_dt"] = pd.to_datetime(
        df["sls_due_dt"].astype("string"), format="%Y%m%d", errors="coerce"
    )
df["sls_sales"] = pd.to_numeric(df["sls_sales"], errors="coerce")
df["sls_quantity"] = pd.to_numeric(df["sls_quantity"], errors="coerce")
df["sls_price"] = pd.to_numeric(df["sls_price"], errors="coerce")

order_line_count = df.groupby("sls_ord_num")["sls_ord_num"].transform("size")
multi_line_orders = order_line_count > 1
standardized_order_dt = df.groupby("sls_ord_num")["sls_order_dt"].transform("min")
df.loc[multi_line_orders, "sls_order_dt"] = standardized_order_dt[multi_line_orders]
df.loc[multi_line_orders & df["sls_order_dt"].isna(), "sls_order_dt"] = df.loc[
        multi_line_orders & df["sls_order_dt"].isna(),
        "sls_ship_dt",
    ]
df.loc[~multi_line_orders, "sls_order_dt"] = df.loc[~multi_line_orders, "sls_ship_dt"]

invalid_sales = df["sls_sales"].isna() | (df["sls_sales"] < 0)
valid_price = df["sls_price"].notna() & (df["sls_price"] >= 0)
df.loc[invalid_sales & valid_price, "sls_sales"] = (
        df.loc[invalid_sales & valid_price, "sls_quantity"]
        * df.loc[invalid_sales & valid_price, "sls_price"]
    )

invalid_price = df["sls_price"].isna() | (df["sls_price"] < 0)
valid_sales = df["sls_sales"].notna() & (df["sls_sales"] >= 0)
valid_quantity = df["sls_quantity"].notna() & (df["sls_quantity"] != 0)
df.loc[invalid_price & valid_sales & valid_quantity, "sls_price"] = (
        df.loc[invalid_price & valid_sales & valid_quantity, "sls_sales"]
        / df.loc[invalid_price & valid_sales & valid_quantity, "sls_quantity"]
    )

df["sls_order_dt"] = df["sls_order_dt"].dt.date
df["sls_ship_dt"] = df["sls_ship_dt"].dt.date
df["sls_due_dt"] = df["sls_due_dt"].dt.date
df["sls_quantity"] = df["sls_quantity"].astype("Int64")

df.to_csv("clean_CRM_sales_details.csv", index=False)

#Ensure schema exists
cursor.execute("""/* Ensure schema exists */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Transformation')
    EXEC('CREATE SCHEMA Transformation');
""")

#Create table
cursor.execute("""
/* clean_CRM_sales_details.csv -> Transformation.CRM_sales_details */
IF OBJECT_ID('Transformation.CRM_sales_details','U') IS NULL
CREATE TABLE Transformation.CRM_sales_details (
    [sls_ord_num]   VARCHAR(30)    NULL,
    [sls_prd_key]   VARCHAR(50)    NULL,
    [sls_cust_id]   INT            NULL,
    [sls_order_dt]  DATE            NULL,  
    [sls_ship_dt]   DATE            NULL,
    [sls_due_dt]    DATE            NULL,
    [sls_sales]     DECIMAL(18,2)  NULL,
    [sls_quantity]  INT            NULL,
    [sls_price]     DECIMAL(18,2)  NULL
);

""")

#truncate table to avoid overwriting data
cursor.execute(f"TRUNCATE TABLE Transformation.CRM_sales_details")

#set up bulk insert data from cleaned CSV file
bulk_sql = f"""
BULK INSERT Transformation.CRM_sales_details
FROM '{r"C:\Users\Alienware\Desktop\t2\clean_CRM_sales_details.csv"}'
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