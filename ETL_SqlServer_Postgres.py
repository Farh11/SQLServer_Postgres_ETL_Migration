from sqlalchemy import create_engine
import pyodbc
import pandas as pd
import os

#get password from environment var
pwd = os.environ['PGPASS']
uid = os.environ['PGUID']

print(f"Attempting to connect with UID: {uid}")


#sql db details
driver = "{ODBC Driver 17 for SQL Server}"
server = "DESKTOP-BOGVQRI\SQLEXPRESS"
database = "AdventureWorksDW2019"

#extract data from sql server
def extract():
    src_conn = None
    try:
        connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={uid};PWD={pwd};Trusted_connection=yes'
        src_conn = pyodbc.connect(connection_string)
        src_cursor = src_conn.cursor()
        # execute query
        src_cursor.execute("""
            SELECT t.name as table_name
            FROM sys.tables t
            where t.name IN ('DimProduct', 'DimProductSubcategory', 'DimProductCategory', 'DimSalesTerritory', 'FactInternetSales')
        """)
        src_tables =src_cursor.fetchall()

        for tbl in src_tables:
            # query and load save data to dataframe
            df = pd.read_sql_query(f'SELECT * FROM {tbl[0]}', src_conn)
            load(df, tbl[0])
    except Exception as e:
        print("Data extract error: " + str(e))
    finally:
        if src_conn:
            src_conn.close()

#load data to postgres
def load(df, tbl):
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql://{uid}:{pwd}@localhost:5432/AdventureWorks', pool_pre_ping=True)
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save df to postgres
        df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False)
        rows_imported += len(df)
        # add elapsed time top final print out
        print("Data imported successful")
    except Exception as e:
        print("Data load error: "+ str(e))

try:
    #call extract function
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))