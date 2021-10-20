import pandas as pd
import pymysql
import json
from sqlalchemy import create_engine
import snowflake
import os
import time



def csv_to_snowflake():
    try:
        with open('dbinfo.json') as fp:
            dbinfo = json.loads(fp.read())

        connect_str = f"snowflake://{dbinfo['SF_USER']}:{dbinfo['SF_PWD']}@{dbinfo['SF_ACCOUNT']}/{dbinfo['SF_DB']}/{dbinfo['SF_SCHEMA']}?warehouse={dbinfo['SF_WH']}"
        engine = create_engine(connect_str)
        connection =  engine.connect()
        connection.execute("""
        create or replace file format csv_format
  type = csv
  field_delimiter = ','
  skip_header = 1
  null_if = ('NULL', 'null')
  empty_field_as_null = true
  compression = gzip;
        """)
        connection.execute("copy to DM_M_CC_SICK_CD_001D from @poc_stage/DM_M_CC_SICK_CD_001D.csv file_format=csv ON_ERROR=CONTINUE;")

    except Exception as e:
        print(e)


def mig_velocity_check():
    with open("dbinfo.json") as con:
        dbinfo = json.loads(con.read())

    # files = ['DM_M_CC_MT_CD_003.csv', 'DM_M_CC_SICK_CD_001D.csv', 'DW_W_CC_MCI_FACT_001.csv', 'DW_W_CC_META_CD_001B.csv']
    files = ['DM_M_CC_MT_CD_003.csv', 'DM_M_CC_SICK_CD_001D.csv', 'DW_W_CC_MCI_FACT_001.csv', 'DW_W_CC_META_CD_001B.csv']
    path_prefix =os.path.join(os.getcwd(), "work_files")
    full_path_list = [os.path.join(path_prefix, data) for data in files]


    # MySQL Check
    mysql_start_time = time.time()
    for file_name, full_path in zip(files, full_path_list):
        table_name = file_name[3:-4] # 앞 3자리 빼고 뒤 확장자 뺀 나머지가 table_name
        print(table_name)

        df = pd.read_csv(full_path, encoding='euc-kr')
        cnx = create_engine(f"mysql+pymysql://{dbinfo['MYSQL_USER']}:{dbinfo['MYSQL_PWD']}@{dbinfo['MYSQL_HOST']}/{dbinfo['MYSQL_DB']}?charset=utf8mb4")
        cnx.execute(f"TRUNCATE TABLE {table_name}")
        df.to_sql(name=f'{table_name}', con=cnx, index=False, if_exists='append', chunksize=16000)
        # result = cnx.execute(f"SELECT count(*) FROM {table_name}").fetchone()
        # print(result)
    mysql_end_time=time.time()


    # Snowflake
    snow_start_time = time.time()
    for file_name, full_path in zip(files, full_path_list):
        table_name = file_name[3:-4]  # 앞 3자리 빼고 뒤 확장자 뺀 나머지가 table_name
        print(table_name)

        cnx_snow = create_engine(f"snowflake://{dbinfo['SF_USER']}:{dbinfo['SF_PWD']}@{dbinfo['SF_ACCOUNT']}/{dbinfo['SF_DB']}/{dbinfo['SF_SCHEMA']}?warehouse={dbinfo['SF_WH']}?role={dbinfo['SF_ROLE']}")
        df = pd.read_csv(full_path, encoding='euc-kr')
        cnx_snow.execute(f"USE WAREHOUSE {dbinfo['SF_WH']}")
        cnx_snow.execute(f"USE DATABASE {dbinfo['SF_DB']}")
        cnx_snow.execute(f"TRUNCATE TABLE {table_name}")
        df.to_sql(name=f'{table_name}', con=cnx_snow, index=False, if_exists='append', chunksize=16000)
        # result = cnx_snow.execute(f"SELECT count(*) FROM {table_name}").fetchone()
        # print(result)
    snow_end_time=time.time()

    print(f"mysql execution time = {mysql_end_time-mysql_start_time:.2f}\n snowflake execution time = {snow_end_time-snow_start_time:.2f}")

if __name__ == "__main__":
    mig_velocity_check()