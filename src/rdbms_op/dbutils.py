import pandas as pd
import pymysql
import json


def sql_to_csv():
    with open('test.json') as fp:
        dbinfo = json.loads(fp.read())
    conn = pymysql.connect(host=dbinfo['MARIADB_HOST'],
                           user=dbinfo['MARIADB_USER'],
                           password=dbinfo['MARIADB_PWD'],
                           port=dbinfo['MARIADB_PORT'],
                           db=dbinfo['MARIADB_DB'])

    dbt = [('DW', 'W_CC_MT_FACT_001'), ('DW', 'W_CC_MT_FACT_003'), ('DW', 'W_CC_MT_FACT_004')]
    for dbn, tbn in dbt:
        sql_query = pd.read_sql_query(f'select * from {dbn}.{tbn};', con=conn)
        df=pd.DataFrame(sql_query)
        df.to_csv(f"{dbn}_{tbn}.csv", index=False)
        print(f"success: {dbn}_{tbn}.csv export")


def csv_to_snowflake():
    with open('test.json') as fp:
        dbinfo = json.loads(fp.read())
    connect_str = f"snowflake://{dbinfo['SF_USER']}:{dbinfo['SF_USER']}@{self.host}"

if __name__ == "__main__":
    sql_to_csv()




    """
    fail
    ('DW', 'W_CC_MT_FACT_001'),
    ('DW', 'W_CC_MT_FACT_003'),
    ('DW', 'W_CC_MT_FACT_004'), 
    
    success
    ('DW', 'W_CC_MCI_FACT_001')
    ('DM', 'M_CC_MT_CD_003')
    ('DW', 'W_CC_META_CD_001B'),
    ('DM', 'M_CC_SICK_CD_001D')
    """