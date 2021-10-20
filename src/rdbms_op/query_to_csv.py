import json
import pymysql
import pandas as pd


def sql_to_csv():
    with open('dbinfo.json') as fp:
        dbinfo = json.loads(fp.read())
    conn = pymysql.connect(host=dbinfo['MARIADB_HOST'],
                           user=dbinfo['MARIADB_USER'],
                           password=dbinfo['MARIADB_PWD'],
                           port=dbinfo['MARIADB_PORT'],
                           db=dbinfo['MARIADB_DB'])

    dbt = [('DW', 'W_CC_MCI_FACT_001'), ('DM', 'M_CC_MT_CD_003'), ('DW', 'W_CC_META_CD_001B'), ('DM', 'M_CC_SICK_CD_001D'),
           ('DW', 'W_CC_MT_FACT_001'), ('DW', 'W_CC_MT_FACT_003'), ('DW', 'W_CC_MT_FACT_004')]
    for dbn, tbn in dbt:
        sql_query = pd.read_sql_query(f'select * from {dbn}.{tbn};', con=conn)
        df=pd.DataFrame(sql_query)
        df.to_csv(f"{dbn}_{tbn}.csv", index=False, encoding='utf-8-sig')
        print(f"success: {dbn}_{tbn}.csv export")


if __name__ == "__main__":
    sql_to_csv()