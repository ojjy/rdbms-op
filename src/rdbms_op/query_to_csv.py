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
        # utf-8-sig로 encoding해야 csv에서 파일이 깨지지 않는다.

        df.to_csv(f"{dbn}_{tbn}.csv", index=False, encoding='utf-8-sig')
        print(f"success: {dbn}_{tbn}.csv export")


if __name__ == "__main__":
    sql_to_csv()


# References
# https://velog.io/@ha_zzi/csv-encoding-%ED%95%9C%EA%B8%80%EC%9D%B4-%EA%B9%A8%EC%A7%88-%EB%95%8C
# https://stackoverflow.com/questions/57152985/what-is-the-difference-between-utf-8-and-utf-8-sig