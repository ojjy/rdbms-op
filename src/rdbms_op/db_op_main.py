
from rdbms_op.snow_con import Snowflakedb
import json
import sqlparse
from rdbms_op.alchemy_con import SQLAlchemycon

"""
connector type means the string of connection to use sqlalchemy library such as
snowflake: "snowflake"
mariadb: "mariadb+mariadbconnector"
mysql: "mysql+pymysql"
mssql: "mssql+pymssql"
postgre: "postgresql"


references.
https://docs.sqlalchemy.org/en/14/core/engines.html
"""

if __name__ == "__main__":
    with open('dbinfo.json') as fp:
        dbinfo = json.loads(fp.read())

    SQLAlchemycon(host=dbinfo['SF_HOST'],
                  user=dbinfo['SF_USER'],
                  pwd=dbinfo['SF_PWD'],
                  port=dbinfo['SF_PORT'],
                  db=dbinfo['SF_DB'],
                  connector_type=dbinfo['SF_CON'])
###############################################################################
    # with MYSQLdb(host=dbinfo['MYSQL_HOST'],
    #              user=dbinfo['MYSQL_USER'],
    #              pwd=dbinfo['MYSQL_PWD'],
    #              port=dbinfo['MYSQL_PORT'],
    #              db=dbinfo['MYSQL_DB']) as mysql:
    #     version = mysql.query('SELECT VERSION()')
    #     print(version)
        # with open(file="test1.sql", mode="r", encoding='utf-8') as sql:
        #     test = sqlparse.split(sql.read())
        #     # test = sqlparse.format(test, reindent=False, identifier_case='lower',keyword_case='lower')
        #     # print(test)
        #     for idx, stmt in enumerate(test):
        #         print(idx, stmt)
        #         print("\n\n")
        #         mysql.execute(stmt)

    # MYSQLdb.validate(host=dbinfo['MYSQL_HOST'],
    #           user=dbinfo['MYSQL_USER'],
    #           pwd=dbinfo['MYSQL_PWD'],
    #           port=dbinfo['MYSQL_PORT'])
################################################################################
    Snowflakedb.validate(user=dbinfo['SF_USER'],
                         pwd=dbinfo['SF_PWD'],
                         account=dbinfo['SF_ACCOUNT'],
                         warehouse=dbinfo['SF_WAREHOUSE'],
                         database=dbinfo['SF_DB'],
                         schema=dbinfo['SF_SCHEMA']
                         )

# ################################################################################

    # SQLAlchemycon.validate()
    #
    # sqlalchemyconn = SQLAlchemycon(user=dbinfo["SF_USER"], pwd=dbinfo["SF_PWD"], host=dbinfo["SF_ACCOUNT"])
    # version = sqlalchemyconn.execute("SELECT CURRENT_VERSION()").fetchone()
    # print(version)
# ###############################################################################
#     postgrecon = Postgredb(user= dbinfo['POSTGRE_USER'], pwd = dbinfo['POSTGRE_PWD'], host=dbinfo['POSTGRE_HOST'], database = dbinfo['POSTGRE_DB'], port=dbinfo['POSTGRE_PORT'])



