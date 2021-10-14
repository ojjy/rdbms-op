from db_con import MYSQLdb, Snowflakedb
import json
import sqlparse

if __name__ == "__main__":
    with open('test.json') as fp:
        dbinfo = json.loads(fp.read())
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



