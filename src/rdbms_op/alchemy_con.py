from rdbms_op.db_con import DBMS
import json
from sqlalchemy import create_engine
import urllib.parse

class SQLAlchemycon(DBMS):
    def __init__(self,wh, schema, **kwargs):
        super().__init__(**kwargs)
        self._connector_type=kwargs['connector_type']

    def setup(self, wh, schema):
        with open("dbinfo.json") as fp:
            dbinfo = json.loads(fp.read())
        #     dbconnector://(user):(password)@(host):(port)/(db)
        # need to use urllib lib because password include reserved character like @

        if self._connector_type == "snowflake":
            self._con = f"{self._connector_type}://{self.user}:{self.pwd}@{self.host}/{self.db}/{dbinfo['SF_SCHEMA']}?warehouse={self.wh}"
        elif self._connector_type == "mysql+pymysql":
            self._con = f"{self._connector_type}://{self.user}:{self.pwd}'@'{self.host:{self.port}}/{self.db}?charset=utf8mb4"
        elif self._connector_type == "mariadb+mariadbconnector":
            password = urllib.parse.quote_plus(f"{dbinfo['MARIADB_PWD']}")
            self._con = f"{self._connector_type}://{self.user}:{password}@{self.host}:{self.port}/{self.db}"
        elif self._connector_type == "postgresql":
            self._con = f"{self._connector_type}://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.db}"
        elif self._connector_type == "mssql+pymssql":
            self._con = f"{self._connector_type}://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.db}"
        print(self._con)
        connection = create_engine(self._con)
        connection.execute("create table tbl(col1 int, col2 varchar(8));")
        connection.execute("insert into tbl values(1, 'test');")
        result = connection.execute("select * from tbl;").fetchone()
        print(result)
        connection.execute("drop table tbl;")


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.close()

    def cursor(self):
        return self

    def fetchall(self):
        return self.fetchall()

    def fetchone(self):
        return self.fetchone()

    def connection(self):
        return self._con

    def execute(self, sql, params=None):
        return self._con.execute(sql)

    def query(self, sql, params):
        return self._con.execute((sql, params or ())).fetchall()


    def commit(self):
        return self.commit()

    def close(self):
        return self.close()

    @classmethod
    def validate(self):
        connect_str = f"snowflake://{dbinfo['SF_USER']}:{dbinfo['SF_PWD']}@{dbinfo['SF_ACCOUNT']}"
        try:
            engine = create_engine(connect_str)
            self.con = engine.connect()
            version = self.con.execute("SELECT CURRENT_VERSION()").fetchone()[0]
            print(version)
        except Exception as e:
            print(e)

        finally:
            self.con.close()
            engine.dispose()