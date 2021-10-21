from rdbms_op.db_con import DBMS
import snowflake.connector

class Snowflakedb(DBMS):
    def __init__(self, warehouse, **kwargs):
        super().__init__(**kwargs)
        self.warehouse = warehouse
        self._con = snowflake.connector.connect(user=self.user,
                                                password=self.pwd,
                                                account=self.host,
                                                warehouse=self.warehouse,
                                                database=self.db)
        self._cursor = self._con.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.close()

    @property
    def connection(self):
        return self._con

    @property
    def cursor(self):
        return self._cursor

    def execute(self, sql, params):
        return self._cursor.execute(sql, params or ())

    def query(self, sql, params):
        self._cursor.execute(sql, params or ())
        return self._cursor.fetchall()

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    def commit(self):
        return self.commit()

    def close(self, commit=True):
        self.commit()
        return self.close()

    @classmethod
    def validate(self, **kwargs):
        try:
            con = snowflake.connector.connect(user = kwargs['user'],
                                              password=kwargs['pwd'],
                                              account=kwargs['account'],
                                              warehouse=kwargs['warehouse'],
                                              database=kwargs['database'],
                                              schema=kwargs['schema'])
            cur = con.cursor()
            version = cur.execute("SELECT current_version()").fetchone()[0]
            print(version)

        except Exception as e:
            print(e)