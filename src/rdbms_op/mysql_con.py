from rdbms_op.db_con import DBMS
import pymysql

class MYSQLdb(DBMS):
    def __init__(self, port, database='ds', **kwargs):
        super().__init__(**kwargs)
        self.port = port
        self._con = pymysql.connect(user=self.user,
                                    password=self.pwd,
                                    host=self.host,
                                    port=self.port,
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

    def execute(self, sql, params=None):
        return self._cursor.execute(sql, params or ())

    def query(self, sql, params=None):
        self._cursor.execute(sql, params or ())
        return self.fetchall()

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    def commit(self):
        self.connection.commit()

    def close(self, commit=True):
        self.commit()
        return self._con.close()

    @classmethod
    def validate(self, user, pwd, host, port):
        with pymysql.connect(user=user, password=pwd, host=host, port=port) as mysqldb:
            cur = mysqldb.cursor()
            cur.execute("SELECT VERSION()")
            version = cur.fetchone()
            print(version)
