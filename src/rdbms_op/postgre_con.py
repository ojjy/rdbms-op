from rdbms_op.db_con import DBMS
import psycopg2

class Postgredb(DBMS):
    def __init__(self, port, database, **kwargs):
        super().__init__(**kwargs)
        self.port = port
        self.database = database
        self._con = psycopg2.connect(host=self.host, user=self.user, password=self.pwd, port=self.port)
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

    def close(self):
        self.commit()
        return self.close()
