"""
developer: YJ
date: 211006
description: DB Connectors

References
https://stackoverflow.com/questions/44765482/multiple-constructors-the-pythonic-way
https://stackoverflow.com/questions/1984325/explaining-pythons-enter-and-exit
https://www.postgresqltutorial.com/postgresql-python/connect/
https://stackoverflow.com/questions/38076220/python-mysqldb-connection-in-a-class/38078544
https://docs.python.org/3/library/abc.html
https://www.geeksforgeeks.org/dunder-magic-methods-python/
https://eine.tistory.com/entry/%ED%8C%8C%EC%9D%B4%EC%8D%AC%EC%97%90%EC%84%9C-%EC%96%B8%EB%8D%94%EB%B0%94%EC%96%B8%EB%8D%94%EC%8A%A4%EC%BD%94%EC%96%B4-%EC%9D%98-%EC%9D%98%EB%AF%B8%EC%99%80-%EC%97%AD%ED%95%A0

언더스코어 _ 의 의미
1. 인터프리터에서 마지막 사용한 값 불러올때 사용
2. 무시하는 값
3. 루프에서 사용
4. 숫자 구분
5. 클래스
 - 앞의 하나의 언더바 _variable: 내부사용용
 - 뒤에 하나의 언더바 varialble_: 파이썬 키워드에 해당하는 이름으로 명명할때
 - 앞의 두개 언더바 __variable:
 - 앞뒤 두개 언더바 __variable__: magic method
"""

from abc import ABC, abstractmethod
import pymysql
import json
import snowflake.connector
from sqlalchemy import create_engine
import psycopg2


class DBMS(ABC):
    def __init__(self, **kwargs):
        self.host = kwargs['host']
        self.user = kwargs['user']
        self.pwd = kwargs['pwd']
        self.db = kwargs['db']
        self._con = None
        self._cursor = None

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @abstractmethod
    def connection(self):
        pass

    @abstractmethod
    def cursor(self):
        pass

    @abstractmethod
    def execute(self, sql, params):
        pass

    @abstractmethod
    def query(self, sql, params):
        pass

    @abstractmethod
    def fetchone(self):
        pass

    @abstractmethod
    def fetchall(self):
        pass

    @abstractmethod
    def commit(self):
        pass

    @abstractmethod
    def close(self):
        pass


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

class SQLAlchemycon(DBMS):
    def __init__(self, port=None, **kwargs):
        super().__init__(**kwargs)
        connect_str = f"snowflake://{self.user}:{self.pwd}@{self.host}"
        self.port = port
        engine = create_engine(connect_str)
        self._con = engine.connect()

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



