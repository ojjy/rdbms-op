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


class DBMS(ABC):
    def __init__(self, **kwargs):
        self.host = kwargs['host']
        self.user = kwargs['user']
        self.pwd = kwargs['pwd']
        self.db = kwargs['db']
        self.port = kwargs['port']
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










