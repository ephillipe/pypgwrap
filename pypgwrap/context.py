import ast

__author__ = 'Erick Almeida'

import os
from connection import get_pool
import pypgwrap


class ContextManager(object):
    def __enter__(self):
        import uuid
        self._key = uuid.uuid4()
        self.pool = get_pool()
        self.close_on_exit = ast.literal_eval(os.getenv('PYPGWRAP_CLOSE_CONNECTION_ON_EXIT', "False"))
        return self

    def __exit__(self, type, value, traceback):
        conn = self.pool.getconn(key=self._key, exactly=True)
        if conn:
            is_exception = isinstance(value, Exception) or ((not (type is None)) and issubclass(type, Exception))
            if not is_exception:
                conn.commit()
            else:
                conn.rollback()
            self.pool.putconn(conn, key=self._key, close=self.close_on_exit)
    @property
    def key(self):
        return self._key
