__author__ = 'Erick Almeida'

from connection import __connection_pool__
import pypgwrap


class ContextManager(object):
    def __enter__(self):
        import uuid
        self._key = uuid.uuid4()
        self.pool = __connection_pool__
        return self

    def __exit__(self, type, value, traceback):
        conn = pypgwrap.connection(key=self._key)
        is_exception = isinstance(value, Exception) or ((not (type is None)) and issubclass(type, Exception))
        if not is_exception:
            conn.commit(context_transaction=True)
        else:
            conn.rollback(context_transaction=True)
        self.pool.putconn(conn.connection, key=self._key)

    @property
    def key(self):
        return self._key
