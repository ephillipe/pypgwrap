__author__ = 'Erick Almeida'

from connection import __connection_pool__


class ContextManager(object):
    def __enter__(self):
        import uuid

        self._key = uuid.uuid4()
        self.pool = __connection_pool__
        return self

    def __exit__(self, type, value, traceback):
        connection = self.pool.getconn(self._key)
        if not isinstance(value, Exception):
            connection.commit(context_transaction=True)
        else:
            connection.rollback(context_transaction=True)
        self.pool.putconn(connection, self._key)

    @property
    def key(self):
        return self._key
