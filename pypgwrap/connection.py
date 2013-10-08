from psycopg2._psycopg import OperationalError
from psycopg2.pool import PoolError

__author__ = 'Erick Almeida'

import os
from collections import namedtuple
from psycopg2.extras import DictCursor, NamedTupleCursor
from pool import SimpleConnectionPool, ThreadedConnectionPool
from cursor import cursor, PreparedStatement

__connection_pool__ = SimpleConnectionPool()


class SafeNamedTupleCursor(NamedTupleCursor):
    def _make_nt(self, namedtuple=namedtuple):
        return namedtuple("Record", [d[0] for d in self.description or ()], rename=True)


def config_pool(max_pool=5, pool_expiration=5, url=None, pool_manager=SimpleConnectionPool):
    import urlparse

    params = urlparse.urlparse(url or os.environ.get('DATABASE_URL') or 'postgres://localhost/')
    global __connection_pool__
    __connection_pool__ = pool_manager()
    __connection_pool__.configure(expiration=pool_expiration,
                                  maxconn=max_pool,
                                  database=params.path[1:],
                                  user=params.username,
                                  password=params.password,
                                  host=params.hostname,
                                  port=params.port)


def get_pool():
    global __connection_pool__
    return __connection_pool__


class connection(object):
    def __init__(self, hstore=False, log=None, logf=None, default_cursor=DictCursor, key=None):
        self.pool = get_pool()
        self.key = key
        try:
            self.connection = self.pool.getconn(self.key)
        except (PoolError, OperationalError) as e:
            self.connection = None
            raise e
        self.hstore = hstore
        self.log = log
        self.logf = logf or (lambda cursor: cursor.query)
        self.default_cursor = default_cursor
        self.prepared_statement_id = 0

    def prepare(self, statement, params=None, name=None, call_type=None):
        """
            >>> db = connection()
            >>> p1 = db.prepare('SELECT name FROM doctest_t1 WHERE id = $1')
            >>> p2 = db.prepare('UPDATE doctest_t1 set name = $2 WHERE id = $1',('int','text'))
            >>> db.execute(p2,(1,'xxxxx'))
            1
            >>> db.query_one(p1,(1,))
            ['xxxxx']
            >>> db.execute(p2,(1,'aaaaa'))
            1
            >>> db.query_one(p1,(1,))
            ['aaaaa']
        """
        if not name:
            self.prepared_statement_id += 1
            name = '_pstmt_%03.3d' % self.prepared_statement_id
        if params:
            params = '(' + ','.join(params) + ')'
        else:
            params = ''
        with self.cursor() as c:
            c.execute('PREPARE %s %s AS %s' % (name, params, statement))
        if call_type is None:
            if statement.lower().startswith('select'):
                call_type = 'query'
            else:
                call_type = 'execute'
        return PreparedStatement(self, name, call_type)

    def cursor(self, cursor_factory=None):
        return cursor(self.connection,
                      cursor_factory or self.default_cursor,
                      self.hstore,
                      self.log,
                      self.logf)

    def __getattr__(self, name):
        def _wrapper(*args, **kwargs):
            with self.cursor() as c:
                return getattr(c, name)(*args, **kwargs)

        return _wrapper

    def commit(self, context_transaction=False):
        if self.connection:
            if self.key and not context_transaction:
                raise Exception('Connection was associated with Connection Context. Commits are not allowed. Use context_transaction if you want do it.')
            self.connection.commit()

    def rollback(self, context_transaction=False):
        if self.connection:
            if self.key and not context_transaction:
                raise Exception('Connection was associated with Connection Context. Rollbacks are not allowed.')
            self.connection.rollback()

    def __enter__(self, name=None):
        return self

    def __exit__(self, type, value, traceback):
        if not self.key:
            if not isinstance(value, Exception):
                self.commit()
            else:
                self.rollback()

    def __del__(self):
        if not self.key and self.connection:
            self.pool.putconn(self.connection)
