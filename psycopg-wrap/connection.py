import os
import urlparse
from collections import namedtuple
from psycopg2.extras import DictCursor, NamedTupleCursor
from psycopg2.pool import ThreadedConnectionPool

from cursor import cursor


class SafeNamedTupleCursor(NamedTupleCursor):
    def _make_nt(self, namedtuple=namedtuple):
        return namedtuple("Record", [d[0] for d in self.description or ()], rename=True)


class connection(object):
    def __init__(self, url=None, hstore=False, log=None, logf=None, min=1, max=5,
                 default_cursor=DictCursor, default_pool=ThreadedConnectionPool):
        params = urlparse.urlparse(url or
                                   os.environ.get('DATABASE_URL') or
                                   'postgres://localhost/')
        self.pool = default_pool(min, max,
                                 database=params.path[1:],
                                 user=params.username,
                                 password=params.password,
                                 host=params.hostname,
                                 port=params.port,
        )
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

    def shutdown(self):
        if self.pool:
            self.pool.closeall()
            self.pool = None

    def cursor(self, cursor_factory=None):
        return cursor(self.pool,
                      cursor_factory or self.default_cursor,
                      self.hstore,
                      self.log,
                      self.logf)

    def __del__(self):
        self.shutdown()

    def __getattr__(self, name):
        def _wrapper(*args, **kwargs):
            with self.cursor() as c:
                return getattr(c, name)(*args, **kwargs)

        return _wrapper


class PreparedStatement(object):
    def __init__(self, connection, name, call_type='query'):
        self.connection = connection
        self.name = name
        self.call_type = call_type

    def deallocate(self):
        self.connection.execute('DEALLOCATE %s' % self.name)

    def execute(self, *params):
        return self.connection.execute(self, params)

    def query(self, *params):
        return self.connection.query(self, params)

    def query_one(self, *params):
        return self.connection.query_one(self, params)

    def query_dict(self, key, *params):
        return self.connection.query_dict(self, key, params)

    def __call__(self, *params):
        return getattr(self.connection, self.call_type)(self, params)


if __name__ == '__main__':
    import code
    import doctest
    import sys

    tables = (('doctest_t1', '''id SERIAL PRIMARY KEY,
                               name TEXT NOT NULL,
                               count INTEGER NOT NULL DEFAULT 0,
                               active BOOLEAN NOT NULL DEFAULT true'''),
              ('doctest_t2', '''id SERIAL PRIMARY KEY,
                               value TEXT NOT NULL,
                               doctest_t1_id INTEGER NOT NULL REFERENCES doctest_t1(id)'''),
    )
    db = connection()
    if sys.argv.count('--interact'):
        db.log = sys.stdout
        code.interact(local=locals())
    else:
        try:
            # Setup tables
            db.drop_table('doctest_t1')
            db.drop_table('doctest_t2')
            for (name, schema) in tables:
                db.create_table(name, schema)
            for i in range(10):
                id = db.insert('doctest_t1', {'name': chr(97 + i) * 5}, returning='id')['id']
                _ = db.insert('doctest_t2', {'value': chr(97 + i) * 2, 'doctest_t1_id': id})
                # Run tests
            doctest.testmod(optionflags=doctest.ELLIPSIS)
        finally:
            # Drop tables
            db.drop_table('doctest_t1')
            db.drop_table('doctest_t2')