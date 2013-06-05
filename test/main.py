__author__ = 'Erick Almeida'

import unittest
from pypgwrap import connection
from pypgwrap.context import ConnectionContext
from pypgwrap.connection import config_pool


class MyTestCase(unittest.TestCase):
    def setUp(self):
        super(MyTestCase, self).setUp()
        config_pool(max_pool=10, pool_expiration=1, url='postgres://postgres:150282@localhost/')
        self.tables = (('doctest_t1', '''id SERIAL PRIMARY KEY,
                                   name TEXT NOT NULL,
                                   count INTEGER NOT NULL DEFAULT 0,
                                   active BOOLEAN NOT NULL DEFAULT true'''),
                       ('doctest_t2', '''id SERIAL PRIMARY KEY,
                                   value TEXT NOT NULL,
                                   doctest_t1_id INTEGER NOT NULL REFERENCES doctest_t1(id)''')
        )

    def drop_tables(self, db):
        db.drop_table('doctest_t1')
        db.drop_table('doctest_t2')

    def create_tables(self, db, fill=False):
        for (name, schema) in self.tables:
            db.create_table(name, schema)
        if fill:
            for i in range(10):
                id = db.insert('doctest_t1', {'name': chr(97 + i) * 5}, returning='id')['id']
                _ = db.insert('doctest_t2', {'value': chr(97 + i) * 2, 'doctest_t1_id': id})

    def test_basic_functions(self):
        import code
        import doctest
        import sys

        db = connection()
        if sys.argv.count('--interact'):
            db.log = sys.stdout
            code.interact(local=locals())
        else:
            try:
                # Setup tables
                self.drop_tables(db)
                self.create_tables(db, fill=True)
                # Run tests
                doctest.testmod(optionflags=doctest.ELLIPSIS)
            finally:
                # Drop tables
                self.drop_tables(db)
        self.assertEqual(True, True)

    def test_connection_auto_commit(self):
        import code
        import sys

        with connection() as db:
            if sys.argv.count('--interact'):
                db.log = sys.stdout
                code.interact(local=locals())
            else:
                self.drop_tables(db)
                self.create_tables(db, fill=True)

        with connection() as db:
            try:
                exists = db.check_table('doctest_t1')
                self.assertEqual(exists, True, 'Tabela era para existir, mas nao foi encontrada. Auto-commit falhou.')
            finally:
                self.drop_tables(db)

    def test_connection_commit_by_key(self):

        with ConnectionContext() as context:

            with connection(key=context.key) as db:
                # Setup tables
                self.drop_tables(db)
                self.create_tables(db, fill=True)

            with connection(key=context.key) as db:
                try:
                    exists = db.check_table('doctest_t1')
                    self.assertEqual(exists, True, 'Tabela era para existir, mas nao foi encontrada.')
                finally:
                    # Drop tables
                    self.drop_tables(db)

            db = connection(key=context.key)
            exists = db.check_table('doctest_t1')
            self.assertEqual(exists, False, 'Tabela nao era para existir, mas foi encontrada.')

            db2 = connection()
            exists = db2.check_table('doctest_t1')
            self.assertEqual(exists, False, 'Tabela nao era para existir, mas foi encontrada.')

        db = connection()
        exists = db.check_table('doctest_t1')
        self.assertEqual(exists, False, 'Tabela nao era para existir, mas foi encontrada.')


if __name__ == '__main__':
    unittest.main()

