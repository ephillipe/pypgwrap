__author__ = 'erick.dnt'

import unittest
from psycopgwrap import connection
from psycopgwrap.connection import config_pool


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

    def test_something(self):
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
                db.drop_table('doctest_t1')
                db.drop_table('doctest_t2')
                db.commit()
                for (name, schema) in self.tables:
                    db.create_table(name, schema)
                db.commit()
                for i in range(10):
                    id = db.insert('doctest_t1', {'name': chr(97 + i) * 5}, returning='id')['id']
                    _ = db.insert('doctest_t2', {'value': chr(97 + i) * 2, 'doctest_t1_id': id})
                db.commit()
                # Run tests
                doctest.testmod(optionflags=doctest.ELLIPSIS)
            finally:
                # Drop tables
                db.drop_table('doctest_t1')
                db.drop_table('doctest_t2')
                db.commit()
        self.assertEqual(True, True)

    def test_connection_auto_commit(self):
        import code
        import doctest
        import sys

        with connection() as db:
            if sys.argv.count('--interact'):
                db.log = sys.stdout
                code.interact(local=locals())
            else:
                try:
                    # Setup tables
                    db.drop_table('doctest_t1')
                    db.drop_table('doctest_t2')
                    for (name, schema) in self.tables:
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
        self.assertEqual(True, True)

    def test_connection_commit_by_key(self):
        import code
        import doctest
        import sys
        import uuid

        key = uuid.uuid4()

        with connection(key=key) as db:
            if sys.argv.count('--interact'):
                db.log = sys.stdout
                code.interact(local=locals())
            else:
                try:
                    # Setup tables
                    db.drop_table('doctest_t1')
                    db.drop_table('doctest_t2')
                    for (name, schema) in self.tables:
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
        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()

