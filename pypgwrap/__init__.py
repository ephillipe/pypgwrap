from connection import connection
from connection import config_pool
from context import ContextManager

__author__ = 'Erick Almeida'
version = "0.1.13"
__doc__ = """

    pypgwrap - efficient PostgreSQL database wrapper
    -------------------------------------------

    The 'efficient' module provides a efficient wrapper over psycopg2 supporting a
    Python API for common sql functions, explicit and implicit transactions mechanism and
    connection pooling.

    This is not intended to provide ORM-like functionality, just to make it
    easier to interact with PostgreSQL from python code for simple use-cases
    and allow direct SQL access for more complex operations.

    The module wraps the excellent 'psycopg2' library and most of the
    functionality is provided by this behind the scenes, except for pooling.

    The module provides:

        * Simplified handling of connections/cursor
            * Connection pool, single or multithreaded (inherited by psycopg2.pool)
            * Cursor context handler
            * Context Manager for explicit transactions
        * Python API to wrap basic SQL functionality
            * Simple select,update,delete,join methods extending the cursor
              context handler (also available as stand-alone methods which
              create an implicit cursor for simple queries) (from pgwrap)
        * Query results as dict (using psycopg2.extras.DictCursor)
        * Callable prepared statements
        * Logging support

    Basic usage
    -----------

        >>> import pypgwrap
        >>> pypgwrap.config_pool(max_pool=10, pool_expiration=1, url='postgres://localhost/', pool_manager=SimpleConnectionPool)
        >>> db = pypgwrap.connection()
        >>> with db.cursor() as c:
        ...     c.query('select version()')
        [['PostgreSQL...']]
        >>> v = db.query_one('select version()')
        >>> v
        ['PostgreSQL...']
        >>> v.items()
        [('version', 'PostgreSQL...')]
        >>> v['version']
        'PostgreSQL...'


    Basic usage, with transaction
    -----------
        Init pool at application start:

            >>> import pypgwrap
            >>> pypgwrap.config_pool(max_pool=10, pool_expiration=1, url='postgres://localhost/', pool_manager=SimpleConnectionPool)

        Explicit transactions:

            >>> db = pypgwrap.connection()
            >>> db.create_table('t1', '''id SERIAL PRIMARY KEY,
                                       name TEXT NOT NULL,
                                       count INTEGER NOT NULL DEFAULT 0,
                                       active BOOLEAN NOT NULL DEFAULT true''')
            >>> id0 = db.insert('doctest_t1', {'name': 'name_one'}, returning='id')['id']
            >>> id1 = db.insert('doctest_t1', {'name': 'name_two'}, returning='id')['id']
            >>> db.commit()

        Implicity transactions:

            >>> with pypgwrap.connection() as db:
            >>>     db.create_table('t1', '''id SERIAL PRIMARY KEY,
                                          name TEXT NOT NULL,
                                          count INTEGER NOT NULL DEFAULT 0,
                                          active BOOLEAN NOT NULL DEFAULT true''')
            >>>     id0 = db.insert('doctest_t1', {'name': 'name_one'}, returning='id')['id']
            >>>     id1 = db.insert('doctest_t1', {'name': 'name_two'}, returning='id')['id']

        Distributed transactions:

            >>> import uuid
            >>> key = uuid.uuid4()

            >>> with pypgwrap.connection(key=key) as db:
            >>>     db.create_table('t1', '''id SERIAL PRIMARY KEY,
                                          name TEXT NOT NULL,
                                          count INTEGER NOT NULL DEFAULT 0,
                                          active BOOLEAN NOT NULL DEFAULT true''')
            >>>     id0 = db.insert('doctest_t1', {'name': 'name_one'}, returning='id')['id']
            >>>     id1 = db.insert('doctest_t1', {'name': 'name_two'}, returning='id')['id']

            >>> db2 = pypgwrap.connection(key=key)
            >>> id3 = db.insert('doctest_t1', {'name': 'name_three'}, returning='id')['id']
            >>> id4 = db.insert('doctest_t1', {'name': 'name_four'}, returning='id')['id']

            >>> db3 = pypgwrap.connection(key=key)
            >>> db3.commit()

        Distributed transactions, with ContextManager:

            >>> with pypgwrap.ContextManager() as context:

            >>>     with pypgwrap.connection(key=context.key) as db:
            >>>         db.create_table('t1', '''id SERIAL PRIMARY KEY,
                                              name TEXT NOT NULL,
                                              count INTEGER NOT NULL DEFAULT 0,
                                              active BOOLEAN NOT NULL DEFAULT true''')
            >>>         id0 = db.insert('doctest_t1', {'name': 'name_one'}, returning='id')['id']
            >>>         id1 = db.insert('doctest_t1', {'name': 'name_two'}, returning='id')['id']

            >>>     db2 = pypgwrap.connection(key)
            >>>     id3 = db.insert('doctest_t1', {'name': 'name_three'}, returning='id')['id']
            >>>     id4 = db.insert('doctest_t1', {'name': 'name_four'}, returning='id')['id']


    Connection
    ----------

    The config_pool need some parameters:
        - max_pool: Maximum of connections created and mainteined in memory
        - pool_expiration: Idle time (in minutes) for close and destroy memory connection
        - url: Url with connection parameters

    The intention of this method is to call at application start up, only!

    The connection class provides methods to return a cursor object or execute SQL queries
    directly (using an implicit cursor).

    The connection context provides the following basic methods:

        cursor          - create a new instance of cursor class
        commit          - Commit transaction (called implicitly on exiting context handler)
        rollback        - Rollback transaction

    Cursor
    ------

    The module provides a cursor context handler wrapping the psycopg2 cursor.

    The cursor object uses the psycopg2 'DictCursor' by default (which
    returns rows as a pseudo python dictionary) however this can be overridden
    by providing a 'cursor_factory' parameter to the constructor.

    >>> db = pypgwrap.connection()
    >>> with db.cursor() as c:
    ...     c.query('select version()')
    [['PostgreSQL...']]

    The cursor context provides the following basic methods:

        execute         - execute SQL query and return rowcount
        query           - execute SQL query and fetch results
        query_one       - execute SQL query and fetch first result
        query_dict      - execute SQL query and return results as dict
                          keyed on specified key (which should be unique)

    In addition the cursor can use the SQL API methods described below or
    access the underlying psycopg2 cursor (via the self.cursor attribute).

    The cursor methods are also available as standalone functions which
    run inside an implicit cursor object.

    SQL API
    -------

    The cursor class also provides a simple Python API for common SQL
    operations.  The basic methods provides are:

        select          - single table select (with corresponding select_one, select_dict methods)
        join            - two table join (with corresponding join_one, join_dict methods)
        insert          - SQL insert
        update          - SQL update
        delete          - SQL delete

    The methods can be parameterised to customise the associated query
    (see db module for detail):

        where           - 'where' clause as dict (column operators can be
                          specified using the colunm__operator format)

                          where = {'name':'abc','status__in':(1,2,3)}

        columns         - list of columns to be returned - these can
                          be real columns or expressions. If spefified
                          as a tuple the column is explicitly named
                          using the AS operator

                          columns = ('name',('status > 1','updated'))

        order           - sort order as list (use 'column__desc' to
                          reverse order)

                          order = ('name__desc',)

        limit           - row limit (int)

        offset          - offset (int)

        on              - join columns (as tuple)

        values          - insert data as dict

        returning       - columns to return (string)

    The methods are also available as standalone functions which create an
    implicit cursor object.

    Basic usage:

        >>> db.create_table('t1','id serial,name text,count int')
        >>> db.create_table('t2','id serial,t1_id int,value text')
        >>> db.log = sys.stdout
        >>> db.insert('t1',{'name':'abc','count':0},returning='id,name')
        INSERT INTO t1 (name) VALUES ('abc') RETURNING id,name
        [1, 'abc']
        >>> db.insert('t2',{'t1_id':1,'value':'t2'})
        INSERT INTO t2 (t1_id,value) VALUES (1,'t2')
        1
        >>> db.select('t1')
        SELECT * FROM t1
        [[1, 'abc', 0]]
        >>> db.select_one('t1',where={'name':'abc'},columns=('name','count'))
        SELECT name, count FROM t1 WHERE name = 'abc'
        ['abc', 0]
        >>> db.join(('t1','t2'),columns=('t1.id','t2.value'))
        SELECT t1.id, t2.value FROM t1 JOIN t2 ON t1.id = t2.t1_id
        [[1, 't2']]
        >>> db.insert('t1',{'name':'abc'},returning='id')
        INSERT INTO t1 (name) VALUES ('abc') RETURNING id
        [2]
        >>> db.update('t1',{'name':'xyz'},where={'name':'abc'})
        UPDATE t1 SET name = 'xyz' WHERE name = 'abc'
        2
        >>> db.update('t1',{'count__func':'count + 1'},where={'count__lt':10},returning="id,count")
        UPDATE t1 SET count = count + 1 WHERE count < 10 RETURNING id,count
        [[1, 1]]

    Prepared Statements
    -------------------

        Prepared statements can be created using the

            connection.prepare(stmt,params,name,call_type)

            stmt      : prepared statement (with parameters identified
                        in the statement using the psql $1,$2... notation)
            params    : list of optional parameter types (usually not
                        needed - infered by psql)
            name      : name for the prepared statement (usually
                        autogenerated)
            call_type : method used when instance called as method
                        (defaults to 'query')

        The constructor returns a PreparedStatement object which can be used
        instead of an sql statement in the connection.execute and
        connection.query_xxx methods.

        >>> p = db.prepare('UPDATE t1 SET name = $2 WHERE id = $1')
        PREPARE _pstmt_001  AS UPDATE t1 SET name = $2 WHERE id = $1
        >>> with db.cursor() as c:
        ...     c.execute(p,(1,'xxx'))
        EXECUTE _pstmt_001 (1,'xxx')

        The PreparedStatement object can also be called directly using the
        execute/query/query_one/query_dict methods. The instance is also
        directly callable using the method type identified in 'call_type'

        >>> p = db.prepare('UPDATE t1 SET name = $2 WHERE id = $1')
        PREPARE _pstmt_001  AS UPDATE t1 SET name = $2 WHERE id = $1
        >>> p.execute(1,'xxx')
        EXECUTE _pstmt_001 (1,'xxx')
        >>> p(1,'xxx')
        EXECUTE _pstmt_001 (1,'xxx')

    Logging
    -------

        To enable logging the connection.log attribute can be set to either an
        instance of logging.Logger or a file-like object (supporting the write
        method).

        The log message is generated using the self.logf function (called with
        the cursor object as a parameter). By default this just returns the
        query string however can be customised as needed. A cursor.timestamp
        attribute is available to allow execution time to be tracked.

        >>> db.log = sys.stdout
        >>> db.logf = lambda c : '[%f] %s' % (time.time() - c.timestamp,c.query)
        >>> db.query('SELECT * FROM t1')
        [0.000536] SELECT * FROM t1

    Changelog
    ---------

        *   0.1.0     03-06-2013  Initial import
        *   0.1.1     10-06-2013  Transaction context issues
        *   0.1.2     11-06-2013  ContextManager commit issues
        *   0.1.3     07-08-2013  ContextManager __exit__ fail on TypeError exception
        *   0.1.4     07-08-2013  ContextManager __exit__ fail on TypeError exception
        *   0.1.5     08-10-2013  - ThreadedConnectionPool fix when pool is exausted or max_con of Postgres is reached.
                                  - Created a param [pool_manager] in config_pool method. Params: SimpleConnectionPool,
                                  ThreadedConnectionPool. In Multthread enviroments must use ThreadedConnectionPool.
        *   0.1.6     14-10-2014  - Bugfix. Fix import of OperationalError. Avoid use protected member of psycopg2.
                                  - Change "from psycopg2._psycopg import OperationalError" to
                                    "from psycopg2 import OperationalError"
        *   0.1.11    04-09-2015  Non Threaded AutoCloseConnectionPool to use with pgpool
        *   0.1.12    04-09-2015  Deleted class AutoCloseConnectionPool.
                                  - Create configuration in env PYPGWRAP_CLOSE_CONNECTION_ON_EXIT to control
                                    when pypgwrap disable pool. When this env variable is True, all connections
                                    is discarded when execution is finished. No pooling is persisted. Util to use with
                                    PgPool or external pooling tools.
        *   0.1.13    17-09-2015  Fix use of ast.literal_eval to read Environment variable

    Author
    ------

        *   Erick Phillipe R. de Almeida (ephillipe@gmail.com)

    Master Repository/Issues
    ------------------------

        *   https://github.com/ephillipe/pypgwrap

    Credits
    ------------------------
        pypgwrap is inherited from pgwrap, an excelent wraper for Postgres but with lacks
        *   https://github.com/paulchakravarti/pgwrap

        Pooling is iherited from Psycopg2
        * https://github.com/psycopg/psycopg2/

    About me
    ------------------------
        *   http://about.me/erick.almeida
        *   http://erickalmeida.brandyourself.com



"""



