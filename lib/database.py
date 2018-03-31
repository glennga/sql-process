# coding=utf-8
"""
Contains functions to interact with a SQLite database.

Usage: Database.rollback_wrapper(exception, handler, database_connection)
       Database.random_name(is_join)
       Database.description(database_cursor, SQL_string, handler)
       Database.execute(database_cursor, SQL_string, handler, tuples, is_fetch)
       Database.executemany(database_cursor, SQL_string, handler, tuples)
       Database.connect(database_file, handler)
"""

import random
import sqlite3 as sql
import string

from lib.error import ErrorHandle


class Database:
    """
    All SQLite3 database operations. These include connecting, execution, name generation,
    and rollback.
    """

    @staticmethod
    def rollback_wrapper(e, handler, conn):
        """ Handler wrapper to rollback the current state of the database. This is meant to be
        wrapped in another lambda to fit the normal handler signature.

        :param e: Exception to pass to the handler.
        :param handler: Handler to use with the given exception.
        :param conn: Connection to database to rollback.
        :return: None.
        """
        # Rollback and close the connection.
        conn.rollback(), conn.close()
        handler(e)

    @staticmethod
    def random_name(is_join):
        """ Generate a random string for use as a temporary table name. The suffix 'JJJJJ'
        indicates that the table is going to be used to hold the results of a join,
        and the suffix 'TTTTT' indicates that the table was copied from some other node.

        :param is_join: Flag that indicates if the table is going to be used for a join or not.
        :return: String containing the generated table name.
        """
        p = ''.join(random.choice(string.ascii_uppercase) for _ in range(10))
        return p + ('JJJJJ' if is_join else 'TTTTT')

    @staticmethod
    def description(cur, s, handler=ErrorHandle.default_handler):
        """ Collect the description field of the SQL execution. This holds the column names
        associated with the result returned.

        :param cur: Cursor to an open database connection.
        :param s: SQL string to execute, and pull the description off of.
        :param handler: Handler to use when the SQL execution fails.
        :return: The column names associated with the execution of 'S'.
        """
        return ErrorHandle.attempt_operation(lambda: cur.execute(s).description,
                                             sql.Error, handler, True)

    @staticmethod
    def execute(cur, s, handler=ErrorHandle.default_handler, tup=None, fetch=False):
        """ Execute some statement with the given database cursor. If the 'tup' argument is not
        None, then it is assumed that the given statement is prepared. If 'fetch' is raised,
        the resultant is returned.

        :param cur: Cursor to an open database connection.
        :param s: SQL string to execute.
        :param handler: Handler to use when the SQL execution fails.
        :param tup: Tuple to use with the execution of 'S', given that 'S' is a prepared statement.
        :param fetch: Flag that enables the return of the execution of 'S'
        :return: An error associated with the SQL if the execution was not successful. None if the
            execution was successful but fetch is not raised. Otherwise, the resultant of 'S'.
        """
        e = lambda: cur.execute(s).fetchall() if tup is None else cur.execute(s, tup).fetchall()
        return ErrorHandle.attempt_operation(e, sql.Error, handler, fetch)

    @staticmethod
    def executemany(cur, s, tups, handler=ErrorHandle.default_handler):
        """ Execute some prepared statement with the given database cursor. 'tups' represents an
        iterable of tuples. 'S' will be executed for all tuples in 'tups'.

        :param cur: Cursor to an open database connection.
        :param s: Prepared SQL string to execute.
        :param tups: Tuples to use when 'S' is executed.
        :param handler: Handler to use when the SQL execution fails.
        :return: An error associated with the SQL if the execution was not successful. None
            otherwise.
        """
        e = lambda: cur.executemany(s, tups)
        return ErrorHandle.attempt_operation(e, sql.Error, handler, False)

    @staticmethod
    def connect(f, handler=ErrorHandle.default_handler):
        """ Connect to the given database file, and return the resulting connection and cursor.

        :param f: Filename of the database to connect to.
        :param handler: Handler to use when a database connection cannot be established.
        :return: An error associated with the connection failure if a connection could not be
            established. Otherwise, the database connection and cursor in that order.
        """
        conn = ErrorHandle.attempt_operation(lambda: sql.connect(f), sql.Error, handler, True)

        # Return the database and cursor if a connection could be made.
        if ErrorHandle.is_error(conn):
            return ErrorHandle.wrap_error_tag('Could not connect to the database.'), ''
        else:
            return conn, conn.cursor()
