import random
import sqlite3 as sql
import string

from lib.error import ErrorHandle


class Database:
    """

    """

    @staticmethod
    def rollback_wrapper(e, handler, conn):
        """

        :param e:
        :param conn:
        :return:
        """
        # Rollback and close the connection.
        conn.rollback(), conn.close()
        handler(e)

    @staticmethod
    def random_name(is_join):
        """

        :param is_join:
        :return:
        """
        suffix = 'JJJJJ' if is_join else 'TTTTT'
        return ''.join(random.choices(string.ascii_uppercase, k=10)) + suffix

    @staticmethod
    def description(cur, s, handler=ErrorHandle.default_handler):
        """ Collect the description field of the SQL execution. This holds the

        :param cur:
        :param s:
        :param handler:
        :return:
        """
        return ErrorHandle.attempt_operation(lambda: cur.execute(s).description,
                                             sql.Error, handler, True)

    @staticmethod
    def execute(cur, s, handler=ErrorHandle.default_handler, tup=None, fetch=False):
        """

        :param cur:
        :param s:
        :param handler:
        :param tup:
        :param fetch:
        :return:
        """
        e = lambda: cur.execute(s).fetchall() if tup is None else cur.execute(s, tup).fetchall()
        return ErrorHandle.attempt_operation(e, sql.Error, handler, fetch)

    @staticmethod
    def executemany(cur, s, handler=ErrorHandle.default_handler, tups=None):
        """

        :param cur:
        :param s:
        :param handler:
        :param tups:
        :return:
        """
        e = lambda: cur.executemany(s) if tups is None else cur.executemany(s, tups)
        return ErrorHandle.attempt_operation(e, sql.Error, handler, False)

    @staticmethod
    def connect(f, handler=ErrorHandle.default_handler):
        """

        :param f:
        :param handler:
        :return:
        """
        conn = ErrorHandle.attempt_operation(lambda: sql.connect(f), sql.Error, handler, True)

        # Return the database and cursor if a connection could be made.
        if ErrorHandle.is_error(conn):
            return ErrorHandle.wrap_error_tag('Could not connect to the database.'), ''
        else:
            return conn, conn.cursor()
