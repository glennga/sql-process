# coding=utf-8
"""
Listens for a command list to be sent to the given port, and returns a command response list
through that same port. Given an operation code OP, the following occurs:

OP : 'YS' -> Execute an insertion SQL statement, and wait for additional statements.
   : 'YZ' -> Execute a SQL statement, and don't wait for additional statements.
   : 'YY' -> Don't execute a SQL statement, and don't wait for additional statements.
   : 'E' -> Execute a SQL statement and return tuples if applicable.
   : 'C' -> Record a DDL to the catalog database.
   : 'K' -> Record partitioning information to the catalog database.
   : 'U' -> Lookup the node URIs on the catalog database and return these.
   : 'P' -> Lookup the fields for a given table and return this.
   : 'B' -> Ship a given table to the current node, and perform a join.

Usage: python parDBd.py [hostname] [port]

Error: 2 - Incorrect number of arguments.
       3 - Unable to bind a socket with specified hostname and port.
"""

import pickle
import random
import socket
import string
import sys
import time

from lib.catalog import LocalCatalog
from lib.dissect import SQLFile, ClusterCFG
from lib.error import ErrorHandle


def insert_single_on_db(k_n, r):
    """ Perform a single insertion SQL operation on the passed database. Do not wait for a
    terminating operation code.

    :param k_n: Socket connection to send response through.
    :param r: List passed to socket, containing the name of the database file and the SQL.
    :return: None.
    """
    # Handle all errors by sending the error string through the socket.
    f, s, tup, r_i = r[1], r[2], r[3], r
    handle_error = lambda a: ErrorHandle.write_socket(k_n, str(a))

    # Create our connection.
    c = ErrorHandle.sql_connect(f, handle_error)
    if ErrorHandle.is_error(c):
        return
    conn, cur = c

    # Execute the command. Return the error if any exist, or return a sucesss method.
    if not ErrorHandle.is_error(ErrorHandle.sql_execute(cur, s, handle_error, tup)):
        conn.commit(), conn.close()
        ErrorHandle.write_socket(k_n, ['EY', 'Success'])
    else:
        conn.close()


def insert_on_db(k_n, r):
    """ Perform the given insertion SQL operation on the passed database. Wait for the
    terminating 'YZ' and 'YY' operation codes.

    :param k_n: Socket connection to send response through.
    :param r: List passed to socket, containing the name of the database file and the SQL.
    :return: None.
    """
    # Handle all errors by sending the error string through the socket.
    f, s, tup, r_i, is_error = r[1], r[2], r[3], r, False
    handle_error = lambda a: ErrorHandle.write_socket(k_n, str(a))

    # Create our connection.
    c = ErrorHandle.sql_connect(f, handle_error)
    if ErrorHandle.is_error(c):
        return
    conn, cur = c

    while True:
        # Execute the command. Return the error if any exist.
        if not ErrorHandle.is_error(ErrorHandle.sql_execute(cur, s, handle_error, tup)):
            ErrorHandle.write_socket(k_n, ['EY', 'Success'])

        # Interpret the current operation code.
        r_i = ErrorHandle.read_socket(k_n, handle_error)
        if ErrorHandle.is_error(r_i):
            continue

        # Stop if YZ or YY, proceed otherwise.
        if r_i[0] == 'YY':
            break
        elif r_i[0] == 'YZ' and not \
                ErrorHandle.is_error(ErrorHandle.sql_execute(cur, s, handle_error, tup)):
            ErrorHandle.write_socket(k_n, ['EY', 'Success'])
            break
        else:
            f, s, tup = r_i[1], r_i[2], r_i[3]

    # Commit our changes and close our connection.
    conn.commit(), conn.close()
    ErrorHandle.write_socket(k_n, ['EY', 'Success'])


def execute_on_db(k_n, r):
    """ Perform the given SQL operation on the passed database. Return any tuples if the
    statement is a SELECT statement.

    :param k_n: Socket connection to send response through.
    :param r: List passed to socket, containing the name of the database file and the SQL.
    :return: None.
    """
    # Handle all errors by sending the error string through the socket.
    f, s, result = r[1], r[2], []
    handle_error = lambda a: ErrorHandle.write_socket(k_n, str(a))

    # Create our connection.
    c = ErrorHandle.sql_connect(f, handle_error)
    if ErrorHandle.is_error(c):
        return
    conn, cur = c

    # Message length must match. Return an error if this is false.
    if len(r) != 3:
        handle_error('Error: Incorrect number of items sent to socket.')
        return

    # Execute the command. Return the error if any exist.
    result = ErrorHandle.sql_execute(cur, s, handle_error, fetch=True)
    if ErrorHandle.is_error(result):
        conn.close()
        return
    else:
        conn.commit(), conn.close()

    # If the statement is a selection, send all resulting tuples.
    if SQLFile.is_select(s):
        for i, r_t in enumerate(result):
            # If this is the last tuple, append the ending operation code.
            ErrorHandle.write_socket(k_n, ['EZ' if i + 1 == len(result) else 'ES', r_t])
        if len(result) == 0:
            ErrorHandle.write_socket(k_n, ['EZ', 'No tuples found.'])
    else:
        # Otherwise, assume that the statement has passed.
        ErrorHandle.write_socket(k_n, ['EZ', 'Success'])


def return_columns(k_n, r):
    """ Return the columns associated with a table through the given socket.

    :param k_n: Socket connection to send response through.
    :param r: List passed to socket, containing the name of the database file and the table name.
    :return: None.
    """
    f, tname, col = r[1], r[2], []
    c = ErrorHandle.sql_connect(f, lambda a: ErrorHandle.write_socket(k_n, str(a)))
    if ErrorHandle.is_error(c):
        return
    conn, cur = c

    # Execute the command. Return the error if any exist.
    col = ErrorHandle.sql_execute(cur, 'SELECT * '
                                       'FROM ' + tname + 'LIMIT 1;',
                                  lambda a: ErrorHandle.write_socket(k_n, str(a)),
                                  fetch=True, desc=True)
    conn.close()

    # Return the column names.
    if not ErrorHandle.is_error(col):
        ErrorHandle.write_socket(k_n, ['EP', [col[i][0] for i in range(len(col))]])


def store_from_ship(sock_n, cur, temp_name):
    """

    :param sock_n:
    :param cur:
    :param temp_name:
    :return:
    """
    # Wait for a response. Wait 1 second for other node to respond after.
    r = ErrorHandle.read_socket(sock_n)
    if ErrorHandle.is_error(r):
        return r
    time.sleep(1.0)

    # Execute the insertion.
    operation, resultant = r
    r = ErrorHandle.sql_execute(cur, 'INSERT INTO ' + temp_name +
                                ' VALUES (' +
                                ''.join(['?, ' for _ in range(len(resultant) - 1)]) + '?);',
                                tup=resultant)
    if ErrorHandle.is_error(r):
        return r

    # Repeat. Operation code 'ES' marks the start of a message, and 'EZ' marks the end.
    while operation != 'EZ':
        r = ErrorHandle.read_socket(sock_n)
        if ErrorHandle.is_error(r):
            return r

        operation, resultant = r
        y = ErrorHandle.sql_execute(cur, 'INSERT INTO ' + temp_name +
                                    ' VALUES (' +
                                    ''.join(['?, ' for _ in range(len(resultant) - 1)]) + '?);',
                                    tup=resultant)
        if ErrorHandle.is_error(y):
            return y
    return 'Success'


def copy_table(k_n, cur, node, f_s, tnames):
    """

    :param k_n:
    :param cur:
    :param node:
    :param f_s:
    :param tnames:
    :return:
    """
    # Create socket to secondary node.
    host, port, f = ClusterCFG.parse_uri(node)
    sock_n = ErrorHandle.create_client_socket(host, port)
    if ErrorHandle.is_error(sock_n):
        ErrorHandle.write_socket(k_n, sock_n)
        sock_n.close()
        return sock_n

    # Grab the SQL used to create the table.
    ErrorHandle.write_socket(sock_n, ['E', f_s[1], 'SELECT sql '
                                                   'FROM sqlite_master '
                                                   'WHERE name="{}"'.format(tnames[1])])
    r = ErrorHandle.read_socket(sock_n)
    if ErrorHandle.is_error(r):
        sock_n.close()
        return r
    operation, create_sql = r

    # Execute the create table statement on the local database with a new table name.
    new_table = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10)) + 'TTTTT'
    r = ErrorHandle.sql_execute(cur, create_sql[0].replace(tnames[1], new_table, 1) + ';')
    if ErrorHandle.is_error(r):
        sock_n.close()
        return r

    sock_n.close()
    return create_sql, new_table


def ship(k_n, r):
    """

    :param k_n:
    :param r:
    :return:
    """
    f_s, tnames, node = r[1], r[2], r[3]
    error_handle = lambda a: ErrorHandle.write_socket(k_n, str(a))

    # Connect to local database.
    c = ErrorHandle.sql_connect(f_s[0], error_handle)
    if ErrorHandle.is_error(c):
        return
    conn, cur = c

    # Copy the table from our remote node to our local node.
    r = copy_table(k_n, cur, node, f_s, tnames)
    if ErrorHandle.is_error(r):
        error_handle(r)
        return
    create_sql, new_table = r

    # Create socket to secondary node.
    host, port, f = ClusterCFG.parse_uri(node)
    sock_n = ErrorHandle.create_client_socket(host, port)
    if ErrorHandle.is_error(sock_n):
        error_handle(sock_n)
        return

    # Retrieve data from secondary node.
    ErrorHandle.write_socket(sock_n, ['E', f_s[1], 'SELECT * FROM ' + tnames[1]])
    r = store_from_ship(sock_n, cur, new_table)
    if ErrorHandle.is_error(r):
        error_handle(r)
        return

    # Return the name of the table created if successful.
    conn.commit(), conn.close()
    ErrorHandle.write_socket(k_n, ['EB', new_table])


def interpret(k_n, r):
    """ Given a socket and a message through the socket, interpret the message. The result should
    be a list of length greater than 1, and the first element should be in the space ['E', 'C',
    'U', 'P', 'K', 'YS'].

    :param k_n: Socket connection pass response through.
    :param r: List passed to socket, containing the name of the database file and the SQL.
    :return: None.
    """

    # If our response is not an list, return an error.
    if not hasattr(r, '__iter__'):
        ErrorHandle.write_socket(k_n, 'Error: Input not a list.'), k_n.close()
        return

    if r[0] == 'YS':
        # Execute multiple insertion operations on a database.
        insert_on_db(k_n, r)
    elif r[0] == 'YZ':
        # Execute a single insertion operation on a database.
        insert_single_on_db(k_n, r)
    elif r[0] == 'YY':
        # Do not perform any action. Send a success message.
        ErrorHandle.write_socket(k_n, ['EY', 'Success'])
    elif r[0] == 'E':
        # Execute an operation on a database.
        execute_on_db(k_n, r)
    elif r[0] == 'C':
        # Execute DDL on the catalog table.
        LocalCatalog.record_ddl(k_n, r)
    elif r[0] == 'K':
        # Insert partition data into the catalog table.
        LocalCatalog.record_partition(k_n, r)
    elif r[0] == 'U':
        # Return the URIs associated with each node.
        LocalCatalog.return_node_uris(k_n, r)
    elif r[0] == 'P':
        # Return the columns associated with the given table.
        return_columns(k_n, r)
    elif r[0] == 'B':
        # Ship a table from a remote node to here, and perform a join.
        ship(k_n, r)
    else:
        ErrorHandle.write_socket(k_n, 'Error: Operation code invalid.')


if __name__ == '__main__':
    # Ensure that we only have 2 arguments.
    if len(sys.argv) != 3:
        print('Usage: python3 parDBd.py [hostname] [port]')
        exit(2)

    # Create the socket.
    sock = socket.socket()
    r_n = ErrorHandle.attempt_operation(lambda: sock.bind((sys.argv[1], int(sys.argv[2]))),
                                        OSError, lambda a: print('Error: ' + str(a)))
    if ErrorHandle.is_error(r_n):
        exit(3)

    while True:
        # Listen and wait for a connection on this socket.
        sock.listen(1)
        k, addr = sock.accept()

        try:
            # Retrieve the sent data. Unpickle the data. Interpret the command.
            interpret(k, ErrorHandle.read_socket(k))
        except EOFError as e:
            ErrorHandle.write_socket(k, str(e))
        except ConnectionResetError as e:
            # Ignore when a connection is forcibly closed, or the socket has timed out.
            pass

        k.close()
