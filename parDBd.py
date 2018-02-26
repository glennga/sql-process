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

Usage: python parDBd.py [hostname] [port]

Error: 2 - Incorrect number of arguments.
       3 - Unable to bind a socket with specified hostname and port.
"""

import pickle
import socket
import sqlite3 as sql
import sys

from catalog import LocalCatalog
from dissect import SQLFile


def insert_single_on_db(k, r):
    """ Perform a single insertion SQL operation on the passed database. Do not wait for a
    terminating operation code.

    :param k: Socket connection to send response through.
    :param r: List passed to socket, containing the name of the database file and the SQL.
    :return: None.
    """
    f, s, tup, r_i = r[1], r[2], r[3], r
    try:
        conn = sql.connect(f)
    except sql.Error as e:
        k.send(pickle.dumps(str(e)))
        return
    cur = conn.cursor()

    # Execute the command. Return the error if any exist.
    try:
        cur.execute(s, tup)
    except sql.Error as e:
        k.send(pickle.dumps(str(e)))

    # Otherwise, no error exists. Send the success message.
    conn.commit(), conn.close()
    k.send(pickle.dumps(['EY', 'Success']))


def insert_on_db(k, r):
    """ Perform the given insertion SQL operation on the passed database. Wait for the
    terminating 'YZ' and 'YY' operation codes.

    :param k: Socket connection to send response through.
    :param r: List passed to socket, containing the name of the database file and the SQL.
    :return: None.
    """
    f, s, tup, r_i = r[1], r[2], r[3], r
    try:
        conn = sql.connect(f)
    except sql.Error as e:
        k.send(pickle.dumps(str(e)))
        return
    cur = conn.cursor()

    # Wait for the terminating operation codes.
    while True:
        # Execute the command. Return the error if any exist.
        try:
            cur.execute(s, tup)
            k.send(pickle.dumps(['EY', 'Success']))
            r_i = pickle.loads(k.recv(4096))

            # Do not proceed if 'YY' (stop waiting, don't execute) operation code is passed.
            if r_i[0] == 'YY':
                break
            elif r_i[0] == 'YZ':
                cur.execute(r_i[2], r_i[3])
                k.send(pickle.dumps(['EY', 'Success']))
                break
            else:
                f, s, tup = r_i[1], r_i[2], r_i[3]

        except (sql.Error, EOFError) as e:
            k.send(pickle.dumps(str(e)))

    # Otherwise, no error exists. Commit our changes and close our connection.
    conn.commit(), conn.close()
    k.send(pickle.dumps(['EY', 'Success']))


def execute_on_db(k, r):
    """ Perform the given SQL operation on the passed database. Return any tuples if the
    statement is a SELECT statement.

    :param k: Socket connection to send response through.
    :param r: List passed to socket, containing the name of the database file and the SQL.
    :return: None.
    """
    f, s, result = r[1], r[2], []
    try:
        conn = sql.connect(f)
    except sql.Error as e:
        k.send(pickle.dumps(str(e)))
        return
    cur = conn.cursor()

    # Execute the command. Return the error if any exist.
    try:
        if len(r) == 3:
            result = cur.execute(s).fetchall()
        else:
            k.send(pickle.dumps('Incorrect number of items sent to socket.'))
    except sql.Error as e:
        k.send(pickle.dumps(str(e)))

    # Otherwise, no error exists. Send the resulting tuples (if any exist).
    conn.commit(), conn.close()

    # If the statement is a selection, send all resulting tuples.
    if SQLFile.is_select(s):
        for i, r_t in enumerate(result):
            # If this is the last tuple, append the ending operation code.
            k.send(pickle.dumps(['EZ' if i + 1 == len(result) else 'ES', r_t]))
        if len(result) == 0:
            k.send(pickle.dumps(['EZ', 'No tuples found.']))
    else:
        # Otherwise, assume that the statement has passed.
        k.send(pickle.dumps(['EZ', 'Success']))


def return_columns(k, r):
    """ Return the columns associated with a table through the given socket.

    :param k: Socket connection to send response through.
    :param r: List passed to socket, containing the name of the database file and the table name.
    :return: None.
    """
    f, tname, col = r[1], r[2], []
    try:
        conn = sql.connect(f)
    except sql.Error as e:
        k.send(pickle.dumps(str(e)))
        return
    cur = conn.cursor()

    # Execute the command. Return the error if any exist.
    try:
        col = cur.execute('SELECT * FROM ' + tname + ' LIMIT 1').description
    except sql.Error as e:
        k.send(pickle.dumps(str(e)))

    # Otherwise, no error exists. Send the resulting tuples (if any exist).
    conn.commit(), conn.close()

    # Return the column names.
    k.send(pickle.dumps(['EP', [col[i][0] for i in range(len(col))]]))


def interpret(k, r):
    """ Given a socket and a message through the socket, interpret the message. The result should
    be a list of length greater than 1, and the first element should be in the space ['E', 'C',
    'U', 'P', 'K', 'YS'].

    :param k: Socket connection pass response through.
    :param r: List passed to socket, containing the name of the database file and the SQL.
    :return: None.
    """

    # If our response is not an list, return an error.
    if not hasattr(r, '__iter__'):
        k.send(pickle.dumps('Input not an list.')), k.close()
        return

    if r[0] == 'YS':
        # Execute multiple insertion operations on a database.
        insert_on_db(k, r)
    elif r[0] == 'YZ':
        # Execute a single insertion operation on a database.
        insert_single_on_db(k, r)
    elif r[0] == 'YY':
        # Do not perform any action. Send a success message
        k.send(pickle.dumps(['EY', 'Success']))
    elif r[0] == 'E':
        # Execute an operation on a database.
        execute_on_db(k, r)
    elif r[0] == 'C':
        # Insert ddl into the catalog table.
        LocalCatalog.record_ddl(k, r)
    elif r[0] == 'K':
        # Insert partition data into the catalog table.
        LocalCatalog.record_partition(k, r)
    elif r[0] == 'U':
        # Return the URIs associated with each node.
        LocalCatalog.return_node_uris(k, r)
    elif r[0] == 'P':
        # Return the columns associated with the given table.
        return_columns(k, r)
    else:
        k.send(pickle.dumps('Operation code invalid.'))


if __name__ == '__main__':
    # Ensure that we only have 2 arguments.
    if len(sys.argv) != 3:
        print('Usage: python3 parDBd.py [hostname] [port]')
        exit(2)

    # Create the socket.
    sock = socket.socket()
    try:
        sock.bind((sys.argv[1], int(sys.argv[2])))
    except OSError as e:
        print('Socket Error: ' + str(e))
        exit(3)

    while True:
        # Listen and wait for a connection on this socket.
        sock.listen(1)
        k, addr = sock.accept()

        try:
            # Retrieve the sent data. Unpickle the data. Interpret the command.
            interpret(k, pickle.loads(k.recv(4096)))
        except EOFError as e:
            k.send(pickle.dumps(str(e)))
        except ConnectionResetError as e:
            # Ignore when a connection is forcibly closed, or the socket has timed out.
            pass

        k.close()
