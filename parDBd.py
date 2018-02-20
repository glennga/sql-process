# coding=utf-8
"""
Lists for a command list to be sent to the given port. There exists two
TODO: Finish this description.

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


def execute_on_db(k, r):
    """ Insert the given response string to a database file.

    :param k: Socket connection to send response through.
    :param r: List passed to socket, containing the name of the database file and the SQL.
    :return: None.
    """
    f, s, result = r[1], r[2], []
    conn = sql.connect(f)
    cur = conn.cursor()

    # Execute the command. Return the error if any exist.
    try:
        # Support for prepared statements, dependent on the length of the input array.
        if len(r) == 3:
            result = cur.execute(s).fetchall()
        elif len(r) == 4:
            result = cur.execute(s, tuple(r[3])).fetchall()
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
    """ TODO: Finish description.

    :param k:
    :param r:
    :return:
    """
    f, tname, col = r[1], r[2], []
    conn = sql.connect(f)
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
    'U', 'P', 'K'].

    :param k: Socket connection pass response through.
    :param r: List passed to socket, containing the name of the database file and the SQL.
    :return: None.
    """

    # If our response is not an list, return an error.
    if not hasattr(r, '__iter__'):
        k.send(pickle.dumps('Input not an list.')), k.close()
        return

    if r[0] == 'E':
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

        # Retrieve the sent data. Unpickle the data.
        try:
            r = pickle.loads(k.recv(4096))

            # Interpret the command.
            interpret(k, r)
        except EOFError as e:
            k.send(pickle.dumps(str(e)))
        except ConnectionResetError as e:
            # Ignore when a connection is forcibly closed.
            pass

        k.close()
