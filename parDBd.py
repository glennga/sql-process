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

import catalog
import dissect


def execute_on_db(k, r):
    """ Insert the given response string to a database file.

    :param k: Socket connection to send response through.
    :param r: List passed to socket, containing the name of the database file and the SQL.
    :return: None.
    """
    f, s = r[1], r[2]
    conn = sql.connect(f)
    cur = conn.cursor()

    # Execute the command. Return the error if any exist.
    try:
        result = cur.execute(s).fetchall()
    except sql.Error as e:
        k.send(pickle.dumps(str(e)))

    # Otherwise, no error exists. Send the resulting tuples (if any exist).
    conn.commit(), conn.close()

    # If the statement is a selection, send all resulting tuples.
    if dissect.is_select(s):
        for i, tuple in enumerate(result):
            # If this is the last tuple, append the ending operation code.
            k.send(pickle.dumps(['EZ' if i + 1 == len(result) else 'ES', tuple]))
    else:
        k.send(pickle.dumps(['EZ', 'Success']))


if __name__ == '__main__':
    # Ensure that we only have 2 arguments.
    if len(sys.argv) != 3:
        print('Usage: python3 parDBd.py [hostname] [port]')
        exit(2)

    try:
        # Create the socket.
        sock = socket.socket()
        sock.bind((sys.argv[1], int(sys.argv[2])))
    except OSError as e:
        print('Socket Error: ' + str(e))
        exit(3)

    while True:
        # Listen and wait for a connection on this socket.
        sock.listen(1)
        k, addr = sock.accept()

        # Retrieve the sent data. Unpickle the data.
        r = ['']
        try:
            r = pickle.loads(k.recv(4096))
        except EOFError as e:
            k.send(pickle.dumps(str(e)))
            k.close()

        # If our response is not an list, return an error.
        if not hasattr(r, '__iter__'):
            k.send(pickle.dumps('Input not an list.'))
            k.close()

        # Execute an operation on a database.
        if r[0] == 'E':
            execute_on_db(k, r)

        # Insert metadata into the catalog table.
        elif r[0] == 'C':
            cluster, s = r[1], r[2]
            catalog.log_local(k, cluster, s)

        k.close()
