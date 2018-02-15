# coding=utf-8
"""
Contains functions to manage the catalog table, locally (client side requests) and remotely (server
side actions).

Usage: is_connect([catalog-node-info])
       create_table_local([catalog-filename])
       log_remote([cluster-info], [DDL-statement])
       log_local([server-socket], [cluster-info], [DDL-statement])
"""

import pickle
import socket
import sqlite3 as sql

import dissect

def is_connect(c):
    """ Given information about the catalog node, check if a connection is possible to the
    catalog node.

    :param c: The value of the key-value pair inside clustercfg.
    :return: True if a connection can be achieved. False otherwise.
    """
    host, port = c.split(':')
    port = port.split('/')[0]

    # Create our socket and attempt to connect.
    try:
        sock = socket.socket()
        sock.connect((host, int(port)))
    except OSError:
        sock.close()
        return False

    sock.close()
    return True

def verify_node_count(c, tname, n):
    """ TODO: Finish this description.

    :param c:
    :param tname:
    :param n:
    :return:
    """
    host, port = c.split(':')
    port = port.split('/')[0]

    # Create our socket and attempt to connect.
    try:
        sock = socket.socket()
        sock.connect((host, int(port)))
    except OSError:
        sock.close()
        return ''

    # Grab the number of nodes in the given table (maximum of nodeid).
    # TODO: Finish grabbing MAX(nodeid).

    # Return true if n matches the number we just found. Otherwise false.

def node_uris(c):
    """ Given the URI of the catalog node, grab all of the node URIs in the cluster.

    :param c: The URI (value of the key-value) pair inside clustercfg of the catalog node.
    :return: False if a connection cannot be achieved. Otherwise, a list of all node URIs in the
    cluster in sequential order.
    """
    host, port = c.split(':')
    port = port.split('/')[0]

    # Create our socket and attempt to connect.
    try:
        sock = socket.socket()
        sock.connect((host, int(port)))
    except OSError:
        sock.close()
        return False

    # TODO: Finish collecting node URIs here.


def create_table_local(f):
    """ Given the name of the database file, create the table. This information is specified in
    the homework assignment.

    :param f: Location of the database file to store this table on.
    :return: None.
    """
    conn = sql.connect(f)

    cur = conn.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS dtables ('
                'tname CHARACTER(32), '
                'nodedriver CHARACTER(64), '
                'nodeurl CHARACTER(128), '
                'nodeuser  CHARACTER(16), '
                'nodepasswd  CHARACTER(16), '
                'partmtd INT, '
                'nodeid INT, '
                'partcol CHARACTER(32), '
                'partparam1 CHARACTER(128), '
                'partparam2 CHARACTER(128)); ')


def log_remote(cluster, s):
    """ Given information about the cluster and the DDL statement to execute, lookup the location
    of the catalog node and pass the request there.

    :param cluster: List containing the catalog node URI, and a list of cluster node URIs.
    :param s: DDL statement to pass to the catalog node.
    :return: False if the remote cluster node does not perform the operation successfully. True
    otherwise.
    """
    host, port = cluster[0].split(':')
    port = port.split('/')[0]

    # Create our socket.
    try:
        sock = socket.socket()
        sock.connect((host, int(port)))
    except OSError:
        sock.close()
        return False

    # Pickle our command list ('C', cluster, and DDL), and send our message.
    sock.send(pickle.dumps(['C', cluster, s]))

    # Wait for a response to be sent back, and return this response.
    response = sock.recv(4096)
    r = pickle.loads(response) if response != b'' else 'Failed to receive response.'

    if r != 'Success':
        sock.close()
        return False

    # Return true if there are no hiccups.
    sock.close()
    return True


def log_local(k, cluster, s):
    """ Given node URIs of the cluster and the DDL statement to execute, insert metadata
    about which nodes and which tables are affected.

    :param k: Socket connection to send response through.
    :param cluster: List containing the catalog node URI, and a list of cluster node URIs.
    :param s: The SQL being executed.
    :return: None.
    """
    f = cluster[0].split('/')[1]

    # Connect to SQLite database using the filename.
    conn = sql.connect(f)
    cur = conn.cursor()
    create_table_local(f)

    # Determine the table being operated on in the DDL.
    table = dissect.table(s)
    if table is False:
        k.send(pickle.dumps('No table found. SQL statement formatted incorrectly.'))
        return

    # Assemble our tuples and perform the insertion.
    try:
        tuples = [(table, cluster[i + 1], i + 1) for i in range(len(cluster[1]))]
        cur.executemany('INSERT INTO dtables VALUES (?, NULL, ?, NULL, NULL, NULL, ?, NULL, '
                        'NULL, NULL)', tuples)
    except sql.Error as e:
        k.send(pickle.dumps(str(e))), conn.rollback(), conn.close()
        return

    # No error exists. Commit and send the success message.
    conn.commit(), conn.close()
    k.send(pickle.dumps('Success'))
