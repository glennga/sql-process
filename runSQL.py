# coding=utf-8
"""
Executes a **simple** single SQL statement on a cluster of computers. This SQL statement is
specified in 'sqlfile', and access information about each computer in the cluster is specified
in 'clustercfg'.

Usage: python runSQL.py [clustercfg] [sqlfile]

Error: TODO: Compile all of the error codes.
"""

import pickle
import socket
import sys
from multiprocessing import Process
from subprocess import call

import catalog
import dissect

# Used to store the node IDs of each **successful** execution.
successful_nodes = []


def collect_socket(k):
    """ Given the socket and the name of the node, grab the operation and the tuple results. This
    is to be executed after sending out an execute command 'E'.

    :param k: Open to socket to wait for a response on.
    :return: Error message if a two element list is not received. Otherwise, the operation
    and tuple as a list.
    """
    # TODO: Put more comments in here.
    response = k.recv(4096)
    try:
        r = pickle.loads(response)
        if isinstance(r, str) and r == '':
            return 'Failed to receive response.'
        elif isinstance(r, str):
            return r
        else:
            operation, resultant = r[0], r[1:]
    except (EOFError, IndexError) as e:
        return str(e)

    return operation, resultant


def execute_sql(node_uri, n, s):
    """ Given the URI of a node from the clustercfg file and the SQL to execute, send the SQL to
    the appropriate node. Print any return messages or errors that occur.

    :param node_uri: Node URI from the clustercfg file (right side of key-value pair).
    :param n: Node number that this operation is working on.
    :param s: SQL statement to execute on the node.
    :return: None.
    """
    host, sock = node_uri.split(':')[0], ''
    port, f = node_uri.split(':')[1].split('/')

    # Create our socket.
    try:
        sock = socket.socket()
        sock.connect((host, int(port)))
    except OSError as e:
        print('Error on Node: ' + str(n) + ': ' + str(e)), sock.close()
        return

    # Pickle our command list ('E', filename, and SQL), and send our message.
    sock.send(pickle.dumps(['E', f, s]))

    # Wait for a response.
    r = collect_socket(sock)
    if isinstance(r, str):
        print('Error on Node ' + str(n) + ': ' + r), sock.close()
        return
    else:
        operation, resultant = r
        print('Node ' + str(n) + ': ' + resultant)

    # Repeat. Operation code 'ES' marks the start of a message, and 'EZ' marks the end.
    while operation == 'ES':
        r = collect_socket(sock)
        if isinstance(r, str):
            print('Error on Node ' + str(n) + ': ' + r), sock.close()
            return
        else:
            operation, resultant = r
            print('Node ' + str(n) + ': ' + resultant)

    # End is reached. The operation was successful.
    successful_nodes.append(n), sock.close()


if __name__ == '__main__':
    # Ensure that we have only two arguments.
    if len(sys.argv) != 3:
        print('Usage: python3 runSQL.py [clustercfg] [sqlfile]')
        exit(2)

    # Parse both the clustercfg and sqlfile. Ensure that both are properly formatted.
    catalog_uri, s = dissect.clustercfg_catalog(sys.argv[1]), dissect.sqlfile(sys.argv[2])
    if isinstance(catalog_uri, str):
        print(catalog_uri), exit(3)
    elif not s:
        print('There exists no terminating semicolon in "sqlfile". No statement executed.'), exit(4)

    # If the given SQL is a DDL, call 'runDDL' instead.
    # TODO: Finish this.
    if True:
        call(['python3', 'runDDL.py', sys.argv[1], sys.argv[2]]), exit(0)

    # Collect our node URIs. Do not proceed if the catalog node cannot be reached.
    node_uris = catalog.node_uris(catalog_uri)
    if not node_uris:
        print('Cannot connect to the catalog. No statement executed.'), exit(5)

    # For every node in the cluster dictionary, execute the given statement and display any errors.
    processes = []
    for i, node in enumerate(node_uris):
        processes.append(Process(target=execute_sql, args=(node, i + 1, s)))
        processes[i].start()
    [b.join() for b in processes]

    # Display a summary. Which nodes were successful and which nodes were not.
    for i, node in enumerate(node_uris):
        print('Node ' + str(i) + ' [' + node + ']:')
        print('Successful' if (i + 1) in successful_nodes else 'Failure')
