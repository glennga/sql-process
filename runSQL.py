# coding=utf-8
"""
Executes a **simple** single SQL statement on a cluster of computers. This SQL statement is
specified in 'sqlfile', and access information about each computer in the cluster is specified
in 'clustercfg'.

Usage: python runSQL.py [clustercfg] [sqlfile]

Error: 2 - Incorrect number of arguments.
       3 - There exists an error with the clustercfg file.
       4 - There exists an error with the SQL file.
       5 - Catalog could not be reached or the table was not found.
       6 - Table not found in SQL file.
"""

import ctypes
import pickle
import socket
import sys
from multiprocessing import Process, Array
from subprocess import call

from catalog import RemoteCatalog
from dissect import SQLFile, ClusterCFG


def collect_socket(k):
    """ Given the socket and the name of the node, grab the operation and the tuple results. This
    is to be executed after sending out an execute command 'E'.

    :param k: Open to socket to wait for a response on.
    :return: Error message if a list is not received. Otherwise, the operation and tuple as a list.
    """
    response = k.recv(4096)
    try:
        r = pickle.loads(response) if response != b'' else 'No response received.'

        if isinstance(r, str) and r == '':
            # An empty string here means we didn't receive a response.
            return 'Failed to receive response.'
        elif isinstance(r, str):
            # A string entry here means that an error occurred. Return the resultant.
            return r
        elif hasattr(r, '__iter__') and len(r) >= 2:
            # The response is correctly formatted. Return the operation and the resultant.
            operation, resultant = r
        else:
            # Resultant is a single-element list.
            return 'Invalid response. Response returned is a singular element.'
    except EOFError as e:
        return str(e)

    return operation, resultant


def execute_sql(node_uri, n, s, s_nodes):
    """ Given the URI of a node from the clustercfg file and the SQL to execute, send the SQL to
    the appropriate node. Print any return messages or errors that occur.

    :param node_uri: Node URI from the clustercfg file (right side of key-value pair).
    :param n: Node number that this operation is working on.
    :param s: SQL statement to execute on the node.
    :return: None.
    """
    host, sock = node_uri.split(':', 1)[0], ''
    port, f = node_uri.split(':', 1)[1].split('/', 1)

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
        if isinstance(resultant, str):
            print('Node ' + str(n) + ': ' + resultant)
        else:
            print('Node ' + str(n) + ': [' + ''.join([str(x) + ', ' for x in resultant]) + ']')

    # Repeat. Operation code 'ES' marks the start of a message, and 'EZ' marks the end.
    while operation != 'EZ':
        r = collect_socket(sock)
        if isinstance(r, str):
            print('Error on Node ' + str(n) + ': ' + r), sock.close()
            return
        else:
            operation, resultant = r
            print('Node ' + str(n) + ': [' + ''.join([str(x) + ', ' for x in resultant]) + ']')

    # End is reached. The operation was successful.
    s_nodes[n - 1] = n
    sock.close()


if __name__ == '__main__':
    # Ensure that we have only two arguments.
    if len(sys.argv) != 3:
        print('Usage: python3 runSQL.py [clustercfg] [sqlfile]'), exit(2)

    # Parse both the clustercfg and sqlfile. Ensure that both are properly formatted.
    c_r, s_r = ClusterCFG.catalog_uri(sys.argv[1]), SQLFile.as_string(sys.argv[2])
    if isinstance(c_r, str):
        print('Error: ' + c_r), exit(3)
    elif isinstance(s_r, str):
        print('Error: ' + s_r), exit(4)
    catalog_uri, s = c_r[0], s_r[0]

    # If the given SQL is a DDL, call 'runDDL' instead.
    if SQLFile.is_ddl(s):
        call(['python', 'runDDL.py', sys.argv[1], sys.argv[2]]), exit(0)

    # Determine the working table.
    t_table = SQLFile.table(s)
    if not t_table:
        print('Error: Table could not be found in \'sqlfile\'.'), exit(6)

    # Collect our node URIs. Do not proceed if the catalog node cannot be reached.
    node_uris = RemoteCatalog.return_node_uris(catalog_uri, t_table)
    if isinstance(node_uris, str):
        print('Catalog Error: ' + node_uris), exit(5)

    # Create the shared memory segment (successfully completed nodes).
    s_nodes = Array(ctypes.c_int, len(node_uris))

    # For every node in the cluster, execute the given statement and display any errors.
    processes = []
    for i, node in enumerate(node_uris):
        processes.append(Process(target=execute_sql, args=(node, i + 1, s, s_nodes)))
        processes[i].start()
    [b.join() for b in processes]

    # Display a summary: which nodes were successful and which nodes were not.
    print('\nSummary: ')
    for i, node in enumerate(node_uris):
        sp = 'Node ' + str(i + 1) + '[' + node + ']: '
        print(sp + ('Successful' if (i + 1) in s_nodes else 'Failed'))