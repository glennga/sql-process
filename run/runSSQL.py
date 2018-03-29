# coding=utf-8
"""
TODO: Fix this documentation, wrong clustercfg
Executes a **simple** single SQL statement on a cluster of computers. This SQL statement is
specified in 'sqlfile', and access information about each computer in the cluster is specified
in 'clustercfg'.

Usage: python runSSQL.py [clustercfg] [sqlfile]

Error: 2 - Incorrect number of arguments.
       3 - There exists an error with the clustercfg file.
       4 - There exists an error with the SQL file.
       5 - Catalog could not be reached or the table was not found.
       6 - Table not found in SQL file.
"""

import random
import sys
from threading import Thread

from lib.error import ErrorHandle
from lib.catalog import RemoteCatalog
from lib.dissect import SQLFile, ClusterCFG

# Used to store the node IDs of each **successful** execution.
successful_nodes = []


def execute_sql(node_uri, n, s_n):
    """ Given the URI of a node from the clustercfg file and the SQL to execute, send the SQL to
    the appropriate node. Print any return messages or errors that occur.

    :param node_uri: Node URI from the clustercfg file (right side of key-value pair).
    :param n: Node number that this operation is working on.
    :param s_n: SQL statement to execute on the node.
    :return: None.
    """
    host, port, f = ClusterCFG.parse_uri(node_uri)

    # Create our socket, and use this to seed random.
    sock = ErrorHandle.create_client_socket(host, port)
    random.seed(sock.getsockname())
    if ErrorHandle.is_error(sock):
        print('Error: Node ' + str(n) + '-' + sock)
        return

    # Pickle our command list ('E', filename, and SQL), and send our message.
    ErrorHandle.write_socket(sock, ['E', f, s_n])

    # Wait for a response.
    r = ErrorHandle.read_socket(sock)
    if ErrorHandle.is_error(r):
        print('Error: Node ' + str(n) + ' - ' + r), sock.close()
        return

    operation, resultant = r
    if isinstance(resultant, str):
        print('Node ' + str(n) + ': ' + resultant)
    else:
        print('Node ' + str(n) + ': [' + ''.join([str(x) + ', ' for x in resultant]) + ']')

    # Repeat. Operation code 'ES' marks the start of a message, and 'EZ' marks the end.
    while operation != 'EZ':
        r = ErrorHandle.read_socket(sock)
        if ErrorHandle.is_error(r):
            print('Error: Node ' + str(n) + ' - ' + r)
            return
        else:
            operation, resultant = r
            print('Node ' + str(n) + ': [' + ''.join([str(x) + ', ' for x in resultant]) + ']')

    # End is reached. The operation was successful.
    successful_nodes.append(int(n))
    sock.close()


if __name__ == '__main__':
    # Ensure that we have only two arguments.
    if len(sys.argv) != 3:
        print('Usage: python3 runSSQL.py [clustercfg] [sqlfile]'), exit(2)

    # Parse both the clustercfg and sqlfile. Ensure that both are properly formatted.
    catalog_uri, s = ClusterCFG.catalog_uri(sys.argv[1]), SQLFile.as_string(sys.argv[2])
    if ErrorHandle.is_error(catalog_uri):
        print(catalog_uri), exit(3)
    elif ErrorHandle.is_error(s):
        print(s), exit(4)

    # Determine the working table.
    t_table = SQLFile.table(s)
    if ErrorHandle.is_error(t_table):
        print(t_table), exit(6)

    # Collect our node URIs. Do not proceed if the catalog node cannot be reached.
    node_uris = RemoteCatalog.return_node_uris(catalog_uri, t_table)
    if ErrorHandle.is_error(node_uris):
        print(node_uris), exit(5)

    # For every node in the cluster, execute the given statement and display any errors.
    threads = []
    for i, node in enumerate(node_uris):
        threads.append(Thread(target=execute_sql, args=(node, i + 1, s)))
        threads[i].start()
    [b.join() for b in threads]

    # Display a summary: which nodes were successful and which nodes were not.
    print('\nSummary: ')
    for i, node in enumerate(node_uris):
        sp = 'Node ' + str(i + 1) + '[' + node + ']: '
        print(sp + ('Successful' if (i + 1) in successful_nodes else 'Failed'))
