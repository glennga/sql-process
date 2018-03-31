# coding=utf-8
"""
Execute a single SQL statement that does not involve a join, on a cluster of computers.

Usage: python runSSQL.py [clustercfg] [sqlfile]
"""

import sys, os

from lib.catalog import RemoteCatalog
from lib.dissect import SQLFile, ClusterCFG
from lib.error import ErrorHandle
from lib.network import Network
from lib.parallel import Parallel

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
    handler = lambda e: ErrorHandle.fatal_handler('[Node ' + n + ']: ' + str(e))

    # Create our socket, and use this to seed random.
    sock = Network.open_client(host, port, handler)

    # Pickle our command list ('E', filename, and SQL), and send our message.
    Network.write(sock, ['E', f, s_n])

    # Wait for a response to be sent back, and print the response.
    a = Network.read(sock, handler)
    operation, resultant = ErrorHandle.act_upon_error(a, handler, True)
    if resultant != 'No tuples found.':
        print('Node ' + str(n) + ': | |' + ''.join([str(x) + ' | ' for x in resultant]) + '|')
    else:
        print('Node ' + str(n) + ': | | No tuples found. | |')

    # Repeat. Operation code 'ES' marks the start of a message, and 'EZ' marks the end.
    while operation != 'EZ':
        a = Network.read(sock, handler)
        operation, resultant = ErrorHandle.act_upon_error(a, handler, True)
        if resultant != 'No tuples found.':
            print('Node ' + str(n) + ': | |' + ''.join([str(x) + ' | ' for x in resultant]) + '|')
        else:
            print('Node ' + str(n) + ': | | No tuples found. | |')

    # End is reached. The operation was successful.
    successful_nodes.append(int(n))
    sock.close()


if __name__ == '__main__':
    # Ensure that we have only two arguments.
    if len(sys.argv) != 3:
        ErrorHandle.fatal_handler('Usage: python3.6 runSSQL.py [clustercfg] [ddlfile]')

    # Collect the catalog node URI and the SQL file.
    catalog_uri = ErrorHandle.act_upon_error(ClusterCFG.catalog_uri(sys.argv[1]),
                                             ErrorHandle.fatal_handler, True)
    s = ErrorHandle.act_upon_error(SQLFile.as_string(sys.argv[2]), ErrorHandle.fatal_handler, True)

    # Determine the working table.
    t_table = ErrorHandle.act_upon_error(SQLFile.table(s), ErrorHandle.fatal_handler, True)

    # Collect our node URIs. Do not proceed if the catalog node cannot be reached.
    r = RemoteCatalog.return_node_uris(catalog_uri, t_table)
    node_uris = ErrorHandle.act_upon_error(r, ErrorHandle.fatal_handler, True)

    # For every node in the cluster, execute the given statement and display any errors.
    Parallel.execute_n(node_uris, execute_sql, lambda i, b: (b, i + 1, s))

    # Display a summary: which nodes were successful and which nodes were not.
    print('\nSummary: ')
    for i, node in enumerate(node_uris):
        sp = 'Node ' + str(i + 1) + '[' + node + ']: '
        print(sp + ('Successful' if (i + 1) in successful_nodes else 'Failed'))
