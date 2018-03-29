# coding=utf-8
"""
Executes a single DDL statement on a cluster of computers. This DDL statement is specified in
'ddlfile', and access information about each computer in the cluster is specified in 'clustercfg'.

Usage: python runDDL.py [clustercfg] [ddlfile]

Error: 2 - Incorrect number of arguments.
       3 - [clustercfg] is not correctly formatted.
       4 - [ddlfile] is not correctly formatted.
       5 - Unable to connect to the catalog node.
       6 - Node entries in the cluster configuration file are improperly formatted.
       7 - Unable to connect to the catalog node (most likely lost connection here).
"""

import sys

from lib.catalog import RemoteCatalog
from lib.dissect import ClusterCFG, SQLFile
from lib.error import ErrorHandle
from lib.network import Network
from lib.parallel import Parallel

# Used to store the node IDs of each **successful** execution.
successful_nodes = []


def execute_ddl(node_uri, name, s_n):
    """ Given node information from the clustercfg file and the DDL to execute, send the DDL to a
    node for it to be executed. Print any errors that pop up.

    :param node_uri: Node information from the clustercfg file (right side of key-value pair).
    :param name: Name of the node (left side of key-value pair).
    :param s_n: DDL statement to run on the node.
    :return None.
    """
    [host, port, f], n = ClusterCFG.parse_uri(node_uri), name.split('.', 1)[0].split('node')[1]
    handler = lambda e: ErrorHandle.fatal_handler('[Node ' + n + ']: ' + str(e))

    # Create our socket.
    sock = Network.open_client(host, port, handler)

    # Pickle our command list ('E', filename, and DDL), and send our message.
    Network.write(sock, ['E', f, s_n])

    # Wait for a response to be sent back, and return the response.
    a = Network.read(sock, handler)
    ErrorHandle.act_upon_error(a, handler)

    # Append the success to the successful nodes list.
    print('Successful Execution on Node: ' + n), successful_nodes.append(int(n))
    sock.close()


if __name__ == '__main__':
    # Ensure that we have only two arguments.
    if len(sys.argv) != 3:
        ErrorHandle.fatal_handler('Usage: python3 runDDL.py [clustercfg] [ddlfile]')

    # Collect the catalog node URI and the DDL file.
    catalog_uri = ErrorHandle.act_upon_error(ClusterCFG.catalog_uri(sys.argv[1]),
                                             ErrorHandle.fatal_handler, True)
    s = ErrorHandle.act_upon_error(SQLFile.as_string(sys.argv[2]),
                                   ErrorHandle.fatal_handler, True)

    # Test our connection to the catalog. Do not execute if logging cannot occur.
    if not RemoteCatalog.ping(catalog_uri):
        ErrorHandle.fatal_handler('Cannot connect to the catalog. No statement executed.')

    # Collect our node URIs. Do not proceed if these are not valid.
    node_uris = ErrorHandle.act_upon_error(ClusterCFG.node_uris(sys.argv[1]),
                                           ErrorHandle.fatal_handler, True)

    # For every node in the cluster dictionary, execute the given statement and display any errors.
    Parallel.execute_n(node_uris, execute_ddl, lambda i, b: (b, 'node' + str(i + 1), s))

    # Update the metadata on the catalog node.
    r = RemoteCatalog.record_ddl(catalog_uri, [e for i, e in enumerate(node_uris)
                                               if (i + 1) in successful_nodes], s)
    ErrorHandle.act_upon_error(r, ErrorHandle.fatal_handler, False)

    # Display a summary: which nodes were successful and which nodes were not.
    print('\nSummary: ')
    for i, node in enumerate(node_uris):
        sp = 'Node ' + str(i + 1) + '[' + node + ']: '
        print(sp + ('Successful' if (i + 1) in successful_nodes else 'Failed'))
