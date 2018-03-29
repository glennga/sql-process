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
from threading import Thread

from lib.catalog import RemoteCatalog
from lib.dissect import ClusterCFG, SQLFile
from lib.error import ErrorHandle

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

    # Create our socket.
    sock = ErrorHandle.create_client_socket(host, port)
    if ErrorHandle.is_error(sock):
        print('Error: Node -' + n + ' ' + sock), sock.close()
        return

    # Pickle our command list ('E', filename, and DDL), and send our message.
    ErrorHandle.write_socket(sock, ['E', f, s_n])

    # Wait for a response to be sent back, and return the response.
    r = ErrorHandle.read_socket(sock)

    # Display the error, and append to the success IDs list if appropriate.
    if r[0] == 'EZ' and r[1] == 'Success':
        print('Successful Execution on Node: ' + n), successful_nodes.append(int(n))
    else:
        print('Error: Node - ' + n + ' ' + r)

    sock.close()


if __name__ == '__main__':
    # Ensure that we have only two arguments.
    if len(sys.argv) != 3:
        print('Usage: python3 runDDL.py [clustercfg] [ddlfile]'), exit(2)

    # Collect the catalog node URI and the DDL file.
    catalog_uri, s = ClusterCFG.catalog_uri(sys.argv[1]), SQLFile.as_string(sys.argv[2])
    if ErrorHandle.is_error(catalog_uri):
        print(catalog_uri), exit(3)
    elif ErrorHandle.is_error(s):
        print(s), exit(4)

    # Test our connection to the catalog. Do not execute if logging cannot occur.
    if not RemoteCatalog.ping(catalog_uri):
        print('Error: Cannot connect to the catalog. No statement executed.'), exit(5)

    # Collect our node URIs. Do not proceed if these are not valid.
    node_uris = ClusterCFG.node_uris(sys.argv[1])
    if ErrorHandle.is_error(node_uris):
        print(node_uris), exit(6)

    # For every node in the cluster dictionary, execute the given statement and display any errors.
    threads = []
    for i, node in enumerate(node_uris):
        threads.append(Thread(target=execute_ddl, args=(node, 'node' + str(i + 1), s)))
        threads[i].start()

    # Wait until threads are finished. Update the metadata on the catalog node.
    [b.join() for b in threads]
    r_c = RemoteCatalog.record_ddl(catalog_uri, [e for i, e in enumerate(node_uris)
                                                 if (i + 1) in successful_nodes], s)
    if ErrorHandle.is_error(r_c):
        print(r_c), exit(7)

    # Display a summary: which nodes were successful and which nodes were not.
    print('\nSummary: ')
    for i, node in enumerate(node_uris):
        sp = 'Node ' + str(i + 1) + '[' + node + ']: '
        print(sp + ('Successful' if (i + 1) in successful_nodes else 'Failed'))