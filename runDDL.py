# coding=utf-8
"""
Executes a single DDL statement on a cluster of computers. This DDL statement is specified in
'ddlfile', and access information about each computer in the cluster is specified in 'clustercfg'.

Usage: python runDDL.py [clustercfg] [ddlfile]

Error: 2 - Incorrect number of arguments.
       3 - [clustercfg] is not correctly formatted.
       4 - [ddlfile] is not correctly formatted.
       5 - Unable to connect to the catalog node.
       6 - Catalog node was not updated.
"""

import pickle
import socket
import sys
from threading import Thread

from catalog import RemoteCatalog
from dissect import ClusterCFG, SQLFile

# Used to store the node IDs of each **successful** execution.
successful_nodes = []


def execute_ddl(node, name, s):
    """ Given node information from the clustercfg file and the DDL to execute, send the DDL to a
    node for it to be executed. Print any errors that pop up.

    :param node: Node information from the clustercfg file (right side of key-value pair).
    :param name: Name of the node (left side of key-value pair).
    :param s: DDL statement to run on the node.
    :return None.
    """
    host, sock, n = node.split(':', 1)[0], '', name.split('.', 1)[0].split('node')[1]
    port, f = (node.split(':', 1)[1]).split('/', 1)

    # Create our socket.
    try:
        sock = socket.socket()
        sock.connect((host, int(port)))
    except OSError as e:
        print('Error on Node ' + n + ': ' + str(e)), sock.close()
        return

    # Pickle our command list ('E', filename, and DDL), and send our message.
    sock.send(pickle.dumps(['E', f, s]))

    # Wait for a response to be sent back, and return the response.
    response = sock.recv(4096)
    r = pickle.loads(response) if response != b'' else 'Failed to receive response.'

    # Display the error, and append to the success IDs list if appropriate.
    if r[0] == 'EZ' and r[1] == 'Success':
        print('Successful Execution on Node: ' + n), successful_nodes.append(int(n))
    else:
        print('Error on Node ' + n + ': ' + r)

    sock.close()


if __name__ == '__main__':
    # Ensure that we have only two arguments.
    if len(sys.argv) != 3:
        print('Usage: python3 runDDL.py [clustercfg] [ddlfile]'), exit(2)

    # Collect the catalog node URI and the DDL file.
    c_r, s_r = ClusterCFG.catalog_uri(sys.argv[1]), SQLFile.as_string(sys.argv[2])
    if isinstance(c_r, str):
        print('Error: ' + c_r), exit(3)
    elif isinstance(s_r, str):
        print('Error: ' + s_r), exit(4)
    catalog_uri, s = c_r[0], s_r[0]

    # Test our connection to the catalog. Do not execute if logging cannot occur.
    if not RemoteCatalog.ping(catalog_uri):
        print('Error: Cannot connect to the catalog. No statement executed.'), exit(5)

    # Collect our node URIs. Do not proceed if these are not valid.
    node_uris = ClusterCFG.node_uris(sys.argv[1])
    if isinstance(node_uris, str):
        print('Error: ' + node_uris), exit(6)

    # For every node in the cluster dictionary, execute the given statement and display any errors.
    threads = []
    for i, node in enumerate(node_uris):
        threads.append(Thread(target=execute_ddl, args=(node, 'node' + str(i + 1), s)))
        threads[i].start()

    # Wait until threads are finished. Update the metadata on the catalog node.
    [b.join() for b in threads]
    r_c = RemoteCatalog.record_ddl(catalog_uri, [e for i, e in enumerate(node_uris)
                                                 if (i + 1) in successful_nodes], s)
    if isinstance(r_c, str):
        print('Catalog Error: ' + r_c), exit(7)
    elif len(successful_nodes) > 0:
        print('Catalog Node Update Successful.')
