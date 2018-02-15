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

import catalog
import dissect

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
    host, sock, n = node.split(':')[0], '', name.split('.')[0].split('node')[1]
    port, f = (node.split(':')[1]).split('/')

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
    r = pickle.loads(response) if response != '' else 'Failed to receive response.'

    # Display the error, and append to the success IDs list if appropriate.
    if hasattr(r, '__iter__') and r[0] == 'EZ' and r[1] == 'Success':
        print('Successful Execution on Node: ' + n), successful_nodes.append(int(n))
    else:
        print('Error on Node ' + n + ': ' + r)

    sock.close()


if __name__ == '__main__':
    # Ensure that we have only two arguments.
    if len(sys.argv) != 3:
        print('Usage: python3 runDDL.py [clustercfg] [ddlfile]')
        exit(2)

    # TODO: Recreate clustercfg_ddl here.
    # Parse both the clustercfg and ddlfile. Ensure that both are properly formatted.
    c, s = dissect.clustercfg_ddl(sys.argv[1]), dissect.sqlfile(sys.argv[2])
    if isinstance(c, str) or not c:
        print(c), exit(3)
    elif not s:
        print('There exists no terminating semicolon in "ddlfile". No statement executed.'), exit(4)

    # Test our connection to the catalog. Do not execute if logging cannot occur.
    if not catalog.is_connect(c[0]):
        print('Cannot connect to the catalog. No statement executed.'), exit(5)

    # For every node in the cluster dictionary, execute the given statement and display any errors.
    threads = []
    for i, node in enumerate(c[1]):
        threads.append(Thread(target=execute_ddl, args=(node, 'node' + str(i + 1), s)))
        threads[i].start()

    # Wait until threads are finished. Update the metadata on the catalog node.
    [b.join() for b in threads]
    if not catalog.log_remote([c[0], [e for i, e in enumerate(c[1])
                                      if (i + 1) in successful_nodes]], s):
        print('Logging to catalog failed. '), exit(6)
