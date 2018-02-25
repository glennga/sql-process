# coding=utf-8
"""
Given a CSV of tuples and a configuration file of the partitioning, insert the tuples into the
cluster. An assumption is made that the CSV is valid (CSV must arrive from a reputable source).

Usage: python loadCSV.py [clustercfg] [csv]

Error: 2 - Incorrect number of arguments.
       3 - The 'clustercfg' file is not properly formatted.
       4 - The number of nodes specified in 'clustercfg' and the catalog node do not match.
       5 - The node URIs from the catalog node could not be collected.
       6 - The first node in the cluster could not be reached.
       7 - There exists a node in the cluster that could not be reached (not just the first).
"""

import csv
import pickle
import socket
import sys

from catalog import RemoteCatalog
from dissect import ClusterCFG


def create_socket(n_i):
    """ Given a node URI, attempt to create a socket.

    :param n_i: Node URI to create socket with.
    :return: False if the socket could not be created. The appropriate socket otherwise.
    """
    host, sock = n_i.split(':', 1)[0], socket.socket()
    port, f = n_i.split(':', 1)[1].split('/', 1)

    try:
        # Attept to create a socket...
        sock.connect((host, int(port)))
    except OSError:
        # If this is not successful, then close the socket and return false.
        sock.close()
        return False

    # Otherwise, return the socket and the file.
    return sock, f


def send_insert(k, s, f, ell):
    """ Construct the appropriate command list and send this over the given socket.

    :param k: Socket to send command list through.
    :param s: To-be-prepared SQL string to execute on the node.
    :param f: Database filename to record to.
    :param ell: Tuples to attach to SQL string.
    :return: String containing the error if an error occurred on the node. Otherwise, true.
    """
    # Send our command.
    k.send(pickle.dumps(['E', f, s, ell]))

    # Receive our response.
    response = k.recv(4096)
    try:
        r = pickle.loads(response) if response != b'' else 'Failed to receive response.'
    except EOFError as e:
        return str(e)

    # String response indicates an error. Return this.
    if isinstance(r, str):
        return r
    elif r[0] == 'EZ' and r[1] == 'Success':
        return True


def send_insert_selective(s_l, sock_f):
    """ Construct the appropriate command list for a list of SQL strings, sockets, & files,
    and send them to each node iff an insert is to actually occur (invalid SQL strings come from
    partitioned nodes that don't perform inserts).

    :param s_l: List of to-be-prepared SQL strings.
    :param sock_f: List of lists of sockets (first element) and database filenames (second element).
    :return: False if there exists errors on any nodes. True otherwise.
    """
    is_error_free = True

    # Finalize the insertion string. Remove if the current node does not insert anything.
    removed = []
    for i, s_t in enumerate(s_l):
        if s_t[0][0] == 'INSERT INTO ' + r_d['tname'] + ' VALUES ':
            removed.append(i)
        else:
            s_t[0] = s_t[0][0][:-1]
            s_t[0] += ';'

    # Perform the insertion for each node in the cluster that has a valid insertion statement.
    for i, sock_f_i in enumerate(sock_f):
        if i not in removed:
            # List must be flattened beforehand.
            response = send_insert(sock_f_i[0], s_l[i][0], sock_f_i[1],
                                   [x for sublist in s_l[i][1] for x in sublist])
            if isinstance(response, str):
                print('Error on Node ' + str(i) + ': ' + response)
                is_error_free = False

    # Operation was successful, return true.
    return is_error_free


def nopart_load(n, c, r_d, f):
    """ There exists no partitioning. We execute each insertion on every node in the cluster.
    Display any errors that occur.

    :param n: List of node URIs.
    :param c: Catalog node URI.
    :param r_d: Dictionary of partitioning information.
    :param f: Name of the CSV file.
    :return: None.
    """
    # For each node in the node URIs, construct a socket.
    sock_f, is_error_free = list(map(lambda x: create_socket(x), n)), True
    if not all(sock_f):
        print('All nodes in cluster could not be reached.'), exit(7)

    # Read every line of the CSV.
    csv_l = []
    with open(f) as csv_f:
        [csv_l.append(x) for x in csv.reader(csv_f)]

    # Construct the insertion string.
    s = 'INSERT INTO ' + r_d['tname'] + ' VALUES '
    for ell in csv_l:
        if len(ell) != 0:
            s += '('
            s += ''.join(['?,' for _ in range(len(ell) - 1)])
            s += '?),'
    s = s[:-1]
    s += ';'

    # Perform the insertion for each node in the cluster. List must be flattened beforehand.
    for i, sock_f_i in enumerate(sock_f):
        response = send_insert(sock_f_i[0], s, sock_f_i[1],
                               [x for sublist in csv_l for x in sublist])

        if isinstance(response, str):
            print('Error on Node ' + str(i) + ': ' + response)
            is_error_free = False

    # Close all sockets.
    list(map(lambda x: x.close(), list(zip(*sock_f))[0]))
    print('Insertion was ' + 'successful.' if is_error_free else 'not successful.')


def hashpart_load(n, c, r_d, f):
    """ There exists a hash partition on the cluster. Determine which data gets inserted into
    where appropriately. The hash function: X = ( partcol mod partparam1 ) + 1 is applied.

    :param n: List of node URIs.
    :param c: Catalog node URI.
    :param r_d: Dictionary of partitioning information.
    :param f: Name of the CSV file.
    :return: None.
    """
    try:
        p = int(r_d['param1'])
    except ValueError:
        print('\'partition.param1\' is not an integer.')
        return

    # For each node in the node URIs, construct a socket.
    h, s_l = lambda b: (b % p) + 1, [[[], []] for _ in n]
    sock_f = list(map(lambda x: create_socket(x), n))
    [s_t[0].append('INSERT INTO ' + r_d['tname'] + ' VALUES ') for s_t in s_l]
    if not all(sock_f):
        print('All nodes in cluster could not be reached.'), exit(7)

    # Determine the index of the partitioned column.
    try:
        y = r_d['col_s'].index(r_d['partcol'])
    except (ValueError, KeyError):
        print('Error: `partition.column` does not exist in table, or is incorrectly formatted.')
        return

    # Read every line of the CSV.
    with open(f) as csv_f:
        for line in csv.reader(csv_f):
            s_l[h(int(line[y])) - 1][0][0] += '('
            s_l[h(int(line[y])) - 1][0][0] += ''.join(['?,' for _ in range(len(line) - 1)])
            s_l[h(int(line[y])) - 1][0][0] += '?),'
            s_l[h(int(line[y])) - 1][1].append(line)

    # Insert the data into their respective nodes.
    print('Insertion was ' + 'successful.' if
    send_insert_selective(s_l, sock_f) else 'not successful.')
    list(map(lambda x: x.close(), list(zip(*sock_f))[0]))

    # Update the partition information in the catalog node.
    response_p = RemoteCatalog.update_partition(c, r_d, len(n))
    if isinstance(response_p, str):
        print('Catalog Error: ' + response_p)
    else:
        print('Catalog node has been updated with the partitions.')


def rangepart_load(n, c, r_d, f):
    """ There exists a range partitioning on the cluster. Determine which data gets inserted into
    where appropriately. Each range is applied as such: partparam1 < partcol <= partparam2.

    :param n: List of node URIs.
    :param c: Catalog node URI.
    :param r_d: Dictionary of partitioning information.
    :param f: Name of the CSV file.
    :return: None.
    """
    # For each node in the node URIs, construct a socket.
    r_bounds, s_l = list(zip(r_d['param1'], r_d['param2'])), [[[], []] for _ in n]
    sock_f = list(map(lambda x: create_socket(x), n))
    [s_t[0].append('INSERT INTO ' + r_d['tname'] + ' VALUES ') for s_t in s_l]
    if not all(sock_f):
        print('All nodes in cluster could not be reached.'), exit(7)

    # Ranges must be ordered from lower to higher.
    if any(list(map(lambda a: a[0] > a[1], r_bounds))):
        print('\'param1\' must be less than \'param2\'.')
        return

    # Determine the index of the partitioned column.
    try:
        y = r_d['col_s'].index(r_d['partcol'])
    except (ValueError, KeyError):
        print('Error: `partition.column` does not exist in table, or is incorrectly formatted.')
        return

    # Read every line of the CSV.
    with open(f) as csv_f:
        for line in csv.reader(csv_f):
            for i, bounds in enumerate(r_bounds):
                if bounds[0] < int(line[y]) <= bounds[1]:
                    s_l[i][0][0] += '('
                    s_l[i][0][0] += ''.join(['?,' for _ in range(len(line) - 1)])
                    s_l[i][0][0] += '?),'
                    s_l[i][1].append(line)

    # Insert the data into their respective nodes.
    print('Insertion was ' + 'successful.' if
    send_insert_selective(s_l, sock_f) else 'not successful.')
    list(map(lambda x: x.close(), list(zip(*sock_f))[0]))

    # Update the partition information in the catalog node.
    response_p = RemoteCatalog.update_partition(c, r_d, len(n))
    if isinstance(response_p, str):
        print('Catalog Error: ' + response_p)
    else:
        print('Catalog node has been updated with the partitions.')


if __name__ == '__main__':
    # Ensure that we only have 2 arguments.
    if len(sys.argv) != 3:
        print('Usage: python3 loadCSV.py [clustercfg] [csv]'), exit(2)

    # Dissect the given clustercfg for partitioning and catalog information.
    r = ClusterCFG.load(sys.argv[1])
    if isinstance(r, str):
        print('Error: ' + r), exit(3)
    catalog_uri, r_d, numnodes = r

    # Collect our node URIs. Do not proceed if the catalog node cannot be reached.
    node_uris = RemoteCatalog.return_node_uris(catalog_uri, r_d['tname'])
    if isinstance(node_uris, str):
        print('Catalog Error: ' + node_uris), exit(5)

    # If the number of nodes here does not match the nodes in catalog, return with an error.
    if r_d['partmtd'] in [1, 2] and numnodes != len(node_uris):
        print('Incorrect number of nodes specified in \'clustercfg\'.'), exit(4)

    # Extract the columns from the table, using the first node.
    r_s = create_socket(node_uris[0])
    if r_s is not False:
        sock, f = r_s
    else:
        print('Could not connect to first node in the cluster.'), exit(6)

    sock.send(pickle.dumps(['P', f, r_d['tname']]))
    response = sock.recv(4096)
    try:
        r_d.update({'col_s': pickle.loads(response)[1]}) if response != b'' \
            else print('Error: Failed to receive response.') and exit(6)
    except EOFError as e:
        print('Error: ' + str(e)), exit(6)

    # Determine the partitioning. Use the appropriate load function when determined.
    [nopart_load, rangepart_load, hashpart_load][r_d['partmtd']] \
        (node_uris, catalog_uri, r_d, sys.argv[2])
