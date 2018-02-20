# coding=utf-8
"""
TODO: Finish this description.
TODO: We assume that the CSV is valid.

Usage: python loadCSV.py [clustercfg] [csv]

Error: TODO: Finish the error codes here.
"""

import csv
import pickle
import socket
import sys

from catalog import RemoteCatalog
from dissect import ClusterCFG


def create_socket(n_i):
    """ TODO: Finish the documentation here.

    :param n_i:
    :return:
    """
    host, sock = n_i.split(':', 1)[0], socket.socket()
    port, f = n_i.split(':', 1)[1].split('/', 1)

    sock.connect((host, int(port)))
    return sock, f


def send_insert(k, s, f, ell):
    """ TODO: Finish this description.

    :param k:
    :param s:
    :param f:
    :param ell:
    :return:
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
    """

    :param s_l:
    :param sock_f:
    :return:
    """

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
                list(map(lambda x: x.close(), list(zip(*sock_f))[0]))
                return False

    # Operation was successful, return true.
    return True


def nopart_load(n, c, r_d, f):
    """ TODO: Finish this description.
    # If there exists no partition, we execute each insert to every node in the cluster.

    :param n:
    :param c:
    :param r_d:
    :param f:
    :return:
    """
    # For each node in the node URIs, construct a socket.
    sock_f = list(map(lambda x: create_socket(x), n))

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
            list(map(lambda x: x.close(), list(zip(*sock_f))[0]))
            return

    # Close all sockets.
    list(map(lambda x: x.close(), list(zip(*sock_f))[0]))
    print('Insertion was successful.')




def hashpart_load(n, c, r_d, f):
    """ TODO: Finish this description.
    TODO: Note assumption on param1 being an integer.

    :param n:
    :param c:
    :param r_d:
    :param f:
    :return:
    """
    # For each node in the node URIs, construct a socket.
    h, s_l = lambda b: (b % int(r_d['param1'])) + 1, [[[], []] for _ in n]
    sock_f = list(map(lambda x: create_socket(x), n))
    [s_t[0].append('INSERT INTO ' + r_d['tname'] + ' VALUES ') for s_t in s_l]

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
    if send_insert_selective(s_l, sock_f):
        print('Insertion was successful.')

    # Update the partition information in the catalog node.
    response_p = RemoteCatalog.update_partition(c, r_d, len(n))
    if isinstance(response_p, str):
        print('Catalog Error: ' + response_p)
    else:
        print('Catalog node has been updated with the partitions.')

    # Close all sockets.
    list(map(lambda x: x.close(), list(zip(*sock_f))[0]))


def rangepart_load(n, c, r_d, f):
    """ TODO: Finish this description.
    TODO: Note that the range is: partparam1 < partcol <= partparam2.

    :param n:
    :param c:
    :param r_d:
    :param f:
    :return:
    """
    # For each node in the node URIs, construct a socket.
    r_bounds, s_l = list(zip(r_d['param1'], r_d['param2'])), [[[], []] for _ in n]
    sock_f = list(map(lambda x: create_socket(x), n))
    [s_t[0].append('INSERT INTO ' + r_d['tname'] + ' VALUES ') for s_t in s_l]

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
    if send_insert_selective(s_l, sock_f):
        print('Insertion was successful.')
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
        print('\'numnodes\' specified does not match number of nodes in catalog node'), exit(4)

    # Extract the columns from the table, using the first node.
    sock, f = create_socket(node_uris[0])
    sock.send(pickle.dumps(['P', f, r_d['tname']]))
    response = sock.recv(4096)
    try:
        r_d.update({'col_s': pickle.loads(response)[1]}) if response != b'' \
            else print('Error: Failed to receive response.') and exit(6)
    except EOFError as e:
        print('Error: ' + str(e))

    # Determine the partitioning. Use the appropriate load function when determined.
    [nopart_load, rangepart_load, hashpart_load][r_d['partmtd']] \
        (node_uris, catalog_uri, r_d, sys.argv[2])
