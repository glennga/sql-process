# coding=utf-8
"""
Given a CSV of tuples and a configuration file of the partitioning, insert the tuples into the
cluster. An assumption is made that the CSV is valid, and does not contain any SQL injections (CSV
must arrive from a reputable source).

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
    host, sock_n = n_i.split(':', 1)[0], socket.socket()
    port, f_n = n_i.split(':', 1)[1].split('/', 1)

    try:
        # Attept to create a socket...
        sock_n.connect((host, int(port)))
    except OSError:
        # If this is not successful, then close the socket and return false.
        sock_n.close()
        return False

    # Otherwise, return the socket and the file.
    return sock_n, f_n


def send_insert(k, s_l, p_l, f_n):
    """ Construct the appropriate command list and send this over the given socket.

    :param k: Socket to send command list through.
    :param s_l: List of prepared SQL strings to execute on the node.
    :param p_l: List of parameters to attach to SQL strings when executing on the node.
    :param f_n: Database filename to record to.
    :return: String containing the error if an error occurred on the node. Otherwise, true.
    """
    # Send our command. Only perform for non-empty entries.
    for i, s, p in zip([x for x in range(len(s_l))], s_l, p_l):
        if len(s) != 0 or len(p) != 0:
            k.send(pickle.dumps(['YS' if i != (len(s_l) - 1) else 'YZ', f_n, s, p]))
            r_b = k.recv(4096)
            try:
                r_nb = pickle.loads(r_b) if r_b != b'' else 'Failed to receive response.'
            except EOFError as e_1:
                return str(e_1)

            # String response indicates an error. Return this.
            if isinstance(r_nb, str):
                return r_nb
            elif r_nb[0] == 'EY' and r_nb[1] == 'Success' and i == len(s_l) - 1:
                return True

        elif i == len(s_l) - 1:
            # We have reached the end of our list but have nothing to send.
            k.send(pickle.dumps(['YY']))
            r_b = k.recv(4096)
            try:
                r_nb = pickle.loads(r_b) if r_b != b'' else 'Failed to receive response.'
                if isinstance(r_nb, str):
                    return r_nb
            except EOFError as e_1:
                return str(e_1)

            # The operation above was successful. Return true.
            return True


def send_insert_selective(s_l, p_l, sock_f):
    """ Construct the appropriate command list for a list of SQL strings, sockets, & files,
    and send them to each node iff an insert is to actually occur (invalid SQL strings come from
    partitioned nodes that don't perform inserts).

    :param s_l: List of prepared SQL strings.
    :param p_l: List of parameters to attach the SQL strings.
    :param sock_f: List of lists of sockets (first element) and database filenames (second element).
    :return: False if there exists errors on any nodes. True otherwise.
    """
    is_error_free = True

    # Perform the insertion for each node in the cluster that has a valid insertion statement.
    for i, sock_f_i in enumerate(sock_f):
        r_nb = send_insert(sock_f_i[0], s_l[i], p_l[i], sock_f_i[1])
        if isinstance(r_nb, str):
            print('Error on Node ' + str(i) + ': ' + r_nb)
            is_error_free = False

    # Operation was successful, return true.
    return is_error_free


def nopart_load(n, c, r_dl, f_l):
    """ There exists no partitioning. We execute each insertion on every node in the cluster.
    Display any errors that occur.

    :param n: List of node URIs.
    :param c: Catalog node URI.
    :param r_dl: Dictionary of partitioning information.
    :param f_l: Name of the CSV file.
    :return: None.
    """
    # For each node in the node URIs, construct a socket.
    sock_f, is_error_free = list(map(lambda x: create_socket(x), n)), True
    if not all(sock_f):
        print('All nodes in cluster could not be reached.'), exit(7)

    # Read every line of the CSV.
    csv_l = []
    with open(f_l) as csv_f:
        [csv_l.append(x) for x in csv.reader(csv_f)]
    s_l, p_l = [[] for _ in csv_l], [[] for _ in csv_l]

    # Construct a list of insertion strings.
    for i, ell in enumerate(csv_l):
        if len(ell) != 0:
            s_l[i] = 'INSERT INTO ' + r_dl['tname'] + ' VALUES '
            s_l[i] += '(' + ''.join(['?, ' for _ in range(len(ell) - 1)]) + '?);'
            p_l[i] = ell

    # Perform the insertion for each node in the cluster. List must be flattened beforehand.
    for i, sock_f_i in enumerate(sock_f):
        r_nb = send_insert(sock_f_i[0], s_l, p_l, sock_f_i[1])

        if isinstance(r_nb, str):
            print('Error on Node ' + str(i) + ': ' + r_nb)
            is_error_free = False

    # Close all sockets.
    list(map(lambda x: x.close(), list(zip(*sock_f))[0]))
    print('Insertion was ' + ('successful.' if is_error_free else 'not successful.'))

    # Update the partition information in the catalog node.
    response_p = RemoteCatalog.update_partition(c, r_dl, len(n))
    if isinstance(response_p, str):
        print('Catalog Error: ' + response_p)
    else:
        print('Catalog node has been updated with the partitions.')


def hashpart_load(n, c, r_dl, f_l):
    """ There exists a hash partition on the cluster. Determine which data gets inserted into
    where appropriately. The hash function: X = ( partcol mod partparam1 ) + 1 is applied.

    :param n: List of node URIs.
    :param c: Catalog node URI.
    :param r_dl: Dictionary of partitioning information.
    :param f_l: Name of the CSV file.
    :return: None.
    """
    try:
        p = int(r_dl['param1'])
    except ValueError:
        print('\'partition.param1\' is not an integer.')
        return

    # For each node in the node URIs, construct a socket.
    h = lambda b: (b % p) + 1
    sock_f = list(map(lambda x: create_socket(x), n))
    if not all(sock_f):
        print('All nodes in cluster could not be reached.'), exit(7)

    # Determine the index of the partitioned column.
    try:
        y = r_dl['col_s'].index(r_dl['partcol'])
    except (ValueError, KeyError):
        print('Error: `partition.column` does not exist in table, or is incorrectly formatted.')
        return

    # Read every line of the CSV.
    csv_l = []
    with open(f_l) as csv_f:
        [csv_l.append(x) for x in csv.reader(csv_f)]
    s_l, p_l = [[[] for _ in csv_l] for q in n], [[[] for _ in csv_l] for q in n]

    # Construct a list of insertion strings.
    for i, ell in enumerate(csv_l):
        if len(ell) != 0:
            h_ell = h(int(ell[y])) - 1
            s_l[h_ell][i] = 'INSERT INTO ' + r_dl['tname'] + ' VALUES '
            s_l[h_ell][i] += '(' + ''.join(['?, ' for _ in range(len(ell) - 1)]) + '?);'
            p_l[h_ell][i] = ell

    # Insert the data into their respective nodes.
    b = send_insert_selective(s_l, p_l, sock_f)
    print('Insertion was ' + ('successful.' if b else 'not successful.'))
    list(map(lambda x: x.close(), list(zip(*sock_f))[0]))

    # Update the partition information in the catalog node.
    response_p = RemoteCatalog.update_partition(c, r_dl, len(n))
    if isinstance(response_p, str):
        print('Catalog Error: ' + response_p)
    else:
        print('Catalog node has been updated with the partitions.')


def rangepart_load(n, c, r_dl, f_l):
    """ There exists a range partitioning on the cluster. Determine which data gets inserted into
    where appropriately. Each range is applied as such: partparam1 < partcol <= partparam2.

    :param n: List of node URIs.
    :param c: Catalog node URI.
    :param r_dl: Dictionary of partitioning information.
    :param f_l: Name of the CSV file.
    :return: None.
    """
    # For each node in the node URIs, construct a socket.
    r_bounds = list(zip(r_dl['param1'], r_dl['param2']))
    sock_f = list(map(lambda x: create_socket(x), n))
    if not all(sock_f):
        print('All nodes in cluster could not be reached.'), exit(7)

    # Ranges must be ordered from lower to higher.
    if any(list(map(lambda a: a[0] > a[1], r_bounds))):
        print('\'param1\' must be less than \'param2\'.')
        return

    # Determine the index of the partitioned column.
    try:
        y = r_dl['col_s'].index(r_dl['partcol'])
    except (ValueError, KeyError):
        print('Error: `partition.column` does not exist in table, or is incorrectly formatted.')
        return

    # Read every line of the CSV.
    csv_l = []
    with open(f_l) as csv_f:
        [csv_l.append(x) for x in csv.reader(csv_f)]
    s_l, p_l = [[[] for _ in csv_l] for q in n], [[[] for _ in csv_l] for q in n]

    # Construct a list of insertion strings.
    for i, ell in enumerate(csv_l):
        if len(ell) != 0:
            for j, bounds in enumerate(r_bounds):
                if bounds[0] < int(ell[y]) <= bounds[1]:
                    s_l[j][i] = 'INSERT INTO ' + r_dl['tname'] + ' VALUES '
                    s_l[j][i] += '(' + ''.join(['?, ' for _ in range(len(ell) - 1)]) + '?);'
                    p_l[j][i] = ell

    # Insert the data into their respective nodes.
    print('Insertion was ' + ('successful.' if
                              send_insert_selective(s_l, p_l, sock_f) else 'not successful.'))
    list(map(lambda x: x.close(), list(zip(*sock_f))[0]))

    # Update the partition information in the catalog node.
    response_p = RemoteCatalog.update_partition(c, r_dl, len(n))
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
    r_s, f, sock = create_socket(node_uris[0]), '', ''
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
