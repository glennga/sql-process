# coding=utf-8
"""
Partitions and loads a CSV of tuples into a table of a cluster of computers. An assumption is made
that the CSV is valid (normalized to the desired table), and does not contain any SQL injections
(CSV must arrive from a reputable source).

Usage: python runLCSV.py [clustercfg] [csv]
"""

import csv
import sys

from lib.catalog import RemoteCatalog
from lib.dissect import ClusterCFG
from lib.error import ErrorHandle
from lib.network import Network


def create_socket(n_i):
    """ Given a node URI, attempt to create a socket. Any errors here are assumed to be fatal.

    :param n_i: Node URI to create socket with.
    :return An open socket to the desired node.
    """
    host, port, f_n = ClusterCFG.parse_uri(n_i)
    return Network.open_client(host, port, ErrorHandle.fatal_handler), f_n


def send_insert(k, s_l, p_l, f_n, handler):
    """ Construct the appropriate command list and send this over the given socket.

    TODO: Fix the documentation below.
    :param k: Socket to send command list through.
    :param s_l: List of prepared SQL strings to execute on the node.
    :param p_l: List of parameters to attach to SQL strings when executing on the node.
    :param f_n: Database filename to record to.
    :param handler: Handler to call when reading from the socket fails.
    :return: String containing the error if an error occured while reading. Otherwise,
        an operation message with an empty list.
    """
    # Send our command. Only perform for non-empty entries.
    for i, s, p in zip([x for x in range(len(s_l))], s_l, p_l):
        if len(s) != 0 or len(p) != 0:
            if i == len(s_l) - 1:
                Network.write(k, ['YZ', f_n, s, p])
                return Network.read(k, handler)
            else:
                Network.write(k, ['YS', f_n, s, p])
                Network.read(k, handler)

        elif i == len(s_l) - 1:
            # We have reached the end of our list but have nothing to send.
            Network.write(k, ['YY'])
            return Network.read(k, handler)


def send_insert_selective(s_l, p_l, sock_f):
    """ Construct the appropriate command list for a list of SQL strings, sockets, & files,
    and send them to each node iff an insert is to actually occur (invalid SQL strings come from
    partitioned nodes that don't perform inserts).

    :param s_l: List of prepared SQL strings.
    :param p_l: List of parameters to attach the SQL strings.
    :param sock_f: List of lists of sockets (first element) and database filenames (second element).
    :return: None.
    """
    # Perform the insertion for each node in the cluster that has a valid insertion statement.
    for i, sock_f_i in enumerate(sock_f):
        handler = lambda e: list(map(lambda x: x.close(), list(zip(*sock_f))[0])) and \
                            ErrorHandle.fatal_handler('[Node ' + str(i) + ']: ' + str(e))

        # If at any point we encounter an error, we stop.
        ErrorHandle.act_upon_error(send_insert(sock_f_i[0], s_l[i], p_l[i], sock_f_i[1], handler),
                                   handler)


def read_csv(f_l):
    """ Read a CSV into a CSV reader, and return any errors that arise as a result. If there are
    no errors, return the CSV reader.

    :param f_l: CSV file to read.
    :return: A string containing the error if the file was not successfully read. Otherwise,
        the CSV reader.
    """

    def _read():
        """

        :return:
        """
        csv_l = []
        with open(f_l) as csv_f:
            [csv_l.append(x) for x in csv.reader(csv_f)]

        return csv_l

    return ErrorHandle.attempt_operation(_read, FileNotFoundError, ErrorHandle.default_handler,
                                         True)


def nopart_load(n, c, r_dl, f_l):
    """ There exists no partitioning. We execute each insertion on every node in the cluster.
    Display any errors that occur.

    :param n: List of node URIs.
    :param c: Catalog node URI.
    :param r_dl: Dictionary of partitioning information.
    :param f_l: Name of the CSV file.
    :return: None.
    """
    # Construct a list of insertion strings.
    csv_l = ErrorHandle.act_upon_error(read_csv(f_l), ErrorHandle.fatal_handler, True)
    s_l, p_l = [[] for _ in csv_l], [[] for _ in csv_l]
    for i, ell in enumerate(csv_l):
        if len(ell) != 0:
            s_l[i] = 'INSERT INTO ' + r_dl['tname'] + ' VALUES '
            s_l[i] += '(' + ''.join(['?, ' for _ in range(len(ell) - 1)]) + '?);'
            p_l[i] = ell

    # For each node in the node URIs, construct a socket.
    sock_f = list(map(lambda x: create_socket(x), n))
    close_sockets = lambda s_f: list(map(lambda x: x.close(), list(zip(*s_f))[0]))

    # Perform the insertion for each node in the cluster. List must be flattened beforehand.
    for i, sock_f_i in enumerate(sock_f):
        handler_i = lambda e: ErrorHandle.fatal_handler('[Node ' + str(i) + ']: ' + str(e))
        send_insert(sock_f_i[0], s_l, p_l, sock_f_i[1],
                    lambda e: handler_i(e) and close_sockets(sock_f))

    # Close all sockets.
    close_sockets(sock_f)
    print('Insertion was successful.')

    # Update the partition information in the catalog node.
    response_p = RemoteCatalog.update_partition(c, r_dl, len(n))
    if ErrorHandle.is_error(response_p):
        print(response_p)
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
    # 'param1' is the number of nodes.
    p = ErrorHandle.attempt_operation(lambda: int(r_dl['param1']), ValueError,
                                      ErrorHandle.default_handler, True)
    ErrorHandle.act_upon_error(p, ErrorHandle.fatal_handler)

    # Determine the index of the partitioned column.
    y = ErrorHandle.attempt_operation(lambda: r_dl['col_s'].index(r_dl['partcol']),
                                      (ValueError, KeyError), ErrorHandle.fatal_handler, True)
    ErrorHandle.act_upon_error(y, ErrorHandle.fatal_handler)

    # Construct a list of insertion strings.
    h = lambda b: (b % p) + 1
    csv_l = ErrorHandle.act_upon_error(read_csv(f_l), ErrorHandle.fatal_handler, True)
    s_l, p_l = [[[] for _ in csv_l] for q in n], [[[] for _ in csv_l] for q in n]
    for i, ell in enumerate(csv_l):
        if len(ell) != 0:
            h_ell = h(int(ell[y])) - 1
            s_l[h_ell][i] = 'INSERT INTO ' + r_dl['tname'] + ' VALUES '
            s_l[h_ell][i] += '(' + ''.join(['?, ' for _ in range(len(ell) - 1)]) + '?);'
            p_l[h_ell][i] = ell

    # For each node in the node URIs, construct a socket.
    sock_f = list(map(lambda x: create_socket(x), n))
    close_sockets = lambda s_f: list(map(lambda x: x.close(), list(zip(*s_f))[0]))

    # Insert the data into their respective nodes.
    send_insert_selective(s_l, p_l, sock_f)
    print('Insertion was successful.'), close_sockets(sock_f)

    # Update the partition information in the catalog node.
    response_p = RemoteCatalog.update_partition(c, r_dl, len(n))
    if ErrorHandle.is_error(response_p):
        print(response_p)
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
    # Ranges must be ordered from lower to higher.
    r_bounds = list(zip(r_dl['param1'], r_dl['param2']))
    if any(list(map(lambda a: a[0] > a[1], r_bounds))):
        ErrorHandle.fatal_handler('\'param1\' must be less than \'param2\'.')

    # Determine the index of the partitioned column.
    y = ErrorHandle.attempt_operation(lambda: r_dl['col_s'].index(r_dl['partcol']),
                                      (ValueError, KeyError), ErrorHandle.fatal_handler, True)

    # Construct a list of insertion strings.
    csv_l = ErrorHandle.act_upon_error(read_csv(f_l), ErrorHandle.fatal_handler, True)
    s_l, p_l = [[[] for _ in csv_l] for q in n], [[[] for _ in csv_l] for q in n]
    for i, ell in enumerate(csv_l):
        if len(ell) != 0:
            for j, bounds in enumerate(r_bounds):
                if bounds[0] < int(ell[y]) <= bounds[1]:
                    s_l[j][i] = 'INSERT INTO ' + r_dl['tname'] + ' VALUES '
                    s_l[j][i] += '(' + ''.join(['?, ' for _ in range(len(ell) - 1)]) + '?);'
                    p_l[j][i] = ell

    # For each node in the node URIs, construct a socket.
    sock_f = list(map(lambda x: create_socket(x), n))
    if not all(sock_f):
        print('Error: All nodes in cluster could not be reached.'), exit(7)

    # For each node in the node URIs, construct a socket.
    sock_f = list(map(lambda x: create_socket(x), n))
    close_sockets = lambda s_f: list(map(lambda x: x.close(), list(zip(*s_f))[0]))

    # Insert the data into their respective nodes.
    send_insert_selective(s_l, p_l, sock_f)
    print('Insertion was successful.'), close_sockets(sock_f)

    # Update the partition information in the catalog node.
    response_p = RemoteCatalog.update_partition(c, r_dl, len(n))
    if ErrorHandle.is_error(response_p):
        print(response_p)
    else:
        print('Catalog node has been updated with the partitions.')


if __name__ == '__main__':
    # Ensure that we only have 2 arguments.
    if len(sys.argv) != 3:
        ErrorHandle.fatal_handler('python3 runLCSV.py [clustercfg] [csv]')

    # Dissect the given clustercfg for partitioning and catalog information.
    catalog_uri, r_d, numnodes = ErrorHandle.act_upon_error(ClusterCFG.load(sys.argv[1]),
                                                            ErrorHandle.fatal_handler, True)

    # Collect our node URIs. Do not proceed if the catalog node cannot be reached.
    y = RemoteCatalog.return_node_uris(catalog_uri, r_d['tname'])
    node_uris = ErrorHandle.act_upon_error(y, ErrorHandle.fatal_handler, True)

    # If the number of nodes here does not match the nodes in catalog, return with an error.
    if r_d['partmtd'] in [1, 2] and numnodes != len(node_uris):
        ErrorHandle.fatal_handler('Incorrect number of nodes specified in \'clustercfg\'.')

    # Extract the columns from the table, using the first node.
    sock, f = create_socket(node_uris[0])

    # Read our response from the socket. Handle errors appropriately.
    Network.write(sock, ['P', f, r_d['tname']])
    response = ErrorHandle.act_upon_error(Network.read(sock, ErrorHandle.fatal_handler),
                                          ErrorHandle.fatal_handler, True)

    # Determine the partitioning. Use the appropriate load function when determined.
    r_d.update({'col_s': response[1]}), sock.close()
    [nopart_load, rangepart_load, hashpart_load][r_d['partmtd']] \
        (node_uris, catalog_uri, r_d, sys.argv[2])
