# coding=utf-8
"""
TODO: Fix this documentation, wrong clustercfg
Executes a single SQL statement on a cluster of computers containing a join between two tables.
This SQL statement is specified in 'sqlfile', and access information about each computer in the
cluster is specified in 'clustercfg'.

Usage: python runDJSQL.py [clustercfg] [sqlfile]

Error: 2 - Incorrect number of arguments.
       3 - There exists an error with the clustercfg file.
       4 - There exists an error with the SQL file.
       5 - Catalog could not be reached or the table was not found.
       6 - Table not found in SQL file.
"""

import random
import re
import string
import sys
from threading import Thread

from lib.catalog import RemoteCatalog
from lib.dissect import SQLFile, ClusterCFG
from lib.error import ErrorHandle

# Used to store the node URIs and joined tables of each successful execution.
successful_joins = []


def remove_temp_tables(node_uri):
    """

    :param node_uri:
    :return:
    """
    [host, port, f], tables = ClusterCFG.parse_uri(node_uri), []

    # Create the socket to the first node.
    sock = ErrorHandle.create_client_socket(host, port)
    if ErrorHandle.is_error(sock):
        return sock

    # Determine all tables to remove.
    ErrorHandle.write_socket(sock, ['E', f, 'SELECT tbl_name '
                                            'FROM sqlite_master '
                                            'WHERE type="table" '
                                            'AND tbl_name LIKE "%TTTTT"'])
    r = ErrorHandle.read_socket(sock)
    if ErrorHandle.is_error(r):
        return r
    operation, resultant = r

    # Store our result.
    tables.append(resultant[0])

    # Repeat. Operation code 'ES' marks the start of a message, and 'EZ' marks the end.
    while operation != 'EZ':
        r = ErrorHandle.read_socket(sock)
        if not ErrorHandle.is_error(r):
            operation, resultant = r
            tables.append(resultant[0])

    # Delete all temporary tables.
    for table in tables:
        ErrorHandle.write_socket(sock, ['E', f, 'DROP TABLE {}'.format(table)])
        r = ErrorHandle.read_socket(sock)
        if ErrorHandle.is_error(r):
            return r

    return 'Success'


def ship_to_remote(host, port, f, t_tables_n, node_uri_2):
    """

    :param host:
    :param port:
    :param f:
    :param t_tables_n:
    :param node_uri_2:
    :return:
    """
    # Create the socket to the first node.
    sock = ErrorHandle.create_client_socket(host, port)
    if ErrorHandle.is_error(sock):
        return sock

    # Inform node 1 to retrieve and store a table from node 2.
    ErrorHandle.write_socket(sock, ['B', f, t_tables_n, node_uri_2])

    # Wait for a response that this operation was successful.
    r = ErrorHandle.read_socket(sock)
    if ErrorHandle.is_error(r):
        sock.close()
        return

    # Return the temporary table name.
    sock.close()
    return r[1]


def execute_join(node_uri_1, node_uri_2, n, s_n, t_tables_n):
    """ Given the URI of two nodes from the catalog database and the SQL to execute, join two
    tables across two nodes and store the result in the first table.

    :param node_uri_1: Node URI of the first node to join (and to store the result to).
    :param node_uri_2: Node URI of the second node to join.
    :param n: Join number that this operation is working on.
    :param s_n: Join statement to execute ........... TODO FINISH THIS
    :param t_tables_n: asdsasdasd
    :return:
    """
    host_1, port_1, f_1 = ClusterCFG.parse_uri(node_uri_1)
    host_2, port_2, f_2 = ClusterCFG.parse_uri(node_uri_2)
    temp_s, error_handle = s_n, lambda a: print('Error: Join ' + str(n) + ' - ' + str(a))

    # Inform node 1 to grab a table from node 2 iff node 1 and node 2 are remote.
    if node_uri_2 != node_uri_1:
        temp_name = ship_to_remote(host_1, port_1, [f_1, f_2], t_tables_n, node_uri_2)
        if ErrorHandle.is_error(temp_name):
            error_handle(temp_name), exit(8)

        # Replace all instances of the second table with the temporary table name.
        temp_s = re.sub(r'\b{}\b'.format(t_tables_n[1]), temp_name, s_n)

    # Create the socket to the first node.
    sock = ErrorHandle.create_client_socket(host_1, port_1)
    if ErrorHandle.is_error(sock):
        error_handle(sock), exit(9)

    # Inform node 1 to join the two tables, and store the result in a temp table.
    new_table = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10)) + 'ZZZZZTTTTT'
    ErrorHandle.write_socket(sock, ['E', f_1,
                                    'CREATE TABLE {} '.format(new_table) +
                                    'AS WITH A AS ( {} )'.format(temp_s) +
                                    'SELECT * '
                                    'FROM A;'])

    # Handle errors appropriately.
    r = ErrorHandle.read_socket(sock, error_handle)
    if not ErrorHandle.is_error(r):
        successful_joins.append([node_uri_1, new_table])
    else:
        print('Error: Join ' + str(n) + ' - ' + r), exit(7)
    sock.close()


def execute_union():
    pass


def display_join(node):
    """
    
    :param node: 
    :return: 
    """
    pass


def join_prop_sel_clean(t_tables_n, node_uris_1_n, node_uris_2_n, s_n):
    """

    :return: 
    """

    # For every join, execute the given statement and display any errors.
    threads = []
    for i, n_1 in enumerate(node_uris_1_n):
        for j, n_2 in enumerate(node_uris_2_n):
            threads.append(Thread(target=execute_join,
                                  args=(n_1, n_2, i * len(node_uris_2_n) + j, s_n, t_tables_n)))
            threads[-1].start()
    [b.join() for b in threads]

    # Propagate our changes to a single node (i.e. the first node).
    threads = []
    for i, sj in enumerate(successful_joins):
        threads.append(Thread(target=execute_union, args=(node_uris_1_n[0], i, sj,)))
        threads[-1].start()
    [b.join() for b in threads]

    # Perform the selection, and display the results.
    display_join(node_uris_1_n[0])

    # Remove the temporary tables created.
    threads = []
    for i, node in enumerate(node_uris_1_n):
        threads.append(Thread(target=remove_temp_tables, args=(node, i,)))
        threads[-1].start()
    [b.join() for b in threads]


if __name__ == '__main__':
    # Ensure that we have only two arguments.
    if len(sys.argv) != 3:
        print('Usage: python3 runDJSQL.py [clustercfg] [sqlfile]'), exit(2)

    # Parse both the clustercfg and sqlfile. Ensure that both are properly formatted.
    catalog_uri, s = ClusterCFG.catalog_uri(sys.argv[1]), SQLFile.as_string(sys.argv[2])
    if ErrorHandle.is_error(catalog_uri):
        print(catalog_uri), exit(3)
    elif ErrorHandle.is_error(s):
        print(s), exit(4)

    # Determine the working tables.
    t_tables = SQLFile.table(s)
    if ErrorHandle.is_error(t_tables):
        print(t_tables), exit(6)

    # Collect our node URIs. Do not proceed if the catalog node cannot be reached.
    node_uris_1 = RemoteCatalog.return_node_uris(catalog_uri, t_tables[0])
    print(node_uris_1) and exit(5) if ErrorHandle.is_error(node_uris_1) else None
    node_uris_2 = RemoteCatalog.return_node_uris(catalog_uri, t_tables[1])
    print(node_uris_2) and exit(5) if ErrorHandle.is_error(node_uris_2) else None

    # Perform the join, propagation, selection, and cleanup.
    join_prop_sel_clean(t_tables, node_uris_1, node_uris_2, s)
