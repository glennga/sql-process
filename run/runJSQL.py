# coding=utf-8
"""
TODO: Fix this documentation, wrong clustercfg
Executes a single SQL statement on a cluster of computers containing a join between two tables.
This SQL statement is specified in 'sqlfile', and access information about each computer in the
cluster is specified in 'clustercfg'.

Usage: python runJSQL.py [clustercfg] [sqlfile]

Error: 2 - Incorrect number of arguments.
       3 - There exists an error with the clustercfg file.
       4 - There exists an error with the SQL file.
       5 - Catalog could not be reached or the table was not found.
       6 - Table not found in SQL file.
"""

import re
import sys

from lib.catalog import RemoteCatalog
from lib.database import Database
from lib.dissect import SQLFile, ClusterCFG
from lib.error import ErrorHandle
from lib.network import Network
from lib.parallel import Parallel

# Used to store the node URIs and joined tables of each successful execution.
successful_joins = []


def remove_temp_tables(node_uri, _):
    """

    :param node_uri:
    :return:
    """
    [host, port, f], tables = ClusterCFG.parse_uri(node_uri), []

    # Create the socket to the first node.
    sock = Network.open_client(host, port, ErrorHandle.fatal_handler)
    net_handler = lambda e_n: Network.close_wrapper(e_n, ErrorHandle.fatal_handler, sock)

    # Determine all tables to remove.
    Network.write(sock, ['E', f, 'SELECT tbl_name '
                                 'FROM sqlite_master '
                                 'WHERE type="table" '
                                 'AND tbl_name LIKE "%TTTTT" OR tbl_name LIKE "%JJJJJ"'])

    # Wait for a response to be sent back, and print the response.
    a = Network.read(sock, net_handler)
    operation, resultant = ErrorHandle.act_upon_error(a, net_handler, True)
    tables.append(resultant[0])

    # Repeat. Operation code 'ES' marks the start of a message, and 'EZ' marks the end.
    while operation != 'EZ':
        a = Network.read(sock, net_handler)
        operation, resultant = ErrorHandle.act_upon_error(a, net_handler, True)
        tables.append(resultant[0])

    # Delete all temporary tables.
    for table in tables:
        Network.write(sock, ['E', f, 'DROP TABLE {}'.format(table)])
        ErrorHandle.act_upon_error(Network.read(sock, net_handler), net_handler)

    # Close our socket.
    sock.close()


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
    sock = Network.open_client(host, port, ErrorHandle.fatal_handler)
    net_handler = lambda e_n: Network.close_wrapper(e_n, ErrorHandle.fatal_handler, sock)

    # Inform node 1 to retrieve and store a table from node 2.
    Network.write(sock, ['B', f, t_tables_n, node_uri_2])

    # Wait for a response that this operation was successful.
    a = Network.read(sock, net_handler)
    operation, resultant = ErrorHandle.act_upon_error(a, net_handler, True)

    # Return the temporary table name.
    sock.close()
    return resultant


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
    temp_s, handler = s_n, lambda e: ErrorHandle.fatal_handler('[Join ' + n + ']: ' + str(e))

    # Inform node 1 to grab a table from node 2 iff node 1 and node 2 are remote.
    if node_uri_2 != node_uri_1:
        a = ship_to_remote(host_1, port_1, [f_1, f_2], t_tables_n, node_uri_2)
        temp_name = ErrorHandle.act_upon_error(a, ErrorHandle.fatal_handler, True)

        # Replace all instances of the second table with the temporary table name.
        temp_s = re.sub(r'\b{}\b'.format(t_tables_n[1]), temp_name, s_n)

    # Create the socket to the first node.
    sock_1 = Network.open_client(host_1, port_1, ErrorHandle.fatal_handler)
    net_handler = lambda e_n: Network.close_wrapper(e_n, handler, sock_1)

    # Inform node 1 to join the two tables, and store the result in a temp table.
    new_table = Database.random_name(True)
    Network.write(sock_1, ['E', f_1,
                           'CREATE TABLE {} '.format(new_table) +
                           'AS WITH A AS ( {} )'.format(temp_s) +
                           'SELECT * '
                           'FROM A;'])

    # Handle errors appropriately. Append to our shared memory.
    Network.read(sock_1, net_handler)
    successful_joins.append([node_uri_1, new_table])
    sock_1.close()


def execute_union(node, n, join_list):
    """

    :param node:
    :param n:
    :param join_list:
    :return:
    """
    host, port, f = ClusterCFG.parse_uri(node)

    # Create the socket to the first node.
    sock = Network.open_client(host, port, ErrorHandle.fatal_handler)
    net_handler = lambda e_n: Network.close_wrapper(e_n, ErrorHandle.fatal_handler, sock)

    #

    pass


def display_join(node):
    """
    
    :param node: 
    :return: 
    """
    pass


if __name__ == '__main__':
    # Ensure that we have only two arguments.
    if len(sys.argv) != 3:
        ErrorHandle.fatal_handler('Usage: python3 runJSQL.py [clustercfg] [ddlfile]')

    # Parse both the clustercfg and sqlfile. Ensure that both are properly formatted.
    catalog_uri = ErrorHandle.act_upon_error(ClusterCFG.catalog_uri(sys.argv[1]),
                                             ErrorHandle.fatal_handler, True)
    s = ErrorHandle.act_upon_error(SQLFile.as_string(sys.argv[2]),
                                   ErrorHandle.fatal_handler, True)

    # Determine the working tables.
    t_tables = ErrorHandle.act_upon_error(SQLFile.table(s), ErrorHandle.fatal_handler, True)

    # Collect the node URIs for the first table. Do not proceed if we cannot reach the catalog.
    r_1 = RemoteCatalog.return_node_uris(catalog_uri, t_tables[0])
    nu_1 = ErrorHandle.act_upon_error(r_1, ErrorHandle.fatal_handler, True)

    # Collect the node URIs for the second table.
    r_2 = RemoteCatalog.return_node_uris(catalog_uri, t_tables[1])
    nu_2 = ErrorHandle.act_upon_error(r_2, ErrorHandle.fatal_handler, True)

    # For every join, execute the given statement and display any errors.
    Parallel.execute_nm(nu_2, nu_1, execute_join,
                        lambda y, x, b_2, b_1: (b_1, b_2, x * len(nu_2) + y, s, t_tables))

    # Propagate our changes to a single node (i.e. the first node).
    Parallel.execute_n(successful_joins, execute_union, lambda x, b: (nu_1[0], x, b))

    # Perform the selection, and display the results.
    display_join(nu_1[0])

    # Remove the temporary tables created.
    Parallel.execute_n(nu_1, remove_temp_tables, lambda x, b: (b, x))
