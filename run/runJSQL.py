# coding=utf-8
"""
Execute a single SQL statement involving a join between two tables on a cluster of computers.

Usage: python runJSQL.py [clustercfg] [sqlfile]
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


def find_temp_tables(node_uri):
    """ Retrieve all of the temporary tables that exist in the given node.

    :param node_uri: URI of the node to retrieve the temporary tables from.
    :return: List containing the temporary tables on the current node.
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

    sock.close()
    return tables


def remove_temp_table(node_uri, table):
    """ Given the URI of a node and a table that exists in that node, remove the given table from
    the node.

    :param node_uri: URI of the node to remove the table from.
    :param table: Name of the table to remove from the node.
    :return: None.
    """
    host, port, f = ClusterCFG.parse_uri(node_uri)

    # Create the socket to the first node.
    sock = Network.open_client(host, port, ErrorHandle.fatal_handler)
    net_handler = lambda e_n: Network.close_wrapper(e_n, ErrorHandle.fatal_handler, sock)

    # Delete the table.
    Network.write(sock, ['E', f, 'DROP TABLE {}'.format(table)])
    ErrorHandle.act_upon_error(Network.read(sock, net_handler), net_handler)

    # Close our socket.
    sock.close()


def ship_to_remote(host, port, f, t_tables_n, nu_2_n):
    """ Given a

    :param host:
    :param port:
    :param f:
    :param t_tables_n:
    :param nu_2_n:
    :return:
    """
    # Create the socket to the first node.
    sock = Network.open_client(host, port, ErrorHandle.fatal_handler)
    net_handler = lambda e_n: Network.close_wrapper(e_n, ErrorHandle.fatal_handler, sock)

    # Inform node 1 to retrieve and store a table from node 2.
    Network.write(sock, ['B', f, t_tables_n, nu_2_n])

    # Wait for a response that this operation was successful.
    a = Network.read(sock, net_handler)
    operation, resultant = ErrorHandle.act_upon_error(a, net_handler, True)

    # Return the temporary table name.
    sock.close()
    return resultant


def execute_join(nu_1_n, nu_2_n, n, s_n, t_tables_n):
    """ Given the URI of two nodes from the catalog database and the SQL to execute, join two
    tables across two nodes and store the result in the first table.

    :param nu_1_n: Node URI of the first node to join (and to store the result to).
    :param nu_2_n: Node URI of the second node to join.
    :param n: Join number that this operation is working on.
    :param s_n: Join statement to execute ........... TODO FINISH THIS
    :param t_tables_n: asdsasdasd
    :return:
    """
    host_1, port_1, f_1 = ClusterCFG.parse_uri(nu_1_n)
    host_2, port_2, f_2 = ClusterCFG.parse_uri(nu_2_n)
    temp_s, handler = s_n, lambda e: ErrorHandle.fatal_handler('[Join ' + n + ']: ' + str(e))

    # Inform node 1 to grab a table from node 2 iff node 1 and node 2 are remote.
    if nu_2_n != nu_1_n:
        a = ship_to_remote(host_1, port_1, [f_1, f_2], t_tables_n, nu_2_n)
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
    successful_joins.append([nu_1_n, new_table])
    sock_1.close()


def execute_union(source_list, join_list):
    """

    :param source_list:
    :param join_list:
    :return:
    """
    master_node_uri, master_table = source_list
    slave_node_uri, slave_table = join_list
    host_1, port_1, f_1 = ClusterCFG.parse_uri(master_node_uri)

    # If necessary, ship the joins to our master.
    if master_node_uri != slave_node_uri:
        host_2, port_2, f_2 = ClusterCFG.parse_uri(slave_node_uri)

        # Set our new table name appropriately.
        a = ship_to_remote(host_1, port_1, [f_1, f_2], [master_table, slave_table], slave_node_uri)
        slave_table = ErrorHandle.act_upon_error(a, ErrorHandle.fatal_handler, True)

    # Create the socket to the first node.
    sock = Network.open_client(host_1, port_1, ErrorHandle.fatal_handler)
    net_handler = lambda e_n: Network.close_wrapper(e_n, ErrorHandle.fatal_handler, sock)

    # Inform our node to union the table specified in join_list (insert the difference).
    Network.write(sock, ['E', f_1,
                         'INSERT INTO {} '.format(master_table) +
                         'SELECT * '
                         'FROM {} '.format(slave_table) +
                         'EXCEPT SELECT * '
                         'FROM {};'.format(master_table)])

    # Handle our errors.
    ErrorHandle.act_upon_error(Network.read(sock, net_handler), net_handler)
    sock.close()


def display_join(master_list):
    """
    
    :param master_list:
    :return: 
    """
    node_uri, table = master_list
    host, port, f = ClusterCFG.parse_uri(node_uri)

    # Create the socket to the first node.
    sock = Network.open_client(host, port, ErrorHandle.fatal_handler)
    net_handler = lambda e_n: Network.close_wrapper(e_n, ErrorHandle.fatal_handler, sock)

    # Gather result from join + union.
    Network.write(sock, ['E', f, 'SELECT * '
                                 'FROM {}'.format(table)])

    # Wait for a response to be sent back, and print the response.
    a = Network.read(sock, net_handler)
    operation, resultant = ErrorHandle.act_upon_error(a, net_handler, True)
    print('[' + ''.join([str(x) + ', ' for x in resultant]) + ']')

    # Repeat. Operation code 'ES' marks the start of a message, and 'EZ' marks the end.
    while operation != 'EZ':
        a = Network.read(sock, net_handler)
        operation, resultant = ErrorHandle.act_upon_error(a, net_handler, True)
        print('[' + ''.join([str(x) + ', ' for x in resultant]) + ']')

    sock.close()


if __name__ == '__main__':
    # Ensure that we have only two arguments.
    if len(sys.argv) != 3:
        ErrorHandle.fatal_handler('Usage: python3 runJSQL.py [clustercfg] [ddlfile]')

    # Parse both the clustercfg and sqlfile. Ensure that both are properly formatted.
    catalog_uri = ErrorHandle.act_upon_error(ClusterCFG.catalog_uri(sys.argv[1]),
                                             ErrorHandle.fatal_handler, True)
    s = ErrorHandle.act_upon_error(SQLFile.as_string(sys.argv[2]), ErrorHandle.fatal_handler, True)

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

    # Propagate our changes to a single node (i.e. the first node). This must be sequential.
    list(map(lambda s_j: execute_union(successful_joins[0], s_j), successful_joins[1:]))

    # Perform the selection, and display the results.
    display_join(successful_joins[0])

    # Remove the temporary tables created. Execute in parallel along nodes.
    rem = lambda b: list(map(lambda a: remove_temp_table(b, a), find_temp_tables(b)))
    Parallel.execute_n(nu_1, rem, lambda _, b: (b, ))
