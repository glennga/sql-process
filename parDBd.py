# coding=utf-8
"""
Listens for a command list to be sent to the given port, and returns a command response list
through that same port. Given an operation code OP, the following occurs:

OP : 'YS' -> Execute an insertion SQL statement, and wait for additional statements.
   : 'YZ' -> Execute a SQL statement, and don't wait for additional statements.
   : 'YY' -> Don't execute a SQL statement, and don't wait for additional statements.
   : 'YX' -> Rollback to the last stable state.
   : 'E' -> Execute a SQL statement and return tuples if applicable.
   : 'C' -> Record a DDL to the catalog database.
   : 'K' -> Record partitioning information to the catalog database.
   : 'U' -> Lookup the node URIs on the catalog database and return these.
   : 'P' -> Lookup the fields for a given table and return this.
   : 'B' -> Ship a given table to the current node.

Usage: python parDBd.py [hostname] [port]
"""

import sys

from lib.catalog import LocalCatalog
from lib.database import Database
from lib.dissect import SQLFile, ClusterCFG
from lib.error import ErrorHandle
from lib.network import Network
from lib.parallel import Parallel


def execute_prepared(k_n, r):
    """ Perform a single insertion SQL operation on the passed database. Do not wait for a
    terminating operation code.

    :param k_n: Socket connection to send response through.
    :param r: Command list passed through the same socket.
    :return: None.
    """
    f, s, tup, r_i = r[1], r[2], r[3], r

    # Create our connection.
    conn, cur = Database.connect(f, ErrorHandle.raise_handler)
    sql_handler = lambda e_n: Database.rollback_wrapper(e_n, ErrorHandle.raise_handler, conn)

    # Execute the command. Return the error if any exist, or return a success method.
    Database.execute(cur, s, sql_handler, tup)

    # Return a success message.
    conn.commit(), conn.close()
    Network.write(k_n, ['EY', 'Success'])


def execute_multiple_prepared(k_n, r):
    """ Perform the given insertion SQL operation on the passed database. Wait for the
    terminating 'YZ' and 'YY' operation codes.

    :param k_n: Socket connection to send response through.
    :param r: Command list passed through the same socket.
    :return: None.
    """
    f, s, tup, r_i, is_commit = r[1], r[2], r[3], r, True
    net_handler = lambda e_n: Network.close_wrapper(e_n, ErrorHandle.raise_handler, k_n)

    # Create our connection.
    conn, cur = Database.connect(f, ErrorHandle.raise_handler)
    sql_handler = lambda e_n: Database.rollback_wrapper(e_n, ErrorHandle.raise_handler, conn)

    while True:
        # Execute the command. Return the error if any exist.
        Database.execute(cur, s, sql_handler, tup)
        Network.write(k_n, ['EY', 'Success'])

        # Interpret the current operation code.
        result = Network.read(k_n, net_handler)

        # Stop if YZ, YY, or YX proceed otherwise.
        if result[0] == 'YY':
            break
        elif result[0] == 'YZ':
            ErrorHandle.act_upon_error(Database.execute(cur, s, sql_handler, tup), net_handler)
            break
        elif result[0] == 'YX':
            is_commit = False
            break
        else:
            f, s, tup = result[1], result[2], result[3]

    # Commit our changes (if desired) and close our connection.
    conn.commit() if is_commit else None, conn.close()
    Network.write(k_n, ['EY', 'Success'])


def execute_on_db(k_n, r):
    """ Perform the given SQL operation on the passed database. Return any tuples if the
    statement is a SELECT statement.

    :param k_n: Socket connection to send response through.
    :param r: Command list passed through the same socket.
    :return: None.
    """
    f, s = r[1], r[2]

    # Create our connection.
    conn, cur = Database.connect(f, ErrorHandle.raise_handler)
    sql_handler = lambda e_n: Database.rollback_wrapper(e_n, ErrorHandle.raise_handler, conn)

    # Execute the command. Return the error if any exist.
    result = Database.execute(cur, s, sql_handler, fetch=True)
    conn.commit(), conn.close()

    # If the statement is a selection, send all resulting tuples.
    if SQLFile.is_select(s):
        for i, r_t in enumerate(result):
            # If this is the last tuple, append the ending operation code.
            Network.write(k_n, ['EZ' if i + 1 == len(result) else 'ES', r_t])
        if len(result) == 0:
            Network.write(k_n, ['EZ', 'No tuples found.'])

    else:
        # Otherwise, assume that the statement has passed.
        Network.write(k_n, ['EZ', 'Success'])


def return_columns(k_n, r):
    """ Return the columns associated with a table through the given socket.

    :param k_n: Socket connection to send response through.
    :param r: Command list passed through the same socket.
    :return: None.
    """
    f, tname = r[1], r[2]

    # Create our connection.
    conn, cur = Database.connect(f, ErrorHandle.raise_handler)
    sql_handler = lambda e_n: Database.rollback_wrapper(e_n, ErrorHandle.raise_handler, conn)

    # Execute the command. Return the error if any exist.
    col = Database.description(cur, 'SELECT * '
                                    'FROM ' + tname + ' LIMIT 1;', sql_handler)
    conn.close()

    # Return the column names.
    Network.write(k_n, ['EP', [col[i][0] for i in range(len(col))]])


def store_from_ship(sock_n, conn, temp_name):
    """ Helper method for the ship procedure. The initial request for tuples is sent outside of
    here, but this handles all subsequent requests. Tuples are then stored in the table that was
    just created (pass in temp_name).

    :param k_n: Socket connection to send response through.
    :param conn: Cursor to an open database connection.
    :param temp_name: Name of the table created to store the results.
    :return: None.
    """
    sql_handler = lambda e_n: Database.rollback_wrapper(e_n, ErrorHandle.raise_handler, conn)
    net_handler = lambda e_n: Network.write(sock_n, ['YX']) and \
                              Network.close_wrapper(e_n, ErrorHandle.raise_handler, sock_n)

    # Read from our socket.
    operation, resultant = Network.read(sock_n, net_handler)

    # Handle the case where no tuples exist. (Given string = 'No tuples found.').
    if resultant == 'No tuples found.':
        return

    # Execute the insertion.
    Database.execute(conn.cursor(), 'INSERT INTO ' + temp_name +
                     ' VALUES (' + ''.join(['?, ' for _ in range(len(resultant) - 1)]) + '?);',
                     sql_handler, resultant)

    # Repeat. Operation code 'ES' marks the start of a message, and 'EZ' marks the end.
    while operation != 'EZ':
        operation, resultant = Network.read(sock_n, net_handler)

        # Perform the insertion again.
        Database.execute(conn.cursor(), 'INSERT INTO ' + temp_name +
                         ' VALUES (' + ''.join(['?, ' for _ in range(len(resultant) - 1)]) + '?);',
                         sql_handler, resultant)


def copy_table(conn, node, f_s, tnames):
    """ Helper method for the ship procedure. This copies the schema from the other table by
    requesting the 'sql' field from the metadata table of the remote node. This is then executed
    given a new table name, so as to avoid conflicts in nodes that share a partition with the
    remote.

    :param conn: Cursor to an open database connection.
    :param node: Node URI of the node to send the request to (the remote node).
    :param f_s: List of database filenames, in order of the current, then remote node.
    :param tnames: List of tables involved in the join, in order of current, then remote node.
    :return: The creation schema of tnames[1] at the remote node, and the name of the new table.
    """
    sql_handler = lambda e_n: Database.rollback_wrapper(e_n, ErrorHandle.raise_handler, conn)
    host, port, f = ClusterCFG.parse_uri(node)

    # Create socket to secondary node.
    sock_n = Network.open_client(host, port, ErrorHandle.raise_handler)
    net_handler = lambda e_n: Network.close_wrapper(e_n, ErrorHandle.raise_handler, sock_n)

    # Grab the SQL used to create the table.
    Network.write(sock_n, ['E', f_s[1], 'SELECT sql '
                                        'FROM sqlite_master '
                                        'WHERE name="{}"'.format(tnames[1])])
    operation, create_sql = Network.read(sock_n, net_handler)

    # Execute the create table statement on the local database with a new table name.
    new_table = Database.random_name(False)
    new_sql = create_sql[0].replace(tnames[1], new_table, 1) + ';'
    Database.execute(conn.cursor(), new_sql, sql_handler)

    sock_n.close()
    return create_sql, new_table


def ship(k_n, r):
    """ Ship a table from a remote node to the current one.

    :param k_n: Socket connection to pass **response** through (not to initiate transfer).
    :param r: Command list passed through the same socket.
    :return: None.
    """
    f_s, tnames, node = r[1], r[2], r[3]
    host, port, f = ClusterCFG.parse_uri(node)

    # Connect to local database.
    conn, cur = Database.connect(f_s[0], ErrorHandle.raise_handler)

    # Copy the table from our remote node to our local node.
    create_sql, new_table = copy_table(conn, node, f_s, tnames)

    # Create socket to secondary node.
    sock_n = Network.open_client(host, port, ErrorHandle.raise_handler)

    # Retrieve data from the secondary node.
    Network.write(sock_n, ['E', f_s[1], 'SELECT * FROM ' + tnames[1]])
    store_from_ship(sock_n, conn, new_table)

    # Return the name of the table created if successful.
    conn.commit(), conn.close()
    Network.write(k_n, ['EB', new_table])


def interpret_base(k_n, r):
    """ Given a socket and a message through the socket, interpret the message. The result should
    be a list.

    :param k_n: Socket connection pass response through.
    :param r: Command list passed to the socket.
    :return: None.
    """

    # If our response is not an list, return an error.
    if not hasattr(r, '__iter__'):
        ErrorHandle.raise_handler('Input not a list.')

    if r[0] == 'YS':
        # Execute multiple insertion operations on a database.
        execute_multiple_prepared(k_n, r)
    elif r[0] == 'YZ':
        # Execute a single insertion operation on a database.
        execute_prepared(k_n, r)
    elif r[0] == 'YY':
        # Do not perform any action. Send a success message.
        Network.write(k_n, ['EY', 'Success'])
    elif r[0] == 'E':
        # Execute an operation on a database.
        execute_on_db(k_n, r)
    elif r[0] == 'C':
        # Execute DDL on the catalog table.
        LocalCatalog.record_ddl(k_n, r)
    elif r[0] == 'K':
        # Insert partition data into the catalog table.
        LocalCatalog.record_partition(k_n, r)
    elif r[0] == 'U':
        # Return the URIs associated with each node.
        LocalCatalog.return_node_uris(k_n, r)
    elif r[0] == 'P':
        # Return the columns associated with the given table.
        return_columns(k_n, r)
    elif r[0] == 'B':
        # Ship a table from a remote node to here.
        ship(k_n, r)
    else:
        Network.write(k_n, ErrorHandle.wrap_error_tag('Operation code invalid.'))


def interpret(k_n):
    """ Wrapper for the interpret_base function. This is to be used when spawning a new process.

    :param k_n: Current socket connection to a given client.
    :return: None.
    """
    try:
        # Retrieve the sent data. Unpickle the data. Interpret the command.
        interpret_base(k_n, Network.read(k_n, ErrorHandle.raise_handler))
    except ConnectionResetError:
        # Ignore when a connection is forcibly closed, or the socket has timed out.
        pass
    except Exception as e:
        # An exception has been thrown. Inform the client.
        Network.write(k_n, ErrorHandle.wrap_error_tag(e))


if __name__ == '__main__':
    # Ensure that we only have 2 arguments.
    if len(sys.argv) != 3:
        ErrorHandle.fatal_handler('Usage: python3 parDBd.py [hostname] [port]')

    # Create the socket.
    sock = Network.open_server(sys.argv[1], sys.argv[2], ErrorHandle.fatal_handler)

    while True:
        # Listen and wait for a connection on this socket.
        sock.listen(1)
        k, addr = sock.accept()

        # Zombie prevention, check if my children are alive.
        Parallel.check_children()

        # Hand this off to another process and close the current connection.
        Parallel.spawn_process(interpret, (k,))
        k.close()
