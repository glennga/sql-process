# coding=utf-8
"""
Contains functions to manage the catalog table, locally (client side requests) and remotely (server
side actions).

Usage: LocalCatalog.create_dtable([catalog database filename])
       LocalCatalog.record_ddl([socket], [command list])
       LocalCatalog.record_partition([socket], [command list])
       LocalCatalog.return_node_uris([socket], [command list])

       RemoteCatalog.ping([catalog node URI])
       RemoteCatalog.record_ddl([catalog node URI], [successful node URIs], [executed DDL])
       RemoteCatalog.return_node_uris([catalog node URI], [table name])
       RemoteCatalog.update_partition([catalog node URI], [partition dictionary], [number of nodes])
"""

import sqlite3 as sql

from lib.dissect import SQLFile, ClusterCFG
from lib.error import ErrorHandle


class LocalCatalog:
    """ All catalog operations for the catalog node itself (operating locally). """

    @staticmethod
    def create_dtable(f):
        """ Given the name of the database file, create the table. This information is specified in
        the homework assignment.

        :param f: Location of the database file to store this table on.
        :return: None.
        """

        conn, cur = ErrorHandle.sql_connect(f)
        cur.execute('CREATE TABLE IF NOT EXISTS dtables ('
                    'tname CHARACTER(32), '
                    'nodedriver CHARACTER(64), '
                    'nodeurl CHARACTER(128), '
                    'nodeuser  CHARACTER(16), '
                    'nodepasswd  CHARACTER(16), '
                    'partmtd INT, '
                    'nodeid INT, '
                    'partcol CHARACTER(32), '
                    'partparam1 CHARACTER(128), '
                    'partparam2 CHARACTER(128)); ')

    @staticmethod
    def _perform_ddl(cur, ddl, table, node_uris):
        """

        :param cur:
        :return:
        """

        # Perform the DROP DDL.
        if SQLFile.is_drop_ddl(ddl):
            cur.execute('DELETE FROM dtables W'
                        'HERE tname = ?', (table,))
            return 'Success'

        # If the table exists, do not proceed. Exit with an error.
        e = cur.execute('SELECT 1 '
                        'FROM dtables '
                        'WHERE tname = ? '
                        'LIMIT 1',
                        (table,)).fetchone()
        if e is not None:
            return 'Error: Table exists in cluster.'

        # Perform the INSERTION DDL.
        tuples = [(table, node_uris[i], i + 1) for i in range(len(node_uris))]
        cur.executemany('INSERT INTO dtables '
                        'VALUES (?, NULL, ?, NULL, NULL, NULL, ?, NULL, NULL, NULL)',
                        tuples)

    @staticmethod
    def record_ddl(k, r):
        """ Given node URIs of the cluster and the DDL statement to execute, insert metadata
        about which nodes and which tables are affected.

        :param k: Socket connection to send response through.
        :param r: List passed to socket, containing the filename associated with the catalog node
            database the cluster node URIs, and the SQL being executed.
        :return: None.
        """
        f, node_uris, ddl = r[1:4]

        # Connect to SQLite database using the filename.
        c = ErrorHandle.sql_connect(f, lambda a: ErrorHandle.write_socket(k, str(a)))
        if ErrorHandle.is_error(c):
            return
        conn, cur = c

        # Create the table if it does not exist.
        LocalCatalog.create_dtable(f)

        # Determine the table being operated on in the DDL.
        table = SQLFile.table(ddl)
        if ErrorHandle.is_error(table):
            ErrorHandle.write_socket(k, table)
            return

        # Assemble our tuples and perform the insertion/deletion.
        r = ErrorHandle.attempt_operation(lambda: LocalCatalog._perform_ddl(cur, ddl, table,
                                                                            node_uris), sql.Error)

        # Handle the errors.
        if ErrorHandle.is_error(r):
            ErrorHandle.write_socket(k, r), conn.rollback(), conn.close()
        else:
            conn.commit(), conn.close()
            ErrorHandle.write_socket(k, ['EC', 'Success'])

    @staticmethod
    def _record_specific_partition(r_d, numnodes, cur):
        """

        :param r_d:
        :param numnodes:
        :param cur:
        :return:
        """
        # No partitioning has been specified. Create the appropriate entries.
        if r_d['partmtd'] == 0:
            for i in range(1, numnodes + 1):
                cur.execute('UPDATE dtables '
                            'SET partmtd = 0 '
                            'WHERE nodeid = ? AND tname = ?',
                            (i, r_d['tname']))

        # Range partitioning has been specified. Create the appropriate entries.
        if r_d['partmtd'] == 1:
            for i in range(1, numnodes + 1):
                cur.execute('UPDATE dtables '
                            'SET partcol = ?, partparam1 = ?, partparam2 = ?, partmtd = 1 '
                            'WHERE nodeid = ? AND tname = ?',
                            (r_d['partcol'], r_d['param1'][i - 1], r_d['param2'][i - 1],
                             i, r_d['tname']))

        # Hash partitioning has been specified. Create the appropriate entries.
        elif r_d['partmtd'] == 2:
            for i in range(1, numnodes + 1):
                cur.execute('UPDATE dtables '
                            'SET partcol = ?, partparam1 = ?, partmtd = 2 '
                            'WHERE nodeid = ? AND tname = ?',
                            (r_d['partcol'], r_d['param1'], i, r_d['tname']))

    @staticmethod
    def record_partition(k, r):
        """ Record the partition to catalog database, assuming the working node is the catalog
        node. Using the catalog database filename, the partition dictionary, and the specified
        number of nodes from the received command list. Return an acknowledgement through the
        given socket.

        :param k: Socket to send acknowledgement through.
        :param r: Command list passed through the same socket.
        :return: None.
        """
        f, r_d, numnodes = r[1:4]

        # Connect to SQLite database using the filename.
        c = ErrorHandle.sql_connect(f, lambda a: ErrorHandle.write_socket(k, str(a)))
        if ErrorHandle.is_error(c):
            return
        conn, cur = c

        # Record the partition.
        if ErrorHandle.is_error(ErrorHandle.attempt_operation(
                lambda: LocalCatalog._record_specific_partition(r_d, numnodes, cur), sql.Error,
                lambda a: ErrorHandle.write_socket(k, str(a)))):
            conn.rollback(), conn.close()
            return

        # No errors have occured. Send the success message.
        conn.commit(), conn.close()
        ErrorHandle.write_socket(k, ['EK', 'Success'])

    @staticmethod
    def return_node_uris(k, r):
        """ Return a list of node URIs stored in the 'dtables' table, which informs the client
        machine (not this node) how to contact the cluster. Using the catalog database filename
        and the name of table associated with the cluster.

        :param k: Socket to send node URI list to.
        :param r: Command list passed through the same socket.
        :return: None.
        """
        f, tname = r[1:3]

        # Connect to the catalog.
        c = ErrorHandle.sql_connect(f, lambda a: ErrorHandle.write_socket(k, str(a)))
        if ErrorHandle.is_error(c):
            return
        conn, cur = c

        # Grab the node URIs belonging to the given table. Return the results.
        p = ErrorHandle.sql_execute(cur, 'SELECT nodeurl FROM dtables WHERE tname = ?',
                                    lambda a: ErrorHandle.write_socket(k, str(a)) \
                                              and conn.rollback(),
                                    (tname,), True)

        conn.close()
        if not ErrorHandle.is_error(p):
            ErrorHandle.write_socket(k, 'Error: Table ' + tname + ' not found.' if len(p) == 0
                                     else ['EU', p])


class RemoteCatalog:
    """ All catalog operations on general nodes (i.e. calls the catalog node). """

    @staticmethod
    def ping(c):
        """ Given information about the catalog node, check if a connection is possible to the
        catalog node.

        :param c: The value of the key-value pair inside clustercfg.
        :return: True if a connection can be achieved. False otherwise.
        """
        host, port, f = ClusterCFG.parse_uri(c)

        # Create our socket and attempt to connect.
        sock = ErrorHandle.create_client_socket(host, port)
        if ErrorHandle.is_error(sock):
            return False

        sock.close()
        return True

    @staticmethod
    def record_ddl(c, success_nodes, ddl):
        """ Given information about the cluster and the DDL statement to execute, store the DDL
        in the catalog node.

        TODO: Fix the documentation below.
        :param c: The URI associated with the catalog.
        :param success_nodes: List containing the URIs of the successfully executed nodes.
        :param ddl: DDL statement to pass to the catalog node.
        :return The resulting error if the appropriate response is not returned successfully. True
            otherwise.
        """
        host, port, f = ClusterCFG.parse_uri(c)

        # Create our socket.
        sock = ErrorHandle.create_client_socket(host, port)
        if ErrorHandle.is_error(sock):
            return 'Error: Socket could not be established.'

        # Pickle our command list ('C', cluster, and DDL), and send our message.
        r = 'Error: No nodes were successful.'
        if len(success_nodes) != 0:
            ErrorHandle.write_socket(sock, ['C', f, success_nodes, ddl])

            # Wait for a response to be sent back, and return this response.
            r = ErrorHandle.read_socket(sock)
        sock.close()

        # Check for errors in the response.
        if ErrorHandle.is_error(r):
            return r
        elif r[0] != 'EC' or r[1] != 'Success':
            return 'Error: Response not as expected.'
        else:
            return 'Success.'

    @staticmethod
    def return_node_uris(c, tname):
        """ Given the catalog URI and the name of table, grab the node URIs from the catalog node.

        :param c: The URI associated with the catalog.
        :param tname: Name of the table associated with the cluster to search.
        :return: The resulting error if the appropriate response is not returned successfully.
            Otherwise, a list of node URIs.
        """
        host, port, f = ClusterCFG.parse_uri(c)

        # Create our socket.
        sock = ErrorHandle.create_client_socket(host, port)
        if ErrorHandle.is_error(sock):
            return sock

        # Pickle our command list ('U', filename, and tname), and send our message.
        ErrorHandle.write_socket(sock, ['U', f, tname])

        # Wait for a response to be sent back, and record this response.
        r = ErrorHandle.read_socket(sock)
        sock.close()
        if ErrorHandle.is_error(r):
            return r

        # Confirm the returned operation code.
        if r[0] != 'EU':
            return 'Error: Return message not as expected.'
        else:
            # Flatten the returned list.
            return [x[0] for x in r[1]]

    @staticmethod
    def update_partition(c, r_d, numnodes):
        """ Given information about the catalog and partitioning information, store the partition
        in the catalog node.

        :param c: The URI associated with the catalog.
        :param r_d: Dictionary containing partitioning entries.
        :param numnodes: Number of nodes associated with the cluster.
        :return: The resulting error if the appropriate response is not returned successfully.
            Otherwise, true.
        """
        host, port, f = ClusterCFG.parse_uri(c)

        # Create our socket.
        sock = ErrorHandle.create_client_socket(host, port)
        if ErrorHandle.is_error(sock):
            return sock

        # Pickle our command list ('K', f, r_d, numnodes), and send our message.
        ErrorHandle.write_socket(sock, ['K', f, r_d, numnodes])
        r = ErrorHandle.read_socket(sock)

        # Handle our errors appropriately.
        if ErrorHandle.is_error(r):
            return r
        elif r[0] == 'EK' and r[1] == 'Success':
            return 'Success'
        else:
            return 'Error: Returned message is not as expected.'
