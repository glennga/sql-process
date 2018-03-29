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

from lib.database import Database
from lib.dissect import SQLFile, ClusterCFG
from lib.error import ErrorHandle
from lib.network import Network


class LocalCatalog:
    """ All catalog operations for the catalog node itself (operating locally). Errors here are
    handled by raising an exception. parDBd is meant to catch these errors and send these over to
    the client. """

    @staticmethod
    def create_dtable(f):
        """ Given the name of the database file, create the table. This information is specified in
        the homework assignment.

        :param f: Location of the database file to store this table on.
        :return: None.
        """

        conn, cur = Database.connect(f, ErrorHandle.raise_handler)
        Database.execute(cur, 'CREATE TABLE IF NOT EXISTS dtables ('
                              'tname CHARACTER(32), '
                              'nodedriver CHARACTER(64), '
                              'nodeurl CHARACTER(128), '
                              'nodeuser  CHARACTER(16), '
                              'nodepasswd  CHARACTER(16), '
                              'partmtd INT, '
                              'nodeid INT, '
                              'partcol CHARACTER(32), '
                              'partparam1 CHARACTER(128), '
                              'partparam2 CHARACTER(128)); ', ErrorHandle.raise_handler)

    @staticmethod
    def _perform_ddl(cur, ddl, table, node_uris):
        """

        :param cur:
        :return:
        """

        # Perform the DROP DDL.
        if SQLFile.is_drop_ddl(ddl):
            Database.execute(cur, 'DELETE FROM dtables '
                                  'WHERE tname = ?', ErrorHandle.raise_handler, (table,))
            return

        # If the table exists, do not proceed. Exit with an error.
        e = Database.execute(cur, 'SELECT 1 '
                                  'FROM dtables '
                                  'WHERE tname = ? '
                                  'LIMIT 1', ErrorHandle.raise_handler, (table,), True)
        if len(e) != 0:
            raise sql.Error(ErrorHandle.wrap_error_tag('Table exists in cluster.'))

        # Perform the INSERTION DDL.
        tuples = [(table, node_uris[i], i + 1) for i in range(len(node_uris))]
        Database.executemany(cur, 'INSERT INTO dtables '
                                  'VALUES (?, NULL, ?, NULL, NULL, NULL, ?, NULL, NULL, NULL)',
                             ErrorHandle.raise_handler, tuples)


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
        conn, cur = Database.connect(f, ErrorHandle.raise_handler)

        # Create the table if it does not exist.
        LocalCatalog.create_dtable(f)

        # Determine the table being operated on in the DDL.
        table = ErrorHandle.act_upon_error(SQLFile.table(ddl), ErrorHandle.raise_handler, True)

        # Assemble our tuples and perform the insertion/deletion.
        LocalCatalog._perform_ddl(cur, ddl, table, node_uris)
        conn.commit()

        # If we have reached this point, we are successful. Send the appropriate message.
        Network.write(k, ['EC', 'Success'])

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
                Database.execute(cur, 'UPDATE dtables '
                                      'SET partmtd = 0 '
                                      'WHERE nodeid = ? AND tname = ?',
                                 ErrorHandle.raise_handler, (i, r_d['tname']))

        # Range partitioning has been specified. Create the appropriate entries.
        elif r_d['partmtd'] == 1:
            for i in range(1, numnodes + 1):
                Database.execute(cur, 'UPDATE dtables '
                                      'SET partcol = ?, partparam1 = ?, '
                                      'partparam2 = ?, partmtd = 1 '
                                      'WHERE nodeid = ? AND tname = ?',
                                 ErrorHandle.raise_handler,
                                 (r_d['partcol'], r_d['param1'][i - 1], r_d['param2'][i - 1], i,
                                  r_d['tname']))

        # Hash partitioning has been specified. Create the appropriate entries.
        elif r_d['partmtd'] == 2:
            for i in range(1, numnodes + 1):
                Database.execute(cur, 'UPDATE dtables '
                                      'SET partcol = ?, partparam1 = ?, partmtd = 2 '
                                      'WHERE nodeid = ? AND tname = ?',
                                 ErrorHandle.raise_handler,
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
        f, r_d, numnodes = r[1], r[2], r[3]

        # Connect to SQLite database using the filename.
        conn, cur = Database.connect(f, ErrorHandle.raise_handler)
        sql_handler = lambda e_n: Database.rollback_wrapper(e_n, ErrorHandle.raise_handler, conn)

        # Record the partition.
        e = lambda: LocalCatalog._record_specific_partition(r_d, numnodes, cur)
        ErrorHandle.attempt_operation(e, sql.Error, sql_handler)

        # No errors have occurred. Send the success message.
        conn.commit(), conn.close()
        Network.write(k, ['EK', 'Success'])

    @staticmethod
    def return_node_uris(k, r):
        """ Return a list of node URIs stored in the 'dtables' table, which informs the client
        machine (not this node) how to contact the cluster. Using the catalog database filename
        and the name of table associated with the cluster.

        :param k: Socket to send node URI list to.
        :param r: Command list passed through the same socket.
        :return: None.
        """
        f, tname = r[1], r[2]

        # Connect to the catalog.
        conn, cur = Database.connect(f, ErrorHandle.raise_handler)
        sql_handler = lambda e_n: Database.rollback_wrapper(e_n, ErrorHandle.raise_handler, conn)

        # Grab the node URIs belonging to the given table. Return the results.
        p = Database.execute(cur, 'SELECT nodeurl '
                                  'FROM dtables '
                                  'WHERE tname = ?', sql_handler, (tname,), True)
        conn.close()

        # If there exist no tables here, throw an error.
        if len(p) == 0:
            raise sql.Error(ErrorHandle.wrap_error_tag('Table ' + tname + ' not found.'))
        else:
            Network.write(k, ['EU', p])


class RemoteCatalog:
    """ All catalog operations on general nodes (i.e. calls the catalog node). These are
    non-fatal, and a string or boolean error is returned instead. """

    @staticmethod
    def ping(c):
        """ Given information about the catalog node, check if a connection is possible to the
        catalog node.

        :param c: The value of the key-value pair inside clustercfg.
        :return: True if a connection can be achieved. False otherwise.
        """
        host, port, f = ClusterCFG.parse_uri(c)

        # Create our socket and attempt to connect.
        sock = Network.open_client(host, port)
        if ErrorHandle.is_error(sock):
            return False

        # Send a dummy message. Response must not be an error.
        response = Network.write(sock, ['YY'])
        if ErrorHandle.is_error(response):
            return response

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

        # Only proceed if there exists at least one successful node.
        if len(success_nodes) == 0:
            return ErrorHandle.wrap_error_tag('No nodes were successful.')

        # Create our socket.
        sock = Network.open_client(host, port)
        if ErrorHandle.is_error(sock):
            sock.close()
            return ErrorHandle.wrap_error_tag('Socket could not be established.')

        # Otherwise, record the DDl.
        Network.write(sock, ['C', f, success_nodes, ddl])

        # Wait for a response to be sent back, and return the appropriate message.
        net_handler = lambda e: Network.close_wrapper(e, ErrorHandle.default_handler, sock)
        if not ErrorHandle.is_error(Network.read(sock, net_handler)):
            sock.close()
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
        sock = Network.open_client(host, port)
        if ErrorHandle.is_error(sock):
            return ErrorHandle.wrap_error_tag('Socket could not be established.')

        # Pickle our command list ('U', filename, and tname), and send our message.
        Network.write(sock, ['U', f, tname])

        # Wait for a response to be sent back, and record this response.
        net_handler = lambda e: Network.close_wrapper(e, ErrorHandle.default_handler, sock)
        response = Network.read(sock, net_handler)

        # If an error exists, return the error.
        if ErrorHandle.is_error(response):
            return response

        # Otherwise, return the node URIs.
        return [x[0] for x in response[1]]

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
        sock = Network.open_client(host, port)
        if ErrorHandle.is_error(sock):
            return ErrorHandle.wrap_error_tag('Socket could not be established.')

        # Pickle our command list ('K', f, r_d, numnodes), and send our message.
        Network.write(sock, ['K', f, r_d, numnodes])

        # Wait for a response to be sent back, and record this response.
        net_handler = lambda e: Network.close_wrapper(e, ErrorHandle.default_handler, sock)
        response = Network.read(sock, net_handler)

        # If an error exists, return the error.
        if ErrorHandle.is_error(response):
            return response

        # Otherwise, return the success message.
        return 'Success'