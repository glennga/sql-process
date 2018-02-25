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

import pickle
import socket
import sqlite3 as sql

from dissect import SQLFile


class LocalCatalog:
    """ All catalog operations for the catalog node itself (operating locally). """

    @staticmethod
    def create_dtable(f):
        """ Given the name of the database file, create the table. This information is specified in
        the homework assignment.

        :param f: Location of the database file to store this table on.
        :return: None.
        """
        conn = sql.connect(f)

        cur = conn.cursor()
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
        conn = sql.connect(f)
        cur = conn.cursor()
        LocalCatalog.create_dtable(f)

        # Determine the table being operated on in the DDL.
        table = SQLFile.table(ddl)
        if table is False:
            k.send(pickle.dumps('No table found. SQL statement formatted incorrectly.'))
            return

        # Assemble our tuples and perform the insertion/deletion.
        try:
            if SQLFile.is_drop_ddl(ddl):
                cur.execute('DELETE FROM dtables WHERE tname = ?', (table,))
            else:
                e = cur.execute('SELECT 1 FROM dtables WHERE tname = ? LIMIT 1',
                                (table,)).fetchone()

                # If the table exists, delete all entries before performing the insertion.
                if e is not None:
                    cur.execute('DELETE FROM dtables WHERE tname = ?', (table,))

                tuples = [(table, node_uris[i], i + 1) for i in range(len(node_uris))]
                cur.executemany('INSERT INTO dtables VALUES (?, NULL, ?, NULL, NULL, NULL, ?, '
                                'NULL, NULL, NULL)', tuples)
        except sql.Error as e:
            k.send(pickle.dumps(str(e))), conn.rollback(), conn.close()
            return

        # No error exists. Commit and send the success message.
        conn.commit(), conn.close()
        k.send(pickle.dumps(['EC', 'Success']))

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
        conn = sql.connect(f)
        cur = conn.cursor()

        try:
            # Range partitioning has been specified. Create the appropriate entries.
            if r_d['partmtd'] == 1:
                for i in range(1, numnodes + 1):
                    cur.execute('UPDATE dtables SET partcol = ?, partparam1 = ?, partparam2 = ?, '
                                'partmtd = 1 WHERE nodeid = ? AND tname = ?',
                                (r_d['partcol'], r_d['param1'][i - 1], r_d['param2'][i - 1],
                                 i, r_d['tname']))

            # Hash partitioning has been specified. Create the appropriate entries.
            elif r_d['partmtd'] == 2:
                for i in range(1, numnodes + 1):
                    cur.execute('UPDATE dtables SET partcol = ?, partparam1 = ?, partmtd = 2 '
                                'WHERE nodeid = ? AND tname = ?',
                                (r_d['partcol'], r_d['param1'], i, r_d['tname']))
        except sql.Error as e:
            k.send(pickle.dumps(str(e))), conn.rollback(), conn.close()

        # No errors have occured. Send the success message.
        conn.commit(), conn.close()
        k.send(pickle.dumps(['EK', 'Success']))

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

        # Connect to SQLite database using the filename.
        conn = sql.connect(f)
        cur = conn.cursor()

        # Grab the node URIs belonging to the given table. Return the results.
        try:
            p = cur.execute('SELECT nodeurl FROM dtables WHERE tname = ?', (tname,)).fetchall()
            k.send(pickle.dumps('Table ' + tname + ' not found.' if len(p) == 0 else ['EU', p]))
            conn.close()
        except sql.Error as e:
            k.send(pickle.dumps(str(e))), conn.rollback(), conn.close()


class RemoteCatalog:
    """ All catalog operations on general nodes (i.e. calls the catalog node). """

    @staticmethod
    def ping(c):
        """ Given information about the catalog node, check if a connection is possible to the
        catalog node.

        :param c: The value of the key-value pair inside clustercfg.
        :return: True if a connection can be achieved. False otherwise.
        """
        host, port = c.split(':', 1)
        port = port.split('/', 1)[0]

        # Create our socket and attempt to connect.
        sock = socket.socket()
        try:
            sock.connect((host, int(port)))
        except OSError:
            sock.close()
            return False

        sock.close()
        return True

    @staticmethod
    def record_ddl(catalog_uri, success_nodes, ddl):
        """ Given information about the cluster and the DDL statement to execute, store the DDL
        in the catalog node.

        :param catalog_uri: The URI associated with the catalog.
        :param success_nodes: List containing the URIs of the successfully executed nodes.
        :param ddl: DDL statement to pass to the catalog node.
        :return The resulting error if the appropriate response is not returned successfully. True
            otherwise.
        """
        host, port = catalog_uri.split(':', 1)
        port, f = port.split('/', 1)

        # Create our socket.
        sock = socket.socket()
        try:
            sock.connect((host, int(port)))
        except OSError:
            sock.close()
            return 'Socket could not be established.'

        # Pickle our command list ('C', cluster, and DDL), and send our message.
        sock.send(pickle.dumps(['C', f, success_nodes, ddl]))

        # Wait for a response to be sent back, and return this response.
        response = sock.recv(4096)
        r = pickle.loads(response) if response != b'' else 'Failed to receive response.'
        sock.close()

        # String response indicates an error. Return this.
        if isinstance(r, str):
            return r
        elif r[0] == 'EC' and r[1] == 'Success':
            return True

    @staticmethod
    def return_node_uris(catalog, tname):
        """ Given the catalog URI and the name of table, grab the node URIs from the catalog node.

        :param catalog: The URI associated with the catalog.
        :param tname: Name of the table associated with the cluster to search.
        :return: The resulting error if the appropriate response is not returned successfully.
            Otherwise, a list of node URIs.
        """
        host, port = catalog.split(':', 1)
        port, f = port.split('/', 1)

        # Create our socket.
        sock = socket.socket()
        try:
            sock.connect((host, int(port)))
        except OSError:
            sock.close()
            return 'Socket could not be established.'

        # Pickle our command list ('U', filename, and tname), and send our message.
        sock.send(pickle.dumps(['U', f, tname]))

        # Wait for a response to be sent back, and record this response.
        response = sock.recv(4096)
        r = pickle.loads(response) if response != b'' else 'Failed to receive response.'
        sock.close()

        # A list returned indicates the message was successful. Flatten the returned list.
        if r[0] == 'EU':
            return [x[0] for x in r[1]]
        else:
            # Otherwise, an error exists. Return the error.
            return r

    @staticmethod
    def update_partition(catalog, r_d, numnodes):
        """ Given information about the catalog and partitioning information, store the partition
        in the catalog node.

        :param catalog: The URI associated with the catalog.
        :param r_d: Dictionary containing partitioning entries.
        :param numnodes: Number of nodes associated with the cluster (only required for hash and
            range partitioning).
        :return: The resulting error if the appropriate response is notreturned successfully.
            Otherwise, true.
        """
        host, port = catalog.split(':', 1)
        port, f = port.split('/', 1)

        # Create our socket.
        sock = socket.socket()
        try:
            sock.connect((host, int(port)))
        except OSError:
            sock.close()
            return 'Socket could not be established.'

        # Pickle our command list ('K', f, r_d, numnodes), and send our message.
        sock.send(pickle.dumps(['K', f, r_d, numnodes]))
        response = sock.recv(4096)
        try:
            r = pickle.loads(response) if response != b'' else 'Failed to receive response.'
        except EOFError as e:
            return str(e)

        # String response indicates an error. Return this.
        if isinstance(r, str):
            return str(r)
        elif r[0] == 'EK' and r[1] == 'Success':
            return True
