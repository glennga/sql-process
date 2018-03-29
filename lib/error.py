# ALL ERROR HANDLING GOES HERE.

import struct
import pickle
import random
import socket
import time
import sqlite3 as sql


class ErrorHandle:
    @staticmethod
    def attempt_operation(operation, watch, handler=lambda _: None, result=False):
        """

        :param operation:
        :param watch:
        :param handler:
        :param result:
        :return:
        """
        r = ''
        try:
            r = operation()
        except watch as e:
            handler(e)
            return 'Error: ' + str(e)

        return r if result is True else 'Success'

    @staticmethod
    def create_client_socket(host, port):
        """

        :param host:
        :param port:
        :return:
        """
        sock = socket.socket()

        try:
            sock.connect((host, int(port)))
        except OSError as e:
            sock.close()
            return 'Error: ' + str(e)

        return sock

    @staticmethod
    def is_error(i):
        """

        :param i:
        :return:
        """
        # TODO: Finish
        return isinstance(i, str) and i.startswith('Error:')

    @staticmethod
    def open_file(f, on_file):
        """

        :param f:
        :param on_file:
        :return:
        """
        try:
            with open(f) as file_f:
                return on_file(file_f)
        except FileNotFoundError as e:
            return 'Error: ' + str(e)

    @staticmethod
    def write_socket(k, message):
        """ This method should not throw an error, but it's usage is very error prone. Hence,
        it's place here ):< Here, we prepend the message length to our message prior to sending.
        The socket reading should account for this.

        :param k:
        :param message:
        :return: None.
        """
        packet = pickle.dumps(message)
        packet = struct.pack('!I', len(packet)) + packet

        # Send our message through the socket.
        k.send(packet)


    @staticmethod
    def read_socket(k, handler=lambda _: None):
        """

        :param k:
        :param handler:
        :return:
        """
        # Read our socket for the length.
        buf = b''
        while len(buf) < 4:
            buf += k.recv(4 - len(buf))
        ell = struct.unpack('!I', buf)[0]

        # Read the packet from the socket, using the given length.
        try:
            packet = k.recv(ell)
            return pickle.loads(packet)

        except Exception as e:
            # Something bad happened...
            handler(e)
            return 'Error: ' + str(e)

    @staticmethod
    def sql_connect(f, handler=lambda _: None):
        """

        :param f:
        :param handler:
        :return:
        """
        try:
            conn = sql.connect(f)
        except sql.Error as e:
            handler(e)
            return 'Error: Could not connect to the database.'

        return conn, conn.cursor()

    @staticmethod
    def sql_execute(cur, s, handler=lambda _: None, tup=None, fetch=False, desc=False):
        """

        :param cur:
        :param s:
        :param handler:
        :param tup:
        :param fetch:
        :param desc:
        :return:
        """
        r = ''

        try:
            # Execute a prepared statement if tuples are passed.
            if tup is None and desc is False:
                r = cur.execute(s).fetchall()
            elif tup is None and desc is True:
                r = cur.execute(s).description
            else:
                r = cur.execute(s, tup).fetchall()
        except sql.Error as e:
            handler(e)
            return 'Error: ' + str(e)

        # Fetch the results of the fetch if desired.
        return 'Success' if fetch is False else r
