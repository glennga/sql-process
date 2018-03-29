import pickle
import socket
import struct

from lib.error import ErrorHandle


class Network:
    """

    """
    @staticmethod
    def close_wrapper(e, handler, sock):
        """

        :param e:
        :param conn:
        :return:
        """
        # Close the connection to the socket.
        sock.close()
        handler(e)


    @staticmethod
    def open_client(host, port, handler=ErrorHandle.default_handler):
        """

        :param host:
        :param port:
        :param handler:
        :return:
        """
        sock = socket.socket()

        # Attempt to connect to the given host.
        r = ErrorHandle.attempt_operation(lambda: sock.connect((host, int(port))),
                                          OSError, handler, False)

        # Close this socket if this is not successful.
        if ErrorHandle.is_error(r):
            sock.close()
            return r
        else:
            return sock

    @staticmethod
    def open_server(host, port, handler=ErrorHandle.default_handler):
        """

        :param host:
        :param port:
        :param handler:
        :return:
        """
        sock = socket.socket()

        # Attempt to bind the socket to the port.
        r = ErrorHandle.attempt_operation(lambda: sock.bind((host, int(port))),
                                          OSError, handler, False)

        # Close this socket if this is not successful.
        if ErrorHandle.is_error(r):
            sock.close()
            return r
        else:
            return sock

    @staticmethod
    def write(k, message):
        """

        :param k:
        :param message:
        :return:
        """
        packet = pickle.dumps(message)
        packet = struct.pack('!I', len(packet)) + packet

        # Send our message through the socket.
        k.send(packet)

    @staticmethod
    def read(k, handler=ErrorHandle.default_handler):
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

        # Read our packet and return the result (error or not).
        return ErrorHandle.attempt_operation(lambda: pickle.loads(k.recv(ell)),
                                             Exception, handler, True)
