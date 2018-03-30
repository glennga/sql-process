# coding=utf-8
"""
Contains function to communicate with another process through sockets. Prior to sending and
unwrapping, messages are formatted as such:

| message_length ------- | [operation_code, data] --------- |  Operation Packet
| message_length ------- | error_string ------------------- |  Error Packet

Usage: Network.close_wrapper(exception, handler, socket_connection)
       Network.open_client(host, port, handler)
       Network.open_server(host, port, handler)
       Network.write(socket, message)
       Network.read(socket, handler)

"""

import pickle
import socket
import struct

from lib.error import ErrorHandle


class Network:
    """
    All socket operations. This includes socket creation, writing, and reading. The serialization
    and message length prefixing are handled here as well.
    """

    @staticmethod
    def close_wrapper(e, handler, sock):
        """ Handler wrapper to close the current sock. This is meant to be wrapped in another
        lambda to fit the normal handler signature.

        :param e: Exception to pass to the handler.
        :param handler: Handler to use with the given exception.
        :param sock: Socket to close.
        :return: None.
        """
        # Close the connection to the socket.
        sock.close()
        handler(e)

    @staticmethod
    def open_client(host, port, handler=ErrorHandle.default_handler):
        """ Create a client socket and connect to some host and a given port.

        :param host: Host to connect to.
        :param port: Port to connect to host through, given as a string.
        :param handler: Handler to use if a socket cannot be created.
        :return: String containing the error if a socket cannot be crated. The client socket
            otherwise.
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
        """ Create a server socket and listen on a given port.

        :param host: Host to bind to.
        :param port: Port to listen for connections on, given as a string.
        :param handler: Handler to use if a socket cannot be created.
        :return: String containing the error if a socket cannot be crated. The server socket
            otherwise.
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
        """ Send a formatted packet through the socket. The packet consists of a prefixed message
        length, and the message itself.

        :param k: Socket to send the message through.
        :param message: Message to send to socket.
        :return: None.
        """
        packet = pickle.dumps(message)
        packet = struct.pack('!I', len(packet)) + packet

        # Send our message through the socket.
        k.send(packet)

    @staticmethod
    def read(k, handler=ErrorHandle.default_handler):
        """ Receive a formatted packet through the socket. Read the prefixed message length
        first, which determines how many bytes to read afterward. Return the unwrapped packet.

        :param k: Socket to receive the message through.
        :param handler: Handler to use if the message cannot be read.
        :return: A string containing the error if the message cannot be read. Otherwise,
            the message sent by the other end of the socket.
        """
        # Read our socket for the length.
        buf = b''
        while len(buf) < 4:
            buf += k.recv(4 - len(buf))
        ell = struct.unpack('!I', buf)[0]

        # Read our packet and return the result (error or not).
        return ErrorHandle.attempt_operation(lambda: pickle.loads(k.recv(ell)),
                                             Exception, handler, True)
