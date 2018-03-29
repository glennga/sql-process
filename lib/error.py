# ALL ERROR HANDLING GOES HERE.

import struct
import pickle
import random
import socket
import time
import sqlite3 as sql
import re

class ErrorHandle:
    """

    """

    @staticmethod
    def default_handler(_):
        """ Default error handler for exception. Does nothing.

        :param _: The exception thrown.
        :return: None.
        """
        return None

    @staticmethod
    def fatal_handler(e):
        """ Handler that exits with an error message, and an error code of -1.

        :param e: The exception thrown.
        :return: None.
        """
        # If 'Error: ' exists in the string, strip it out before printing.
        clean_e = e.replace('Error: ', '') if isinstance(e, str) else e
        print('Error: ' + str(clean_e))
        exit(-1)

    @staticmethod
    def raise_handler(e):
        """ Handler that reraises the given exception.

        :param e: The exception thrown.
        :return: None.
        """
        raise e

    @staticmethod
    def attempt_operation(operation, watch, handler=default_handler, result=False):
        """

        :param operation:
        :param watch:
        :param handler:
        :param result:
        :return:
        """
        try:
            r = operation()
        except watch as e:
            handler(e)
            return 'Error: ' + str(e).replace('Error: ', '')

        return r if result is True else 'Success'

    @staticmethod
    def is_error(i):
        """

        :param i:
        :return:
        """
        # TODO: Finish
        return isinstance(i, str) and i.startswith('Error:')

    @staticmethod
    def act_upon_error(i, handler=default_handler, result=False):
        """

        :param i:
        :param handler:
        :param result
        :return:
        """
        if ErrorHandle.is_error(i):
            handler(i)
        else:
            return 'Success' if not result else i

    @staticmethod
    def wrap_error_tag(i):
        """

        :param i:
        :return:
        """
        if ErrorHandle.is_error(i):
            return i
        elif isinstance(i, str):
            return 'Error: ' + i
        else:
            return 'Error: ' + str(i)