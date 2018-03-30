# coding=utf-8
"""
Contains functions to manage error handling. The approach I use here is "ask for forgiveness"
rather than actively trying to prevent the error from popping up.

Usage: ErrorHandle.default_handler(string_or_exception)
       ErrorHandle.fatal_handler(string_or_exception)
       ErrorHandle.raise_handler(string_or_exception)

       ErrorHandle.is_error(error_string)
       ErrorHandle.wrap_error_tag(string_or_exception)

       ErrorHandle.act_upon_error(string_or_exception, handler, is_return_result)
       ErrorHandle.attempt_operation(operation, exception_to_watch, handler, is_return_result)
"""


class ErrorHandle:
    """
    All error handling operations. The approach used here is to "ask for forgiveness" as opposed
    to premature error prevention. Users can pass handlers to deal with exception if thrown,
    or use more common ones here.
    """

    @staticmethod
    def default_handler(_):
        """ Default error handler for exception. Does nothing.

        :param _: The exception thrown.
        :return: None.
        """
        pass

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
    def is_error(i):
        """ Given an object returned by some method here, we check for the error tag. The error
        tag used here is the prefix 'Error: '.

        :param i: Object to check error for.
        :return: True if the passed object is an *error string* (not exception). False otherwise.
        """
        return isinstance(i, str) and i.startswith('Error:')

    @staticmethod
    def wrap_error_tag(i):
        """ Wrap an exception or string with the error tag. This allows this object to be
        recognized as an error by 'is_error'.

        :param i: Object to wrap an error tag around.
        :return: Error string with 'i' casted to a string.
        """
        if ErrorHandle.is_error(i):
            return i
        elif isinstance(i, str):
            return 'Error: ' + i
        else:
            return 'Error: ' + str(i)

    @staticmethod
    def act_upon_error(i, handler=default_handler, result=False):
        """ If 'i' represents an error, then pass it through the given handler. Otherwise,
        return the object or a success message. Useful shorthand for constant error checking.

        :param i: Object to be checked.
        :param handler: Operation to perform if 'i' represents an error.
        :param result: Flag to return the passed object or not.
        :return: A success message if 'result' is not raised and the passed object does not
            represent and error. 'i' otherwise.
        """
        if ErrorHandle.is_error(i):
            handler(i)
        else:
            return 'Success' if not result else i

    @staticmethod
    def attempt_operation(operation, watch, handler=default_handler, result=False):
        """ Attempt some operation and watch for some exception. In the event we catch what we
        are looking for, handle the operation. This differs from 'act_upon_error' in that we
        perform our operation here, instead of being passed the result and performing the
        checking afterward.

        :param operation: Operation to perform in try-catch.
        :param watch: Exception to watch for.
        :param handler: Operation to perform if we catch our exception.
        :param result: Flag to return the result of the operation or not.
        :return: A success message if 'result' is not raised and the operation does not throw.
            The result of the operation otherwise.
        """
        try:
            r = operation()
        except watch as e:
            handler(e)
            return 'Error: ' + str(e).replace('Error: ', '')

        return r if result is True else 'Success'
