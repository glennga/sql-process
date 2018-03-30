# coding=utf-8
"""
Contains functions to run tasks in parallel, be it through threads or processes.

Usage: Parallel.execute_n(iterable, operation, argument_constructor)
       Parallel.execute_nm(outer_iterable, inner_iterable, operation, argument_constructor)

       Parallel.spawn_process(operation, argument_list)
       Parallel.check_children()
"""

from multiprocessing import Process, active_children
from threading import Thread


class Parallel:
    """
    All functions meant to handle parallel execution of some operation. Processes are used by the
    parDBd daemon to keep it's listening port open, and threads are used for all other parallel
    tasks.
    """

    @staticmethod
    def execute_n(n, operation, argument_constructor):
        """ Execute some operation on every element in some list. The specifics on how the
        arguments are presented to the operation are detailed in 'argument_constructor'.

        :param n: Iterable to pass elements to the given operation.
        :param operation: Operation to execute in parallel.
        :param argument_constructor: Creates the argument tuple given elements from n.
        :return: None.
        """
        threads = []

        # Iterate through N, and construct the appropriate thread based on each iteration.
        for i, b in enumerate(n):
            threads.append(Thread(target=operation, args=argument_constructor(i, b)))
            threads[-1].start()

        # Wait for all of our threads to finish.
        [d.join() for d in threads]

    @staticmethod
    def execute_nm(n, m, operation, argument_constructor):
        """ Execute some operation on every element in two lists. List M specifies items that can
        be run in parallel for some element in list N. The specifics on how the arguments are
        presented to the operation are detailed in 'argument_constructor'.

        :param n: Outer iterable to pass elements to the given operation. This is **serial**.
        :param m: Inner iterable to pass elements to the given operation. This is **parallel**.
        :param operation: Operation to execute in parallel.
        :param argument_constructor: Creates the argument tuple given elements from n and m.
        :return: None.
        """
        threads = []

        # Iterate through N, for every M, and construct the appropriate thread.
        for i, b_1 in enumerate(n):
            for j, b_2 in enumerate(m):
                threads.append(Thread(target=operation, args=argument_constructor(i, j, b_1, b_2)))
                threads[-1].start()

            # Wait for every b_1 thread set to finish.
            [d.join() for d in threads]

    @staticmethod
    def spawn_process(operation, arguments):
        """ Execute some operation given an argument tuple as another process (not a thread).
        This allows for true multithreading, as Python's thread library only spawns user threads.

        :param operation: Operation to execute in parallel.
        :param arguments: Arguments tuple to pass to the current operation.
        :return: None.
        """
        p = Process(target=operation, args=arguments)
        p.start()

    @staticmethod
    def check_children():
        """ List of active processes spawned from the current process. This implicitly calls the
        join method, and is used in zombie prevention.

        :return: List of active processes spawned from the current process.
        """
        return active_children()
