from threading import Thread
from multiprocessing import Process, active_children

class Parallel:
    """

    """

    @staticmethod
    def execute_n(n, operation, argument_constructor):
        """

        :param n:
        :param operation:
        :param argument_constructor:
        :return:
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
        """

        :param n:
        :param m:
        :param operation:
        :param argument_constructor:
        :return:
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
        """

        :param operation:
        :param arguments:
        :return:
        """
        p = Process(target=operation, args=arguments)
        p.start()

    @staticmethod
    def check_children():
        """

        :return:
        """
        return active_children()