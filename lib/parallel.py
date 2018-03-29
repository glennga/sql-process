from threading import Thread

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
