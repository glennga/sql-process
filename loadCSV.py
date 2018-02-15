# coding=utf-8
"""
TODO: Finish this description.
TODO: We assume that the CSV is valid.

Usage: python loadCSV.py [clustercfg] [csv]

Error: TODO: Finish the error codes here.
"""

import csv
import sys

import dissect

def nopart_load(n, c, r_d, f):
    """ TODO: Finish this description.

    :param n:
    :param c:
    :param r_d:
    :param f:
    :return:
    """

    # If there exists no partition, we execute each insert to every node in the cluster.

    # Read every line of the CSV.
    with open(f, 'rb') as csv_f:
        csv_r = csv.reader(csv_f)

        # Perform the insertion.

        # If there exists an error, print the error and break early.

def hashpart_load(n, c, r_d, f):
    """ TODO: Finish this description.
    TODO: Note assumption on param1 being an integer.

    :param n:
    :param c:
    :param r_d:
    :param f:
    :return:
    """
    h = lambda b : (b % int(r_d['param1'])) + 1

    # If there exists hash partitioning, determine the index of partitioned column.
    # TODO: Determine the index of the partition.

    # Read every line of the CSV.
    with open(f, 'rb') as csv_f:
        csv_r = csv.reader(csv_f)

        # Determine the node to insert the entry to using the hash function.

        # Perform the insertion.

        # If there exists an error, print the error and break early.

def rangepart_load(n, c, r_d, f):
    """ TODO: Finish this description.
    TODO: Note that the range is: partparam1 < partcol <= partparam2.

    :param n:
    :param c:
    :param r_d:
    :param f:
    :return:
    """

    # If there exists range partitioning, determine the index of partitioned column.
    # TODO: Determine the index.

    # Construct a list of ranges, ordered by node ids.
    r_bounds = zip(r_d['param1'], r_d['param2'])

    # Read every line of the CSV.
    with open(f, 'rb') as csv_f:
        csv_r = csv.reader(csv_f)

        # Determine the node to insert the entry to using the range.

        # Perform the insertion.

        # If there exists an error, print the error and break early.

if __name__ == '__main__':
    # Ensure that we only have 2 arguments.
    if len(sys.argv) != 3:
        print('Usage: python3 loadCSV.py [clustercfg] [csv]'), exit(2)

    # Dissect the given clustercfg for partitioning and catalog information.
    r = dissect.clustercfg_load(sys.argv[1])
    if not hasattr(r, '__iter__'):
        print('Error: ' + r), exit(3)
    catalog_uri, r_d = r

    # Grab the node URIs of our cluster.
    node_uris = []

    # Determine the partitioning. Use the appropriate load function when determined.
    [nopart_load, hashpart_load, rangepart_load][r_d['partmtd']](node_uris, catalog_uri, r_d, sys.argv[2])