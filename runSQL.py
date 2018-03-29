# coding=utf-8
"""
Executes various functions on a cluster of nodes. Function is determined using the given arguments.

| Cluster config file contains... | SQL file contains... | Function
| tablename                       | N/A                  | Bulk load a CSV to the cluster.
| NOT tablename                   | DDL                  | Execute a DDL across the cluster.
| NOT tablename                   | No joins             | Execute (simple) SQL across the cluster.
| NOT tablename                   | Joins are present    | Execute (join) SQL across the cluster.

Usage: python runSQL.py [clustercfg] [sqlfile/csvfile]

TODO: Finish the error codes below.
Error: 2 - Incorrect number of arguments.
       3 - There exists an error with the clustercfg file.
       4 - There exists an error with the SQL file.

"""

import os
import subprocess
import sys

from lib.dissect import ClusterCFG, SQLFile
from lib.error import ErrorHandle

if __name__ == '__main__':
    # Ensure that we have only two arguments.
    if len(sys.argv) != 3:
        print('Usage: python3 runSQL.py [clustercfg] [sqlfile/csvfile]'), exit(2)

    r = ClusterCFG.is_runLSCV(sys.argv[1])
    if ErrorHandle.is_error(r):
        print('Error: ' + r), exit(3)

    # The user wants to load a CSV.
    if r:
        print('Desired Function: Bulk CSV load')
        exit(subprocess.call(['python' + ('3' if (os.name != 'nt') else ''), 'run/runLCSV.py',
                              sys.argv[1], sys.argv[2]]))

    # The user wants to execute some SQL, we look at the SQL file.
    s = SQLFile.as_string(sys.argv[2])
    if ErrorHandle.is_error(s):
        print(s), exit(4)

    # The user wants to execute a DDL.
    if SQLFile.is_ddl(s):
        print('Desired Function: Cluster DDL Execution')
        exit(subprocess.call(['python' + ('3' if (os.name != 'nt') else ''), 'run/runDDL.py',
                              sys.argv[1], sys.argv[2]]))
    elif SQLFile.is_join(s):
        # The user wants to execute SQL involving a join.
        print('Desired Function: Cluster SQL Selection with Join')
        exit(subprocess.call(['python' + ('3' if (os.name != 'nt') else ''), 'run/runDJSQL.py',
                              sys.argv[1], sys.argv[2]]))
    else:
        # The user wants to execute SQL without joins.
        print('Desired Function: Cluster SQL Execution')
        exit(subprocess.call(['python' + ('3' if (os.name != 'nt') else ''), 'run/runSSQL.py',
                              sys.argv[1], sys.argv[2]]))
