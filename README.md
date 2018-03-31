# Naive SQL Processing for Parallel DBMS
## Overview

**GitHub Repository located here:** https://github.com/glennga/sql-process

This repository contains code to execute a SQLite statement on every node in a cluster, and to load data from a CSV into the cluster using different partitioning methods.

The node that initiates these operations is known as the *client* node. This client node can exist as a part of the cluster, or completely separate from the cluster. The nodes that hold the data (in databases) will be referred to as *server* nodes. There exists a special server node that holds information about how to reach each node in the cluster and metadata about the cluster itself (partitions, number of nodes, etc...). This node is known as the *catalog* node.

Client operations that involve execution of a single statement across several machines (running a DDL or a SQL statement without joins) are depicted below:

![](images/run-simple-diagram.png)

If a client wishes to load a CSV file onto the cluster, the actions below occur:

![](images/run-load-diagram.png)

For all operations that involve a join between two (no more, no less) tables across the cluster, the following occur:

![](images/run-join-diagram.png)

The repository has been tested with the TPC-H benchmark, broken into comments and orders tables. These can be found in the `test/data` folder.

## Getting Started
1. To get started, all nodes in your cluster should have `python3` and `git`. To access your data outside of Python, install `sqlite3` as well:
    ```
    apt-get install -y python3 python3-pip git sqlite3
    ```

2. Install `antlr4` and `configparser` for Python.
    ```
    pip3 install antlr4-python3-runtime
    pip3 install configparser
    ```

3. With all of these installed, clone this repository onto your client and every node in your cluster.
    ```
    git clone https://github.com/glennga/sql-process.git
    ```

4. The client node is the node that will perform some operation on the cluster of nodes. Every client node must contain a `clustercfg` file which holds some description of the cluster. *This file will vary depending on your desired client operation.* More specifications on this format is specified in the **Format of File: clustercfg** section below.

    An example `clustercfg` is depicted to create a table (DDL execution) across all nodes of a three-node system:
    ```
    catalog.hostname=10.0.0.3:50001/cat.db

    numnodes=3
    node1.hostname=10.0.0.3:50001/node1.db
    node2.hostname=10.0.0.3:50002/node2.db
    node2.hostname=10.0.0.3:50003/node3.db
    ```

    Another example is depicted to load a CSV file onto the cluster using hash partitioning on a three-node system:
    ```
    catalog.hostname=10.0.0.3:50001/mycatdb

    tablename=BOOKS

    partition.method=hash
    partition.column=isbn
    partition.param1=3
    ```

    Another example file for executing SQL on the cluster is depicted below:
    ```
    catalog.hostname=10.0.0.3:50001/cat.db
    ```

4. Create the desired second argument file, which must also be stored on your client node. *This file also varies for each client program.* More specifications can be found in the **Format of File: csv** and **Format of File: sqlfile (or ddlfile)**  sections below.

    An example of a DDL file is depicted below:
    ```
    CREATE TABLE BOOKS(isbn char(14), title char(80), price
    decimal);
    ```

    An example of a SQL file is depicted below:
    ```
    SELECT * FROM BOOKS;
    ```

    An example of a CSV file is depicted below:
    ```
    123323232,Database Systems,Ramakrishnan,Raghu
    234323423,Operating Systems,Silberstein,Adam
    ```

5. For each node in your cluster, navigate to this repository and start the server daemon. Specify the hostname in the first argument, and the port number in the second:
    ```
    python3 parDBd.py [hostname] [port]
    ```

6. For an empty cluster, use `runSQL.py` with the `clustercfg` specified as the first argument, and the `ddlfile` in the second:
    ```
    python3 runSQL.py [clustercfg] [ddlfile]
    ```

    If this is successful, you should see some variant of the output below:
    ```
    Successful Execution on Node: 2
    Successful Execution on Node: 3
    Successful Execution on Node: 1

    Summary:
    Node 1[10.0.0.3:50001/node1.db]: Successful
    Node 2[10.0.0.3:50002/node2.db]: Successful
    Node 3[10.0.0.3:50003/node3.db]: Successful
    ```

7. With a table defined, the next operation that normally follows is the insertion of data. Use `runSQL.py` with the `clustercfg` specified as the first argument, and the CSV to load in the second:
    ```
    python3 runSQL.py [clustercfg] [csv]
    ```

    If this is successful, you should see the output below:
    ```
    Insertion was successful.
    Catalog node has been updated with the partitions.
    ```

9. To view the data you just inserted, again use `runSQL.py` with the `clustercfg` specified as the first argument, and the `sqlfile` in the second:

    ```
    python3 runSQL.py [clustercfg] [sqlfile]
    ```

    If this is successful, you should see some variant of the output below:
    ```
    Node 1: | | No tuples found. | |
    Node 2: | | No tuples found. | |
    Node 3: | | 123323232 | Database Systems | Ramakrishnan | Raghu | |
    Node 3: | | 234323423 | Operating Systems | Silberstein | Adam | |

    Summary:
    Node 1[10.0.0.3:50001/node1.db]: Successful
    Node 2[10.0.0.3:50002/node2.db]: Successful
    Node 3[10.0.0.3:50003/node3.db]: Successful
    ```


## Usage
### Directory Overview
```
sql-process/
|-- lib/
    |-- __init__.py
    |-- parse/
       |-- __init__.py
       |-- SQLite.g4
       |-- * ANTLR generated files *
    |-- catalog.py
    |-- database.py
    |-- dissect.py
    |-- error.py
    |-- listen.py
    |-- network.py
    |-- parallel.py
|-- test/
    |-- data/
        |-- comments.csv
        |-- orders.csv
        |-- * SQLite database files * 
    |-- runDDL/
        |-- * runDDL test files *
    |-- runLCSV/
        |-- * runLCSV test files *
    |-- runSSQL/
        |-- * runSSQL test files *
    |-- runJSQL/
        |-- * runJSQL test files *
|-- parDBd.py
|-- runSQL.py
|-- runDDL.py
|-- runLCSV.py
|-- runSSQL.py
|-- runJSQL.py
```

All libraries are defined in the `lib` folder. These contain functions that are shared among several of the client and server programs. Also included here are the generated ANTLR files, used to generate and walk a parse tree for some SQLite statement.

The main programs are `runSQL.py` (client) and `parDBd.py` (server daemon). `runSQL.py` determines the desired function by reading the passed configuration file (`clustercfg`) and the second argument (`csv` or `sqlfile`). Each function exists as it's own client program, and can be used with or without the use of `runSQL.py`.

All tests are located in the `test` folder. This tests each function of `runSQL.py`: `runDDL.py`, `runLCSV.py`, `runSSQL.py`, and `runJSQL.py`.

### Format of File: clustercfg
The `clustercfg` file holds information about the cluster required to perform the desired operation. For each `clustercfg` file:

1. Each entry must be formatted as a `<key>=<value>` pair. There must exist no spaces between the `=` character for the key and the value.
2. The key `catalog.hostname` must exist.

Every catalog node entry must be formatted as such:
```
catalog.hostname=[catalog-hostname]:[catalog-port]/[catalog-database-file]
```
1. There must exist a colon character separating the hostname and the port, with no spaces between the two.
2. There must exist a forward slash character separating the port and the database file, with no spaces between the two.

The specifics for each `clustercfg` file are listed below. The `Key Format` and `Value Format` are specified like `<key>=<format>` in `clustercfg`:

Client Program | Key Format | Value Format | Description
--- | ---  | --- | ---
`runDDL.py` | `numnodes` | `[number of nodes]` |Specifies the number of nodes in the cluster.
`runDDL.py` | `node[node-id].hostname` | `[node hostname]:[node port]/[database file]` | Specifies the URIs of each node in the cluster. See special instructions below.
`runLCSV.py` | `tablename` | `[name of table]` | Specifies the table that exists in the cluster (logged in the catalog node) to insert the data to.
`runLCSV.py` | `partition.method` | `[hash, range, notpartition]` | Specifies the partition method used to insert the data with. **This must be in the space [hash, range, notpartition]**.
`runLCSV.py` (hash or range partitioning) | `partition.column` | `[column in table]` | Specifies the column to use with the partition. This **must** be a numeric column.
`runLCSV.py` (hash partitioning) | `partition.param1` | `[number of nodes in cluster]` | Specifies the number of nodes in the cluster. The hash function (simple mod-based hashing) used corresponds to this number.
`runLCSV.py` (range partitioning) | `numnodes` | `[number of nodes]` | Specifies the number of nodes in the cluster.
`runLCSV.py` (range partitioning) | `partition.node[node-id].param1` | `[floor of specific column]` | Species the minimum value of the specified column that this node will store. A value of `-inf` can be used to represent a limitless lower bound. See special instructions below. This **must** be less than the corresponding `param2`.
`runLCSV.py` (range partitioning) | `partition.node[node-id].param2` | `[ceiling of specific column]` | Species the maximum value of the specified column that this node will store. A value of `+inf` can be used to represent a limitless upper bound. See special instructions below. This **must** be greater than the corresponding `param1`.

For all `node[node-id]` and `partition.node[node-id].param[1/2]` entries:
1. Node-IDs are 1-indexed. The first node must start at 1, and the last node must end at `N = numnodes`.
2. There must exist no gaps between the indexing of nodes IDs (e.g. no `node1`, `node2`, `node5`, `node6`).
3. There must exist a colon character separating the hostname and the port, with no spaces between the two.
4. There must exist a forward slash character separating the port and the database file, with no spaces between the two.

### Format of File: csv
The `csv` file holds a list of tuples to insert into your cluster.

1. Tuples must be normalized (of the same length, no missing entries).
2. Tuple fields must be separated by commas.
3. Tuples themselves must be separated by newlines.
4. The number of fields for a given tuple must match the number of fields in the specified table.

### Format of File: sqlfile (or ddlfile)
The SQL file holds a single SQLite statement to execute on all nodes in the cluster.

1. This statement must be written in SQLite 3 (https://www.sqlite.org/lang.html).
2. This statement must be terminated with a semicolon.
3. There must only exist one statement. If there exists multiple statements in this file, then only the first will be executed.

### Client Program: runDDL.py
The `runDDL.py` file holds the code to create a table across all nodes in the catalog, and to setup the required metadata in the catalog node. The arguments to this script are the cluster configuration file and the DDL statement to execute:
```
python3 runDDL.py [clustercfg] [ddlfile]
python3 runSQL.py [clustercfg] [ddlfile]
```

Using the given arguments, the following occurs:
1. Collect the catalog URI from the `clustercfg` file. Collect the DDL statement from the `ddlfile`. If this cannot be performed, the program exits with an error.
2. Test the connection to the catalog node. This is meant to prioritize the logging of the metadata over executing statements without any history. If this is not successful, then an error is returned to the console and the program exits.
3. Collect the node URIs from the `clustercfg` file. If an error exists with the node formatting here, the program exits with an error.
4. If a connection to the catalog is able to be established, then the an execution command list is sent. This occurs in parallel, and spawns `N = numnodes` threads.
    1. For each thread (node), an attempt to connect to a socket is made. If this is not successful, then an error message is printed to the console and the program exits here.
    2. If the connection is successful, then the execution command list (specified in the **Protocol Design** section) is pickled and sent over the socket.
    3. A response command list from the daemon is waited for. If there exists no response or the response is an error, then a message is printed to the console and the program exits here.
    5. Otherwise, a success message is printed to the console. The success is recorded in a list (denoted as `successful_nodes` here), and the connection to the daemon is closed.
5. Once all threads are done executing, log all successful execution commands from the `successful_nodes` list to the catalog node. If this is not successful, the program exits with an error.
6. Otherwise, a summary is printed.

### Client Program: runLCSV.py

The `runLCSV.py` file holds the code to load a comma-separated-file of tuples to a cluster of nodes. The location of each tuple is determined by the partitioning specified in the cluster configuration file. The arguments to this script are the cluster configuration file and the CSV of the tuples.

```
python3 loadCSV.py [clustercfg] [csv]
python3 runSQL.py [clustercfg] [csv]
```

Using the given arguments, the following occurs:

1. Collect the catalog URI, and the partitioning information from the `clustercfg` file. If the `clustercfg` file is not properly formatted, the program exits with an error.
2. Collect the node URIs from the catalog node. If this is not successful, then an error is returned to the console and the program exits.
3. Verify the partitioning parameters collected with the number of node URIs retrieved. For hash partitioning, this means that `partition.param1 = |node URIs|`. For range partitioning, this means that `numnodes = |node URIs|`.
4. Collect the columns from the first node in the node URIs list. If this is not successful, then an error is printed to the console and the program exits with an error.
5. Connect to all nodes in the cluster. If any of these cannot be reached, then an error is printed to the console and the program exits with an error to preserve ACID. It wouldn't be ideal to only insert some of the data.
6. Execute the appropriate insertion based on the specified `partition.method` parameter.
   - If `nopartition` is specified, then the insertion is performed across all nodes. A SQL statement is prepared and sent over the socket with the rows to insert. This operation is performed for each node, serially (starting at node 1, ending at node N).
   - If `hash` is specified, then a tuple is assigned to a node using the simple hash function: `H(X) = (column mod partition.param1) + 1`. The value for `column` is found by determining the index of the specified `partition.column` for a given line in the CSV. The SQL statement is prepared appropriately and stored in memory. Once all statements are constructed, the appropriate SQL and parameters are sent to each appropriate socket, serially (starting at node 1, ending at node N).
   - If `range` is specified, then a tuple is assigned to a node using the ranges specified with `partition.node[node-id].param[1 or 2]`. `param1` indicates the lower bound that `column` must meet for a given node, and `param2` indicates the upper bound. If any of these bounds overlap, then the all nodes meeting the condition are passed the tuple. The value for `column` is found by determining the index of the specified `partition.column` for a given line in the CSV. The SQL statement is prepared appropriately and stored in memory. Once all statements are constructed, the appropriate SQL and parameters are sent to each appropriate socket, serially (starting at node 1, ending at node N).
   - If there are any errors in the processes, we attempt to tell the server daemon to not commit any changes as we close with an error ourselves. Again, the reasoning behind not attempting to proceed from here is to preserve the ACID property. We do not want incomplete data in our cluster.
7. If the insertion is successful, print a success message to the console.

### Client Program: runSSQL.py
The `runSSQL.py` file holds the code to execute a _trivial_ (no joins) SQLite statement across all nodes in the cluster. For nontrivial SQLite statements, use the `runJSQL.py` program. If the operation is a selection, then the results are displayed to the console. The arguments to this script are the cluster configuration file, and the SQL file to execute.

```
python3 runSSQL.py [clustercfg] [sqlfile]
python3 runSQL.py [clustercfg] [sqlfile]
```

Using the given arguments, the following occurs:
1. Collect the catalog URI from the `clustercfg` file. Collect the SQL statement from the `sqlfile`. If this cannot be performed, the program exits with an error.
4. Collect the node URIs from the catalog node. If this is not successful, the an error is returned to the console and the program exits.
3. The table name is parsed from the SQL statement. If this is an improperly formatted SQLite statement, then an error is returned to the console and the program exits.
4. Send the execution command list. This occurs in parallel, and creates `N = |Node URIs|` threads.
    1. For each process (node), an attempt to connect to the socket is made. If this is not successful, then an error message is printed to the console and the routine exits here.
    2. If the connection is successful, then the execution command list is pickled and sent over the socket.
    3. A response command list from the daemon is waited for. If there exists no response or an error is returned from the socket, then a message is printed to the console and the routine exits here.
    4. If the response command list is valid, then return any results from the socket. Listen and repeat until the terminating command string is sent.
    5. Record the successful operation in to the shared data section (a global list) and close the connection to the daemon.
5. Once all processes are done executing, print a summary block that informs the client of the end state of all processes (i.e. failed or succeeded).

### Client Program: runJSQL.py

The `runSSQL.py` file holds the code to execute a nontrivial (with joins) SQLite statement across all nodes in the cluster. *This will only work for statements that involve joins between exactly two tables.* For trivial SQLite statements, use the `runSSQL.py` program. If the operation is a selection, then the results are displayed to the console. The arguments to this script are the cluster configuration file, and the SQL file to execute.

```
python3 runJSQL.py [clustercfg] [sqlfile]
python3 runSQL.py [clustercfg] [sqlfile]
```

Using the given arguments, the following occurs. For brevity, this is more generalized than the described sequence in `runSSQL.py`:

1. Collect the catalog URI, and the partitioning information from the `clustercfg` file. If the `clustercfg` file is not properly formatted, the program exits with an error.
2. Determine the tables involved in the join. If there does not exist exactly 2 tables here, then the program exists with an error.
3. Collect the node URIs from the catalog node for both tables. If this is not successful, the an error is returned to the console and the program exits.
4. Execute the join. The basic algorithm used here is the Nested Loop Join, with the nodes for the first table acting as the outer loop. To avoid access to multiple resources but still work in parallel, we spawn  `N = |Node URIS for Table 2|` threads for a given node of table 1. 
   1. The thread is passed two node URIs pointing to different partitions (`P1, P2`) of two tables (`T1, T2`) . If these node URIs are not the same, then we inform `P1` to store `P2`'s table. If this is not successful, the program exits with an error message.
   2. Perform the given SQL statement between `P1` and `P2`, and store the result in a new table. If an error occurs, display it and exit the program.
5. The results of the join now exist scattered among every node for table 1 (the outer loop of the Nested Loop Join). Move the results of each join to one master node. This cannot be performed in parallel, as the master node would be shared between all threads/processes.
   1. This method is passed two node URIs pointing to two different partitions (`P1, P2`) of two tables (`T1, T2`). Here, `P1` is the master partition with the master table `T1`. `P2` is the slave partition, with the slave table `T2`. If the two node URIs are not the same, then we inform `P1` to store `P2`'s table. If this is not successful, the program exits with an error message.
   2. Perform a and store a union `P1 <- P1 U P2`  by taking the set difference between the two and storing the result. If this is not successful, the program exits with an error message.
6. Request the result of the join from the master node, and display any results to console. Again, if this is not successful, the program exits with an error message.
7. Perform a cleanup operation in parallel, spawning `N = |Node URIs for Table 1|` and removing any tables created in the join. Exit with an error if necessary.

### Server Program: parDBd.py

The `parDBd.py` file holds the code to be run on all nodes in the cluster. This is the server daemon. The arguments to this script are the hostname and the port:
```
python3 parDBd.py [hostname] [port]
```

Using the specified arguments, the daemon listens on a given port. Once a connection is made, the following happens:
1. Spawn a new process to handle the execution of the command. Loop to listen for more connections and make the daemon available to other clients.
2. Following the spawned process, we retrieve the first four bytes. This will inform us of the packet length = `ell`.
3. Read `ell` bytes and deserialize the packet to obtain a _command list_. If this is not successful or the received object is not a list, an error is returned through the socket and the connection is closed. The format command to the daemon must be specified in the **Protocol Design** section (before serialization).
4. Perform the desired operation based on the first element in the command list, or the _operation code_. If an error occurs during this process, return the error as a string. Otherwise, return a different command list containing the response operation code and the desired information.
5. The current connection is closed, and this specific process dies. This death is acknowledged upon a new connection to daemon.

## Protocol Design
The general design of the communication between the server daemon and the client is as follows:
1. Client serializes a list, whose first element is the operation code and where all following elements are pieces of data required to perform the operation.
2. Client prefixes the packet with the length of this list, as a 4-byte C integer, and sends this list through the socket.
3. Server daemon checks the first 4 bytes of the socket for this integer `ell`, and reads the first `ell` bytes. This is deserialized into the command list, and the server performs the operation specified in the first element.
4. To return a message, the server daemon starts by serializing a list containing the returned operation code and the success message or any other data as the following elements. If an error is received, then a string containing the error is sent instead of a list.
5. The server prefixes the packet with the length of this list, again as a 4-byte C integer and sends this through the socket.
6. The client checks the first 4 bytes of the socket for this integer `ell_2`, and reads the first `ell_2` bytes. This is deserialized into the command list or error string, and the client handles each case appropriately.

The specific operation codes are listed below:

Desired Operation | Operation Code | Client Sends... | Server Returns...
--- | --- | --- | ---
**Client** wants to perform an operation. **Server** wants to inform the client that an error has occured. | --- | --- | `string-containing-the-error`
--- | --- | --- | ---
**Client** wants to execute an insertion SQLite statement on a remote node, and wants to inform the server that more statements are arriving.  **Server** wants to acknowledge that the passed statement was executed successfully. | `YS` | `['YS', database-file-name, insertion-sql-to-execute, parameters-to-attach-to-statement]` | `['EY', 'Success']`
**Client** wants to execute an insertion SQLite statement on a remote node, and wants to inform the server that no more statements are arriving.  **Server** wants to acknowledge that the passed statement was executed successfully. | `YZ` | `['YZ', database-file-name, insertion-sql-to-execute, parameters-to-attach-to-statement]` | `['EY', 'Success']`
**Client** wants to stop the stream after sending various insert statements. **Server** wants to acknowledge this. | `YY` | `['YY']` | `['EY', 'Success']`
**Client** wants to inform rollback any changes that the server has made since it's last commit. **Server** wants to acknowledge that this was successful. | `YX` | `['YX']` | `['EY', 'Success']`
**Client** wants to execute a non-select SQLite statement on a remote node. **Server** wants to inform client that the operation was successful. | `E` | `['E', database-file-name, sql-to-execute]` | `['EZ', 'Success']`
**Client** wants to execute a select SQLite statement on a remote node. **Server** wants to deliver tuples to client, and inform the client that more tuples are on the way. | `E` | `['E', database-file-name, sql-to-execute]` | `['ES', tuple-to-send]`
**Client** wants to execute a select SQLite statement on a remote node. **Server** wants to deliver tuples to client, and inform this the last tuple it will send. | `E` | `['E', database-file-name, sql-to-execute]` | `['EZ', last-tuple-to-send]`
**Client** wants to record a table creation or destroying SQLite statement on the catalog node. **Server** (i.e. the catalog node) wants to inform the client that this operation was successful. | `C` | `['C', database-catalog-file-name, list-of-node-uris, ddl-to-execute]` | `['EC', 'Success']`
**Client** wants to record the type of partitioning used on the catalog node. **Server** (i.e. the catalog node) wants to inform the client that his operation was successful.| `K` | `['K', database-catalog-file-name, dictionary-describing-partition, number-of-nodes-in-cluster]` | `['EK', 'Success']`
**Client** is requesting the node URIs of a specific table from the catalog node. **Server** (i.e. the catalog node) wants to deliver these node URIs to the client. | `U` | `['U', database-catalog-file-name, name-of-table]` | `['EU', list-of-node-uris]`
**Client** is requesting the columns of a specific table from some node in the cluster (it is assumed that all nodes have the same tables). **Server** wants to deliver these columns to the client. | `P` | `['P', database-file-name, table-name]` | `['EP', list-of-columns-in-table]`
**Client** is requesting that the server retrieve a table from a remote node, and store it in it's database. **Server** wants to inform the client of the table name that server stored the remote table as. | `B` | `['B', database-filename-list, name-of-tables-list, remote-node-uris]` | `['EB', name-of-new-table]`

## Troubleshooting
All successful operations are returned with a code of 0. All errors are returned with a code of -1 and the errors are prefixed with the string `Error: `. Below are a list of common errors and their appropriate fixes.

### runSQL.py Errors
*Any other errors found with `runSQL.py` are a result of running the programs below. *
Message | Fix
--- | ---
 `Usage: python3 runSQL.py [clustercfg] [sqlfile/csv]` | An incorrect number of arguments was supplied. There must exist exactly two arguments to this program.
`[Errno 2] No such file or directory: 'XXXXXX'` | The supplied arguments do not exist or cannot be found.
`Could not walk parse tree with given SQL.` | The given SQL file is invalid. Ensure that the format follows the SQLite3 syntax.
`No terminating semicolon.` | The given SQL file contains no terminating semicolon. Add one to the end of your file.

### runDDL.py Errors
Message | Fix
--- | ---
 `Usage: python3 runDDL.py [clustercfg] [ddlfile]` | An incorrect number of arguments was supplied. There must exist exactly two arguments to this program.
`[Errno 2] No such file or directory: 'XXXXXX'` | The supplied arguments do not exist or cannot be found.
`Could not walk parse tree with given SQL.` | The given SQL file is invalid. Ensure that the format follows the SQLite3 syntax.
`No terminating semicolon.` | The given SQL file contains no terminating semicolon. Add one to the end of your file.
`Cannot connect to the catalog. No statement executed.` | The catalog node catalog be reached. Ensure that the daemon is running for the catalog.
`'numnodes' is not defined.` | The given `clustercfg` is not formatted correctly. The 'numnodes' entry was not found.
`Node entries not formatted correctly.` | The given `clustercfg` is not formatted correctly. Double check the **Format of File: clustercfg**.


### runLCSV.py Errors
Message | Fix
--- | ---
 `Usage: python3 runLCSV.py [clustercfg] [csv]` | An incorrect number of arguments was supplied. There must exist exactly two arguments to this program.
`[Errno 2] No such file or directory: 'XXXXXX'` | The supplied arguments do not exist or cannot be found.

### runSSQL.py Errors
Message | Fix
--- | ---
 `Usage: python3 runSSQL.py [clustercfg] [sqlfile]` | An incorrect number of arguments was supplied. There must exist exactly two arguments to this program.
`[Errno 2] No such file or directory: 'XXXXXX'` | The supplied arguments do not exist or cannot be found.
`Could not walk parse tree with given SQL.` | The given SQL file is invalid. Ensure that the format follows the SQLite3 syntax.
`No terminating semicolon.` | The given SQL file contains no terminating semicolon. Add one to the end of your file.
`Cannot connect to the catalog. No statement executed.` | The catalog node catalog be reached. Ensure that the daemon is running for the catalog.

### runJSQL.py Errors
Message | Fix
--- | ---
 `Usage: python3 runJSQL.py [clustercfg] [sqlfile]` | An incorrect number of arguments was supplied. There must exist exactly two arguments to this program.
`[Errno 2] No such file or directory: 'XXXXXX'` | The supplied arguments do not exist or cannot be found.
`Could not walk parse tree with given SQL.` | The given SQL file is invalid. Ensure that the format follows the SQLite3 syntax.
`No terminating semicolon.` | The given SQL file contains no terminating semicolon. Add one to the end of your file.
`Cannot connect to the catalog. No statement executed.` | The catalog node catalog be reached. Ensure that the daemon is running for the catalog.

### parDBd.py Errors
Message | Fix
--- | ---
 `Usage: python3 parDBd.py [hostname] [port]` | An incorrect number of arguments was supplied. There must exist exactly two arguments to this program.
`Could not interpret the given port argument.` | The port number could not be parsed. Ensure that the hostname is specified first, followed by the port.
`[Errno 99] Cannot assign requested address` | A socket cannot be created with the given hostname. Double check the hostname passed.
`Socket Error: [Errno 98] Address is already in use.` | The specified port is already in use. Use another port.






### runDDL.py Errors
Error Code | Message | Fix
--- | --- | ---
2 | `Usage: python3 runDDL.py [clustercfg] [ddlfile]` | An incorrect number of arguments was supplied. There must exist exactly two arguments to this program.
3 | `Error: [Errno 2] No such file or directory: '...'` | The supplied `clustercfg` file cannot be found. Double check the path.
4 | `Error: [Errno 2] No such file or directory: '...'` | The supplied `ddlfile` file cannot be found. Double check the path.
4 | `Error: No terminating semicolon.` | The supplied `ddlfile` does not have a terminating semi-colon to mark the end of the statement.
5 | `Error: Cannot connect to the catalog. No statement executed.` | The catalog node could not be reached. Check your internet connection, the `catalog.hostname` entry in the `clustercfg` file, and make sure that the daemon is running on the catalog node.
6 | `Error: Node entries not formatted correctly.` | The node entries in the `clustercfg` file are not formatted correctly. Check the format in the `runDDL` section.
7 | `Catalog Error: Socket could not be established.` | Double check your internet connection. Somewhere between the executing and logging process, a connection to the catalog node was no longer able to be established.

### runSQL.py Errors
Error Code | Message | Fix
--- | --- | ---
2 | `Usage: python3 runSSQL.py [clustercfg] [sqlfile]` | An incorrect number of arguments was supplied. There must exist exactly two arguments to this program.
3 | `Error: [Errno 2] No such file or directory: '...'` | The supplied `clustercfg` file cannot be found. Double check the path.
4 | `Error: [Errno 2] No such file or directory: '...'` | The supplied `sqlfile` file cannot be found. Double check the path.
4 | `Error: No terminating semicolon.` | The supplied `sqlfile` does not have a terminating semi-colon to mark the end of the statement.
5 | `Catalog Error: Socket could not be established.` | The catalog could not be reached. Check your internet connection, the `catalog.hostname` entry in the `clustercfg` file, and make sure that the daemon is running on the catalog node.
5 | `Catalog Error: Table ... not found.` | The table specified in the `sqlfile` was not found on the catalog. Execute a 'CREATE TABLE' statement instead with the `clustercfg` configuration specifications, or fix the table name.
6 | `Error: Table could not be found in 'sqlfile'.` | A table name could not be parsed from the given `sqlfile`. Double check your SQLite syntax here.


### loadCSV.py Errors
Error Code | Message | Fix
--- | --- | ---
2 | `Usage: python3 runLCSV.py [clustercfg] [csv]` | An incorrect number of arguments was supplied. There must exist exactly two arguments to this program.
3 |`Error: Not found: '...'` | There exists a key error in the `clustercfg` file. Double check the `clustercfg` configuration specifications.
4 | `Incorrect number of nodes specified in 'clustercfg'.` | If hash partitioning is specified, then the number of nodes in the cluster do not match the `partition.param1` in `clustercfg`. If range partitioning is specified, then the number of nodes in the cluster do match the `numnodes` in `clustercfg`. Use `runDDL.py` to change the table schema, or correct your `clustercfg` file.
5 | `Catalog Error: Socket could not be established.` | The catalog could not be reached. Check your internet connection, the `catalog.hostname` entry in the `clustercfg` file, and make sure that the daemon is running on the catalog node.
6 | `Could not connect to first node in the cluster.` | The first node in the cluster could not be reached. Ensure that the daemon is running on all  nodes in the cluster.
7 | `All nodes in cluster could not be reached.` | There exists a node in the cluter that could not be reached. Ensure that the daemon is running on all nodes in the cluster.

