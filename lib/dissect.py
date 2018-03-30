# coding=utf-8
"""
Contains functions to parse (dissect) the configuration file and some SQL file.

Usage: SQLFile.as_string(SQL_file)
       SQLFile.is_join(SQL_string)
       SQLFile.is_ddl(SQL_string)
       SQLFile.is_drop_ddl(SQL_string)
       SQLFile.is_select(SQL_string)
       SQLFile.table(SQL_string)

       ClusterCFG.is_runLCSV(cluster_configuration_file)
       ClusterCFG.parse_uri(node_URI)
       ClusterCFG.catalog_uri(cluster_configuration_file)
       ClusterCFG.node_uris(cluster_configuration_file)
       ClusterCFG.load(cluster_configuration_file)
"""

# noinspection PyCompatibility
from configparser import ConfigParser

from antlr4 import InputStream, CommonTokenStream, ParseTreeWalker

from lib import listen
from lib.error import ErrorHandle
from lib.parse.SQLiteLexer import SQLiteLexer
from lib.parse.SQLiteParser import SQLiteParser


class SQLFile:
    """
    All parsing operations that involve some SQL file. ANTLR is the base of dissecting individual
    SQL statements.
    """

    @staticmethod
    def _open_file(f):
        """ Helper method for opening a file, and returning all of the contents as a string. This
        is meant to be used with ErrorHandle.attempt_operation.

        :param f: File to open and dump.
        :return: All contents of f as a string.
        """
        with open(f) as file_f:
            return file_f.read()

    @staticmethod
    def _generate_parse_tree(s):
        """ Generate the lexer and parse tree for some SQL statement. Just some ANTLR
        boilerplate...

        :param s: SQL statement to generate parse tree for.
        :return: The parse tree for the given SQL statement.
        """
        lexer = SQLiteLexer(InputStream(s))
        lexer.removeErrorListeners()
        parser = SQLiteParser(CommonTokenStream(lexer))
        parser.removeErrorListeners()

        return parser.parse()

    @staticmethod
    def as_string(f):
        """ Parse the given SQL file. The first argument should be returned, whose end is denoted
        with a semicolon.

        :param f: Filename of the SQL file.
        :return: String associated with error if there exists no catalog hostname. Otherwise,
            the SQL to execute.
        """
        s = ErrorHandle.attempt_operation(lambda: SQLFile._open_file(f), FileNotFoundError,
                                          ErrorHandle.default_handler, True)

        # Return any errors.
        if ErrorHandle.is_error(s):
            return s
        elif ';' not in s:
            return ErrorHandle.wrap_error_tag('No terminating semicolon.')
        else:
            return s.split(';', 1)[0]

    @staticmethod
    def is_join(s):
        """ Determine if the given SQL statement has a join or not. This is done by walking the
        parse tree and counting the number of distinct tables. A count > 1 indicates a join is
        required.

        :param s: SQL string to walk through.
        :return: True if more than one unique table exists in 's'. False otherwise.
        """
        return type(SQLFile.table(s)) is list

    @staticmethod
    def is_ddl(s):
        """ Given a SQL string, determine if the statement is a DDL (CREATE TABLE, DROP TABLE) or
        not.

        :param s: SQL string to search for DDL with.
        :return: True if the given statement is a DDL. False otherwise.
        """
        # Create the parse tree for the given SQL string.
        tree = SQLFile._generate_parse_tree(s)

        # Walk the parse tree and determine what type of statement 's' is.
        t = listen.StatementType()
        ParseTreeWalker().walk(t, tree)

        return t.is_ddl

    @staticmethod
    def is_drop_ddl(s):
        """ Given a SQL string, determine if the statement is a DROP TABLE statement or not.

        :param s: SQL string to search for DROP TABLE statement with.
        :return: True if the given statement is a DROP TABLE statement. False otherwise.
        """
        # Create the parse tree for the given SQL string.
        tree = SQLFile._generate_parse_tree(s)

        # Walk the parse tree and determine what type of statement 's' is.
        t = listen.StatementType()
        ParseTreeWalker().walk(t, tree)

        return t.is_drop

    @staticmethod
    def is_select(s):
        """ Given a SQL string, determine if the statement if a SELECT statement or not.

        :param s: SQL string to search for SELECT statement with.
        :return: True if the given statement is a SELECT statement. False otherwise.
        """
        # Create the parse tree for the given SQL string.
        tree = SQLFile._generate_parse_tree(s)

        # Walk the parse tree and determine what type of statement 's' is.
        t = listen.StatementType()
        ParseTreeWalker().walk(t, tree)

        return t.is_select

    @staticmethod
    def table(s):
        """ Given a SQLite string, extract the TABLE associated with the operation.

        :param s: SQL string to extract table from.
        :return: An error statement if there exists no table. Otherwise, the table(s) associated
            with the SQL (a string if there exists one table, otherwise a list of both tables).
        """
        # Create the parse tree for the given SQL string.
        tree = SQLFile._generate_parse_tree(s)

        # Walk the parse tree and find the table name.
        t = listen.TableNameStore()
        ParseTreeWalker().walk(t, tree)

        # Remove duplicates from the table names.
        table_names = list(set(t.table_names))

        # If there exists no table name, return false.
        if len(table_names) == 0:
            return ErrorHandle.wrap_error_tag('No table exists.')
        elif len(table_names) == 1:
            # If there exists one table, return that sole element.
            return table_names[0]
        else:
            return table_names


class ClusterCFG:
    """
    All parsing operations involved with the cluster configuration file. The standard followed here
    is that of a normal *.ini file, so ConfigParser is our base.
    """

    @staticmethod
    def _open_with_dummy(f):
        """ Helper method for opening a configuration file whose contents do not belong to any
        section. This is a requirement for ConfigParser, so we get around this by appending a
        dummy section 'D'. This is also meant to be used with ErrorHandle.attempt_operation.

        :param f: Config file to load as a string.
        :return: The config as a string, with section D prepended.
        """
        with open(f) as file_f:
            return '[D]\n' + file_f.read()

    @staticmethod
    def _construct_config_reader(f):
        """ Helper method used to parse the given file into a ConfigParser object.

        :param f: Config file to load.
        :return: The given config file, as a ConfigParser object.
        """
        config = ConfigParser()

        # Append dummy section to given configuration file.
        config_string = ErrorHandle.attempt_operation(lambda: ClusterCFG._open_with_dummy(f),
                                                      FileNotFoundError,
                                                      ErrorHandle.default_handler, True)

        # Return any errors if they exist.
        if ErrorHandle.is_error(config_string):
            return config_string

        # Otherwise, read and return the config reader.
        config.read_string(config_string)
        return config

    @staticmethod
    def is_runLSCV(f):
        """ Given the cluster configuration file, determine if the desired function is to load a
        CSV file.

        :param f: Cluster configuration filename.
        :return: True if the desired function is to load a CSV. False if 'tablename' is
            not in config file. Otherwise, a string containing the error.
        """
        result = ClusterCFG._construct_config_reader(f)
        if ErrorHandle.is_error(result):
            return result

        return True if 'tablename' in result['D'] else False

    @staticmethod
    def parse_uri(node):
        """ Given a URI, parse the host, port and filename.

        :param node: Node URI to dissect
        :return: Host, port, and filename of the node.
        """
        host = node.split(':', 1)[0]
        port, f = node.split(':', 1)[1].split('/', 1)

        return host, port, f

    @staticmethod
    def catalog_uri(f):
        """ Given the cluster configuration file, grab the catalog node URI.

        :param f: Cluster configuration filename.
        :return: String associated with error if there exists no catalog hostname. Otherwise,
            the URI associated with the catalog node.
        """
        config = ClusterCFG._construct_config_reader(f)
        if ErrorHandle.is_error(config):
            return config

        # Determine the catalog URI.
        if not {'catalog.hostname'}.issubset([k for k in config['D']]):
            return ErrorHandle.wrap_error_tag('\'catalog.hostname\' is not defined.')
        return config['D']['catalog.hostname']

    @staticmethod
    def node_uris(f):
        """ Given the cluster configuration file, grab all the node URIs.

        :param f: Cluster configuration filename.
        :return: String associated with error if the file is not formatted properly. Otherwise,
            a list of node URIs.
        """
        config, nodes = ClusterCFG._construct_config_reader(f), []

        # Ensure that 'numnodes' exist.
        if not {'numnodes'}.issubset([k for k in config['D']]):
            return ErrorHandle.wrap_error_tag('\'numnodes\' is not defined.')

        # Ensure that 'numnodes' is a valid integer.
        n = ErrorHandle.attempt_operation(lambda: int(config['D']['numnodes']),
                                          ValueError, ErrorHandle.default_handler, True)
        if ErrorHandle.is_error(n):
            return ErrorHandle.wrap_error_tag('\'numnodes\' is not a valid integer.')

        # Collect the node URIs.
        collect_nodes = lambda: [nodes.append(config['D']['node' + str(i + 1) + '.hostname'])
                                 for i in range(n)]
        result = ErrorHandle.attempt_operation(collect_nodes, KeyError, ErrorHandle.default_handler)

        # If a KeyError is thrown, then the results were not formatted correctly.
        if ErrorHandle.is_error(result):
            return ErrorHandle.wrap_error_tag('Node entries not formatted correctly.')
        else:
            return nodes

    @staticmethod
    def _partition(p_m, r_d, config):
        """ Helper method for the 'load' function. Returns all of the partitioning information
        from a configuration file reader.

        :param p_m: Partitioning method to parse for. Exists in space ['range', 'notpartition',
            'hash'].
        :param r_d: Current partitioning information as a dictionary.
        :param config: Open configuration file reader.
        :return: String containing the error if the partitioning information is not correctly
            formatted. Otherwise, the partitioning dictionary with the additional partitioning
            information.
        """
        if p_m.lower() == 'range':
            inf_a = lambda b: float('inf') if b == '+inf' else \
                (-float('inf') if b == '-inf' else float(b))

            r_d.update({'partmtd': 1,
                        'partcol': config['D']['partition.column'],
                        'param1': [], 'param2': []})

            # For range partitioning, look for the 'partition.node[i]' entries.
            for i in range(int(config['D']['numnodes'])):
                p1 = inf_a(config['D']['partition.node' + str(i + 1) + '.param1'])
                p2 = inf_a(config['D']['partition.node' + str(i + 1) + '.param2'])
                r_d['param1'].append(p1), r_d['param2'].append(p2)

        elif p_m.lower() == 'hash':
            r_d.update({'partmtd': 2,
                        'partcol': config['D']['partition.column'],
                        'param1': int(config['D']['partition.param1'])})

        elif p_m.lower() == 'notpartition':
            r_d.update({'partmtd': 0})

        else:
            return ErrorHandle.wrap_error_tag('\'partition.method\' not in space [range, hash, '
                                              'notpartition].')

        return r_d

    @staticmethod
    def load(f):
        """ Given the cluster configuration file, collect the catalog node URI, all of the
        partitioning information from the file, and the expected number of nodes.

        :param f: Cluster configuration filename.
        :return: String containing the error if the file is not formatted properly. Otherwise, the
            catalog node URI, the partitioning dictionary, and the expected number of nodes as list.
        """
        config, r_d = ClusterCFG._construct_config_reader(f), {}

        # Determine the catalog URI. Return any errors if they exist (i.e. not a list).
        c_u = ClusterCFG.catalog_uri(f)
        if ErrorHandle.is_error(c_u):
            return c_u

        # Determine the partitioning.
        if not {'partition.method', 'tablename'}.issubset([k for k in config['D']]):
            return ErrorHandle.wrap_error_tag('\'partition.method\' or \'tablename\' is '
                                              'not defined.')
        r_d.update({'tname': config['D']['tablename']})

        # Based on the partitioning specified, parse appropriate sections.
        partition = lambda: ClusterCFG._partition(config['D']['partition.method'], r_d, config)
        r_d = ErrorHandle.attempt_operation(partition, (KeyError, ValueError),
                                            ErrorHandle.default_handler, True)

        # Return error if exists.
        if ErrorHandle.is_error(r_d):
            return r_d + ' is not properly formatted or does not exist.'

        # Determine the node count to pass out.
        numnodes = 0
        if r_d['partmtd'] == 1:
            numnodes = int(config['D']['numnodes'])
        elif r_d['partmtd'] == 2:
            numnodes = r_d['param1']

        return c_u, r_d, numnodes
