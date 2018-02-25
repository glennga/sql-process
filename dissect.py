# coding=utf-8
"""
Contains functions to dissect (parse) various files (clustercfg, sqlfile).

Usage: SQLFile.as_string([SQL file])
       SQLFile.is_ddl([SQL string])
       SQLFile.is_drop_ddl([SQL string])
       SQLFile.is_select([SQL string])
       SQLFile.table([SQL string])

       ClusterCFG.catalog_uri([cluster configuration file])
       ClusterCFG.node_uris([cluster configuration file])
       ClusterCFG.load([cluster configuration file])
"""

from configparser import ConfigParser
from math import inf

from antlr4 import InputStream, CommonTokenStream, ParseTreeWalker

import listen
from parse.SQLiteLexer import SQLiteLexer
from parse.SQLiteParser import SQLiteParser


class SQLFile:
    """ All dissecting operations that deal with the SQL file. """

    @staticmethod
    def as_string(f):
        """ Parse the given SQL file. The first argument should be returned, whose end is denoted
        with a semicolon. The resultant is a **list** with a single element. A single string
         indicates an error.

        :param f: Filename of the SQL file.
        :return: String associated with error if there exists no catalog hostname. Otherwise,
            a one element list with the SQL to execute.
        """
        try:
            with open(f) as sqlfile_f:
                s = sqlfile_f.read()
        except FileNotFoundError as e:
            return str(e)

        # Returning a single element list here...
        return 'No terminating semicolon.' if ';' not in s else [s.split(';', 1)[0]]

    @staticmethod
    def is_ddl(s):
        """ Given a SQL string, determine if the statement is a DDL (CREATE TABLE, DROP TABLE) or
        not.

        :param s: SQL string to search for DDL with.
        :return: True if the given statement is a DDL. False otherwise.
        """
        # Create the parse tree for the given SQL string.
        lexer = SQLiteLexer(InputStream(s))
        lexer.removeErrorListeners()
        parser = SQLiteParser(CommonTokenStream(lexer))
        parser.removeErrorListeners()
        tree = parser.parse()

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
        lexer = SQLiteLexer(InputStream(s))
        lexer.removeErrorListeners()
        parser = SQLiteParser(CommonTokenStream(lexer))
        parser.removeErrorListeners()
        tree = parser.parse()

        # Walk the parse tree and determine what type of statement 's' is.
        t = listen.StatementType()
        ParseTreeWalker().walk(t, tree)

        return t.is_drop

    def is_select(s):
        """ Given a SQL string, determine if the statement if a SELECT statement or not.

        :param s: SQL string to search for SELECT statement with.
        :return: True if the given statement is a SELECT statement. False otherwise.
        """
        # Create the parse tree for the given SQL string.
        lexer = SQLiteLexer(InputStream(s))
        lexer.removeErrorListeners()
        parser = SQLiteParser(CommonTokenStream(lexer))
        parser.removeErrorListeners()
        tree = parser.parse()

        # Walk the parse tree and determine what type of statement 's' is.
        t = listen.StatementType()
        ParseTreeWalker().walk(t, tree)

        return t.is_select

    def table(s):
        """ Given a SQLite string, extract the TABLE associated with the operation.

        :param s: SQL string to extract table from.
        :return: False if the SQL statement does not contain a table (i.e. is formatted incorrectly).
            Otherwise, the table associated with the SQL.
        """
        # Create the parse tree for the given SQL string.
        lexer = SQLiteLexer(InputStream(s))
        lexer.removeErrorListeners()
        parser = SQLiteParser(CommonTokenStream(lexer))
        parser.removeErrorListeners()
        tree = parser.parse()

        # Walk the parse tree and find the table name.
        t = listen.TableNameStore()
        ParseTreeWalker().walk(t, tree)

        # If there exists no table name, return false.
        return False if t.table_name == '' else t.table_name


class ClusterCFG:
    """ All dissecting operations that deal with the clustercfg file. """

    @staticmethod
    def catalog_uri(f):
        """ Given the cluster configuration file, grab the catalog node URI.

        :param f: Cluster configuration filename.
        :return: String associated with error if there exists no catalog hostname. Otherwise,
            a one element list with the URI associated with the catalog node.
        """
        config = ConfigParser()

        # Append dummy section to given configuration file, read the config file.
        try:
            with open(f) as clustercfg_f:
                config_string = '[D]\n' + clustercfg_f.read()
            config.read_string(config_string)
        except FileNotFoundError as e:
            return str(e)

        # Determine the catalog URI.
        if not {'catalog.hostname'}.issubset([k for k in config['D']]):
            return '\'catalog.hostname\' is not defined.'
        return [config['D']['catalog.hostname']]

    @staticmethod
    def node_uris(f):
        """ Given the cluster configuration file, grab all the node URIs.

        :param f: Cluster configuration filename.
        :return: String associated with error if the file is not formatted properly. Otherwise,
            a list of node URIs.
        """
        config = ConfigParser()

        # Append dummy section to given configuration file, read the config file.
        try:
            with open(f) as clustercfg_f:
                config_string = '[D]\n' + clustercfg_f.read()
            config.read_string(config_string)
        except FileNotFoundError as e:
            return str(e)

        # Ensure that 'numnodes' exist.
        if not {'numnodes'}.issubset([k for k in config['D']]):
            return '\'numnodes\' is not defined.'
        try:
            n = int(config['D']['numnodes'])
        except ValueError:
            return '\'numnodes\' is not a valid integer.'

        # Collect the node URIs.
        nodes = []
        try:
            [nodes.append(config['D']['node' + str(i + 1) + '.hostname']) for i in range(n)]
            return nodes
        except KeyError:
            return 'Node entries not formatted correctly.'

    @staticmethod
    def _partition(p_m, r_d, config):
        """ Helper method for the 'load' function. Returns all of the partitioning information
        from a configuration file reader.

        :param p_m: Partitioning method to parse for. Exists in space ['range', 'notparition',
            'hash'].
        :param r_d: Current partitioning information as a dictionary.
        :param config: Open onfiguration file reader.
        :return: String containing the error if the partitioning information is not correctly
            formatted. Otherwise, the partitioning dictionary with the additional partitioning
            information.
        """
        inf_a = lambda b: inf if b == '+inf' else (-inf if b == '-inf' else float(b))

        try:
            if p_m.lower() == 'range':
                r_d.update({'partmtd': 1, 'partcol': config['D']['partition.column'],
                            'param1': [], 'param2': []})

                # For range partitioning, look for the 'partition.node[i]' entries.
                for i in range(int(config['D']['numnodes'])):
                    r_d['param1'].append(inf_a(config['D']['partition.node' +
                                                           str(i + 1) + '.param1']))

                    r_d['param2'].append(inf_a(config['D']['partition.node' +
                                                           str(i + 1) + '.param2']))

            elif p_m.lower() == 'hash':
                r_d.update({'partmtd': 2, 'partcol': config['D']['partition.column'],
                            'param1': int(config['D']['partition.param1'])})

            elif p_m.lower() == 'notpartition':
                r_d.update({'partmtd': 0})

            else:
                return '\'partition.method\' not in space [range, hash, notpartition].'

        except KeyError as e:
            return 'Not found: ' + str(e)
        except ValueError as e:
            return '\'param1\', \'param2\', or \'numnodes\' is not formatted correctly: ' + str(e)

        return r_d

    @staticmethod
    def load(f):
        """ Given the cluster configuration file, collect the catalog node URI, all of the
        partitioning information from the file, and the expected number of nodes.

        :param f: Cluster configuration filename.
        :return: String containing the error if the file is not formatted properly. Otherwise, the
            catalog node URI, the partitioning dictionary, and the expected number of nodes as list.
        """
        config, r_d = ConfigParser(), {}

        # Append dummy section to given configuration file, read the config file.
        with open(f) as clustercfg_f:
            config_string = '[D]\n' + clustercfg_f.read()
        config.read_string(config_string)

        # Determine the catalog URI. Return any errors if they exist (i.e. not a list).
        c_u = ClusterCFG.catalog_uri(f)
        if isinstance(c_u, str):
            return c_u

        # Determine the partitioning.
        if not {'partition.method', 'tablename'}.issubset([k for k in config['D']]):
            return '\'partition.method\' or \'tablename\' is not defined.'
        r_d.update({'tname': config['D']['tablename']})

        # Based on the partitioning specified, parse appropriate sections. Return error if exists.
        r_pd = ClusterCFG._partition(config['D']['partition.method'], r_d, config)
        if not isinstance(r_pd, dict):
            return r_pd
        else:
            r_d = r_pd

        # Determine the node count to pass out.
        numnodes = 0
        if r_d['partmtd'] == 1:
            numnodes = int(config['D']['numnodes'])
        elif r_d['partmtd'] == 2:
            numnodes = r_d['param1']

        return c_u[0], r_d, numnodes
