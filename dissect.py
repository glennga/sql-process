# coding=utf-8
"""
Contains functions to dissect (parse) various files (clustercfg, sqlfile).

Usage: TODO: Update the usage section.
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
        """ TODO: Finish the documentation here.

        :param s:
        :return:
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
        """ TODO: Finish the documentation here.

        :param s:
        :return:
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
        """ TODO: Finish the documentation here.


        :param s:
        :return:
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

        :param s: SQLite to extract table from.
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
        """ TODO: Finish the description here.

        :param f:
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
        """ TODO: Finish the documentation here.

        :param f:
        :return:
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

        nodes = []
        try:
            [nodes.append(config['D']['node' + str(i + 1) + '.hostname']) for i in range(n)]
            return nodes
        except KeyError:
            return 'Node entries not formatted correctly.'

    @staticmethod
    def partition(p_m, r_d, config):
        """ TODO: Finish the description.

        :param p_m:
        :param r_d:
        :param config:
        :return:
        """
        inf_a = lambda b: inf if b == '+inf' else (-inf if b == '-inf' else float(b))

        # TODO: Add comments here.
        try:
            if p_m.lower() == 'range':
                r_d.update({'partmtd': 1, 'partcol': config['D']['partition.column'],
                            'param1': [], 'param2': []})

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
                return '\'partition.method\' not in space [range, hash, notpartition]'

        except KeyError as e:
            return str(e)
        except ValueError as e:
            return '\'param1\', \'param2\', or \'numnodes\' is not formatted correctly: ' + str(e)

        return r_d

    @staticmethod
    def load(f):
        """ TODO: Finish the description here.

        :param f:
        :return:
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
        r_pd = ClusterCFG.partition(config['D']['partition.method'], r_d, config)
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
