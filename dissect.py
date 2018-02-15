# coding=utf-8
"""
Contains functions to dissect (parse) various files (clustercfg, sqlfile).

Usage: clustercfg([clustercfg])
       table([SQL-statement])
       is_select([SQL-statement])
"""

from configparser import ConfigParser
from math import inf

from antlr4 import InputStream, CommonTokenStream, ParseTreeWalker

import catalog
from parse.SQLiteLexer import SQLiteLexer
from parse.SQLiteListener import SQLiteListener
from parse.SQLiteParser import SQLiteParser


class _TableNameStoreListener(SQLiteListener):
    """ Private listener class to record the table name. """
    table_name = ''

    def enterTable_name(self, ctx: SQLiteParser.Table_nameContext):
        """ Called when table_name is found. Records the table_name token to the 'table_name' field.

        :param ctx: Context to parse.
        :return: None.
        """
        self.table_name = ctx.getText()


def sqlfile(f):
    """ Parse the given SQL file. The first argument should be returned, whose end is denoted
    with a semicolon.

    :param f: Filename of the SQL file.
    :return: False if there exists no terminating semicolon. Otherwise, a string containing the
    DDL to execute.
    """
    with open(f) as sqlfile_f:
        s = sqlfile_f.read()

    return False if ';' not in s else s.split(';')[0]


def is_select(s):
    """ Given a SQLite string, determine if the statement is a SELECT operation.

    :param s: SQLite to determine the operation type from.
    :return: True if the given string is a SELECT operation. False otherwise.
    """
    # SELECT statements with resulting tuples always have 'SELECT' as the first word.
    return s.split()[0].upper() == 'SELECT'


def table(s):
    """ Given a SQLite string, extract the TABLE associated with the operation.

    :param s: SQLite to extract table from.
    :return: False if the SQL statement does not contain a table (i.e. is formatted incorrectly).
    Otherwise, the table associated with the SQL.
    """
    # Create the parse tree for the given SQL string.
    lexer = SQLiteLexer(InputStream(s))
    parser = SQLiteParser(CommonTokenStream(lexer))
    tree = parser.parse()

    # Walk the parse tree and find the table name.
    t = _TableNameStoreListener()
    ParseTreeWalker().walk(t, tree)

    # If there exists no table name, return false.
    return False if t.table_name == '' else t.table_name


def clustercfg_catalog(f):
    """ TODO: Finish the description here.

    :param f:
    :return: String associated with error if there exists no catalog hostname. Otherwise,
    a one element list with the URI associated with the catalog node.
    """
    config = ConfigParser()

    # Append dummy section to given configuration file, read the config file.
    with open(f) as clustercfg_f:
        config_string = '[D]\n' + clustercfg_f.read()
    config.read_string(config_string)

    # Determine the catalog URI.
    if not {'catalog.hostname'}.issubset([k for k in config['D']]):
        return '\'catalog.hostname\' is not defined.'
    return [config['D']['catalog.hostname']]


def _clustercfg_partition(p_m, r_d, config):
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
                r_d['param1'].append(inf_a(config['D']['partition.node' + str(i + 1) + '.param1']))
                r_d['param2'].append(inf_a(config['D']['partition.node' + str(i + 1) + '.param2']))

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


def clustercfg_load(f):
    """ TODO: Finish the description here.

    :param f:
    :return: 2 element list containing the catalog URI
    """
    config, r_d = ConfigParser(), {}

    # Append dummy section to given configuration file, read the config file.
    with open(f) as clustercfg_f:
        config_string = '[D]\n' + clustercfg_f.read()
    config.read_string(config_string)

    # Determine the catalog URI. Return any errors if they exist (i.e. not a list).
    catalog_uri = clustercfg_catalog(f)
    if not hasattr(catalog_uri, '__iter__'):
        return catalog_uri

    # Determine the partitioning.
    if not {'partition.method', 'tablename'}.issubset([k for k in config['D']]):
        return '\'partition.method\' or \'tablename\' is not defined.'
    r_d.update({'tname': config['D']['tablename']})

    # Based on the partitioning specified, parse the appropriate sections. Return error if exists.
    r_pd = _clustercfg_partition(config['D']['partition.method'], r_d, config)
    if not isinstance(r_pd, dict):
        return r_pd
    else:
        r_d = r_pd

    # If the number of nodes here does not match the nodes in catalog, return with an error.
    if r_d['partmtd'] == 1:
        r_c = catalog.verify_node_count(catalog_uri, r_d['tname'], int(config['D']['numnodes']))
    elif r_d['partmtd'] == 2:
        r_c = catalog.verify_node_count(catalog_uri, r_d['tname'], r_d['param1'])
    else:
        r_c = True

    if isinstance(r_c, str):
        return r_c
    elif not r_c:
        return '\'numnodes\' specified does not match number of nodes in catalog node'
    else:
        return catalog_uri[0], r_d
