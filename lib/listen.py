# coding=utf-8
"""
Contains listener classes for the SQLite grammar. A static variable is potentially modified when
walking the tree.

Usage: t = listen.TableNameStore()
       ParseTreeWalker().walk(t, tree)

       t = listen.StatementType()
       ParseTreeWalker().walk(t, tree)
"""

from lib.parse.SQLiteListener import SQLiteListener
from lib.parse.SQLiteParser import SQLiteParser


class TableNameStore(SQLiteListener):
    """ Listener class to record table name(s). """

    # Current name(s) associated with the table.
    table_names = []

    def enterTable_name(self, ctx: SQLiteParser.Table_nameContext):
        """ Called when table_name is found. Records the table_name token to the 'table_name' field.

        :param ctx: Context to parse.
        :return: None.
        """
        self.table_names.append(ctx.getText())


class StatementType(SQLiteListener):
    """ Listener class to record the type of statement found. """

    # Flag to indicate if a 'SELECT FROM' statement has been found.
    is_select = False

    # Flag to indicate if a DDL statement has been found.
    is_ddl = False

    # Flag to indicate if a DROP TABLE statement has been found.
    is_drop = False

    def enterSelect_core(self, ctx: SQLiteParser.Select_coreContext):
        """ Called when a 'SELECT FROM' statement is found. Sets the appropriate static flag.

        :param ctx: Context to parse.
        :return: None.
        """
        self.is_select = True

    def enterCreate_table_stmt(self, ctx: SQLiteParser.Create_table_stmtContext):
        """ Called when a 'CREATE TABLE' statement is found. Sets the appropriate static flag.

        :param ctx: Context to parse.
        :return: None.
        """
        self.is_ddl = True

    def enterDrop_table_stmt(self, ctx: SQLiteParser.Drop_table_stmtContext):
        """ Called when a 'DROP TABLE' statement is found. Sets the appropriate static flag.

        :param ctx: Context to parse.
        :return: None.
        """
        self.is_ddl, self.is_drop = True, True
