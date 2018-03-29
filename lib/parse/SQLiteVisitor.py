# Generated from C:/Users/glenn/Documents/College Documents/ICS 421/sql-process\SQLite.g4 by ANTLR 4.7
from antlr4 import *
if __name__ is not None and "." in __name__:
    from .SQLiteParser import SQLiteParser
else:
    from SQLiteParser import SQLiteParser

# This class defines a complete generic visitor for a parse tree produced by SQLiteParser.

class SQLiteVisitor(ParseTreeVisitor):

    # Visit a parse tree produced by SQLiteParser#parse.
    def visitParse(self, ctx:SQLiteParser.ParseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#error.
    def visitError(self, ctx:SQLiteParser.ErrorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#sql_stmt_list.
    def visitSql_stmt_list(self, ctx:SQLiteParser.Sql_stmt_listContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#sql_stmt.
    def visitSql_stmt(self, ctx:SQLiteParser.Sql_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#alter_table_stmt.
    def visitAlter_table_stmt(self, ctx:SQLiteParser.Alter_table_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#analyze_stmt.
    def visitAnalyze_stmt(self, ctx:SQLiteParser.Analyze_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#attach_stmt.
    def visitAttach_stmt(self, ctx:SQLiteParser.Attach_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#begin_stmt.
    def visitBegin_stmt(self, ctx:SQLiteParser.Begin_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#commit_stmt.
    def visitCommit_stmt(self, ctx:SQLiteParser.Commit_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#compound_select_stmt.
    def visitCompound_select_stmt(self, ctx:SQLiteParser.Compound_select_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#create_index_stmt.
    def visitCreate_index_stmt(self, ctx:SQLiteParser.Create_index_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#create_table_stmt.
    def visitCreate_table_stmt(self, ctx:SQLiteParser.Create_table_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#create_trigger_stmt.
    def visitCreate_trigger_stmt(self, ctx:SQLiteParser.Create_trigger_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#create_view_stmt.
    def visitCreate_view_stmt(self, ctx:SQLiteParser.Create_view_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#create_virtual_table_stmt.
    def visitCreate_virtual_table_stmt(self, ctx:SQLiteParser.Create_virtual_table_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#delete_stmt.
    def visitDelete_stmt(self, ctx:SQLiteParser.Delete_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#delete_stmt_limited.
    def visitDelete_stmt_limited(self, ctx:SQLiteParser.Delete_stmt_limitedContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#detach_stmt.
    def visitDetach_stmt(self, ctx:SQLiteParser.Detach_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#drop_index_stmt.
    def visitDrop_index_stmt(self, ctx:SQLiteParser.Drop_index_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#drop_table_stmt.
    def visitDrop_table_stmt(self, ctx:SQLiteParser.Drop_table_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#drop_trigger_stmt.
    def visitDrop_trigger_stmt(self, ctx:SQLiteParser.Drop_trigger_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#drop_view_stmt.
    def visitDrop_view_stmt(self, ctx:SQLiteParser.Drop_view_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#factored_select_stmt.
    def visitFactored_select_stmt(self, ctx:SQLiteParser.Factored_select_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#insert_stmt.
    def visitInsert_stmt(self, ctx:SQLiteParser.Insert_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#pragma_stmt.
    def visitPragma_stmt(self, ctx:SQLiteParser.Pragma_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#reindex_stmt.
    def visitReindex_stmt(self, ctx:SQLiteParser.Reindex_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#release_stmt.
    def visitRelease_stmt(self, ctx:SQLiteParser.Release_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#rollback_stmt.
    def visitRollback_stmt(self, ctx:SQLiteParser.Rollback_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#savepoint_stmt.
    def visitSavepoint_stmt(self, ctx:SQLiteParser.Savepoint_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#simple_select_stmt.
    def visitSimple_select_stmt(self, ctx:SQLiteParser.Simple_select_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#select_stmt.
    def visitSelect_stmt(self, ctx:SQLiteParser.Select_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#select_or_values.
    def visitSelect_or_values(self, ctx:SQLiteParser.Select_or_valuesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#update_stmt.
    def visitUpdate_stmt(self, ctx:SQLiteParser.Update_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#update_stmt_limited.
    def visitUpdate_stmt_limited(self, ctx:SQLiteParser.Update_stmt_limitedContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#vacuum_stmt.
    def visitVacuum_stmt(self, ctx:SQLiteParser.Vacuum_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#column_def.
    def visitColumn_def(self, ctx:SQLiteParser.Column_defContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#type_name.
    def visitType_name(self, ctx:SQLiteParser.Type_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#column_constraint.
    def visitColumn_constraint(self, ctx:SQLiteParser.Column_constraintContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#conflict_clause.
    def visitConflict_clause(self, ctx:SQLiteParser.Conflict_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#expr.
    def visitExpr(self, ctx:SQLiteParser.ExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#foreign_key_clause.
    def visitForeign_key_clause(self, ctx:SQLiteParser.Foreign_key_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#raise_function.
    def visitRaise_function(self, ctx:SQLiteParser.Raise_functionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#indexed_column.
    def visitIndexed_column(self, ctx:SQLiteParser.Indexed_columnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#table_constraint.
    def visitTable_constraint(self, ctx:SQLiteParser.Table_constraintContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#with_clause.
    def visitWith_clause(self, ctx:SQLiteParser.With_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#qualified_table_name.
    def visitQualified_table_name(self, ctx:SQLiteParser.Qualified_table_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#ordering_term.
    def visitOrdering_term(self, ctx:SQLiteParser.Ordering_termContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#pragma_value.
    def visitPragma_value(self, ctx:SQLiteParser.Pragma_valueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#common_table_expression.
    def visitCommon_table_expression(self, ctx:SQLiteParser.Common_table_expressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#result_column.
    def visitResult_column(self, ctx:SQLiteParser.Result_columnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#table_or_subquery.
    def visitTable_or_subquery(self, ctx:SQLiteParser.Table_or_subqueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#join_clause.
    def visitJoin_clause(self, ctx:SQLiteParser.Join_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#join_operator.
    def visitJoin_operator(self, ctx:SQLiteParser.Join_operatorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#join_constraint.
    def visitJoin_constraint(self, ctx:SQLiteParser.Join_constraintContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#select_core.
    def visitSelect_core(self, ctx:SQLiteParser.Select_coreContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#compound_operator.
    def visitCompound_operator(self, ctx:SQLiteParser.Compound_operatorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#cte_table_name.
    def visitCte_table_name(self, ctx:SQLiteParser.Cte_table_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#signed_number.
    def visitSigned_number(self, ctx:SQLiteParser.Signed_numberContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#literal_value.
    def visitLiteral_value(self, ctx:SQLiteParser.Literal_valueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#unary_operator.
    def visitUnary_operator(self, ctx:SQLiteParser.Unary_operatorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#error_message.
    def visitError_message(self, ctx:SQLiteParser.Error_messageContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#module_argument.
    def visitModule_argument(self, ctx:SQLiteParser.Module_argumentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#column_alias.
    def visitColumn_alias(self, ctx:SQLiteParser.Column_aliasContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#keyword.
    def visitKeyword(self, ctx:SQLiteParser.KeywordContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#name.
    def visitName(self, ctx:SQLiteParser.NameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#function_name.
    def visitFunction_name(self, ctx:SQLiteParser.Function_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#database_name.
    def visitDatabase_name(self, ctx:SQLiteParser.Database_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#table_name.
    def visitTable_name(self, ctx:SQLiteParser.Table_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#table_or_index_name.
    def visitTable_or_index_name(self, ctx:SQLiteParser.Table_or_index_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#new_table_name.
    def visitNew_table_name(self, ctx:SQLiteParser.New_table_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#column_name.
    def visitColumn_name(self, ctx:SQLiteParser.Column_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#collation_name.
    def visitCollation_name(self, ctx:SQLiteParser.Collation_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#foreign_table.
    def visitForeign_table(self, ctx:SQLiteParser.Foreign_tableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#index_name.
    def visitIndex_name(self, ctx:SQLiteParser.Index_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#trigger_name.
    def visitTrigger_name(self, ctx:SQLiteParser.Trigger_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#view_name.
    def visitView_name(self, ctx:SQLiteParser.View_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#module_name.
    def visitModule_name(self, ctx:SQLiteParser.Module_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#pragma_name.
    def visitPragma_name(self, ctx:SQLiteParser.Pragma_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#savepoint_name.
    def visitSavepoint_name(self, ctx:SQLiteParser.Savepoint_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#table_alias.
    def visitTable_alias(self, ctx:SQLiteParser.Table_aliasContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#transaction_name.
    def visitTransaction_name(self, ctx:SQLiteParser.Transaction_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLiteParser#any_name.
    def visitAny_name(self, ctx:SQLiteParser.Any_nameContext):
        return self.visitChildren(ctx)



del SQLiteParser