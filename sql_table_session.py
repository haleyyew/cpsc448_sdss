__author__ = 'HY'
import sql_table

class SqlTableSession(sql_table.SqlTable):

    def __init__(self, table_name):
        sql_table.SqlTable.__init__(self, table_name)

