__author__ = 'HY'

def get_foreign_key(table_row, join_attributes_index):
    foreign_key = ""
    for index in join_attributes_index:
        string = table_row[index]
        foreign_key += str(string)
    return foreign_key

def table_join(self_sql_table, my_table_attributes, other_sql_table, other_table_attributes):
    my_table_attributes_index = []
    other_table_attributes_index = []

    if (len(my_table_attributes) != len(other_table_attributes)):
        print "Error in table_join for tables ", self_sql_table.table_name, other_sql_table.table_name
        return
    for join_attr in my_table_attributes:
        index = self_sql_table.attributes.index(join_attr)
        if index<0:
            print "Error in table_join for table ", self_sql_table.table_name, "my_table_attributes"
            return
        my_table_attributes_index.append(index)

    for join_attr in other_table_attributes:
        index = other_sql_table.attributes.index(join_attr)
        if index<0:
            print "Error in table_join for table ", other_sql_table.table_name, "other_table_attributes"
            return
        other_table_attributes_index.append(index)

    self_sql_table.attributes.append(other_sql_table.attributes)
    print "joining tables: ", self_sql_table.table_name, "and", other_sql_table.table_name , ":"
    print self_sql_table.attributes

    for self_row in self_sql_table.table_rows:
        self_row_values = self_sql_table.table_rows[self_row]
        foreign_key = get_foreign_key(self_row_values, my_table_attributes_index)
        self_row_values.append("temp")
        self_row_values[-1] = other_sql_table.get_row(foreign_key)
