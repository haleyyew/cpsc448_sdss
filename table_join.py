__author__ = 'HY'
import sql_table

def table_join(self_sql_table, my_table_attributes, other_sql_table, other_table_attributes):
    my_table_attributes_index = []
    other_table_attributes_index = []

    if (len(my_table_attributes) != len(other_table_attributes)):
        print("Error in table_join for tables ", self_sql_table.table_name, other_sql_table.table_name)
        return
    for join_attr in my_table_attributes:
        index = self_sql_table.attributes.index(join_attr)
        if index<0:
            print("Error in table_join for table ", self_sql_table.table_name)
            return
        my_table_attributes_index.append(index)

    for join_attr in other_table_attributes:
        index = other_sql_table.attributes.index(join_attr)
        if index<0:
            print("Error in table_join for table ", other_sql_table.table_name)
            return
        other_table_attributes_index.append(index)

    joined_table = sql_table.SqlTable(self_sql_table.table_name+other_sql_table.table_name)
    print(joined_table.table_name)

    joined_table.declare_table_attributes(self_sql_table.attributes+other_sql_table.attributes)
    print("The attributes of table are: ", '|'.join(joined_table.attributes))

    joined_table.declare_list_of_keys(self_sql_table.list_of_keys+other_sql_table.list_of_keys)
    print("The list of keys of table are: ", '|'.join(joined_table.list_of_keys))

    for self_row in self_sql_table.table_rows:
        for other_row in other_sql_table.table_rows:

            add = True
            self_row_values = self_sql_table.table_rows[self_row]
            other_row_values = other_sql_table.table_rows[other_row]
            #print("comparing: ", '|'.join(self_row_values+other_row_values))
            for i in range(len(my_table_attributes_index)):
                if self_row_values[my_table_attributes_index[i]] == other_row_values[other_table_attributes_index[i]]:
                    continue
                else:
                    add = False
                    break
            if add:
                joined_table.add_row(self_row_values+other_row_values)

    print("number of rows added: ", joined_table.num_rows)
    return joined_table
