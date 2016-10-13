__author__ = 'HY'

def get_foreign_key(table_row, join_attributes_index):
    """
    Helper function for table_join() to retrieve the foreign key for a row in the table.
    Note that the foreign key in the table must be the primary key in the table being joined with
    """
    foreign_key = ""
    for index in join_attributes_index:
        string = table_row[index]
        foreign_key += str(string)
    return foreign_key

def table_join(self_sql_table, my_table_attributes, other_sql_table, other_table_attributes):
    """
    Joins the current table self_sql_table with another table other_sql_table on
    my_table_attributes = other_table_attributes for each attribute
    """
    my_table_attributes_index = []
    other_table_attributes_index = []

    # If the number of attributes to join on are not equal, then we can't join the 2 tables
    if (len(my_table_attributes) != len(other_table_attributes)):
        print "Error in table_join for tables ", self_sql_table.table_name, other_sql_table.table_name
        return

    # Find the positions of the joining attributes in the row, for both self and other tables
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

    # Add a single item to the attributes field in self table that references the list of attributes
    # in other table.
    # For example, self table has attributes ['ID','date','flag'] and other table has attributes ['statement','sqlID'],
    # then the new attributes becomes ['ID','date','flag', ['statement','sqlID']]
    self_sql_table.attributes.extend(other_sql_table.attributes)
    print "joining tables: ", self_sql_table.table_name, "and", other_sql_table.table_name , ":"
    print self_sql_table.attributes

    counter = 0
    # Add a reference item in the row of self table to reference a row in other table given that the joining condition
    # is true.
    # For example, self table has attributes ['ID','date','flag'] and other table has attributes ['statement','sqlID'],
    # and the joining condition self.'ID'=other.'sqlID' is true,
    # then self table row will be modified to store ['ID','date','flag', ['statement','sqlID']]
    for self_row in self_sql_table.table_rows:
        self_row_values = self_sql_table.table_rows[self_row]
        foreign_key = get_foreign_key(self_row_values, my_table_attributes_index)
        self_row_values.extend(other_sql_table.get_row(foreign_key))
        counter += 1
        if counter%200000 == 0:
            print "I am finding a row in other_sql_table to join with row", counter, "in self_sql_table"
        if (foreign_key !="") and (counter%1000==0):
            print self_row_values
