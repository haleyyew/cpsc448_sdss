__author__ = 'HY'
import csv
import sql_table

def open_csv_file(str, list_of_keys, csv_name):
    counter = 0
    row_size = 0
    csv_file = open(str, 'rt')
    table_name = csv_name.split('.')[0]
    new_sql_table = sql_table.SqlTable(table_name)

    file_reader = csv.reader(csv_file, delimiter=',')
    for row in file_reader:
        if (counter == 0):
            row_size = len(row)
            counter += 1
            new_sql_table.declare_table_attributes(row)
            new_sql_table.declare_list_of_keys(list_of_keys)

            print "The number of attributes for table", new_sql_table.table_name, "is: ", row_size
            print "The attributes of table", new_sql_table.table_name, "are: ", '|'.join(row)

        elif (counter == 1):
            counter += 1

        elif (len(row) < row_size):
            counter += 1
            print "Exception in open_csv_file skipped row: ", '|'.join(row)

        else:
            #print(row[0])
            new_sql_table.add_row(row)
            counter += 1

    return new_sql_table