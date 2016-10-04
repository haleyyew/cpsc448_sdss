import collections
import csv
import ConfigParser

__author__ = 'HY'

import open_csv
import table_join
import pprint

def unit_test3():
    config = ConfigParser.ConfigParser()
    config.read('config.ini')

    sessionlog_keys = config.get('Table1', 'keys').split(',')
    sessionlog_table = open_csv.open_csv_file(config.get('Table1','path'), sessionlog_keys, config.get('Table1','table_name'))

    SqlLog_keys = config.get('Table2','keys').split(',')
    SqlLog_table = open_csv.open_csv_file(config.get('Table2','path'), SqlLog_keys, config.get('Table2','table_name'))

    my_join_attr = config.get('Table1','my_join_attributes').split(',')
    other_join_attr = config.get('Table1','their_join_attributes').split(',')
    table_join.table_join(sessionlog_table,my_join_attr,SqlLog_table,other_join_attr)

    SqlStatement_keys = config.get('Table3','keys').split(',')
    SqlStatement_table = open_csv.open_csv_file(config.get('Table3','path'), SqlStatement_keys, config.get('Table3','table_name'))

    my_join_attr = config.get('Table2','my_join_attributes').split(',')
    other_join_attr = config.get('Table2','their_join_attributes').split(',')
    table_join.table_join(SqlLog_table,my_join_attr,SqlStatement_table,other_join_attr)

    return sessionlog_table



def flatten(l):
    sub_list = []
    for el in l:
        if isinstance(el, list):
            sub_list.extend(flatten(el))
        else:
            sub_list.append(el)
    return sub_list

if __name__ == '__main__':
    table = unit_test3()

    for group in table.special_group:
        #pprint.pprint(table.special_group[group])
        #print("")
        with open(group+'.csv', 'w') as csvfile:
            writer = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            list_of_rows = table.special_group[group]
            for row in list_of_rows:
                flattened_row = flatten(row)
                writer.writerow(flattened_row)

    print "printing the joined table"
    flattened_attributes = flatten(table.attributes)
    print flattened_attributes

