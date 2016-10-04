import collections
import csv
import ConfigParser

__author__ = 'HY'

import open_csv
import table_join
import pprint

def unit_test(config):
    """
    Read sessionlog.csv, sqllog.csv, and sqlstatement.csv and store them in SqlTable class objects
    sessionlog_table, SqlLog_table, SqlStatement_table.
    Then join sessionlog_table with SqlLog_table on ID=sqlID,
    and join SqlLog_table with SqlStatement_table on statementID=statementID
    The two joins will result sessionlog_table storing values for all 3 tables, return sessionlog_table
    """

    sessionlog_keys = config.get('Table1', 'keys').split(',')
    sessionlog_table = open_csv.open_csv_file(config.get('Config','input')+config.get('Table1','path'),
                                              sessionlog_keys, config.get('Table1','table_name'))

    SqlLog_keys = config.get('Table2','keys').split(',')
    SqlLog_table = open_csv.open_csv_file(config.get('Config','input')+config.get('Table2','path'),
                                          SqlLog_keys, config.get('Table2','table_name'))

    my_join_attr = config.get('Table1','my_join_attributes').split(',')
    other_join_attr = config.get('Table1','their_join_attributes').split(',')
    table_join.table_join(sessionlog_table,my_join_attr,SqlLog_table,other_join_attr)

    SqlStatement_keys = config.get('Table3','keys').split(',')
    SqlStatement_table = open_csv.open_csv_file(config.get('Config','input')+config.get('Table3','path'),
                                                SqlStatement_keys, config.get('Table3','table_name'))

    my_join_attr = config.get('Table2','my_join_attributes').split(',')
    other_join_attr = config.get('Table2','their_join_attributes').split(',')
    table_join.table_join(SqlLog_table,my_join_attr,SqlStatement_table,other_join_attr)

    return sessionlog_table

def flatten(l):
    """
    Convert the hierarchical table attributes to a flat representation
    For example, if table has attributes ['ID','date','flag', ['statement','sqlID']], then the
    flattened attributes is ['ID','date','flag','statement','sqlID']
    """

    sub_list = []
    for el in l:
        if isinstance(el, list):
            sub_list.extend(flatten(el))
        else:
            sub_list.append(el)
    return sub_list

if __name__ == '__main__':

    # Read the configuration file
    config = ConfigParser.ConfigParser()
    config.read('config.ini')
    num_of_sessions = int(config.get('Config','num_of_sessions'))
    sessions_counter = 0

    # Read csv files and store contents of each csv into a SqlTable class object
    # Then join tables by joining contents of two SqlTable objects
    table = unit_test(config)

    print "printing the joined table to csv"
    # Convert the hierarchical table attributes to a flat representation
    flattened_attributes = flatten(table.attributes)
    #print flattened_attributes

    # Store the joined session table into csv files where each file stores log entries of a single session
    for group in table.session_group:
        sessions_counter += 1
        if sessions_counter > num_of_sessions:
            break
        #pprint.pprint(table.special_group[group])
        with open(config.get('Config','output')+group+'.csv', 'w') as csvfile:
            writer = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            list_of_rows = table.session_group[group]

            writer.writerow(flattened_attributes)
            for row in list_of_rows:
                flattened_row = flatten(row)
                writer.writerow(flattened_row)



