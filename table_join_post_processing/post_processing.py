import csv
import os

import ConfigParser
import sys
sys.path.insert(0, '../table_join')
import open_csv

__author__ = 'HY'

def filter_entries(row, requestor, error):
    #access interface needs to be skyserver
    if row[requestor] not in ["skyserver.sdss.org","skyserver.sdss3.org" ]:
        print "skip row: wrong requestor",row,[requestor],row
        return False

    #query cannot return errors
    if row[error] != "0":
        print "skip row: invalid query",row[error],row
        return False

    return True

if __name__ == '__main__':
    config = ConfigParser.ConfigParser()
    config.read('../table_join/config.ini')

    sessions_dir = config.get('Config','output')
    for filename in os.listdir(sessions_dir):
        session_table = open_csv.open_csv_file(sessions_dir+filename,
                                              config.get('Table1', 'keys').split(','),
                                              config.get('Table1','table_name'),
                                              0,0,
                                              0,0,
                                              0,0)

        requestor = session_table.attributes.index("requestor")
        error = session_table.attributes.index("error")
        statement_index = session_table.attributes.index("statement")
        output_statements = []
        for row in session_table.table_rows:
            try:
                row_values = session_table.table_rows[row]
                if filter_entries(row_values, requestor, error):
                    statement = []
                    statement.append(row_values[statement_index])
                    output_statements.append(statement)

            except Exception:
                print "Exception occurred in",session_table.table_rows[row]

        with open(config.get('Post-Processing','output')+filename, 'w') as csvfile:
             writer = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
             for row in output_statements:
                writer.writerow(row)
