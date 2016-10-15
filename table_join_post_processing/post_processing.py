import csv
import os

import ConfigParser
import sys
sys.path.insert(0, '../table_join')
import open_csv

__author__ = 'HY'

print_info = 100000

def prnt(counter,*argv):
    if counter%print_info != 0:
        return

    output = ""
    for string in argv:
        output +=", "
        output +=string
    print output
    return

def store(counter,random_storage,filename,row):
    if counter%print_info != 0:
        random_storage[filename] = row
    return random_storage


def filter_entries(row, requestor, error, counter):
    #access interface needs to be skyserver
    if row[requestor] not in ["skyserver.sdss.org","skyserver.sdss3.org" ]:
        prnt(counter, "skip row: wrong requestor",row,[requestor],row)
        return False

    #query cannot return errors
    if row[error] != "0":
        prnt(counter, "skip row: invalid query",row[error],row)
        return False

    return True

def debug():
    while (1):
        try:
            response = raw_input("Please enter command: ")
        except Exception:
            continue
        split_command = response.split()

        if response == "close":
            break

        else:
            try:
                num_of_samples = int(split_command[0])
                counter = 0
                for row in random_storage:
                    counter+=1
                    if counter>num_of_samples:
                        break
                    print random_storage[row]

            except Exception:
                print Exception
                continue
    return

if __name__ == '__main__':
    config = ConfigParser.ConfigParser()
    config.read('../table_join/config.ini')

    sessions_dir = config.get('Config','output')
    counter = 0
    random_storage = {}

    for filename in os.listdir(sessions_dir):
        session_table = open_csv.open_csv_file(sessions_dir+filename,
                                              config.get('Table1', 'keys').split(','),
                                              config.get('Table1','table_name'),
                                              0,0,
                                              0,0,
                                              0,0,0)

        requestor = session_table.attributes.index("requestor")
        error = session_table.attributes.index("error")
        statement_index = session_table.attributes.index("statement")
        output_statements = []
        for row in session_table.table_rows:
            try:
                counter += 1
                row_values = session_table.table_rows[row]
                if filter_entries(row_values, requestor, error, counter):
                    statement = []
                    statement.append(row_values[statement_index])
                    output_statements.append(statement)

            except Exception:
                prnt(counter, "Exception occurred in",session_table.table_rows[row])

        if len(output_statements) == 0:
            continue
        with open(config.get('Post-Processing','output')+filename, 'w') as csvfile:
             writer = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
             for row in output_statements:
                counter += 1
                prnt(counter,row)
                store(counter,random_storage,filename,row)

                writer.writerow(row)

    debug()
