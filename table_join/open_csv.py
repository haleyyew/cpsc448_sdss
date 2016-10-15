__author__ = 'HY'
import csv
import sys
from open_csv_helper import *
import sql_table


def add_row_to_table(row,new_sql_table,sqllog_SqlTable):
    revised_row = []
    try:
        statementID = row[0]
        hits = row[-3]
        TemplateID = row[-2]
        studyperiod = row[-1]
        statement = ""
        for string in row[1:-3]:
            statement +=" "
            statement +=string

        revised_row.extend([statementID,statement,hits,TemplateID,studyperiod])

        if str(revised_row[0]) in sqllog_SqlTable.sqlstatement_group:
            if int(statementID) %print_info_small==0:
                print "adding row with statementID", revised_row[0]
                print revised_row
            new_sql_table.add_row(revised_row)




    except Exception:
        print "Exception in join_elements_to_string, row=",row



def evaluate_row(current_line,previous_line,statementID,row_accumulator,new_sql_table,sqllog_SqlTable):
    evaluation_1 = 1
    evaluation_2 = 1
    evaluation_3 = 1
    evaluation_4 = 1

    prnt ("current_line=",current_line)
    try:
        row_first_item = int(current_line[0].strip())
        if (statementID+1>row_first_item) or (row_first_item-statementID > 68027572):
            raise Exception
    except Exception:
        #current_line is not the beginning of a new csv row
        prnt ("not evaluation_1")
        evaluation_1 = 0

    try:
        first_number = int(current_line[-1].strip())
        second_number = int(current_line[-2].strip())
        third_number = int(current_line[-3].strip())

        if (first_number > 3 or first_number < 0 ):
            raise Exception
    except Exception:
        # current_line is not the end of a csv row
        prnt ("not evaluation_2")
        evaluation_2 = 0

    try:
        first_number = int(previous_line[-1].strip())
        second_number = int(previous_line[-2].strip())
        third_number = int(previous_line[-3].strip())

        if (first_number > 3 or first_number < 0 ):
            raise Exception
    except Exception:
        # previous_line is not the end of a csv row
        prnt ("not evaluation_3")
        evaluation_3 = 0

    try:
        row_first_item = int(previous_line[0].strip())
        if (statementID+1>row_first_item) or (row_first_item-statementID > 68027572):
            raise Exception
    except Exception:
        #current_line is the beginning of a new csv row and
        prnt ("not evaluation_4")
        evaluation_4 = 0

    if evaluation_1 and evaluation_2 and evaluation_3 and evaluation_4:
        prnt ("evaluation_1 and evaluation_2 and evaluation_3 and evaluation_4",current_line,previous_line)
        # previous_line==[statement_ID,statement,hits,TemplateID,studyperiod]
        # current_line==[statement_ID,statement,hits,TemplateID,studyperiod]
        add_row_to_table(current_line,new_sql_table,sqllog_SqlTable)

        statementID +=1

    elif evaluation_1 and evaluation_3 and evaluation_4:
        prnt ("evaluation_1 and evaluation_3 and evaluation_4",current_line,previous_line)
        # previous_line==[statement_ID,statement,hits,TemplateID,studyperiod]
        # current_line==[statement_ID,statement]

        row_accumulator = []
        row_accumulator.extend(current_line)

    elif evaluation_1 and evaluation_2 and evaluation_3:
        prnt ("evaluation_1 and evaluation_2 and evaluation_3",current_line,previous_line)
        # previous_line==[statement,hits,TemplateID,studyperiod]
        # current_line==[statement_ID,statement,hits,TemplateID,studyperiod]

        add_row_to_table(row_accumulator,new_sql_table,sqllog_SqlTable)
        statementID +=1

        row_accumulator = []
        add_row_to_table(current_line,new_sql_table,sqllog_SqlTable)
        statementID +=1

    elif evaluation_1 and evaluation_3:
        prnt ("evaluation_1 and evaluation_3",current_line,previous_line)
        # previous_line==[statement,hits,TemplateID,studyperiod]
        # current_line==[statement_ID,statement]

        add_row_to_table(row_accumulator,new_sql_table,sqllog_SqlTable)
        statementID +=1

        row_accumulator = []
        row_accumulator.extend(current_line)

    elif evaluation_1 and evaluation_2 and statementID==1:
        prnt ("evaluation_1 and evaluation_2 and statementID==1",current_line,previous_line)
        # previous_line==[]
        # current_line==[statement_ID,statement,hits,TemplateID,studyperiod]

        add_row_to_table(current_line,new_sql_table,sqllog_SqlTable)
        statementID +=1

    elif evaluation_1 and evaluation_2:
        prnt ("evaluation_1 and evaluation_2",current_line,previous_line)
        # previous_line==[]
        # current_line==[statement_ID,statement,hits,TemplateID,studyperiod]

        add_row_to_table(current_line,new_sql_table,sqllog_SqlTable)
        statementID +=1

    elif len(current_line)==1 and current_line[0].strip()=="(68027572 rows affected)":
        add_row_to_table(row_accumulator,new_sql_table,sqllog_SqlTable)

    elif not(evaluation_1) and not(evaluation_2):
        prnt ("not(evaluation_1) and not(evaluation_2)",current_line,previous_line)
        # current_line==[statement]
        row_accumulator.extend(current_line)

    elif evaluation_1:
        prnt ("evaluation_1",current_line,previous_line)
        # current_line==[statement_ID,statement]
        row_accumulator = []
        row_accumulator.extend(current_line)

    else:
        row_accumulator.extend(current_line)

    return (row_accumulator,statementID)


def open_sqlstatement_csv(new_sql_table, path, list_of_keys, sqllog_SqlTable, use_alternative_method):
    if use_alternative_method:
        statementID = 1
        row_accumulator = []
        previous_line = []
        counter = 0

        with open(path, 'rt') as csv_file:
            file_reader = csv.reader(csv_file, delimiter=',')
            for current_line in file_reader:

                if counter %print_info==0:
                    print "I am still alive, reading currLineNum", counter
                    print "My table is now", sys.getsizeof(new_sql_table.table_rows), "bytes"
                    print "I have added", new_sql_table.num_rows, "rows"
                    debug_stat = debug(new_sql_table.num_rows, True)
                    if debug_stat:
                        return new_sql_table

                if (counter == 0):
                    row_size = len(current_line)
                    counter += 1
                    new_sql_table.declare_table_attributes(current_line)
                    new_sql_table.declare_list_of_keys(list_of_keys)

                    print "The number of attributes for table", new_sql_table.table_name, "is: ", row_size
                    print "The attributes of table", new_sql_table.table_name, "are: ", '|'.join(current_line)

                # The second row always store redundant information
                elif (counter == 1):
                    counter += 1

                else:
                    row_accumulator,statementID = evaluate_row(current_line,previous_line,
                                                   statementID,row_accumulator,
                                                   new_sql_table,sqllog_SqlTable)
                    previous_line = current_line

    else:
        parseSqlStatement(new_sql_table, path, list_of_keys, sqllog_SqlTable)
    print "I have finished reading sqlstatement.csv"


def open_csv_file(path, list_of_keys, csv_name,
                  sessions_flag, num_of_sessions,
                  sqllog_flag, sessionlog_SqlTable,
                  sqlstatement_flag, sqllog_SqlTable, use_alternative_method):
    """
    Read a csv file in path, and store the contents of the csv file into a newly created SqlTable class object,
    where each row of the csv file is represented as a list of strings and stored by SqlTable.add_row(row)
    """
    counter = 0
    row_size = 0

    table_name = csv_name.split('.')[0]
    new_sql_table = sql_table.SqlTable(table_name)

    if sqlstatement_flag:
        open_sqlstatement_csv(new_sql_table, path, list_of_keys, sqllog_SqlTable, use_alternative_method)
        sqllog_SqlTable.sqlstatement_group = {}
        return new_sql_table

    with open(path, 'rt') as csv_file:
        file_reader = csv.reader(csv_file, delimiter=',')
        for row in file_reader:
            # This first row in the csv file is always the header, which is the attribute names for each column
            if (counter == 0):
                row_size = len(row)
                counter += 1
                new_sql_table.declare_table_attributes(row)
                new_sql_table.declare_list_of_keys(list_of_keys)

                print "The number of attributes for table", new_sql_table.table_name, "is: ", row_size
                print "The attributes of table", new_sql_table.table_name, "are: ", '|'.join(row)

            # The second row always store redundant information
            elif (counter == 1):
                counter += 1

            # If the number of columns of this row is not equal to the number of columns in the header,
            # then this is not a valid row
            elif (len(row) < row_size):
                counter += 1
                print "Exception in open_csv_file skipped row: ", '|'.join(row)

            else:
                if sqllog_flag:
                    check_sqllog_row_and_add(row, sessionlog_SqlTable, new_sql_table)
                else:
                    new_sql_table.add_row(row)
                counter += 1

                # Debugging, I want to know what row am I reading and how many bytes have I stored so far.
                if counter%print_info == 0:
                    print "I am now reading row #", counter
                    print "My table is now", sys.getsizeof(new_sql_table.table_rows), "bytes"
                    print "I have added", new_sql_table.num_rows, "rows"
                    if sessions_flag:
                        print "I have added", len(new_sql_table.session_group) ,"sessions"

                    debug_stat = debug(new_sql_table.num_rows, False)
                    if debug_stat:
                        return new_sql_table


                # I am only reading num_of_sessions sequentially, I will not store any more sessions after this
                if sessions_flag and (len(new_sql_table.session_group) > num_of_sessions):
                    new_sql_table.delete_session_group(row)
                    break

    if sqllog_flag:
        sessionlog_SqlTable.sqllog_group = {}

    return new_sql_table