__author__ = 'HY'
import csv
import re
import sys
import time

import sql_table

print_info = 2000000
max_sql_statement_rows = 68027572
print_info_small = 100
print_info_medium = 200000
print_info_tiny = 5
print_info_pico = 1

def measure_time(early):
    later = time.time()
    difference = int(later - early)
    if difference > 60:
        print "This program is taking too long,",difference,"per loop iteration"
    return later

def debug(num_rows, stop_after_few_rows):
    if stop_after_few_rows:
        if num_rows>print_info_tiny:
            return True
    if num_rows>print_info_small:
        # response = raw_input("Stop?")
        # if response =="y":
            return True
        # else:
        #     return False
    # return False

def print_exception(source, element_name, element, line_number):
    if line_number%print_info_medium == 0:
        print "Exception occurred in",source,",element_name",element_name,"=",element



def check_sqllog_row_and_add(row, sessionlog_SqlTable, new_sql_table):
    if row[0] in sessionlog_SqlTable.sqllog_group:
        if int(row[0])%print_info_small ==0:
            print "adding row with ID", row[0], "== sqlID"
        new_sql_table.add_row(row)


def add_row_from_regex_match(new_sql_table,
                             statementID,queryStatement,hits,TemplateID,studyperiod,
                             sqllog_SqlTable):
    row = []
    row.append(statementID)
    statement_string = ""
    try:
        statement_string = ""
        for string in queryStatement:
            statement_string +=string
    except (Exception):
        print_exception("add_row_from_regex_match", "statementID", statementID, statementID)

    row.append(statement_string)
    row.append(hits)
    row.append(TemplateID)
    row.append(studyperiod)


    if str(row[0]) in sqllog_SqlTable.sqlstatement_group:
        if statementID %print_info_small==0:
            print "adding row with statementID", row[0],
            print row
        new_sql_table.add_row(row)



def parseSqlStatement(new_sql_table, inputFile, list_of_keys, sqllog_SqlTable):
    """
    This is the provided parser for sqlstatement.csv
    """
    queryStatement = []
    j =0
    currLineNum = -1
    prevLineNum =-1
    prevLine=""

    statementID = 0
    hits = 0
    TemplateID = 0
    studyperiod = 0

    current_time = time.time()

    matchCurrLine = ""
    matchCurrLine2 = ""
    matchCurrLine3 = ""
    matchPrevLine = ""

    with open(inputFile, 'rb') as f:
        for currLine in f:
            currLineNum +=1

            current_time = measure_time(current_time)

            if currLineNum %print_info==0:
                print "I am still alive, reading currLineNum", currLineNum
                print "My table is now", sys.getsizeof(new_sql_table.table_rows), "bytes"
                print "I have added", new_sql_table.num_rows, "rows"

                debug_stat = debug(new_sql_table.num_rows, True)
                if debug_stat:
                    return new_sql_table


            if currLineNum == 0:
                match = re.search("(.+),(.+),(.+),(.+),(.+)", currLine)
                row = []
                row.append(match.group(1))
                row.append(match.group(2))
                row.append(match.group(3))
                row.append(match.group(4))
                row.append(match.group(5).strip('\r'))
                new_sql_table.declare_table_attributes(row)
                new_sql_table.declare_list_of_keys(list_of_keys)
                continue
            if currLineNum == 1:
                continue

            matchCurrLine = re.search(r'^(\d+),(.*)', currLine)
            matchCurrLine2 = re.search('^(.*),(\d+),(\d+),(\d+)', currLine)
            matchCurrLine3 = re.search('^(\d+),(.*),(\d+),(\d+),(\d+)', currLine)
            matchPrevLine = re.search('^(.*),(\d+),(\d+),(\d+)', prevLine)
            if ( matchCurrLine and   matchPrevLine ) :
                #wrap up previous statement
                try:
                    queryStatement.append(matchPrevLine.group(1).strip('\n').strip('\r'))
                    hits = int(matchPrevLine.group(2))
                    TemplateID = int(matchPrevLine.group(3))
                    studyperiod = int(matchPrevLine.group(4))
                    add_row_from_regex_match(new_sql_table,
                                         statementID,queryStatement,hits,TemplateID,studyperiod,
                                         sqllog_SqlTable)
                except (Exception):
                    print_exception("parseSqlStatement(matchCurrLine and matchPrevLine)", "currLineNum", currLineNum, currLineNum)

                queryStatement = []

            elif matchCurrLine3:
                try:
                    statementID =  int(matchCurrLine3.group(1))
                    if statementID>max_sql_statement_rows:
                        statementID = -1
                    queryStatement.append(matchCurrLine3.group(2).strip('\n').strip('\r'))
                    hits = int(matchCurrLine3.group(3))
                    TemplateID = int(matchCurrLine3.group(4))
                    studyperiod = int(matchCurrLine3.group(5))

                    add_row_from_regex_match(new_sql_table,
                                         statementID,queryStatement,hits,TemplateID,studyperiod,
                                         sqllog_SqlTable)
                except (Exception):
                    print_exception("parseSqlStatement(matchCurrLine3)", "currLineNum", currLineNum, currLineNum)

                queryStatement = []

            if matchCurrLine:
                # start new statment
                #print "matchCurrLine", currLine
                try:
                    statementID =  int(matchCurrLine.group(1))
                    if statementID>max_sql_statement_rows:
                        print_exception("parseSqlStatement(statementID>max_sql_statement_rows)", "statementID", statementID, statementID)
                        statementID = -1
                    queryStatement= []
                    j = 0
                    queryStatement.append(matchCurrLine.group(2).strip('\n').strip('\r'))
                    j+=1
                except (Exception):
                    print_exception("parseSqlStatement(matchCurrLine)", "currLineNum", currLineNum, currLineNum)

            elif matchCurrLine2:
                #skip this current line
                pass

            elif currLineNum !=0 and currLineNum !=1:
                #print "just adding", currLine
                try:
                    queryStatement.append(currLine.strip('\n').strip('\r'))
                    j+=1
                except (Exception):
                    print_exception("parseSqlStatement(currLineNum !=0 and currLineNum !=1)", "currLineNum", currLineNum, currLineNum)

            prevLine = currLine
            prevLineNum = currLineNum

    matchPrevLine = re.search('^(.*),(\d+),(\d+),(\d+)', prevLine)
    if matchPrevLine:
        try:
            queryStatement.append(matchPrevLine.group(1).strip('\n').strip('\r'))
            hits = int(matchPrevLine.group(2))
            TemplateID = int(matchPrevLine.group(3))
            studyperiod = int(matchPrevLine.group(4))
            add_row_from_regex_match(new_sql_table,
                                 statementID,queryStatement,hits,TemplateID,studyperiod,
                                 sqllog_SqlTable)

        except (Exception):
            print_exception("parseSqlStatement", "currLineNum", currLineNum, currLineNum)
    print "I have finished reading sqlstatement.csv"


def open_csv_file(path, list_of_keys, csv_name,
                  sessions_flag, num_of_sessions,
                  sqllog_flag, sessionlog_SqlTable,
                  sqlstatement_flag, sqllog_SqlTable):
    """
    Read a csv file in path, and store the contents of the csv file into a newly created SqlTable class object,
    where each row of the csv file is represented as a list of strings and stored by SqlTable.add_row(row)
    """
    counter = 0
    row_size = 0

    table_name = csv_name.split('.')[0]
    new_sql_table = sql_table.SqlTable(table_name)

    if sqlstatement_flag:
        parseSqlStatement(new_sql_table, path, list_of_keys, sqllog_SqlTable)
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