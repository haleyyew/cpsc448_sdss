__author__ = 'HY'
import csv
import sql_table
import re
import sys
import time

def measure_time(early):
    later = time.time()
    difference = int(later - early)
    if difference > 60:
        print "This program is taking too long,",difference,"per cycle"
    return later


def check_sqllog_row_and_add(row, sessionlog_SqlTable, new_sql_table):
    if row[0] in sessionlog_SqlTable.sqllog_group:
        if int(row[0])%2 ==0:
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
        # statement_string = ' '.join(queryStatement[statementID][j].strip('\n').strip('\r')
        #             for j in sorted(queryStatement[statementID].keys()))
    except (Exception):
        if statementID %4==0:
            print "Exception occurred in add_row_from_regex_match, statementID=",statementID
    row.append(statement_string)
    row.append(hits)
    row.append(TemplateID)
    row.append(studyperiod)

    if str(row[0]) in sqllog_SqlTable.sqlstatement_group:
        if statementID %4==0:
            print "adding row with statementID", row[0]
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

    with open(inputFile, 'rb') as f:
        for currLine in f:
            currLineNum +=1

            current_time = measure_time(current_time)

            if currLineNum %200000==0:
                print "I am still alive, reading currLineNum", currLineNum

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
                    queryStatement.append(matchPrevLine.group(1))
                    hits = int(matchPrevLine.group(2))
                    TemplateID = int(matchPrevLine.group(3))
                    studyperiod = int(matchPrevLine.group(4))
                    add_row_from_regex_match(new_sql_table,
                                         statementID,queryStatement,hits,TemplateID,studyperiod,
                                         sqllog_SqlTable)
                except (Exception):
                    print "Exception in parseSqlStatement, currLineNum=", currLineNum

                queryStatement = []

            elif matchCurrLine3:
                try:
                    statementID =  int(matchCurrLine3.group(1))
                    if statementID>68027572:
                        statementID = -1
                    queryStatement.append(matchCurrLine3.group(2))
                    hits = int(matchCurrLine3.group(3))
                    TemplateID = int(matchCurrLine3.group(4))
                    studyperiod = int(matchCurrLine3.group(5))

                    add_row_from_regex_match(new_sql_table,
                                         statementID,queryStatement,hits,TemplateID,studyperiod,
                                         sqllog_SqlTable)
                except (Exception):
                    print "Exception in parseSqlStatement, currLineNum=", currLineNum

                queryStatement = []

            if matchCurrLine:
                # start new statment
                #print "matchCurrLine", currLine
                try:
                    statementID =  int(matchCurrLine.group(1))
                    if statementID>68027572:
                        statementID = -1
                    queryStatement= []
                    j = 0
                    queryStatement.append(matchCurrLine.group(2))
                    j+=1
                except (Exception):
                    print "Exception in parseSqlStatement, currLineNum=", currLineNum

            elif matchCurrLine2:
                #skip this current line
                pass

            elif currLineNum !=0 and currLineNum !=1:
                #print "just adding", currLine
                try:
                    queryStatement.append(currLine)
                    j+=1
                except (Exception):
                    print "Exception in parseSqlStatement, currLineNum=", currLineNum

            prevLine = currLine
            prevLineNum = currLineNum

    matchPrevLine = re.search('^(.*),(\d+),(\d+),(\d+)', prevLine)
    if matchPrevLine:
        try:
            queryStatement.append(matchPrevLine.group(1))
            hits = int(matchPrevLine.group(2))
            TemplateID = int(matchPrevLine.group(3))
            studyperiod = int(matchPrevLine.group(4))
            add_row_from_regex_match(new_sql_table,
                                 statementID,queryStatement,hits,TemplateID,studyperiod,
                                 sqllog_SqlTable)

        except (Exception):
            print "Exception in parseSqlStatement, currLineNum=", currLineNum
        queryStatement = []

    print "I have finished reading sqlstatement.csv"



#FUNCTION NOT WORKING YET
def check_sqlstatement_row(error,statementID,queryStatement,row,output_row):
    statementID_queryStatement = 0
    statementID_queryStatement_hits_TemplateID_studyperiod = 1

    try:
        first_item_in_row = int(row[0])
        if first_item_in_row==statementID:
            #found pattern statementID,queryStatement,etc
            return
    except Exception:
        statementID -= 1
        queryStatement.extend(row)

    try:
        last_item_in_row = int(row[-1])
        second_last_item_in_row = int(row[-2])
        third_last_item_in_row = int(row[-3])
    except:
        return
    return

#FUNCTION NOT WORKING YET
def parseSqlStatementNew(new_sql_table, path, list_of_keys, sqllog_SqlTable):
    """
    queryStatement is [item,item,...,item]
    Case1: [statementID,queryStatement,hits,TemplateID,studyperiod]
        add this row to new_sql_table
    Case2: [statementID,queryStatement_incomplete]
        save the statementID, and save queryStatement_incomplete to temp_list
    """
    statementID = 0
    queryStatement = []
    hits = 0
    TemplateID = 0
    studyperiod = 0

    current_time = time.time()
    counter = 0

    with open(path, 'rt') as csv_file:
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

            else:
                statementID +=1

    return



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
                if counter%200000 == 0:
                    print "I am now reading row #", counter
                    print "My table is now", sys.getsizeof(new_sql_table.table_rows), "bytes"
                    print "I have added", new_sql_table.num_rows, "rows"
                    if sessions_flag:
                        print "I have added", len(new_sql_table.session_group) ,"sessions"

                # I am only reading num_of_sessions sequentially, I will not store any more sessions after this
                if sessions_flag and (len(new_sql_table.session_group) > num_of_sessions):
                    new_sql_table.delete_session_group(row)
                    break

    if sqllog_flag:
        sessionlog_SqlTable.sqllog_group = {}

    return new_sql_table