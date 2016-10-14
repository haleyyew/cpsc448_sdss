__author__ = 'HY'
import time


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
    # if stop_after_few_rows:
    #     if num_rows>print_info_tiny:
    #         return True
    # if num_rows>print_info_small:
    #     # response = raw_input("Stop?")
    #     # if response =="y":
    #         return True
    #     # else:
    #     #     return False
    return False

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
            statement_string +=" "
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

