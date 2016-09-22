__author__ = 'HY'

import open_csv
import table_join

def unit_test1():
    ipAll_keys = ['ipAddressString','ipID','mystatus']
    ipAll_table = open_csv.open_csv_file('ipAll.csv', ipAll_keys)

    ipdomain_keys = ['domainID','status']
    ipdomain_table = open_csv.open_csv_file('ipdomain.csv', ipdomain_keys)

    SessionLog_keys = ['sessionID']
    SessionLog_table = open_csv.open_csv_file('SessionLog.csv', SessionLog_keys)

    my_join_attr = ['domainID','mystatus']
    other_join_attr = ['domainID','status']
    joined_table = table_join.table_join(ipAll_table,my_join_attr,ipdomain_table,other_join_attr)

    my_join_attr = ['ipID']
    other_join_attr = ['IpID']
    final_table = table_join.table_join(joined_table,my_join_attr,SessionLog_table,other_join_attr)

    return final_table

def unit_test2():
    session_keys = ['sessionID','FirstEventID','startTime','limitTime']
    session_table = open_csv.open_csv_file('session.csv', session_keys)

    sessionlog_keys = ['sessionID','rankInSession']
    sessionlog_table = open_csv.open_csv_file('sessionlog.csv', sessionlog_keys)

    ipAll_keys = ['ipID']
    ipAll_table = open_csv.open_csv_file('ipAll.csv', ipAll_keys)

    WebLog_keys = []
    WebLog_table = open_csv.open_csv_file('WebLog.csv', WebLog_keys)

    LogSource_keys = ['logID']
    LogSource_table = open_csv.open_csv_file('LogSource.csv', LogSource_keys)

    SqlLog_keys = ['sqlID']
    SqlLog_table = open_csv.open_csv_file('SqlLog.csv', SqlLog_keys)

    SqlStatement_keys = ['statementID']
    SqlStatement_table = open_csv.open_csv_file('SqlStatement.csv', SqlStatement_keys)

    my_join_attr = ['sessionID']
    other_join_attr = ['sessionID']
    session_sessionlog_table = table_join.table_join(session_table,my_join_attr,sessionlog_table,other_join_attr)

    my_join_attr = ['IpID']
    other_join_attr = ['ipID']
    s_s_ipAll_table = table_join.table_join(session_sessionlog_table,my_join_attr,ipAll_table,other_join_attr)

    my_join_attr = ['ipID']
    other_join_attr = ['clientIpID']
    s_s_i_WebLog_table = table_join.table_join(s_s_ipAll_table,my_join_attr,WebLog_table,other_join_attr)

    my_join_attr = ['logID']
    other_join_attr = ['logID']
    s_s_i_W_LogSource_table = table_join.table_join(s_s_i_WebLog_table,my_join_attr,LogSource_table,other_join_attr)

    my_join_attr = ['logID']
    other_join_attr = ['logID']
    s_s_i_W_L_SqlLog_table = table_join.table_join(s_s_i_W_LogSource_table,my_join_attr,SqlLog_table,other_join_attr)

    my_join_attr = ['statementID']
    other_join_attr = ['statementID']
    final_table = table_join.table_join(s_s_i_W_L_SqlLog_table,my_join_attr,SqlStatement_table,other_join_attr)

    return final_table

if __name__ == '__main__':
    unit_test1()
    #unit_test2()