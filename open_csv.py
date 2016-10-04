__author__ = 'HY'
import csv
import sql_table
import re

def parseSqlStatement():
	inputFile="../smallData/sqlStatement.txt"
	queries = dict()
	queryStatement = dict()
	J =0
	currLineNum = -1
	prevLineNum =-1
	prevLine=""

	with open(inputFile, 'rb') as f:
		for currLine in f:

			currLineNum +=1


			matchCurrLine = re.search(r'^(\d+),(.*)', currLine)
			matchCurrLine2 = re.search('^(.*),(\d+),(\d+),(\d+)', currLine)
			matchCurrLine3 = re.search('^(\d+),(.*),(\d+),(\d+),(\d+)', currLine)
			matchPrevLine = re.search('^(.*),(\d+),(\d+),(\d+)', prevLine)
			if ( matchCurrLine and   matchPrevLine ) :
				#wrap up previous statement
				queryStatement[statementID][j] =matchPrevLine.group(1)
				hits = int(matchPrevLine.group(2))
				TemplateID = int(matchPrevLine.group(3))
				studyperiod = int(matchPrevLine.group(4))

				queries[statementID] = dict()
				queries[statementID] ['statementID'] = statementID
				queries[statementID]['statement'] = ''.join(queryStatement[statementID][j] for j in sorted(queryStatement[statementID].keys()))
				queries[statementID]['hits']= hits
				queries[statementID]['TemplateID']= TemplateID
				queries[statementID]['studyperiod'] = studyperiod

			elif matchCurrLine3:
				statementID =  int(matchCurrLine3.group(1))
				queryStatement[statementID][0] = matchCurrLine3.group(2)
				hits = int(matchCurrLine3.group(3))
				TemplateID = int(matchCurrLine3.group(4))
				studyperiod = int(matchCurrLine3.group(5))

				queries[statementID] = dict()
				queries[statementID] ['statementID'] = statementID
				queries[statementID]['statement'] = ''.join(queryStatement[statementID][j] for j in sorted(queryStatement[statementID].keys()))
				queries[statementID]['hits']= hits
				queries[statementID]['TemplateID']= TemplateID
				queries[statementID]['studyperiod'] = studyperiod


			if matchCurrLine:
				# start new statment
				print "matchCurrLine", currLine
				statementID =  int(matchCurrLine.group(1))
				queryStatement[statementID]= dict()
				j = 0
				queryStatement[statementID][j] = matchCurrLine.group(2)
				j+=1

			elif matchCurrLine2:
				#skip this current line
				print "skipping ", currLine

			elif currLineNum !=0 and currLineNum !=1:
				print "just adding", currLine
				queryStatement[statementID][j] = currLine
				j+=1

			prevLine = currLine
			prevLineNum = currLineNum


	matchPrevLine = re.search('^(.*),(\d+),(\d+),(\d+)', prevLine)
	if matchPrevLine:
		queryStatement[statementID][j] =matchPrevLine.group(1)
		hits = int(matchPrevLine.group(2))
		TemplateID = int(matchPrevLine.group(3))
		studyperiod = int(matchPrevLine.group(4))
		queries[statementID] = dict()
		queries[statementID] ['statementID'] = statementID
		queries[statementID]['statement'] = ''.join(queryStatement[statementID][j] for j in sorted(queryStatement[statementID].keys()))
		queries[statementID]['hits']= hits
		queries[statementID]['TemplateID']= TemplateID
		queries[statementID]['studyperiod'] = studyperiod

	for i in sorted(queries.keys()):

		print "\n***query is\n ", queries[i]

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