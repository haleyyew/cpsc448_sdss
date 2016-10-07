__author__ = 'ZZ'

from __future__ import division
import os
import numpy as np
import sys
import random

import re
def test():
	st =  "12,        Select    hello           \n"
	print st
	match =re.search(r"^(\d+),(.*)$",st )	
	#match = re.search(r'\d\s*\d\s*\d', 'xx123xx')
	print match
	if match:
		print "start ", match.group(),  "1 ",match.group(1), "2", match.group(2)
	
	st2= '    ORDER BY x.up_id,1,7709,1\n'
	match = re.search('^(.*),(\d+),(\d+),(\d+)$', st2)
	if match:
		print "start ", match.group(),  "1 ",match.group(1), "2", match.group(2),"3", match.group(3),"4", match.group(4)
	
def parse():
	file1="../output.txt"
	queries = dict()
	queryStatement = dict()
	J =0
	lineNumber = -1 
	with open(file1, 'rb') as f:
		for line in f:
			lineNumber +=1
			if lineNumber == 0 or lineNumber ==1:
				continue
			
			match = re.search(r'^(\d+),(.*)', line)

			if match:

				statementID =  int(match.group(1))
				queryStatement[statementID]= dict()
				j = 0
				queryStatement[statementID][j] = match.group(2)
				j+=1
				continue
			
			
			match = re.search('^(.*),(\d+),(\d+),(\d+)', line)
			if match:

				queryStatement[statementID][j] = match.group(1)
				hits = int(match.group(2))
				TemplateID = int(match.group(3))
				studyperiod = int(match.group(4))
				
				queries[statementID] = dict()
				queries[statementID] ['statementID'] = statementID
				queries[statementID]['statement'] = ''.join(queryStatement[statementID][j] for j in sorted(queryStatement[statementID].keys()))
				queries[statementID]['hits']= hits
				queries[statementID]['TemplateID']= TemplateID
				queries[statementID]['studyperiod'] = studyperiod
				
				continue
				
			
			queryStatement[statementID][j] = line
			j+=1
				
			
	
	for i in queries.keys():
		
		print "\n***query is\n ", queries[i]
		
		
	

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
				#wrap up previous statment 
				print "matchCurrLine and   matchPrevLine ", currLine
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
		
def parseSessionLog():
	#sessionlog-> sessionID,rankInSession,IpID,theTime,type,ID,templateID
	#             1,1,0,2003-02-09 20:02:15.000,1,99042076,0
	inputFile = '../smallData/sessionlog.txt'
	sessionLog = dict()
	with open(inputFile, "rb") as f:
		for line in f:
			match = re.search("(\d+),(\d+),(\d+),(.+),(\d+),(\d+),(\d+)", line)
			if match:
				sessionID= int(match.group(1))
				rankInSession= int(match.group(2))
				IpID=int(match.group(3))
				theTime=match.group(4)
				type_=int(match.group(5))
				ID=int(match.group(6))
				templateID=int(match.group(7))
				
				key = str(sessionID)+"-"+str(rankInSession)
				sessionLog[key] = dict()
				
				sessionLog[key]["sessionID"]= sessionID
				sessionLog[key]["rankInSession"]= rankInSession  
				sessionLog[key]["IpID"]= IpID
				sessionLog[key]["theTime"]= theTime
				sessionLog[key]["type_"]=type_
				sessionLog[key]["ID"]=ID
				sessionLog[key]["templateID"]=templateID
	
		
	for s in sorted(sessionLog.keys()):
		print s, sessionLog[s]
		
def parseSessionLog():
	#sessionlog-> sessionID,rankInSession,IpID,theTime,type,ID,templateID
	#             1,1,0,2003-02-09 20:02:15.000,1,99042076,0
	inputFile = '../smallData/sessionlog.txt'
	sessionLog = dict()
	with open(inputFile, "rb") as f:
		for line in f:
			match = re.search("^(\d+),(\d+),(\d+),(.+),(\d+),(\d+),(\d+)", line)
			if match:
				sessionID= int(match.group(1))
				rankInSession= int(match.group(2))
				IpID=int(match.group(3))
				theTime=match.group(4)
				type_=int(match.group(5))
				ID=int(match.group(6))
				templateID=int(match.group(7))
				
				key = str(sessionID)+"-"+str(rankInSession)
				sessionLog[key] = dict()
				
				sessionLog[key]["sessionID"]= sessionID
				sessionLog[key]["rankInSession"]= rankInSession  
				sessionLog[key]["IpID"]= IpID
				sessionLog[key]["theTime"]= theTime
				sessionLog[key]["type"]=type_
				sessionLog[key]["ID"]=ID
				sessionLog[key]["templateID"]=templateID
	
		
	for s in sorted(sessionLog.keys()):
		print s, sessionLog[s]
		
def parseSqlLog():
	#sessionlog-> sqlID,yy,mm,dd,hh,mi,ss,theTime,logID,clientIpID,requestor,server,dbname,access,elapsed,busy,rows,statementID,error,errorMessageID,isvisible,studyperiod,includeflag
	#             1,2004,3,17,21,3,58,2004-03-17 21:03:58.000,2007,799931,cas.sdss.org,SDSSSQL004,BESTDR2,public,0.033,2.0000001E-3,0,19,0,1,1,1,1
	inputFile = '../smallData/sqllog.txt'
	sqlLog = dict()
	with open(inputFile, "rb") as f:
		for line in f:
			match = re.search("^(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(.+),(\d+),(\d+),(.+),(.+),(.+),(.+),(.+),(.+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+)", line)
			if match:
				sqlID= int(match.group(1))
				yy= int(match.group(2))
				mm= int(match.group(3))
				dd= int(match.group(4))
				hh= int(match.group(5))
				mi= int(match.group(6))
				ss= int(match.group(7))
				theTime= match.group(8) 
				logID= match.group(9)
				clientIpID= match.group(10)
				requestor= match.group(11)
				server= match.group(12)
				dbname= match.group(13)
				access =match.group(14)
				elapsed= float(match.group(15))
				busy = float( match.group(16))
				rows = int(match.group(17))
				statementID =int( match.group(22))
				error= int(match.group(19))
				errorMessageID= int(match.group(20))
				isvisible= int(match.group(21))
				studyperiod= int(match.group(22))
				includeflag = int(match.group(23))
				
				key = sqlID
				sqlLog[key] = dict()
				
				sqlLog[key]["sqlID"]= sqlID
				sqlLog[key]["yy"] = yy
				sqlLog[key]["mm"] = mm
				sqlLog[key]["dd"] = dd
				sqlLog[key]["hh"] = hh
				sqlLog[key]["mi"] = mi
				sqlLog[key]["ss"] = ss
				sqlLog[key]["theTime"] = theTime
 				sqlLog[key]["logID"] =logID
				sqlLog[key]["clientIpID"] = clientIpID
				sqlLog[key]["requestor"] = requestor
				sqlLog[key]["server"] = server
				sqlLog[key]["dbname"] = dbname
				sqlLog[key]["access"] = access
				sqlLog[key]["elapsed"] = elapsed
				sqlLog[key]["busy"] = busy 
				sqlLog[key]["rows"] = rows
				sqlLog[key]["statementID"] = statementID
				sqlLog[key]["error"] = error 
				sqlLog[key]["errorMessageID"] = errorMessageID
				sqlLog[key]["isvisible"] = isvisible
				sqlLog[key]["studyperiod"] = studyperiod
				sqlLog[key]["includeflag"] = includeflag

	
		
	for s in sorted(sqlLog.keys()):
		print s, sqlLog[s]
				
def main():
	parseSqlStatement()
	parseSessionLog()	
	parseSqlLog()

	
if __name__ == "__main__":
	main()
