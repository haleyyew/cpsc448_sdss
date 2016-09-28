--TRACING A SESSION--
--Query1: find the IP address of a session sessionIpID, and how many queries were made in that session sqlqueries
select sessionID, sqlqueries, sessionIpID
into TEMP_TABLE1
from session.csv
where sessionID = 100000;

--Query2: find the sequence of the queries made in a session where each query is indicated by a rank rankInSession, and get the time that the query was made theTime.
select sl.sessionID, sl.rankInSession, sl.theTime
into TEMP_TABLE2
from sessionlog.csv
join TEMP_TABLE1 t1
where sl.sessionID = t1.sessionID;

--Query3: confirm that this IP is valid
select ip.ipID, ip.isvalid
into TEMP_TABLE3
from ipAll.csv ip
join TEMP_TABLE1 t1
where ip.ipID = t1.sessionIpID;

--Query4: get the ID of the SQL query statement statementID for each query made in a session. 
--Also get the number of rows returned by submitting a particular query. 
--Get the access interface i.e. "casjobs" or "public" that the request was sent.
--NOTE theTime in TEMP_TABLE2 and in sqllog.csv are in different granularities, need to preprocess TEMP_TABLE2 first
select sl.clientIpID, sl.theTime, sl.statementID, sl.rows, sl.logID, sl.access
into TEMP_TABLE4
from sqllog.csv sl
join TEMP_TABLE2 t2
join TEMP_TABLE1 t1
where sl.clientIpID = t1.sessionIpID
and sl.theTime = t2.theTime

--Query 5: get the SQL query statement identified by the statementID
select ss.statementID, ss.statement
into TEMP_TABLE5
from sqlstatement.csv ss
join TEMP_TABLE4 t4
where t4.statementID = ss.statementID


--FINDING THE WEBAGENT OF THE QUERY--
--Query: get the class of the web agent string, BOT, or ADMIN, etc.
--NOTE: this query does not work! No weblog.csv row with the correct time corresponding to sqllog.csv can be found
select ls.logID, wl.agentStringID, was.agentID, wa.class
into TEMP_TABLE_AGENT
from LogSource.csv ls
join TEMP_TABLE4 t4
join weblog.csv wl
join weagentstring.csv was
join WebAgent.csv wa
where t4.logID = ls.logID
and ls.logID = wl.logID
and wl.agentStringID = was.agentID
and was.agentID = wa.agentID