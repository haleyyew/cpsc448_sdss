#####awk Explanation###################
awk -F, '$1 ~ /\"0\"/ { print }' ipAll.csv
#-F, : field separator is ,
#~ means match
#~ /\"1\"/ : match things inside //
#\" : match a double quote


#####SessionLog########################
#sessionID
#the ID of the session, equivalent to sessionID in session.csv
#the command below finds a session with ID 100000
awk -F, '$1 ~ /\"100000\"/ { print }' session.csv
#the output is:
# 100000,1,12825,2008-04-21 13:04:05.000,1,114181417,0
# 100000,2,12825,2008-04-21 13:04:06.000,1,114181502,0
# 100000,3,12825,2008-04-21 13:04:10.000,1,114181416,0
# 100000,4,12825,2008-04-21 13:04:15.000,1,114181415,0
# 100000,5,12825,2008-04-21 13:04:15.000,1,114181501,0
# 100000,6,12825,2008-04-21 13:04:22.000,1,114181414,0
# 100000,7,12825,2008-04-21 13:04:26.000,1,116025663,0
# 100000,8,12825,2008-04-21 13:04:34.000,1,114181413,0
# 100000,9,12825,2008-04-21 13:04:34.000,1,116025662,0
# 100000,10,12825,2008-04-21 13:04:40.000,1,114181412,0
# 100000,11,12825,2008-04-21 13:04:58.000,1,114181418,0
#note that in session.csv, sqlqueries says how many queries were made in the session, in this case it was 11

#rankInSession
#a number starting from 1, that orders each log entry of a session to describe the sequence that the entries were entered.
#for example, sessionID 100000 has 11 entries numbered from 1 to 11

#IpID
#the IP is recorded as an ID, so that subsequent access to the database by the same IP address will have the same ID. This is equivalent to ipID in ipAll.csv, clientIpID in weblog.csv, and clientIpID in sqllog.csv
#for example, sessionID 100000 has IpID 12825, and is valid as indicated in ipAll.csv

#theTime
#a timestamp that the log entry was entered, in the form of YYYY-MM-DD hh:mi:ss.mmm
#this is equivalent to theTime in sqllog.csv
#for example, the first entry with sessionID 100000 has theTime 2008-04-21 13:04:05.000
#note this is not equivalent to startTime in session.csv, the output for sessionID 100000:
awk -F, '$1 ~ /\"100000\"/ { print }' session.csv
# "100000","7452136","12825","4/21/2008 1:04:05 PM","4/21/2008 1:04:58 PM","53","11","0","11","0","0"
#where startTime is "4/21/2008 1:04:05 PM", 

#type
#unclear what is this is. Seems there are only 2 possible values for type: 0 or 1
#the following command did not find any row:
awk -F, '$5 != 1 && $5 != 0 { print }' sessionlog.csv

#ID
#the ID of the sqlID in sqllog.csv or hitID in weblog.csv. Note that the sqlID and hitID do not overlap. The first entry in sqlID is 1, and the first entry in hitID is 883383548.
#for example, the 11 entries in sessionlog.csv has corresponding 11 entries in sqllog.csv, the output in sqllog:
# 114181412,2008,4,21,13,4,40,2008-04-21 13:04:40.000,2040,12825,cas.sdss.org,SDSSSQL018,BestDR6,astro,0.013,0.001,1,48063987,0,1,1,1,1
# 114181413,2008,4,21,13,4,34,2008-04-21 13:04:34.000,2040,12825,cas.sdss.org,SDSSSQL018,BestDR6,astro,2.9999999E-2,0.001,1,48063987,0,1,1,1,1
# 114181414,2008,4,21,13,4,22,2008-04-21 13:04:22.000,2040,12825,cas.sdss.org,SDSSSQL018,BestDR6,astro,0.013,0,1,48063987,0,1,1,1,1
# 114181415,2008,4,21,13,4,15,2008-04-21 13:04:15.000,2040,12825,cas.sdss.org,SDSSSQL018,BestDR6,astro,0.013,0.001,1,48063987,0,1,1,1,1
# 114181416,2008,4,21,13,4,10,2008-04-21 13:04:10.000,2040,12825,cas.sdss.org,SDSSSQL018,BestDR6,astro,5.9999999E-2,0.001,1,48063987,0,1,1,1,1
# 114181417,2008,4,21,13,4,5,2008-04-21 13:04:05.000,2040,12825,cas.sdss.org,SDSSSQL018,BestDR6,astro,2.9999999E-2,0.001,1,48063987,0,1,1,1,1
# 114181418,2008,4,21,13,4,58,2008-04-21 13:04:58.000,2040,12825,cas.sdss.org,SDSSSQL018,BestDR6,astro,0.013,0,1,48063987,0,1,1,1,1
# 114181501,2008,4,21,13,4,15,2008-04-21 13:04:15.000,2040,12825,cas.sdss.org,SDSSSQL018,BestDR6,astro,0.11,0.003,1,48063987,0,1,1,1,1
# 114181502,2008,4,21,13,4,6,2008-04-21 13:04:06.000,2040,12825,cas.sdss.org,SDSSSQL018,BestDR6,astro,0.126,0,1,48063987,0,1,1,1,1
# 116025662,2008,4,21,13,4,34,2008-04-21 13:04:34.000,2040,12825,cas.sdss.org,SDSSSQL018,BestDR6,astro,9.3000002E-2,2.0000001E-3,51,48390699,0,1,1,1,1
# 116025663,2008,4,21,13,4,26,2008-04-21 13:04:26.000,2040,12825,cas.sdss.org,SDSSSQL018,BestDR6,astro,7.9999998E-2,0,51,48390699,0,1,1,1,1

#templateID
#unclear what this does, it seems every entry in sessionlog has templateID 0, this command did not give any output:
awk -F, '$7 != 0 { print }' sessionlog.csv


#####SqlLog############################
#There are around 194000000 records in sqllog.csv

#sqlID
#equivalent to ID in sessionlog.csv

#yy
#the YYYY component of theTime

#mm
#the MM component of theTime

#dd
#the DD component of theTime

#hh
#the hh component of theTime

#mi
#the mi component of theTime

#ss
#the ss component of theTime

#theTime
#equivalent to theTime in sessionlog.csv

#logID
#logID is the ID of the server. There are many servers that runs the CAS database, and the Skyserver dataset is an aggregate of all the logs from all the servers. Joining on logID is not helpful at all since there are only 91 servers indicated in LogSource.csv, and each server contains all the sql and web logs.

#clientIpID
#equvalent to IpID in sessionlog

#requestor
#the root that the sql query was submitted. For example, queries can be sent to "cas.sdss.org" or "skyserver.sdss.org" or "skyserver.pha.jhu.edu", etc.
#additionally, many requestors are displayed as an IP address nnn.nnn.nnn.nnn, which is not resolved to a domain name
#this command matched most of the entries in sqllog.csv
awk -F, '$11 != "cas.sdss.org" && $11 != "" && $11 != "skyserver.sdss.org" && $11 != "skyserver.pha.jhu.edu" && $11 != "skyserver2.fnal.gov" && $11 != "?????????" && $11 != "uch-sdss001.fnal.gov"  { print }' sqllog.csv

#server
#the specific server (machine) that received the sql query. For example, if you are querying data release 6 of CAS, then the request is received by SDSSSQL018.
#the server name can be in the form of SDSSDR? or SDSSSQL??? or SQL001DBHost, etc.
awk -F, '$12 != "SDSSDR1" && $12 != "SDSSSQL007" && $12 != "SQL001DBHost" { print }' sqllog.csv

#dbname
#the SDSS data release version that the query requested. For example, in CasJobs, you can choose which data release (DR) to query from, if you choose BestDR6, then it will only search in that data release.
#BestDR? is the name for each data release, but you can also query from your personal database MyDB in CasJobs
awk -F, '$13 !~ /BestDR/ && $13 != "MyDB" && $13 !~ /BESTDR/ && $13 !~ /MYDB/ { print }' sqllog.csv

#access
#the access interface that the query was submitted. The access could be "casjobs", or "public", or "astro", or "collab", or empty, or a number such as 779003130
awk -F, '$14 !~ /casjobs/ && $14 != "public" && $14 !~ /astro/ && $14 !~ /collab/ && $14 != "" { print }' sqllog.csv

#elapsed
#how long the query executed, not sure whether this is seconds or minutes or hours

#busy
#how long to wait before the query can be executed, busy is less than elapsed time. In CasJobs, you will see a message that says whether you are on the queue and how many queries are ahead of you.

#rows
#the number of rows returned by executing a query. Confirmed on CasJobs.
#By submitting the query with statementID 1140007:
# 1140007, SELECT G.objID, GN.distance, G.ra, G.dec, 
 # dbo.fPhotoTypeN(GN.type) as type,G.psfMag_u, 
 # G.psfMagErr_u,G.psfMag_g,G.psfMagErr_g,G.psfMag_r, 
 # G.psfMagErr_r,G.psfMag_i,G.psfMagErr_i,G.psfMag_z, 
 # G.psfMagErr_z,G.dered_u, G.dered_g, G.dered_r, 
 # G.dered_i, G.dered_z,ISNULL(s.z,-9999),s.SpecClass, 
 # s.ObjTypeName 
 # FROM PhotoObjAll as G 
# JOIN dbo.fGetNearbyObjEq(152.48957825, 59.70527649, 0.25) as GN 
 # on G.objID = GN.objID 
 # LEFT OUTER JOIN SpecObj s ON s.bestobjid=GN.objid 
 # ORDER BY distance,1,9840,1
#I got 2 rows back using DR5 on https://skyserver.sdss.org/CasJobs/SubmitJob.aspx

#statementID
#ID for each unique query

#error & errorMessageID
#errorMessageID is a foreign key for sqlerrorMessage.csv
#when error is 0, errorMessageID is always 1
#when error is -1, errorMessageID is 0
awk -F, '$19 != 0  && $20 != 1 { print }' sqllog.csv
awk -F, '$19 != -1  && $20 != 0 { print }' sqllog.csv

#isvisible
#not sure what this means, but the value is always either 0 or 1
awk -F, '$21 != 0  && $21 != 1 { print }' sqllog.csv

#studyperiod
#not sure what this is, but the studyperiod can only be 1, 2, or 3.
#at round 165000000 studyperiod becomes 2, all entries before this are studyperiod 1.
#the following command did not give any output:
awk -F, '$22 != 3  && $22 != 2  && $22 != 1 { print }' sqllog.csv

#includeflag
#not sure what this is, but the value can only be 0 or 1
awk -F, '$22 != 3  && $22 != 2  && $22 != 1 && $23 != 0  && $23 != 1 { print }' sqllog.csv


#####SqlStatement######################

#statementID
#equivalent to statementID in sqllog.csv
awk -F, '$18 == 48063987 { print }' sqllog.csv

#statement
#the sql query statement

#hits
#how many times the statement was executed. 
#For example, entry with statementID 48063987 has 197 hits, and the following command gives 197 rows of output:
awk -F, '$18 == 48063987 { print }' sqllog.csv

#templateID
#although templateID in sessionlog are all 0, the entries in sqlstatement.csv have different templateID
awk -F, '$4 == 685869 { print  }' sqlstatement.csv

#studyperiod
#equivalent to studyperiod in sqllog.csv
