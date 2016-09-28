#####Background Info#################################################
ssh haleyyew@whisky.cs.ubc.ca
cd /ubc/cs/research/connections/data
cd SDSS
cd dataset

head -20 sessionlog.csv
"
sessionID,rankInSession,IpID,theTime,type,ID,templateID
---------,-------------,----,-------,----,--,----------
1,1,0,2003-02-09 20:02:15.000,1,99042076,0
1,2,0,2003-02-09 20:02:15.000,1,99042077,0
1,3,0,2003-02-09 20:02:16.000,1,99042082,0
1,4,0,2003-02-09 20:02:16.000,1,99042083,0
1,5,0,2003-02-09 20:02:34.000,1,115169050,0
1,6,0,2003-02-09 20:02:34.000,1,115169051,0
1,7,0,2003-02-09 20:02:36.000,1,99042080,0
1,8,0,2003-02-09 20:02:36.000,1,99042081,0
1,9,0,2003-02-09 20:02:37.000,1,99042074,0
1,10,0,2003-02-09 20:02:37.000,1,99042075,0
1,11,0,2003-02-09 20:02:50.000,1,99042078,0
1,12,0,2003-02-09 20:02:50.000,1,99042079,0
1,13,0,2003-02-09 20:02:52.000,1,99042072,0
1,14,0,2003-02-09 20:02:52.000,1,99042073,0
"

head -20 session.csv
"sessionID","FirstEventID","sessionIpID","startTime","limitTime","seconds","events","webhits","sqlqueries","sky1hits","sky1pageviews"
"1","1","0","2/9/2003 8:02:15 PM","2/9/2003 8:02:52 PM","37","14","0","14","0","0"


#'s/day/night/' <old >new : substitute 'day' with 'night' for each line from the 'old' file and output to 'new' file
#[^,] : not ,
#[^,]* : 0 or more 'not ,'
#[^,]*, : 0 or more 'not ,' and then match a ','
#\(...\) : use \( and \) to quote important things
#\1 : first remembered pattern
#.* : match anything after the first match
#replace [^,]*,\([^,]*\),.* with \([^,]*\) which is anything in between the first and second comma
#will delete everything not in between the first and second comma!
sed "s/[^,]*,\([^,]*\),.*/\1/" 
#only keep things before first comma
sed "s/\([^,]*\),.*/\1/"
sed "s/\([^,]*\),.*/\1/" <ipAll.csv >ipAll_out.csv


#-F, : field separator is ,
#~ /\"1\"/ : match things inside //
#\" : match a double quote
awk -F, '$1 ~ /\"0\"/ { print }' ipAll.csv
#####################################################################



#####Trace session 100000############################################
#find row with 'sessionID' 100000
awk -F, '$1 ~ /\"100000\"/ { print }' session.csv
"100000","7452136","12825","4/21/2008 1:04:05 PM","4/21/2008 1:04:58 PM","53","11","0","11","0","0"
#column13: made 11 'sqlqueries'
#column3: 'sessionIpID' is 12825

#find that 'sessionID' in sessionlog.csv
awk -F, '$1 == 100000 { print }' sessionlog.csv
"
100000,1,12825,2008-04-21 13:04:05.000,1,114181417,0
100000,2,12825,2008-04-21 13:04:06.000,1,114181502,0
100000,3,12825,2008-04-21 13:04:10.000,1,114181416,0
100000,4,12825,2008-04-21 13:04:15.000,1,114181415,0
100000,5,12825,2008-04-21 13:04:15.000,1,114181501,0
100000,6,12825,2008-04-21 13:04:22.000,1,114181414,0
100000,7,12825,2008-04-21 13:04:26.000,1,116025663,0
100000,8,12825,2008-04-21 13:04:34.000,1,114181413,0
100000,9,12825,2008-04-21 13:04:34.000,1,116025662,0
100000,10,12825,2008-04-21 13:04:40.000,1,114181412,0
100000,11,12825,2008-04-21 13:04:58.000,1,114181418,0
"
#there are 11 events in this session, are they all sql queries?
#use 'sessionIpID' 12825 to join ipAll.csv

#find that 'ipID' in ipAll.csv
awk -F, '$1 ~ /\"12825\"/ { print }' ipAll.csv
"12825","155.198.204.142                                                                                     ","1265","155","198","204","142","","1962","1122","1296","664","0","True"

#find that 'clientIpID' in sqllog.csv
awk -F, '$10 == 12825 { print }' sqllog.csv
"
116025662,2008,4,21,13,4,34,2008-04-21 13:04:34.000,2040,12825,cas.sdss.org,SDSSSQL018,BestDR6,astro,9.3000002E-2,2.0000001E-3,51,48390699,0,1,1,1,1
""
116025663,2008,4,21,13,4,26,2008-04-21 13:04:26.000,2040,12825,cas.sdss.org,SDSSSQL018,BestDR6,astro,7.9999998E-2,0,51,48390699,0,1,1,1,1
"
#and etc. see sqllog_clientIpID12825.csv
#by searching "2008,4,21,13" in all the found rows, found 11 entries, consistent with sessionlog.csv number
#column 18: for each log entry, get its 'statementID'
#9 entries have 'statementID' 48063987, 2 entries have 48390699
#column 17: number of rows returned. 'statementID' 48063987 has 1 row, 48390699 has 51 rows

#search for the 'statementID' in sqlstatement.csv
awk -F, '$1 == 48063987 { print }' sqlstatement.csv
awk -F, '$1 == 48390699 { print }' sqlstatement.csv
#but this session has multiple statements, want to find them all
awk -F, '$1 == 48063987 || $1 == 48390699 { print  }' sqlstatement.csv
#but each entry spans multiple rows, printing one line does not print the whole statement
#this should print the next 100 rows after the matched row
awk -F, '$1 == 48063987 || $1 == 48390699 { print;for(i=0; i<=100; i++) { getline;print;} }' sqlstatement.csv
"
48063987,SELECT u.up_name as name, 
   '<a target=INFO href=http://cas.sdss.org/astrodr6/en/tools/explore/obj.asp?id=' + cast(x.objId as varchar(20)) + '>'+ cast(x.objId as varchar(20)) + '</a>' as objID, p.ra, p.dec, 
   dbo.fPhotoTypeN(p.type) as type,
   p.modelMag_u, p.modelMag_g, p.modelMag_r, p.modelMag_i, p.modelMag_z, p.modelMagErr_u, p.modelMagErr_g, p.modelMagErr_r, p.modelMagErr_i, p.modelMagerr_z, p.z, p.zErr
FROM #x x, #upload u, SpecPhotoAll p
WHERE u.up_id = x.up_id and x.objID=p.objID 
ORDER BY x.up_id,197,685869,1
""
48390699,SELECT u.up_name as name, 
   '<a target=INFO href=http://cas.sdss.org/astrodr6/en/tools/explore/obj.asp?id=' + cast(x.objId as varchar(20)) + '>'+ cast(x.objId as varchar(20)) + '</a>' as objID, p.ra, p.dec, 
   dbo.fPhotoTypeN(p.type) as type,
   p.modelMag_u, p.modelMag_g, p.modelMag_r, p.modelMag_i, p.modelMag_z, p.modelMagErr_u, p.modelMagErr_g, p.modelMagErr_r, p.modelMagErr_i, p.modelMagerr_z
FROM #x x, #upload u, PhotoTag p
WHERE u.up_id = x.up_id and x.objID=p.objID
ORDER BY x.up_id,47,685866,1
"
#BUT temporary tables #x, #upload were used, cannot execute query without #x, #upload

#####VERIFY RESULTS ON CASJOBS#######################################
#RETRY with another statement
awk -F, '$1 == 1140007 { print;for(i=0; i<=100; i++) { getline;print;} }' sqlstatement.csv
'
1140007, SELECT G.objID, GN.distance, G.ra, G.dec, 
 dbo.fPhotoTypeN(GN.type) as type,G.psfMag_u, 
 G.psfMagErr_u,G.psfMag_g,G.psfMagErr_g,G.psfMag_r, 
 G.psfMagErr_r,G.psfMag_i,G.psfMagErr_i,G.psfMag_z, 
 G.psfMagErr_z,G.dered_u, G.dered_g, G.dered_r, 
 G.dered_i, G.dered_z,ISNULL(s.z,-9999),s.SpecClass, 
 s.ObjTypeName 
 FROM PhotoObjAll as G 
JOIN dbo.fGetNearbyObjEq(152.48957825, 59.70527649, 0.25) as GN 
 on G.objID = GN.objID 
 LEFT OUTER JOIN SpecObj s ON s.bestobjid=GN.objid 
 ORDER BY distance,1,9840,1
'
awk -F, '$18 == 1140007 { print }' sqllog.csv
'
1975882,2006,8,14,20,8,18,2006-08-14 20:08:18.000,2035,2004618,cas.sdss.org,SDSSSQL016,BestDR5,public,0.52999997,0.013,2,1140007,0,1,1,1,1
'
#column 17: returned 2 'rows' using DR5
#to confirm, submit CASJobs:
#https://skyserver.sdss.org/CasJobs/SubmitJob.aspx
#Context=DR5 
#got 2 rows back

#find 'requestor' is not "cas.sdss.org"
awk -F, '$11 != "cas.sdss.org" { print }' sqllog.csv
#most others are empty in this field
#find 'access' is not "casjobs"
awk -F, '$14 != "casjobs" { print }' sqllog.csv
#most other 'access' is "public"


#####FIND THE WEBAGENT OF QUERY(UNSUCCESSFUL)########################
#from 'sqlID' 1975882, find the 'logID' in column 9
#'logID' is 2035 
#find that row with 'logID' in LogSource.csv
awk -F, '$1 == 2035 { print }' LogSource.csv
'
2035,FNAL,SQL,DR5e,cas.sdss.org/dr5,SQL,DR5,TSQL,[SDSSSQL016.FNAL.GOV].weblog,2008-11-01 00:00:00.000,1,INACTIVE
'
#go to weblog.csv to find row with that 'logID'
awk -F, '$9 == 2035 { print }' weblog.csv
'
674191238,2010,11,27,0,23,7,2010-11-27 00:23:07,8001,332532,3,213548,114476154,200,404539,2154,339,0,True,False,False,False,1,1,True
674191239,2010,12,24,18,43,15,2010-12-24 18:43:15,8001,332532,3,25413,114476154,200,404539,2154,339,0,True,False,False,False,1,1,True
674191240,2010,12,9,0,3,12,2010-12-09 00:03:12,8001,332532,3,345932,107900588,200,404539,2226,339,0,True,False,False,False,1,1,True
932053883,2012,8,13,9,51,49,2012-08-13 09:51:49,34,4660807,3,417867,102428503,200,577464,0,0,-1,True,False,False,False,1,3,False
932053884,2012,8,10,20,46,35,2012-08-10 20:46:35,34,4764952,3,648794,187041476,200,578150,0,0,-1,True,True,False,False,1,3,False
932053885,2012,8,10,20,46,34,2012-08-10 20:46:34,34,4764952,3,640997,187041476,200,578150,0,0,-1,True,True,False,False,1,3,False
932053886,2012,8,13,9,57,36,2012-08-13 09:57:36,34,4660807,3,647169,102428503,304,577464,0,0,-1,True,False,False,False,1,3,False
932053887,2012,8,13,9,57,36,2012-08-13 09:57:36,34,4660807,3,346888,102428503,304,577464,0,0,-1,True,False,False,False,1,3,False
932053888,2012,8,13,9,57,59,2012-08-13 09:57:59,34,4660807,3,126399,102428503,304,577464,0,0,-1,True,False,False,False,1,3,False
932053889,2012,8,13,9,57,19,2012-08-13 09:57:19,34,4660807,3,126399,102428503,304,577464,0,0,-1,True,False,False,False,1,3,False
932053890,2012,8,13,9,57,50,2012-08-13 09:57:50,41,4660807,3,375895,102428503,404,577464,0,0,-1,True,False,False,False,1,3,False
932053891,2012,8,13,9,57,49,2012-08-13 09:57:49,41,4660807,3,375895,102428503,404,577464,0,0,-1,True,False,False,False,1,3,False
932053892,2012,8,13,9,55,38,2012-08-13 09:55:38,34,4660807,3,341471,102428503,200,577464,0,0,-1,True,False,False,False,1,3,False
932053893,2012,8,13,9,55,38,2012-08-13 09:55:38,34,4660807,3,126399,102428503,304,577464,0,0,-1,True,False,False,False,1,3,False
932053894,2012,8,13,9,52,2,2012-08-13 09:52:02,34,4660807,3,126399,102428503,304,577464,0,0,-1,True,False,False,False,1,3,False
932053895,2012,8,13,9,57,36,2012-08-13 09:57:36,34,4660807,3,126399,102428503,304,577464,0,0,-1,True,False,False,False,1,3,False
932053896,2012,8,13,9,51,49,2012-08-13 09:51:49,34,4660807,3,126399,102428503,304,577464,0,0,-1,True,False,False,False,1,3,False
932053897,2012,8,13,9,51,44,2012-08-13 09:51:44,34,4660807,3,201127,102428503,200,577464,0,0,-1,True,False,False,False,1,3,False
932053898,2012,8,13,9,51,44,2012-08-13 09:51:44,34,4660807,3,134729,102428503,200,577464,0,0,-1,True,False,False,False,1,3,False
932053899,2012,8,13,9,51,43,2012-08-13 09:51:43,34,4660807,3,126399,102428503,304,577464,0,0,-1,True,False,False,False,1,3,False
'
#found 3 'agentStringID' 404539, 577464, and 578150
#BUT none of it is logged at the correct time, the time of the query is in column 8 of sqllog.csv
#get the 'agentStringID' for that row in column 15, 
#use 'agentStringID' to search weagentstring.csv
#get the 'agentID' for that row
#use the 'agentID' to search WebAgent.csv
#find the 'class' for that row
#'class' can either be BOT, ADMIN, etc.