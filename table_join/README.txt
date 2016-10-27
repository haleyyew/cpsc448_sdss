This script opens Skyserver logs csv files, extracts information for each session in sessionlog.csv, and outputs the extracted information to csv files.
Configure the script so that only a specific number of sessions is produced in the output.

Configurations of the script is located in config.ini
The location of source csv files is specified in the 'input'
The location of the output is specified in 'output' 
The number of sessions to extract is specified in 'num_of_sessions'
'random_sampling=1' turns on random sampling, otherwise sequentially add the first num_of_sessions sessions
'sqlstatement_no_regex=1' switches to an alternative method to read sqlstatement.csv, which is recommended

To run the script,
python ./main.py

Documentations of the script can be found inside each .py file

How does the pipeine work for extracting sessions:
First, the script main.py in /table_join reads the config.ini files and get user's specifications. Then the script opens 3 csv files in this order: sessionlog.csv, sqllog.csv, and sqlstatement.csv. While opening sessionlog.csv, it stores each row in the csv file as an entry in a dictionary, the dictionary will store every row read by the script. The key for an entry is the primary key or keys for the sessionlog table, and the value of the entry is the whole row. Each row is stored as a list, where each item in the list correspond to a column value. For example, an entry in the dictionary looks like this: {2_1: [2,1,12825,2008-04-21 13:04:05.000,1,114181417,0]}. 

Note that each row is a list, not a dictionary where the keys are attribute names. This allows more compact storage. To access a particular attribute of a row in the table, I created a look-up list SqlTable.attributes for each SqlTable. When needing to access a particular attribute value for the table, such as rankInSession, compare with the look-up list to know which value in the list, such as [2,1,12825,2008-04-21 13:04:05.000,1,114181417,0], to retrieve.

If the user specified 'random_sampling=1' in config.ini, then the script will randomly select sessions to extract by using resevoir sampling. Only the sessions selected by resevoir sampling are stored in the dictionary. 

The dictionary that stores each row is a field of a python class SqlTable, this field is SqlTable.table_rows. A SqlTable maintains other data structures and methods that help to add rows to the dictionary and retrieve rows. Aditionally, SqlTable will group all rows belonging to the same session together, for example, if there are 11 rows for session 1, then all those 11 rows are stored inside a separate list. SqlTable stores N lists of sessions, this is SqlTable.session_group. The session_group is populated while adding new rows to SqlTable.table_rows. Each item in the list in session_group is a reference to the row stored in table_rows, for example {1:[2_1,2_2,2_3]} is a list for session 1 and the values are keys to access specific rows in table_rows, this improves the memory usage. The efficiency to find all rows belonging to a session when outputting to sesions csv is improved.

SqlTable stores yet another data structure that is used when reading the sqllog.csv, this data structure is a dictionary SqlTable.sqllog_group that stores sqlID as keys. When reading rows of sqllog.csv, the script will store each row of sqllog to another SqlTable. Before storing, the script checks whether sqlID can be found in SqlTable.sqllog_group, if the sqlID is not found, then do not store that row. Not only this method saves memory space usage, SqlTable.sqllog_group also speeds up checking for the sqlID. 

After the script finished reading sessionlog.csv, then will read sqllog.csv, then sqlstatement.csv. A new SqlTable object is created for storing sqllog.csv rows as well as sqlstatement.csv. As mentioned above, to improve memory usage, I do not need to store the entire sqllog.csv and sqlstatement.csv into memory. 

This is because we are not extracting all sessions at a time. So when I read sqllog, I add a row to SqlTable.table_rows only when sqllog.sqlID matched a sessionlog.sqlID, otherwise do not store this row. Similarly for sqlstatement, I only store a row when sqlstatement.statementID matched one of sqllog.statementID in sqllog's SqlTable. I store an addtional data structure SqlTable.sqlstatement_group for sqllog's SqlTable, for the same purposes as storing SqlTable.sqllog_group in sessionlog.

In general I will have 3 SqlTable after reading the 3 dataset csv files, each of the SqlTable's fields and example of the data stored in memory is shown:

SqlTable for sessionlog.csv 
sessionlog.table_rows = {1_1: [1,1,12825,2008-04-21 13:04:05.000,1,114181417,0], 1_2: [1,2,12825,2008-04-21 13:04:05.000,1,114181417,0],...}
sessionlog.session_group = {1:[1_1,1_2,1_3], 2:[2_1,2_2,2_3],...}
sessionlog.sqllog_group = {99042076:0, 99042077:0,...}

SqlTable for sqllog.csv 
sqllog.table_rows = {99042076:[99042076,2004,3,17,21,3,58,2004-03-17 21:03:58.000,2007,799931,skyserver.sdss.org,SDSSSQL004,BESTDR2,public,0.033,2.0000001E-3,0,1,0,1,1,1,1],...}
sqllog.sqlstatement_group = {1:0,2:0,3:0,...}

SqlTable for sqlstatement.csv 
sqlstatement.table_rows = {[1,"SELECT TOP 100 objID, ra ,dec FROM PhotoPrimary WHERE ra > 185 and ra < 185.1 AND dec > 15 and dec < 15.1",1,7709,1],...}

Issues occurred while reading sqlstatement.csv. There is no delimiter for the 'statement' column, and since this column stores the SQL query string, and a string can span multiple rows, each row in the file is most likely a partial row. For example,

3,SELECT u.up_name as name, 
   '<a target=INFO href=http://cas.sdss.org/astrodr6/en/tools/explore/obj.asp?id=' + cast(x.objId as varchar(20)) + '>'+ cast(x.objId as varchar(20)) + '</a>' as objID, p.ra, p.dec, 
   dbo.fPhotoTypeN(p.type) as type,
   p.modelMag_u, p.modelMag_g, p.modelMag_r, p.modelMag_i, p.modelMag_z, p.modelMagErr_u, p.modelMagErr_g, p.modelMagErr_r, p.modelMagErr_i, p.modelMagerr_z, p.z, p.zErr
FROM #x x, #upload u, SpecPhotoAll p
WHERE u.up_id = x.up_id and x.objID=p.objID 
ORDER BY x.up_id,197,685869,1

is a single row for the sqlstatement table, but is stored in multiple rows in the csv file.
To overcome this, I implemented a parser specially designed to parse the csv file. The parser makes use of concepts of regular expression. Each row in the csv file is matched to different patterns, and then I combine multiple rows into a single row that constitute a row stored inside the original sqlstatement table.


Next, I join the 3 tables into a single table and output that to sessions csv. When joining tables, each joining condition is a equality, I make use of the key to retrieve a row in O(1) time. For example, if I need to join table1.sqlID = table2.ID for table1 and table2, I find table1.sqlID first, and use the sqlID as a key to look up in table2's table_rows dictionary. Then I combine table1's row and table2's row.

In the post_processing.py script in table_join_post_processing, I eliminate all rows that does not have the correct requestor and does not have valid SQL statement syntax. However, to eliminate rows where the statement was submitted by a bot, additional tables are needed to be joined to get the bot information. The 'class' of WebAgent.csv specifies "BOT". When I attempted to join the tables, I realized that it is impossible to join with WebAgent since the intermediate tables joined do not contain the sufficient information.  



Debugging:
The script now halts for user to type a command to query rows in the joined table. Here are the types of commands to give:

session <int sessionID> <int rankInSession>
sql <int sqlID>
statement <int statementID>
close

For example, when the console displays "Please enter command:", type "session 1 1" to display the row in the joined sessionlog table that has sessionID=1 and rankInSession=1. "statement 24060378" will print the row in sqlstatement table with statementID=24060378, and so on.
"close" will exit the user interaction mode and start outputting the sessions to csv files.


Recently Fixed:
Fix incorrect extraction of statements in sqlstatement.csv
Allow random sampling of sessions by using reservoir sampling algorithm

