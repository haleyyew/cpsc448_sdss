This script opens Skyserver logs csv files, extracts information for each session in sessionlog.csv, and outputs the extracted information to csv files.
Configure the script so that only a specific number of sessions is produced in the output.

Configurations of the script is located in config.ini
The location of source csv files is specified in the 'input'
The location of the output is specified in 'output' 
The number of sessions to output is specified in 'num_of_sessions'

To run the script,
python ./main.py

Documentations of the script can be found inside each .py file

Efficiency of program:
Each row of a table is represented by a list data type: each item in SqlTable.table_rows is a key-value pair where the value is a list that contains a single row and a key is used to access the list. This allows more compact storage of each row, instead of creating a dictionary for each row where the key is that attribute name. To access a particular attribute of a row in the table, I created a look-up list SqlTable.attributes for each SqlTable. When a row is retrieved, compare with the look-up list to know which value corresponds to which attribute

When joining 2 tables, I do not make a separate copy of the joined table, instead I create a foreign key attribute for one of the 2 tables, for each row in one table, a foreign key references a row in the other table. When I output the joined table to a csv file, I need to consider the hierarchical representation of the rows. A row of a joined table looks like this: [a,b,c,[d,e,f,[g,h,i]]]. So I flattened this nested list before writing to csv.

For each SqlTable, I created a dictionary of session_group, where each key is a session and the value of the session_group is a list of table rows belonging to the session. Each item in the list is a reference to the row stored in table_rows, this improves the memory usage. The session_group is populated when adding new rows to the table, this improves the efficiency to find all rows belonging to a session when outputting to csv.

When joining tables, each joining condition is a equality, I make use of the key to retrieve a row in O(1) time. For example, if I need to joining table1.sqlID = table2.ID for table1 and table2, I find the table1.sqlID, and use the sqlID as a key to look up in table2's dictionary of table rows. This avoids the need of a nested for loop to check every single row finding a matching row.

To improve memory usage, I do not need to store the entire SqlLog and SqlStatement into memory. This is because we are not extracting all sessions at a time. Currently, I only store the sessions in a SqlTable that we want to extract, so when I read SqlLog and SqlStatement, I can add table rows and join table rows at the same time. I add a row of SqlLog only when SqlLog.sqlID matched a session's Session.ID, otherwise do not store this row in SqlLog's table because we do not use it. Similarly for SqlStatement, I only add a row to the table when SqlStatement.statementID matched on of SqlLog.statementID in the SqlLog table.

Debugging:
The script now halts for user to type a command to query rows in the joined table. Here are the types of commands to give:

session <int sessionID> <int rankInSession>
sql <int sqlID>
statement <int statementID>

For example, when the console displays "Please enter command:", type "session 1 1" to display the row in the joined sessionlog table that has sessionID=1 and rankInSession=1. "statement 24060378" will print the row in sqlstatement table with statementID=24060378, and so on


TODO:
Fix incorrect extraction of statements in sqlstatement.csv
Allow random sampling of sessions by using reservoir sampling algorithm
