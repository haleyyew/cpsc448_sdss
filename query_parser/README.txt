This script parses the SQL statements of each session and produce a user-item interaction matrix of all items and all sessions. A matrix of dimension num_of_sessions X sum_of_all_distinct_items is generated.

To include the list of sessions to parse, specify all the sessions in 'sessions' of the config.ini file, in the form of a comma separated list. See example in config.ini 

The output is a csv file 'user_item_matrix.csv' where each row is a session and each column is an item. The value for a particular row and column is the number of occurances of that item for that session.

To run the script:
You need to install IronPython
You need to install SQL Server
You need to install Microsoft .NET Framework
ipy ./query_processing.py

TODO:
Need to consider the rules specified in 'query_processing_rules.sh' to improve the parsing of each SQL statement, to produce more meaningful items in the user-item interaction matrix
Remove the requirement for IronPython, SQL Server, Microsoft .NET Framework
