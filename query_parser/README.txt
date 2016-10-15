This script parses the SQL statements of each session and produce a user-item interaction matrix of all items and all sessions. A matrix of dimension num_of_sessions X sum_of_all_distinct_items is generated.

The output is a csv file 'user_item_matrix.csv' where each row is a session and each column is an item. The value for a particular row and column is the number of occurance of that item for that session.

To run the script:
python ./query_processing.py

Debugging:
When the script halts with "Please enter a command", type a number N and print all sessions with at least N queries
Type "close" to exit debug mode

TODO:
Need to consider the rules specified in 'query_processing_rules.sh' to improve the parsing of each SQL statement, to produce more meaningful items in the user-item interaction matrix
