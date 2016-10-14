This project analyzes SQL query logs stored in SkyServer web interface.
SkyServer provides a public portal for users to submit raw SQL to query the Sloan Digital Sky Server (SDSS) database.

The pipeline of data analysis is:
(1)extract data from query logs
(2)perform data mining algorithms

To extract data and construct a user-item interaction matrix:
run table_join/main.py
run table_join_post_processing/post_processing.py
run query_parser/query_parser.py

README for each of the python scripts can be found inside their respective working directories