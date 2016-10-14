The script post_processing.py processes session csv files in ../output to extract only the statements of each row, then store the statements of each session in ../output_processed.
Additionally, check whether the query was submitted via the standard web interface, and whether the query has correct syntax. If either is false, do not store that statement.

To run the script:
python ./post_processing.py