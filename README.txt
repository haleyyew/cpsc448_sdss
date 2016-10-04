This script opens Skyserver logs csv files, extracts information for each session in sessionlog.csv, and outputs the extracted information to csv files.
Configure the script so that only a specific number of sessions is produced in the output.

Confurations of the script is located in config.ini
The location of source csv files is specified in the 'input'
The location of the output is specified in 'output' 
The number of sessions to output is specified in 'num_of_sessions'

To run the script,
python ./main.py

Documentations of the script can be found inside each .py file