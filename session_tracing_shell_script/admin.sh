ssh haleyyew@whisky.cs.ubc.ca
cd /ubc/cs/research/connections/data/SDSS/dataset

scp report.pdf haleyyew@whisky.cs.ubc.ca:
mv report.pdf /ubc/cs/research/connections/data/SDSS/code

ls -ld /ubc/cs/research/connections/data/SDSS/output

mv -f config.ini /ubc/cs/research/connections/data/SDSS/code
mv -f sql_table.py /ubc/cs/research/connections/data/SDSS/code
mv -f main.py /ubc/cs/research/connections/data/SDSS/code
mv -f open_csv.py /ubc/cs/research/connections/data/SDSS/code
mv -f table_join.py /ubc/cs/research/connections/data/SDSS/code

chmod a+x /ubc/cs/research/connections/data/SDSS/code/config.ini
chmod a+x /ubc/cs/research/connections/data/SDSS/code/sql_table.py
chmod a+x /ubc/cs/research/connections/data/SDSS/code/main.py
chmod a+x /ubc/cs/research/connections/data/SDSS/code/open_csv.py
chmod a+x /ubc/cs/research/connections/data/SDSS/code/table_join.py

mv -f hello.py /ubc/cs/research/connections/data/SDSS/code

mv -f sessionlog.txt /ubc/cs/research/connections/data/SDSS/code
mv -f sqllog.txt /ubc/cs/research/connections/data/SDSS/code
mv -f sqlStatement.txt /ubc/cs/research/connections/data/SDSS/code

chmod -R 0777