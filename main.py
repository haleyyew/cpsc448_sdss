__author__ = 'HY'

import open_csv
import table_join

if __name__ == '__main__':
    ipAll_keys = ['ipAddressString','someOtherID','mystatus']
    ipAll_table = open_csv.open_csv_file('ipAll.csv', ipAll_keys)

    ipdomain_keys = ['domainID','status']
    ipdomain_table = open_csv.open_csv_file('ipdomain.csv', ipdomain_keys)

    my_join_attr = ['domainID','mystatus']
    other_join_attr = ['domainID','status']
    table_join.table_join(ipAll_table,my_join_attr,ipdomain_table,other_join_attr)