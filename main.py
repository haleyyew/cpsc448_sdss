__author__ = 'HY'

import open_csv
import table_join

if __name__ == '__main__':
    ipAll_keys = ['ipAddressString','someOtherID']
    ipAll_table = open_csv.open_csv_file('ipAll.csv', ipAll_keys)

    ipdomain_keys = ['domainID']
    ipdomain_table = open_csv.open_csv_file('ipdomain.csv', ipdomain_keys)

    table_join.table_join(ipAll_table,ipdomain_keys,ipdomain_table,ipdomain_keys)