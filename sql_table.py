__author__ = 'HY'

class SqlTable:

    def __init__(self, table_name):
        self.table_name = table_name
        self.num_rows = 0
        self.table_rows = {}
        self.attributes = []
        self.list_of_keys = []
        self.list_of_key_indexes = []
        self.add_to_session_group = False
        self.session_group = {}

    def declare_table_attributes(self, attributes):
        self.attributes = attributes[:]

    def declare_list_of_keys(self, list_of_keys):
        self.list_of_keys = list_of_keys
        index = 0
        for key in list_of_keys:
            try:
                index = self.attributes.index(key)
                self.list_of_key_indexes.append(index)
            except(Exception):
                print "Exception in declare_list_of_keys for ", self.table_name
                return
        #if len(list_of_keys) > 1:
        if self.table_name == 'sessionlog':
            self.add_to_session_group = True

    def hash_list_of_key_indexes_to_key(self, row_values):
        hash_string = ""
        for i in range(len(self.list_of_key_indexes)):
            index = self.list_of_key_indexes[i]
            hash_string += str(row_values[index])


        return hash_string

    def add_row(self, row_values):
        key = self.hash_list_of_key_indexes_to_key(row_values)
        if key in self.table_rows:
            self.table_rows[key+'__1'] = row_values
        else:
            self.table_rows[key] = row_values
        self.num_rows += 1

        if self.add_to_session_group == True:
            group_key = row_values[self.list_of_key_indexes[0]]
            if group_key in self.session_group:
                self.session_group[group_key].append(self.table_rows[key])
            else:
                group = []
                group.append(self.table_rows[key])
                self.session_group[group_key] = group

    def get_row(self, key):
        row = []
        try:
            row = self.table_rows[key]
        except(Exception):
            print "Exception get_row error: row not found, key=", key

        return row
