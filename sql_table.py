__author__ = 'HY'

class SqlTable:

    def __init__(self, table_name):
        self.table_name = table_name
        self.num_rows = 0
        self.table_rows = {}
        self.attributes = []
        self.list_of_keys = []
        self.list_of_key_indexes = []
        self.add_to_special_group = False
        self.special_group = {}

    def declare_table_attributes(self, attributes):
        self.attributes = attributes[:]

    def declare_list_of_keys(self, list_of_keys):
        self.list_of_keys = list_of_keys
        index = 0
        for key in list_of_keys:
            try:
                #print(key)
                index = self.attributes.index(key)
                #print(index)
                self.list_of_key_indexes.append(index)
            except(Exception):
                print("Exception in declare_list_of_keys of ", self.table_name)
                return
        if len(list_of_keys) > 1:
            self.add_to_special_group = True

    def hash_list_of_key_indexes_to_key(self, row_values):
        hash_string = ""
        for i in range(len(self.list_of_key_indexes)):
            #print(str(row_values[i]))
            index = self.list_of_key_indexes[i]
            hash_string += str(row_values[index])
            #hash_string += "_"

        return hash_string

    def add_row(self, row_values):
        key = self.hash_list_of_key_indexes_to_key(row_values)
        if key in self.table_rows:
            self.table_rows[key+'__1'] = row_values
        else:
            self.table_rows[key] = row_values
        #print("Added row key: ", key, "values: ", '|'.join(self.table_rows[key]))
        #print("")
        self.num_rows += 1

        if self.add_to_special_group == True:
            group_key = row_values[self.list_of_key_indexes[0]]
            if group_key in self.special_group:
                self.special_group[group_key].append(self.table_rows[key])
            else:
                group = []
                group.append(self.table_rows[key])
                self.special_group[group_key] = group

    def get_row(self, key):
        row = []
        try:
            row = self.table_rows[key]
        except(Exception):
            print("get_row error: row not found, key=", key)

        return row
