__author__ = 'HY'

class SqlTable:

    def __init__(self, table_name):
        self.table_name = table_name
        self.num_rows = 0
        self.table_rows = {}
        self.attributes = []
        self.list_of_keys = []
        self.list_of_key_indexes = []

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

    def hash_list_of_key_indexes_to_key(self, row_values):
        hash_string = ""
        for i in range(len(self.list_of_key_indexes)):
            #print(str(row_values[i]))
            index = self.list_of_key_indexes[i]
            hash_string += str(row_values[index])

        return hash_string

    def add_row(self, row_values):
        key = self.hash_list_of_key_indexes_to_key(row_values)
        self.table_rows[key] = row_values
        print("Added row key: ", key, "values: ", '|'.join(self.table_rows[key]))
        self.num_rows += 1

    def get_row(self, key):
        return self.table_rows[key]
