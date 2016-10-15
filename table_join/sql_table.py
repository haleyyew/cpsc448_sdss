__author__ = 'HY'

class SqlTable:
    """
    SqlTable is a class that stores the contents of a single csv file,
    it acts like a table of a SQL database.

    Each SqlTable has a name table_name, number of rows in the table num_rows,
    a dictionary that stores all the rows of the table where the key of the dictionary entries
    is the key of the Skyserver logs tables described in ./data_description_report/report.pdf,
    the attribute names for each column of the table, the key names list_of_keys,
    and a list of numbers list_of_key_indexes used for mapping attribute names to the positions in
    the list of attributes.

    There are 2 special fields add_to_session_group and session_group, used specifically when
    reading the sessionlog.csv file, session_group stores distinct sessions, where each session
    has a list of log entries in sessionlog. In other words, session_group groups all the log entries
    with a common sessionID together.
    """

    def __init__(self, table_name):
        self.table_name = table_name
        self.num_rows = 0
        self.table_rows = {}
        self.attributes = []
        self.list_of_keys = []
        self.list_of_key_indexes = []
        self.add_to_session_group = False
        self.session_group = {}
        self.add_to_sqllog_group = False
        self.sqllog_group = {}
        self.add_to_sqlstatement_group = False
        self.sqlstatement_group = {}

    def declare_table_attributes(self, attributes):
        self.attributes = attributes[:]

    def declare_list_of_keys(self, list_of_keys):
        """
        Add the list_of_keys to SqlTable.list_of_keys, and add the positions of the keys in
        self.attributes to SqlTable.list_of_key_indexes

        This method is always called before adding any rows add_row() to the SqlTable.table_rows
        because add_row() calls hash_list_of_key_indexes_to_key(), which depends on the values stored
        in SqlTable.list_of_keys
        """
        self.list_of_keys = list_of_keys
        index = 0
        for key in list_of_keys:
            try:
                index = self.attributes.index(key)
                self.list_of_key_indexes.append(index)
            except(Exception):
                print "Exception in declare_list_of_keys for ", self.table_name
                return
        # add_to_session_group is set to True only when we are dealing with the sessionlog table
        if self.table_name == 'sessionlog':
            self.add_to_session_group = True
            self.add_to_sqllog_group = True
        if self.table_name == 'sqllog':
            self.add_to_sqlstatement_group = True

    def hash_list_of_key_indexes_to_key(self, row_values):
        """
        Create a "hash" for the keys of the row row_values. The method is redundant and may be removed later.
        For example, if a row has sessionID and rankInSession as its key, sessionID=100 rankInSession=2,
        then the method will return 100_2 as the hash.
        """
        hash_string = ""
        for i in range(len(self.list_of_key_indexes)):
            if i>0:
                hash_string += "_"
            index = self.list_of_key_indexes[i]
            try:
                hash_string += str(row_values[index])
            except Exception:
                print "Exception in self.hash_list_of_key_indexes_to_key, row_values=",row_values

        return hash_string

    def add_row(self, row_values):
        """
        :param row_values: the list of strings produced from reading a comma separated csv row
        :return:
        Stores the row_values as an entry in SqlTable.table_rows, where the key of the entry is the
        hash returned by SqlTable.hash_list_of_key_indexes_to_key
        """
        key = self.hash_list_of_key_indexes_to_key(row_values)
        if key in self.table_rows:
            self.table_rows[key+'__1'] = row_values
        else:
            self.table_rows[key] = row_values
        self.num_rows += 1

        # This if statement evauates to true only when SqlTable.table_name is 'sessionlog', then a
        # reference to the newly stored entry in SqlTable.table_rows is also added to SqlTable.session_group
        if self.add_to_session_group == True:
            group_key = row_values[self.list_of_key_indexes[0]]
            if group_key in self.session_group:
                self.session_group[group_key].append(self.table_rows[key])
            else:
                group = []
                group.append(self.table_rows[key])
                self.session_group[group_key] = group

        if self.add_to_sqllog_group == True:
            key_sqlID = row_values[-2]
            self.sqllog_group[str(key_sqlID)] = 1
        if self.add_to_sqlstatement_group == True:
            key_statementID = row_values[17]
            self.sqlstatement_group[str(key_statementID)] = 1


    def get_row(self, key):
        row = []
        try:
            row = self.table_rows[key]
        except(Exception):
            print "Exception get_row error: row not found, key=", key

        return row

    def delete_session_group(self, row):
        key = row[self.list_of_key_indexes[0]]
        # print "=======",key
        # print key in self.session_group
        print "delete_session_group", key, "no more sessions to add"
        self.session_group.pop(key, None)
