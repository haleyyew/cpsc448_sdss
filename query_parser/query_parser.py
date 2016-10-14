import os

__author__ = 'Haoran Yu'
# import query_parser
import csv
import ConfigParser
import re

class SessionTokens:
    def __init__(self, session_number):
        self.session_number = session_number
        self.tokens_group = {}
        self.tokens_group['select'] = {}
        self.tokens_group['from'] = {}
        self.tokens_group['where'] = {}
        self.tokens_group['orderby'] = {}
        self.tokens_group['groupby'] = {}
        self.all_tokens = {}

    def add_token(self, token_group, token):
        tokens_group_dict = self.tokens_group[token_group]
        if token in tokens_group_dict:
            #print token_group, token
            tokens_group_dict[token] += 1
            #print 'tokens_group_dict[',token,']', tokens_group_dict[token]
        else:
            #print token_group, token
            tokens_group_dict[token] = 1
        # print self.tokens_group

    def print_all_tokens(self):
        print "Printing all tokens for session", self.session_number

        # print self.tokens_group
        # print len(self.tokens_group['select'])

        for group in [ 'select' , 'from' , 'where' , 'orderby' ,'groupby' ] :
            print "=====" + group + ":"
            # print group in self.tokens_group[group]
            tokens_group_dict = self.tokens_group[group]
            # print len(tokens_group_dict)
            for token in tokens_group_dict:
                print token," = ",tokens_group_dict[token]
                self.all_tokens[group+'_'+token] = tokens_group_dict[token]

    def split_string_and_store(self, token_group,list_of_tokens):
        #list_of_tokens = str.split()
        #print list_of_tokens
        for token in list_of_tokens:
            session_tokens.add_token(token_group, token)


def clear_dictionary(partitions):
    keys = ['select' , 'from' , 'where' , 'orderby' ,'groupby' ]
    for key in keys:
        partitions[key] = []
    return partitions

def add_to_partition(word,partitions,last_partition):
    if word in partitions:
        last_partition = word
        return last_partition
    else:
        partitions[last_partition].append(word)
        return last_partition


def process_line(line, partitions):
    # select_from = re.search('^select(.*)from', line)
    # from_where = re.search('^from(.*)where', line)
    # from_orderby = re.search('^from(.*)order by', line)
    # from_groupby = re.search('^from(.*)group by', line)
    # where_groupby = re.search('^where(.*)group by', line)
    # where_orderby = re.search('^where(.*)order by', line)
    # groupby_orderby = re.search('^group by(.*)order by', line)
    # from_select = re.search('^from(.*)select', line)
    # where_select = re.search('^where(.*)select', line)
    # groupby_select = re.search('^group by(.*)select', line)
    #
    # list_of_matches = [select_from,from_where,from_orderby,from_groupby,where_groupby,
    #                    where_orderby,groupby_orderby,from_select,where_select,groupby_select]

    words = line.split()
    last_partition = "select"
    joint_keyword = ""

    for word in words:
        if (word == "group") or (word == "order"):
            joint_keyword = word
            continue
        elif (word == "by") and (joint_keyword != ""):
            word = joint_keyword + word
            joint_keyword = ""

        last_partition = add_to_partition(word,partitions,last_partition)

    return partitions


def parse_simple(session_tokens, filename):

    match = ""
    partitions = {}
    clear_dictionary(partitions)

    with open( filename ) as f :
        for line in f :
            line = line.lower()
            partitions = process_line(line,partitions)

            for key in partitions:
                session_tokens.split_string_and_store(key,partitions[key])
            clear_dictionary(partitions)


if __name__ == '__main__':
    config = ConfigParser.ConfigParser()
    config.read('../table_join/config.ini')
    sessions_dir = config.get('Parser','inputDir')

    list_of_session = os.listdir(sessions_dir)

    all_session_items = []
    for session in list_of_session:
        session = session.split(".")[0]
        session_tokens = SessionTokens(int(session))
        parse_simple(session_tokens, sessions_dir+session+'.csv')
        session_tokens.print_all_tokens()
        all_session_items.append(session_tokens)

    for session_index in range(len(all_session_items)):
        session = all_session_items[session_index]
        print "iterating ", session.session_number

        session_items = session.all_tokens.keys()
        # session_items = session_items.sort()
        if session_index !=0:
            for i in range(session_index):
                prev_session = all_session_items[i]
                prev_session_items = prev_session.all_tokens.keys()
                for item in session_items:
                    if item not in prev_session_items:
                        prev_session.all_tokens[item] = 0

                for item in prev_session_items:
                    if item not in session_items:
                        session.all_tokens[item] = 0

    with open(config.get('Parser','outputDir')+'user_item_matrix'+'.csv', 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        session = all_session_items[-1]
        header = []
        session_all_tokens = session.all_tokens.keys()
        session_all_tokens.sort()
        print session_all_tokens
        for token in session_all_tokens:
            header.append(token)
        writer.writerow(header)
        for session in all_session_items:
            writer.writerow([session.session_number])
            items = session.all_tokens.keys()
            items.sort()
            session_items = []
            for item in items:
                session_items.append(session.all_tokens[item])
            writer.writerow(session_items)