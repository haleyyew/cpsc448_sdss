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

    def split_string_and_store(self, token_group,str):
        list_of_tokens = str.split()
        #print list_of_tokens
        for token in list_of_tokens:
            session_tokens.add_token(token_group, token)

def regex_match(line):
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
    words_len = len(words)
    partition = []
    for i in range(words_len,-1,-1):
        if words[i] in partition:
            return


    return


def parse_simple(session_tokens, filename):

    match = ""

    with open( filename ) as f :
        for line in f :
            line = line.lower()
            list_of_matches = regex_match(line)

            for match in list_of_matches:
                return


if __name__ == '__main__':
    config = ConfigParser.ConfigParser()
    config.read('config.ini')
    sessions = config.get('Config','sessions')
    list_of_session = sessions.split(',')

    all_session_items = []
    for session in list_of_session:
        session_tokens = SessionTokens(int(session))
        parse_simple(session_tokens, session+'.txt')
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

    with open(config.get('Config','outputDir')+'user_item_matrix'+'.csv', 'w') as csvfile:
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