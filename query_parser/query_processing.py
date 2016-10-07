__author__ = 'Haoran Yu'
import query_parser
import csv
import ConfigParser

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

if __name__ == '__main__':
    config = ConfigParser.ConfigParser()
    config.read('config.ini')
    sessions = config.get('Config','sessions')
    list_of_session = sessions.split(',')

    all_session_items = []
    for session in list_of_session:
        session_tokens = SessionTokens(int(session))
        query_parser.parse(session_tokens, session+'.txt')
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