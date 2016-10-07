__author__ = 'Haoran Yu'
import query_parser

class SessionTokens:
    def __init__(self, session_number):
        self.session_number = session_number
        self.tokens_group = {}
        self.tokens_group['select'] = {}
        self.tokens_group['from'] = {}
        self.tokens_group['where'] = {}
        self.tokens_group['orderby'] = {}
        self.tokens_group['groupby'] = {}

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

    def split_string_and_store(self, token_group,str):
        list_of_tokens = str.split()
        #print list_of_tokens
        for token in list_of_tokens:
            session_tokens.add_token(token_group, token)

if __name__ == '__main__':
    global session_tokens
    session_tokens = SessionTokens(100000)
    query_parser.parse(session_tokens, 'query.txt')
    session_tokens.print_all_tokens()
