__author__ = 'Haoran Yu'
import os
import csv
import ConfigParser
import signal
import time
import sys

print_info = 2000000


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
        self.number_of_queries = 0

    def add_token(self, token_group, token):
        tokens_group_dict = self.tokens_group[token_group]
        if token in tokens_group_dict:
            tokens_group_dict[token] += 1

        else:
            tokens_group_dict[token] = 1


    def print_all_tokens(self):
        print "Printing all tokens for session", self.session_number

        for group in [ 'select' , 'from' , 'where' , 'orderby' ,'groupby' ] :
            print "=====" + group + ":"
            tokens_group_dict = self.tokens_group[group]
            for token in tokens_group_dict:
                print token," = ",tokens_group_dict[token]
                self.all_tokens[group+'_'+token] = tokens_group_dict[token]

    def split_string_and_store(self, token_group,list_of_tokens):
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
            session_tokens.number_of_queries +=1
            line = line.lower()
            partitions = process_line(line,partitions)

            for key in partitions:
                session_tokens.split_string_and_store(key,partitions[key])
            clear_dictionary(partitions)

def debug():
    while (1):
        try:
            response = raw_input("Please enter command: ")
        except Exception:
            continue
        split_command = response.split()

        if response == "close":
            break

        else:
            try:
                print len(all_session_items)
                num = int(split_command[0])

                for session in all_session_items:
                    if session.number_of_queries<num:
                        continue
                    print session.print_all_tokens()

            except Exception:
                print Exception
                continue
    return

class Logger(object):
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("execution_log.log", "a")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        pass


if __name__ == '__main__':

    sys.stdout = Logger()
    start = time.time()
    s = signal.signal(signal.SIGINT, signal.SIG_IGN)

    config = ConfigParser.ConfigParser()
    config.read('../table_join/config.ini')
    sessions_dir = config.get('Parser','inputDir')

    # since iterating through all sessions to add all possible tokens takes O(n^n) time,
    # I will only create a matrix using N sessions that has submitted more than M uniques
    only_create_a_matrix_for_N_sessions = int(config.get('Parser','only_create_a_matrix_for_N_sessions'))
    N = int(config.get('Parser','N'))
    session_with_more_than_M_unique_queries = int(config.get('Parser','session_with_more_than_M_unique_queries'))
    M = int(config.get('Parser','M'))

    list_of_session = os.listdir(sessions_dir)

    all_session_items = []
    for session in list_of_session:
        session = session.split(".")[0]
        session_tokens = SessionTokens(int(session))
        parse_simple(session_tokens, sessions_dir+session+'.csv')
        session_tokens.print_all_tokens()
        all_session_items.append(session_tokens)

    # print some info from time to time
    timer1 = time.time()
    timer2 = time.time()

    if only_create_a_matrix_for_N_sessions and session_with_more_than_M_unique_queries:
        sessions_count = 0
        temp_list_of_sessions = []
        for session in all_session_items:
            sessions_count+=1
            if sessions_count > N:
                break
            if session.number_of_queries > M:
                temp_list_of_sessions.append(session)
        if len(temp_list_of_sessions) < 1:
            print "M in session_with_more_than_M_unique_queries is too large, no sessions has that many queries"
            debug()
        all_session_items = temp_list_of_sessions


    print "iterating user-items"
    for session_index in range(len(all_session_items)):
        session = all_session_items[session_index]
        session_items = session.all_tokens.keys()

        timer2 = time.time()
        diff = timer2 - timer1
        if diff > 60:
            print "I am currently iterating session", session.session_number, \
                "with the following session.all_tokens: ",session_items
            timer1 = timer2

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

    print "output session_all_tokens"
    with open(config.get('Parser','outputDir')+'user_item_matrix'+'.csv', 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        session = all_session_items[-1]
        header = []
        session_all_tokens = session.all_tokens.keys()
        session_all_tokens.sort()
        #print session_all_tokens
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


    end = time.time()
    print "program took", (end-start)/60, "minutes"
    #debug()
    signal.signal(signal.SIGINT, s)