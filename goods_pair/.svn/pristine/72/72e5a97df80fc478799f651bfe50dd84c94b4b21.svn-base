#coding=utf8

import sys 

if __name__ == '__main__':
    user_dict = {}
    last_user = ""
    for line in sys.stdin:
        line = line.strip()
        if line == '': 
            continue
        ll = line.split('\t')
        if len(ll) < 2:
            continue
        if not ll[0].isdigit() or not ll[1].isdigit() or ll[0] == '' or ll[1] == '':
            continue
        user_id = ll[0]
        twitter_id = ll[1]
        
        if user_id not in user_dict:
            user_dict[user_id] = set()
        else:
            user_dict[user_id].add(twitter_id)
            
    for user_id, twitter_set in user_dict.items():
        val = ','.join([i for i in twitter_set])
        print "%s\t%s" % (user_id, val)
