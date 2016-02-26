#coding=utf8

import sys 

if __name__ == '__main__':
    twitter_dict = {}
    for line in sys.stdin:
        line = line.strip()
        if line == '': 
            continue
        ll = line.split('\t')
        if len(ll) < 2:
            continue
        if not ll[0].isdigit() or not ll[1].isdigit():
            continue
        twitter_id = ll[0]
        price = int(ll[1])
        if twitter_id not in twitter_dict:
            twitter_dict[twitter_id] = price
        elif price < twitter_dict[twitter_id]:
            twitter_dict[twitter_id] = price
            
    for twitter_id, price in twitter_dict.items():
        print "%s\t%s" % (twitter_id, str(price))
