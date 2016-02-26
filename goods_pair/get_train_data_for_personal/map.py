#! /usr/bin/env python
#coding=utf-8

import sys
import hashlib
import math

# config
deli = '\001'
position_bias_dict = {}

def load_position_bias_dict(position_bias_path):
    position_bias = open(position_bias_path)
    for line in position_bias:
        items = line.strip().split(':')
        pos = int(items[0])
        bias = float(items[1])
        position_bias_dict[pos-1] = bias
    position_bias.close()

def get_sha1(src):
	return hashlib.sha1(src).hexdigest()


#normal
#twitter_id poster_id poster_word pos show click feature_1 ... feature_n = n+6
#individuation
#user_id twitter_id poster_id poster_word pos show click feature_1 ... feature_n = n+7

#train_data=open('train_data', 'w')
if __name__ == "__main__":

    feature_start = 3
    load_position_bias_dict('position_bias.dict')

    for line in sys.stdin:
        items = line.strip().split(deli)
        if str.isdigit(items[0]) == False or str.isdigit(items[1]) == False:
            #sys.stderr.write('%s %s' % (items[0], items[1]))
            continue
        pos = int(items[2]) 
        if pos >= 150:
            continue
        show_nums = str(int(items[0])*position_bias_dict[pos])
        click_nums = items[1]

        sign = get_sha1("*".join(items[feature_start:]))

        print "%s\t%s\t%s\t%s" % (sign, show_nums, click_nums, "\t".join(items[feature_start:]))

