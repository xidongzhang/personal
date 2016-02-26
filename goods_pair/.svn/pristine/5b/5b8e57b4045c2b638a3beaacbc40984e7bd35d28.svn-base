#! /usr/bin/python

import sys

dupdict = {}

train_data = open('train_data', 'w')
line_num = 0
for i in sys.stdin :
    (key, value) = i.strip().split('\t')
    
    items = key.strip().split(':') 
    show = float(items[0])
    click = int(items[1])
    if dupdict.has_key(value):
        dupdict[value][0] += show
        dupdict[value][1] += click
    else:
        dupdict[value] = [show, click]

for key, value in dupdict.items():
    show = value[0]
    click = value[1]
    train_data.write('%.3f:%d\t%s\n' % (show, click, key))
        

