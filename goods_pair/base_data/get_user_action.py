#!/usr/bin/python
#coding=utf-8

import sys
import urllib
import os

def load_fake_orders():
    fake_dict={}
    f = open('fake_orders')
    for line in f:
        try:
            order = line.strip()
        except:
            continue
        fake_dict[order] = 1
    f.close()
    return fake_dict

def getTimeStr(date, hour, minute, second):
    return "%s %s:%s:%s"%(date,hour.zfill(2),minute.zfill(2),second.zfill(2)) 

if '__main__' == __name__:
    fake_dict = load_fake_orders()
    for line in sys.stdin:
        try:
            date, hour, minute, second, user_id, action, query, pos, offset, twitter_id, order_id = line.strip().split('\t')
        except:
            continue
        if str.isdigit(user_id) == False or str.isdigit(twitter_id) == False:
            continue
        timeStr = getTimeStr(date, hour, minute, second)
        if (action == 'statistics-single_twitter' or action == "statistics-click_buy_button_2-cart" or action == 
            'statistics-click_buy_button_2-buy' or action == "order-add" or action == "order-pay"):
            twitter_pos = -1
            if str.isdigit(pos) and str.isdigit(offset):
                twitter_pos = int(pos)+int(offset)+1
            if twitter_pos > 960:
                twitter_pos = 960
            if(order_id != None and order_id != "NULL" and order_id != ""):
                if(order_id in fake_dict):
                    continue
            print "%s\t%s\t%s\t%s\t%s\t%s"%(timeStr, user_id, action, twitter_id, query.strip(), twitter_pos)



