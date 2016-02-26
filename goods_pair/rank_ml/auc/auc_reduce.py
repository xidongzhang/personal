#! /usr/bin/env python

import sys
import time

cur_time = int(time.time())
cur_data = time.strftime('%Y-%m-%d %H:%M:%S')
auc_temp = 0.0
click_sum = 0.0
pre_click_sum = 0.0
no_click_sum = 0.0
no_click = 0.0

last_key = None

for line in sys.stdin:
    (key, value) = line.strip().split("\t")
    (show, click) = value.strip().split("_")
    (key, show, click) = (float(key), float(show), float(click))
    if (click > show) :
        click = show
    if last_key != key :
        #        if last_key :
        #    print "key=%f show=%d click=%d" % (last_key, no_click, click_sum - pre_click_sum)
        auc_temp += (click_sum + pre_click_sum) * no_click / 2.0
        pre_click_sum = click_sum
        no_click = 0.0
        last_key = key

    no_click_sum += show - click
    click_sum += click
    no_click += show - click

auc_temp += (click_sum + pre_click_sum) * no_click / 2.0

auc = auc_temp / (click_sum * no_click_sum)

print "auc=%f\tshow=%d\tclick=%d\ttime=%d\tdata=%s" % (auc, click_sum + no_click_sum, click_sum, cur_time, cur_data)



