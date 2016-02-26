#! /usr/bin/env python

import sys

# rank asending
# pctr desending

for line in sys.stdin :
    (key, show, click) = line.strip().split(" ")
#    print key
#    if key.isdigit() != True :
#        continue
    if (float(click) > float(show)) :
        click = float(show)
    print "%f\t%f_%f" % (float(key), float(show), float(click))
