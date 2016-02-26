#! /usr/bin/env python
#coding=utf-8

import sys

last_fea = None
last_show = 0
last_click = 0

ins_thresh = 1
fea_thresh = 4

if __name__ == "__main__":
    for line in sys.stdin:
        var = line.strip().split("\t")
        type = var[-1]
        if type == '1' :
            pair = var[1].split(":")
            (show, click) = (float(pair[0]), float(pair[1]))
            if (click > 0 or show > ins_thresh) :
                print line.strip()
        elif type == '2' :
            fea = var[0]
            pair = var[1].split(":")
            (show, click) = (float(pair[0]), float(pair[1]))
            
            if last_fea != fea :
                if last_fea and ( last_click > 0 or last_show > fea_thresh):
                    print "%s\t%s:%s\t2" % (last_fea, str(last_show), str(last_click))
                last_fea = fea
                last_show = show
                last_click = click
            else :
                last_show += show
                last_click += click
        else :
            print line.strip()

    if last_fea and ( last_click > 0 or last_show > fea_thresh ) :
        print "%s\t%s:%s\t2" % (last_fea, str(last_show), str(last_click))
