import sys
import os
import time
import json

if __name__ == '__main__':
    input_file = os.environ['map_input_file']
    arr=input_file.split("/")
    inputPath=arr[len(arr)-2]

    for line in sys.stdin:
        line=line.strip()
        if(line==""):
            continue
        if(inputPath=="goods_fea_tag_mid"):
            ll=line.split("\t")
            tid=ll[1]
            try:
                if(int(tid)>2061202555):
                    print tid+"\t2\t"+"\t".join(ll)
            except:
                pass
        else:
            ll=line.strip().split("\t")
            tid = ll[0]
            style_score = ll[1]
            try:
                score_list = style_score.split(" ")
                if len(score_list) != 13:
                    pass
                else:
                    try:
                        scores = map(lambda x:float(x), score_list)
                        style_list = [index for index in range(len(scores)) if scores[index]>=0.3]
                        style_str = '{/c}'.join(map(lambda x:'style_'+str(x), style_list))
                        print "%s\t1\t%s"%(tid, style_str)
                    except:
                        continue
            except:
                pass
