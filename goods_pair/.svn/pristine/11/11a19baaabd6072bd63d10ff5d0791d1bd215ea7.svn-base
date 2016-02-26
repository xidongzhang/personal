import sys

last_tid = None
last_cluster_id = None

if __name__ == "__main__":
    for line in sys.stdin:
        var = line.strip().split("\t")
        type = var[1]
        if type == '1' :
            if len(var)!=3 or var[2]=='':
                continue
            tid = var[0]
            style_str = var[2]
            
            if last_tid != tid :
            	last_tid = tid
                last_style_str = style_str
        elif type == '2' :
            if var[0] == last_tid and last_tid:
            	print "\t".join(var[2:])+'{/c}'+last_style_str
            else:
            	print "\t".join(var[2:])
