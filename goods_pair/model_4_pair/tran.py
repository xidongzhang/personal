import sys

for line in sys.stdin:
    line=line.strip()
    if(line==""):
        continue
    ll=line.split("\t")
    arr = ll[0].split("&")
    arr2 = arr[1].split("#")
    try:
        print "%s\t%s\t%s\t%s"%(arr[0],arr2[0],arr2[1],ll[1])
    except Exception as e:
        pass



