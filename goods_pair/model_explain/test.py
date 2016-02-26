import sys


for line in sys.stdin:
    ll=line.split("\001")
    print "\t".join(ll)

