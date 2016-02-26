#! /usr/bin/python

import sys

part_list = []

for line in sys.stdin:
	part_list.append(map(lambda x : int(x), line.strip().split("\t")))

sorted(part_list, key=lambda part_list : part_list[0])


cnt = 0
for i in range(len(part_list)) :
	cur = part_list[i][1]
	part_list[i][1] = cnt
	cnt += cur

param_list = []
for i in range(len(part_list)) :
	param_list.append(str(part_list[i][1]))

print "_".join(param_list)


