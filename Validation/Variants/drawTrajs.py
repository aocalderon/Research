#!/usr/bin/python

f = open('trajs.txt', 'r')
tid = 0
for line in f.readlines():
	trajectory = line.split(" ")
	time = 0
	for point in trajectory:
		if point == "1":
			print("{0},{1}".format(tid, time))
		time = time + 1
	tid = tid + 1
