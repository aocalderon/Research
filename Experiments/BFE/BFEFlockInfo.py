#!/opt/miniconda3/bin/python

import re

f = open('nohup.out', 'r')

for line in f.readlines():
	if line.find("Nro") == 0:
		params = line.split(":")
		e = int(params[3].split("\t")[0])
		m = int(params[1].split("\t")[0])
		d = int(params[2].split("\t")[0])
	elif line.find("Closing stream") == 0:
		f = 0
		temp = line.split("Total answers: ")
		if len(temp) == 2:
			f = int(temp[1])
		print("BFE\t{0}\t{1}\t{2}\t{3}".format(e, m, d, f))
