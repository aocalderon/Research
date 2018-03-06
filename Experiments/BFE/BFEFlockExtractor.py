#!/opt/miniconda3/bin/python

import re

f = open('nohup.out', 'r')

for line in f.readlines():
	if line.find("Nro") == 0:
		params = line.split(":")
		e = int(params[3].split("\t")[0])
		m = int(params[1].split("\t")[0])
		d = int(params[2].split("\t")[0])
		filename = "BFE_E{0}_M{1}_D{2}.txt".format(e,m,d)
		out = open(filename, "w")
		print("Opening {0}".format(filename))
		flocks = []
	elif re.search(r'^\d+, \d+,', line) is not None:
		flocks.append(line)
	elif line.find("******") == 0:
		print("Saving {0} flocks to {1}".format(len(flocks), filename))
		out.write("".join(sorted(flocks)))
		out.close()
