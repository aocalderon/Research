#!/home/acald013/opt/miniconda3/bin/python

import subprocess
import argparse
import math

## Reading arguments...
parser = argparse.ArgumentParser()
parser.add_argument("--epsilon", "-e", help="Epsilon")
parser.add_argument("--mu", "-m", help="Mu")
parser.add_argument("--delta", "-d", help="Delta")
parser.add_argument("--pointset", "-p", help="Pointset file")
parser.add_argument("--input", "-i", default="/tmp/NotFound.flocks", help="Input file")
parser.add_argument("--method", "-t", default="BFE", help="Method used")
args = parser.parse_args()

## Setting variables...
epsilon = float(args.epsilon)
mu = int(args.mu)
delta = int(args.delta)
method = args.method
saving = True
points = {}

pointset = open(args.pointset, "r")
for point in pointset:
  p = point.split("\t")
  pid = int(p[0])
  x = float(p[1])
  y = float(p[2])
  points[pid] = [x, y]

flockset = open(args.input, "r") 
notfounds = set()
for flock in flockset:
  if flock == "":
    saving = False
    continue
  f = list(map(int, flock.split(" ")))
  for pid in f:
    coordinate = points[pid]
    x = coordinate[0]
    y = coordinate[1]
    notfounds.add("{0}\t{1}\t{2}\t{3}\n".format(pid, x, y, 0))

if(saving):
  filename = "/tmp/{0}NotFounds_E{1}_M{2}_D{3}.tsv".format(method, epsilon, mu, delta)
  newdataset = open(filename, "w")
  for notfound in notfounds:
    newdataset.write(notfound)
  newdataset.close()
