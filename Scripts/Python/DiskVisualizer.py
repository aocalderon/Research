#!/home/acald013/opt/miniconda3/bin/python

import subprocess
import argparse

## Reading arguments...
parser = argparse.ArgumentParser()
parser.add_argument("--epsilon", "-e", help="Epsilon")
parser.add_argument("--mu", "-m", help="Mu")
parser.add_argument("--delta", "-d", help="Delta")
parser.add_argument("--pointset", "-p", default="/home/acald013/Research/Datasets/Buses/buses.tsv", help="Pointset file")
parser.add_argument("--input", "-i", default="/tmp/NotFound.flocks", help="Input file")
parser.add_argument("--output", "-o", default="/tmp/flocks.wkt", help="Output file")
args = parser.parse_args()

## Setting variables...
epsilon = args.epsilon
mu = args.mu
delta = args.delta
points = {}

pointset = open(args.pointset, "r") #opens file with name of "test.txt"
for point in pointset:
  p = point.split("\t")
  pid = int(p[0])
  x = float(p[1])
  y = float(p[2])
  points[pid] = [x, y]

flockset = open(args.input, "r") #opens file with name of "test.txt"
for flock in flockset:
  f = list(map(int, flock.split(" ")))
  minx = float("inf")
  maxx = float("-inf")
  miny = float("inf")
  maxy = float("-inf")
  for pid in f:
    coordinate = points[pid]
    x = coordinate[0]
    y = coordinate[1]
    if(x < minx):
      minx = x
    if(x > maxx):
      maxx = x
    if(y < miny):
      miny = y
    if(y > maxy):
      maxy = y
  mbr = "POLYGON(({0} {1}, {2} {1}, {2} {3}, {0} {3}, {0} {1}))".format(minx, miny, maxx, maxy)
  midx = minx + ((maxx - minx) / 2.0)
  midy = miny + ((maxy - miny) / 2.0)
  print("{0},\"{1}\",{2},{3}".format(flock.replace('\n', ''), mbr, midx, midy))
