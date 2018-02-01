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
parser.add_argument("--output", "-o", default="/tmp/flocks.wkt", help="Output file")
args = parser.parse_args()

## Setting variables...
epsilon = float(args.epsilon)
mu = int(args.mu)
delta = int(args.delta)
points = {}

pointset = open(args.pointset, "r")
for point in pointset:
  p = point.split("\t")
  pid = int(p[0])
  x = float(p[1])
  y = float(p[2])
  points[pid] = [x, y]

flockset = open(args.input, "r") 
for flock in flockset:
  if flock == "":
    continue
  f = list(map(int, flock.split(" ")))
  coordinates = []
  minx = float("inf")
  maxx = float("-inf")
  miny = float("inf")
  maxy = float("-inf")
  for pid in f:
    coordinate = points[pid]
    coordinates.append(coordinate)
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
  maxdist = float("-inf")
  for coordinate1 in coordinates:
    for coordinate2 in coordinates:
      X = coordinate1[0] - coordinate2[0]
      Y = coordinate1[1] - coordinate2[1]
      X2 = X * X
      Y2 = Y * Y
      d = math.sqrt(X2 + Y2)
      if d > maxdist:
        maxdist = d
  print("MAX_DIST={0} EPSILON={1} {2}".format(maxdist, epsilon, maxdist==epsilon))
  if maxdist != float("-inf") and maxdist > epsilon:
    print("ERROR: Maximal distance among flock points ({0}) greater then EPSILON ({1})".format(maxdist, epsilon))
    print("D_ERROR={0}".format(maxdist - epsilon))
    print("{0},\"{1}\",{2},{3}".format(flock.replace('\n', ''), mbr, midx, midy))
