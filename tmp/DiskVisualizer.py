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

notfounds = set()
flockset = open(args.input, "r") 
for flock in flockset:
  # Are there flocks in NonFound.txt file?
  if flock == "":
    saving = False
    continue
  
  # Casting each flock...
  f = list(map(int, flock.split(" ")))
  
  # Getting the MBR for this flock...
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
  
  # Getting mid points for the MBR...
  midx = minx + ((maxx - minx) / 2.0)
  midy = miny + ((maxy - miny) / 2.0)
  
  # Getting max and min distance among points of the flock...
  maxdist = float("-inf")
  mindist = float("inf")
  for coordinate1 in coordinates:
    for coordinate2 in coordinates:
      X = coordinate1[0] - coordinate2[0]
      Y = coordinate1[1] - coordinate2[1]
      if X == 0 and Y == 0:
        continue;
      X2 = X * X
      Y2 = Y * Y
      d = math.sqrt(X2 + Y2)
      if d > maxdist:
        maxdist = d
      if d < mindist:
        mindist = d
  print("MAX_DIST={0} EPSILON={1} {2}".format(maxdist, epsilon, maxdist==epsilon))
  
  # Storing the record...
  notfounds.add("{0},\"{1}\",{2},{3},{4},{5}\n".format(flock.replace('\n', ''), mbr, midx, midy, maxdist, mindist))
  if maxdist != float("-inf") and maxdist > epsilon:
    print("ERROR: Maximal distance among flock points ({0}) greater then EPSILON ({1})".format(maxdist, epsilon))
    print("D_ERROR={0}".format(maxdist - epsilon))
    print("{0},\"{1}\",{2},{3}".format(flock.replace('\n', ''), mbr, midx, midy))

if(saving):
  filename = "/tmp/{0}NotFounds_E{1}_M{2}_D{3}.wkt".format(method, epsilon, mu, delta)
  wkt = open(filename, "w")
  for notfound in notfounds:
    wkt.write(notfound)
  wkt.close()
