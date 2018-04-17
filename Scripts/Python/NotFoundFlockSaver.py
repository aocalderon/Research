#!/home/acald013/opt/miniconda3/bin/python

import subprocess
import argparse
import math
from pysqldf import SQLDF
sqldf = SQLDF(globals())
import pandas as pd
import numpy as np

## Reading arguments...
parser = argparse.ArgumentParser()
parser.add_argument("--epsilon", "-e", default=10, help="Epsilon")
parser.add_argument("--mu", "-m", default=3, help="Mu")
parser.add_argument("--delta", "-d", default=3, help="Delta")
parser.add_argument("--pointset", "-p", help="Pointset file")
parser.add_argument("--input", "-i", default="/tmp/NotFound.flocks", help="Input file")
parser.add_argument("--output", "-o", default="", help="Input file")
parser.add_argument("--method", "-t", default="BFE", help="Method used")
args = parser.parse_args()

## Setting variables...
epsilon = float(args.epsilon)
mu = int(args.mu)
delta = int(args.delta)
method = args.method
filename = args.output
saving = True
points = []
trajs = []
fid = 0

pointset = pd.read_csv('/home/acald013/Research/Datasets/Berlin/berlin0-10.tsv', sep='\t', header=None, names=['id', 'x', 'y', 't'], index_col=['id', 't'])
flockset = open(args.input, "r") 
for flock in flockset:
  if flock == "":
    saving = False
    continue
  fid = fid + 1
  start, end, pids = flock.split(",")
  pids = list(map(int, pids.split(" ")))
  print("{0},{1},{2}".format(start, end, pids))
  for pid in pids:
    query = """
      SELECT 
        id, 'STRINGLINE(' || GROUP_CONCAT(p) || ')' AS wkt 
      FROM 
        ( SELECT 
          id, t, x || ' ' || y AS p 
        FROM 
          pointset 
        WHERE 
          id = {0} 
          AND t BETWEEN {1} AND {2} 
        ORDER BY t ) 
      GROUP BY 
        id
      """.format(pid, start, end)
    traj = sqldf.execute(query)
    record = '{0}\t"{1}"\n'.format(fid, traj['wkt'][0])
    print(record)
    trajs.append(record)
    query = """
      SELECT 
        id, t, 'POINT(' || x || ' ' || y || ')' AS wkt
      FROM
        pointset 
      WHERE 
        id = {0} 
        AND t BETWEEN {1} AND {2}
      """.format(pid, start, end)
    point = sqldf.execute(query)
    point = point.apply(lambda x: '{0},{1},{2},"{3}"\n'.format(fid, x['id'], x['t'], x['wkt']), axis=1).values.tolist()
    print("".join(point))
    points = points + point

if(False):
  if filename == "":
    filename1 = "/tmp/{0}Trajs_E{1}_M{2}_D{3}.tsv".format(method, epsilon, mu, delta)
  newdataset = open(filename, "w")
  for notfound in notfounds:
    newdataset.write(notfound)
  newdataset.close()
