#!/home/acald013/opt/miniconda3/bin/python

import subprocess
import argparse
import math
from pysqldf import SQLDF
sqldf = SQLDF(globals())
import pandas as pd
import numpy as np
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

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
  print("Trajectory and Points for {0},{1},{2}\n".format(start, end, pids))
  pids = list(map(int, pids.split(" ")))
  for pid in pids:
    query = """
      SELECT 
        id, 'LINESTRING(' || GROUP_CONCAT(p) || ')' AS wkt 
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
    traj = '{0}\t"{1}"\n'.format(fid, traj['wkt'][0])
    print(traj)
    trajs.append(traj)
    query = """
      SELECT 
        id, t, x, y
      FROM
        pointset 
      WHERE 
        id = {0} 
        AND t BETWEEN {1} AND {2}
      """.format(pid, start, end)
    pointsInTraj = sqldf.execute(query)
    pointsInTraj = "".join(pointsInTraj.apply(lambda p: '{0}\t{1}\t{2}\t{3}\t{4}\n'.format(fid, int(p['id']), p['x'], p['y'], int(p['t'])), axis=1).values.tolist())
    print(pointsInTraj)
    points.append(pointsInTraj)

if(saving):
  trajsFilename = "/tmp/{0}TrajsNotFound_E{1}_M{2}_D{3}.tsv".format(method, int(epsilon), mu, delta)
  pointsFilename = "/tmp/{0}PointsNotFound_E{1}_M{2}_D{3}.tsv".format(method, int(epsilon), mu, delta)
  trajsFile = open(trajsFilename, "w")
  pointsFile = open(pointsFilename, "w")
  for traj in trajs:
    trajsFile.write(traj)
  trajsFile.close()
  for point in points:
    pointsFile.write(point)
  pointsFile.close()
  command = "scp {0} acald013@bolt:/home/csgrads/acald013/tmp/".format(trajsFilename)
  subprocess.call(command, shell=True)
  command = "scp {0} acald013@bolt:/home/csgrads/acald013/tmp/".format(pointsFilename)
  subprocess.call(command, shell=True)
