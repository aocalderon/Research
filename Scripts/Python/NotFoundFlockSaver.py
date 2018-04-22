##/home/acald013/opt/miniconda3/bin/python
#!/opt/miniconda3/bin/python

import subprocess
import argparse
import math
from pysqldf import SQLDF
sqldf = SQLDF(globals())
import os
import pandas as pd
import numpy as np
from operator import itemgetter
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

## Reading arguments...
parser = argparse.ArgumentParser()
parser.add_argument("--pointset", "-p", default="Datasets/Berlin/berlin0-10.tsv", help="Pointset file")
parser.add_argument("--input", "-i", default="/tmp/NotFound.flocks", help="Input file")
parser.add_argument("--no_save", dest='save', action='store_false', help="Save trajectories and points?")
parser.add_argument("--no_send", dest='send', action='store_false', help="Send to Bolt?")
parser.set_defaults(save=True, send=True)
args = parser.parse_args()

## Setting variables...
research_home = os.environ['RESEARCH_HOME']
pointset = "{0}{1}".format(research_home, args.pointset)
params = ".".join(args.input.split(".")[:-1]).split("/")[-1:][0].split("_")
epsilon = int(float(params[1][1:]))
mu = int(params[2][1:])
delta = int(params[3][1:])
method = params[0]
saving = True
points = []
trajs = []
fid = 0

pointset = pd.read_csv(pointset, sep='\t', header=None, names=['id', 'x', 'y', 't'], index_col=['id', 't'])
flockset = open(args.input, "r") 
for flock in flockset:
  if flock == "":
    saving = False
    continue
  fid = fid + 1
  start, end, pids = flock.split(", ")
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
        id, x, y, t
      FROM
        pointset 
      WHERE 
        id = {0} 
        AND t BETWEEN {1} AND {2}
      ORDER BY
        t, id
      """.format(pid, start, end)
    pointsInTraj = sqldf.execute(query)
    for p in pointsInTraj.values.tolist():
      points.append(p)

if(args.save):
  trajsFilename = "/tmp/{0}_E{1}_M{2}_D{3}_Trajs.tsv".format(method, int(epsilon), mu, delta)
  pointsFilename = "/tmp/{0}_E{1}_M{2}_D{3}_Points.tsv".format(method, int(epsilon), mu, delta)
  trajsFile = open(trajsFilename, "w")
  pointsFile = open(pointsFilename, "w")
  for traj in trajs:
    trajsFile.write(traj)
  trajsFile.close()
  for point in sorted(points, key=itemgetter(3)):
    print(point)
    pointsFile.write("{0}\t{1}\t{2}\t{3}\n".format(int(point[0]),point[1],point[2],int(point[3])))  
  pointsFile.close()
  if(args.send):
    command = "scp {0} acald013@bolt:/home/csgrads/acald013/tmp/".format(trajsFilename)
    subprocess.call(command, shell=True)
    command = "scp {0} acald013@bolt:/home/csgrads/acald013/tmp/".format(pointsFilename)
    subprocess.call(command, shell=True)
