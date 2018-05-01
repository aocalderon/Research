import subprocess
import argparse
import os
import logging
from datetime import datetime

## Reading arguments...
parser = argparse.ArgumentParser()
parser.add_argument("--min_epsilon",  "-e1",    default=40,                 help="Minimum epsilon...")
parser.add_argument("--max_epsilon",  "-e2",    default=50,                 help="Maximum epsilon...")
parser.add_argument("--step_epsilon", "-se",  default=5,                  help="Step in epsilon...")
parser.add_argument("--min_mu",       "-m1",    default=5,                  help="Minimum mu...")
parser.add_argument("--max_mu",       "-m2",    default=5,                  help="Maximum mu...")
parser.add_argument("--step_mu",      "-sm",  default=1,                  help="Step in mu...")
parser.add_argument("--min_delta",    "-d1",    default=5,                  help="Minimum delta...")
parser.add_argument("--max_delta",    "-d2",    default=5,                  help="Maximum delta...")
parser.add_argument("--step_delta",   "-sd",  default=1,                  help="Step in delta...")
parser.add_argument("--path",         "-p",   default="Datasets/Berlin/", help="URL path...")
parser.add_argument("--dataset",      "-i",   default="berlin0-10",       help="Point dataset...")
parser.add_argument("--speed",        "-s",   default=10,                 help="PFlock speed between timestamps...")
parser.add_argument("--partitions",   "-n",   default=1024,               help="PFlock number of partitions...")
parser.add_argument("--iterations",   "-x",   default=1,                  help="Number of iterations...")
args = parser.parse_args()


## Setting variables...
logging.basicConfig(format="%(asctime)s -> %(message)s")
spark_home = os.environ['SPARK_HOME']
x = int(args.iterations)
nodes = ['acald013@dblab-rack12', 'acald013@dblab-rack14', 'acald013@dblab-rack11', 'acald013@dblab-rack15']

## Setting nodes...
def setNodes(n):
  subprocess.call("{0}/sbin/stop-all.sh".format(spark_home), shell=True)
  slaves = open("{0}/conf/slaves".format(spark_home), "w")
  slaves.write("\n".join(nodes[0:n]))
  slaves.close()
  subprocess.call("{0}/sbin/start-all.sh".format(spark_home), shell=True)

## Running FlockFinderBenchmark...
def runFlockFinder(cores):
  for i in range(0, x):
    logging.warning("Iteration {0} has started...".format(i + 1))
    command = "python3 FlockFinderBenchmarker.py -e1 {0} -e2 {1} -se {2} -m1 {3} -m2 {4} -sm {5} -d1 {6} -d2 {7} -sd {8} -p {9} -i {10} -s {11} -n {12} -c {13}".format(
      args.min_epsilon, args.max_epsilon, args.step_epsilon,
      args.min_mu,      args.max_mu,      args.step_mu,
      args.min_delta,   args.max_delta,   args.step_delta,
      args.path, args.dataset, args.speed, args.partitions,
      cores
    )
    subprocess.call(command, shell=True)
    logging.warning("Iteration {0} has ended...\n\n".format(i + 1))

CORES_PER_NODE = 7

for NODES in [2]:
  logging.warning("Setting {0} nodes...\n\n".format(NODES))
  setNodes(NODES)
  logging.warning("{0} nodes has been set...\n\n".format(NODES))
  logging.warning("Running FlockFinder Benchmark...\n\n")
  runFlockFinder(NODES * CORES_PER_NODE)
  logging.warning("FlockFinder Benchmark has been run...\n\n")
  
now = datetime.now().isoformat("_","seconds")
command = "mv nohup.out OutputFlockFinder_E{0}-{1}_M{2}-{3}_D{4}-{5}_{6}.out".format(
args.min_epsilon,args.max_epsilon,args.min_mu,args.max_mu,args.min_delta,args.max_delta,now)
#subprocess.call(command, shell=True)
subprocess.call("{0}/sbin/stop-all.sh".format(spark_home), shell=True)
logging.warning("*** Everything is done!!! ***")
  
