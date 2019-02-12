import subprocess
import argparse
import os
import logging
from datetime import datetime

## Reading arguments...
parser = argparse.ArgumentParser()
parser.add_argument("--nodes", "-n", default=1, help="Number of nodes...")
args = parser.parse_args()


## Setting variables...
spark_home = os.environ['SPARK_HOME']
nodes = ['acald013@dblab-rack11', 'acald013@dblab-rack12', 'acald013@dblab-rack14', 'acald013@dblab-rack15']

## Setting nodes...
def setNodes(n):
  subprocess.call("{0}/sbin/stop-all.sh".format(spark_home), shell=True)
  slaves = open("{0}/conf/slaves".format(spark_home), "w")
  slaves.write("\n".join(nodes[0:n]))
  slaves.write("\n")
  slaves.close()
  subprocess.call("{0}/sbin/start-all.sh".format(spark_home), shell=True)

n = int(args.nodes)
logging.warning("Setting {0} nodes...".format(n))
setNodes(n)
logging.warning("{0} nodes has been set...".format(n))
