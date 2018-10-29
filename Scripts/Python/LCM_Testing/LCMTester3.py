import os
import argparse
from subprocess import call
import logging
import random
from LCMTesterLib import runLCMuno2

## Reading arguments...
parser = argparse.ArgumentParser()
parser.add_argument("--path_file",   "-i",  required=True,      help="Input file...")
parser.add_argument("--prefix_file", "-p",  default="a",        help="Prefix file...")
parser.add_argument("--ext_file",    "-e",  default="txt",      help="Extension file...")
parser.add_argument("--master",      "-m",  default="local[*]", help="Master...")
parser.add_argument("--d_start",     "-ds", default=0,          help="Dataset ID start...")
parser.add_argument("--d_end",       "-de", default=1,          help="Dataset ID end...")
parser.add_argument("--debug",       "-d",  default=False,      help="Activate debug mode.")
args = parser.parse_args()
logging.basicConfig(format="%(asctime)s -> %(message)s")

logging.info("Starting...")
path_file   = args.path_file
prefix_file = args.prefix_file
extension   = args.ext_file
master      = args.master
d_start     = int(args.d_start)
d_end       = int(args.d_end)
jar_file    = "{}/PFlock/target/scala-2.11/pflock_2.11-2.0.jar".format(os.environ['RESEARCH_HOME'])
jar_class   = "--class SPMF.ScalaLCM.LCMClusterTester"
if args.debug != False:
    debug = True
else:
    debug = False

partitions = [900, 1000, 1100, 1200]
cores = [32]

for did in range(d_start, d_end):
    input_file = "{}{}{}.{}".format(path_file, prefix_file, did, extension)
    partition = partitions[random.randint(0, len(partitions) - 1)]
    core = cores[random.randint(0, len(cores) - 1)]
    command = "{} {} {} --input {} --master {} --partitions {} --cores {}".format('spark-submit', jar_class, jar_file, input_file, master, partition, core)
    if(debug):
        print(command)
    call([command], shell=True)
    call(['sort', '/tmp/JavaLCMmax.txt', '-o' , '/tmp/JavaLCMmax_sorted.txt'])
    call(['sort', '/tmp/ScalaLCMmax.txt', '-o' , '/tmp/ScalaLCMmax_sorted.txt'])

    n = runLCMuno2(input_file, debug)
    print("Finding maximal patterns LCM Uno... {} patterns".format(n))
    
    call(['diff', '-s', '/tmp/JavaLCMmax_sorted.txt', '/tmp/sequential_sorted.txt'])
    call(['diff', '-s', '/tmp/ScalaLCMmax_sorted.txt', '/tmp/sequential_sorted.txt'])
       
logging.info("It is done!")
