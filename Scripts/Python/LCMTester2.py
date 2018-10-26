import os
import argparse
from subprocess import call
import logging
import random
from LCMTesterLib import runLCMuno

## Reading arguments...
parser = argparse.ArgumentParser()
parser.add_argument("--input_file", "-i",  required=True,      help="Input file...")
parser.add_argument("--master",     "-m",  default="local[*]", help="Master...")
parser.add_argument("--p_start",    "-ps", default=1,          help="PID start...")
parser.add_argument("--p_end",      "-pe", default=1,          help="PID end...")
parser.add_argument("--debug",      "-d",  default=False,      help="Activate debug mode.")
args = parser.parse_args()
logging.basicConfig(format="%(asctime)s -> %(message)s")

logging.info("Starting...")
input_file    = args.input_file
master        = args.master
p_start       = int(args.p_start)
p_end         = int(args.p_end)
jar_file      = "{}/PFlock/target/scala-2.11/pflock_2.11-2.0.jar".format(os.environ['RESEARCH_HOME'])
jar_class     = "--class SPMF.ScalaLCM.Tester"
if args.debug != False:
    debug = True
else:
    debug = False

partitions = [32, 64, 128, 256]
cores = [2, 3, 4]

for pid in range(p_start, p_end):
    partition = partitions[random.randint(0, len(partitions) - 1)]
    core = cores[random.randint(0, len(cores) - 1)]
    command = "{} {} {} --input {} --master {} --partitions {} --cores {} --pid {}".format('spark-submit', jar_class, jar_file, input_file, master, partition, core, pid)
    if(debug):
        print(command)
    call([command], shell=True)
    call(['sort', '/tmp/JavaLCMmax.txt', '-o' , '/tmp/JavaLCMmax_sorted.txt'])
    call(['sort', '/tmp/ScalaLCMmax.txt', '-o' , '/tmp/ScalaLCMmax_sorted.txt'])
    
    data_in = "/tmp/Partitions_{}_{}.txt".format(partition, core)
    n = runLCMuno(data_in, debug)
    print("Finding maximal patterns LCM Uno... {} patterns".format(n))
    
    call(['diff', '-s', 'JavaLCMmax_sorted.txt', 'results_sequential_sorted.txt'])
    call(['diff', '-s', 'ScalaLCMmax_sorted.txt', 'results_sequential_sorted.txt'])
       
logging.info("It is done!")
