import os
import argparse
from subprocess import call
import logging
from LCMTesterLib import runLCMuno

## Reading arguments...
parser = argparse.ArgumentParser()
parser.add_argument("--input_file", "-i",  required=True,      help="Input file...")
parser.add_argument("--master",     "-m",  default="local[*]", help="Master...")
parser.add_argument("--p_start",    "-ps", default=1,          help="Partitions start...")
parser.add_argument("--p_step",     "-pt", default=1,          help="Partitions step...")
parser.add_argument("--p_end",      "-pe", default=2,          help="Partitions end...")
parser.add_argument("--c_start",    "-cs", default=1,          help="Cores start...")
parser.add_argument("--c_step",     "-ct", default=1,          help="Cores step...")
parser.add_argument("--c_end",      "-ce", default=2,          help="Cores end...")
parser.add_argument("--debug",      "-d",  default=False,      help="Activate debug mode.")
args = parser.parse_args()
logging.basicConfig(format="%(asctime)s -> %(message)s")

logging.info("Starting...")
input_file    = args.input_file
master        = args.master
p_start       = int(args.p_start)
c_start       = int(args.c_start)
p_step        = int(args.p_step)
c_step        = int(args.c_step)
p_end         = int(args.p_end)
c_end         = int(args.c_end)
jar_file      = "{}/PFlock/target/scala-2.11/pflock_2.11-2.0.jar".format(os.environ['RESEARCH_HOME'])
jar_class     = "--class SPMF.ScalaLCM.Tester"
if args.debug != False:
    debug = True
else:
    debug = False

for pid in range(1,10,1):
    for partitions in range(p_start, p_end, p_step):
        for cores in range(c_start, c_end, c_step):
            command = "{} {} {} --input {} --master {} --partitions {} --cores {} --pid {}".format('spark-submit', jar_class, jar_file, input_file, master, partitions, cores, pid)
            if(debug):
                print(command)
            call([command], shell=True)
            call(['sort', '/tmp/JavaLCMmax.txt', '-o' , '/tmp/JavaLCMmax_sorted.txt'])
            call(['sort', '/tmp/ScalaLCMmax.txt', '-o' , '/tmp/ScalaLCMmax_sorted.txt'])
            
            data_in = "/tmp/Partitions_{}_{}.txt".format(partitions, cores)
            n = runLCMuno(data_in, debug)
            print("Finding maximal patterns LCM Uno... {} patterns".format(n))
            
            call(['diff', '-s', 'JavaLCMmax_sorted.txt', 'results_sequential_sorted.txt'])
            call(['diff', '-s', 'ScalaLCMmax_sorted.txt', 'results_sequential_sorted.txt'])
       
logging.info("It is done!")
