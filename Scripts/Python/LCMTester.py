import os
import argparse
from subprocess import call
import logging

## Reading arguments...
parser = argparse.ArgumentParser()
parser.add_argument("--input_file", "-i",  required=True,      help="Input file...")
parser.add_argument("--master",     "-m",  default="local[*]", help="Master...")
parser.add_argument("--p_start",    "-ps", default=1,          help="Partitions...")
parser.add_argument("--p_end",      "-pe", default=1,          help="Partitions...")
parser.add_argument("--c_start",    "-cs", default=1,          help="Cores...")
parser.add_argument("--c_end",      "-ce", default=1,          help="Cores...")
parser.add_argument("--debug",      "-d",  default=False,      help="Activate debug mode.")
args = parser.parse_args()
logging.basicConfig(format="%(asctime)s -> %(message)s")

logging.info("Starting...")
input_file    = args.input_file
master        = args.master
p_start       = int(args.p_start)
c_start       = int(args.c_start)
p_end         = int(args.p_end)
c_end         = int(args.c_end)
debug         = args.debug
jar_file      = "/home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar"
jar_class     = "--class SPMF.ScalaLCM.Tester"

for partitions in range(p_start, p_end):
    for cores in range(c_start, c_end):
        command = "{} {} {} --input {} --master {} --partitions {} --cores {}".format('spark-submit', jar_class, jar_file, input_file, master, partitions, cores)
        if(debug):
            print(command)
        call([command], shell=True)
logging.info("It is done!")
