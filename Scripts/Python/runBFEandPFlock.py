#!/home/acald013/opt/miniconda3/bin/python

import subprocess
import argparse
import os

## Reading arguments...
parser = argparse.ArgumentParser()
parser.add_argument("--epsilon", "-e", help="Epsilon")
parser.add_argument("--mu", "-m", help="Mu")
parser.add_argument("--delta", "-d", help="Delta")
parser.add_argument("--bfe_dataset", "-f", help="BFE dataset")
parser.add_argument("--pflock_partitions", "-p", default= 16, help="PFlock number of partitions")
parser.add_argument("--pflock_jar", "-j", default= "/home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar", help="PFlock jar file")
parser.add_argument("--pflock_path", "-k", help="PFlock path")
parser.add_argument("--pflock_dataset", "-g", help="PFlock dataset")
parser.add_argument("--pflock_extension", "-ext", default="tsv", help="PFlock dataset extension")
args = parser.parse_args()

## Setting variables...
research_home = os.environ['RESEARCH_HOME']
epsilon = args.epsilon
mu = args.mu
delta = args.delta

bfe_dataset = args.bfe_dataset
pflock_path = args.pflock_path
pflock_dataset = args.pflock_dataset
pflock_extension = args.pflock_extension

## Running BFE...
command = "bfe {0} {1} {2} {3}".format(bfe_dataset, epsilon, mu, delta)
subprocess.call(command, shell=True)

pflock_jar = args.pflock_jar
pflock_partitions = args.pflock_partitions
pflock_path = args.pflock_path
pflock_dataset = args.pflock_dataset

## Running PFlock...
command = "spark-submit {0} --partitions {1} --epsilon {2} --mu {3} --delta {4} --path {5} --dataset {6}".format(pflock_jar
    , pflock_partitions
    , epsilon
    , mu
    , delta
    , pflock_path
    , pflock_dataset)
subprocess.call(command, shell=True)

## Sorting and comparing outputs...
bfe_output = "/tmp/BFE_E{0}_M{1}_D{2}.txt".format(epsilon, mu, delta)
pflock_output = "/tmp/PFLOCK_E{0}_M{1}_D{2}.txt".format(epsilon, mu, delta)

bfe = open(bfe_output, "r") 
bfeline = bfe.readline()
pflock = open(pflock_output, "r") 
pflockline = pflock.readline()
pointset = "{0}{1}{2}.{3}".format(research_home, pflock_path, pflock_dataset, pflock_extension)

flock_checker = "/home/acald013/Research/Scripts/Scala/target/scala-2.11/checker_2.11-0.1.jar"

command = "spark-submit {0} {1} {2} {3} {4} {5}".format(flock_checker, pflock_output, bfe_output, epsilon, mu, delta)
subprocess.call(command, shell=True)
command = "~/Research/Scripts/Python/DiskVisualizer.py -e {0} -m {1} -d {2} -p {3} -t {4}".format(epsilon, mu, delta, pointset, "PFlock")
subprocess.call(command, shell=True)
command = "~/Research/Scripts/Python/NotFoundFlockSaver.py -e {0} -m {1} -d {2} -p {3} -t {4}".format(epsilon, mu, delta, pointset, "PFlock")
subprocess.call(command, shell=True)

command = "spark-submit {0} {1} {2} {3} {4} {5}".format(flock_checker, bfe_output, pflock_output, epsilon, mu, delta)
subprocess.call(command, shell=True)
command = "~/Research/Scripts/Python/DiskVisualizer.py -e {0} -m {1} -d {2} -p {3} -t {4}".format(epsilon, mu, delta, pointset, "BFE")
subprocess.call(command, shell=True)
command = "~/Research/Scripts/Python/NotFoundFlockSaver.py -e {0} -m {1} -d {2} -p {3}".format(epsilon, mu, delta, pointset, "BFE")
subprocess.call(command, shell=True)
