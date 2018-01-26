#!/home/acald013/opt/miniconda3/bin/python

import subprocess
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--epsilon", "-e", help="Epsilon")
parser.add_argument("--mu", "-m", help="Mu")
parser.add_argument("--delta", "-d", help="Delta")
parser.add_argument("--bfe_dataset", "-f", help="BFE dataset")
parser.add_argument("--pflock_partitions", "-p", default= 16, help="PFlock number of partitions")
parser.add_argument("--pflock_jar", "-j", default= "/home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar", help="PFlock jar file")
parser.add_argument("--pflock_path", "-k", help="PFlock path")
parser.add_argument("--pflock_dataset", "-g", help="PFlock dataset")
args = parser.parse_args()

epsilon = args.epsilon
mu = args.mu
delta = args.delta

bfe_dataset = args.bfe_dataset
pflock_path = args.pflock_path
pflock_dataset = args.pflock_dataset

command = "bfe {0} {1} {2} {3}".format(bfe_dataset, epsilon, mu, delta)
subprocess.call(command, shell=True)

pflock_jar = args.pflock_jar
pflock_partitions = args.pflock_partitions
pflock_path = args.pflock_path
pflock_dataset = args.pflock_dataset

command = "spark-submit {0} --partitions {1} --epsilon {2} --mu {3} --delta {4} --path {5} --dataset {6}".format(pflock_jar
    , pflock_partitions
    , epsilon
    , mu
    , delta
    , pflock_path
    , pflock_dataset)
#print(command)
subprocess.call(command, shell=True)
