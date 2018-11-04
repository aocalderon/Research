import subprocess
import argparse
import os
import logging
import time
import sys

## Reading arguments...
parser = argparse.ArgumentParser()
parser.add_argument("--epsilon", "-e", default=10, help="Epsilon")
parser.add_argument("--mu", "-m", default=3, help="Mu")
parser.add_argument("--delta", "-d", default=3, help="Delta")
parser.add_argument("--speed", "-s", default=100, help="Speed")
parser.add_argument("--cores", "-c", default=28, help="Speed")
parser.add_argument("--path", "-p", default="Datasets/Berlin/", help="PFlock path")
parser.add_argument("--dataset", "-i", default="berlin0-2", help="PFlock dataset")
parser.add_argument("--extension", "-ext", default="tsv", help="PFlock dataset extension")
parser.add_argument("--no_bfe", dest='bfe', action='store_false', help="Run BFE?")
parser.add_argument("--no_pflock", dest='pflock', action='store_false', help="Run PFlock?")
parser.set_defaults(bfe=True, pflock=True)
args = parser.parse_args()
logging.basicConfig(format="%(asctime)s -> %(message)s")

## Setting variables...
research_home = os.environ['RESEARCH_HOME']
epsilon = args.epsilon
mu = args.mu
delta = args.delta
speed = args.speed
cores = args.cores
path = args.path
dataset = args.dataset
extension = args.extension

## Running BFE...
if(args.bfe):
  logging.warning("BFE_START")
  bfe_dataset = "{0}{1}{2}.{3}".format(research_home, path, dataset, extension)
  command = "bfe {0} {1} {2} {3}".format(bfe_dataset, epsilon, mu, delta)
  logging.warning(command)
  timeBFE = time.time()
  subprocess.call(command, shell=True)
  logging.warning("LOG_BFE,{},{},{},{}".format(epsilon, mu, delta, time.time() - timeBFE))
  logging.warning("BFE_END")

## Running PFlock...
if(args.pflock):
  logging.warning("PFLOCK_START")
  pflock = "{0}{1}".format(research_home, "PFlock/target/scala-2.11/pflock_2.11-2.0.jar")
  command = "spark-submit --class FlockFinderMergeLast {0} --epsilon {1} --epsilon_max {1} --mu {2} --mu_max {2} --delta {3} --delta_max {3} --path {4} --dataset {5} --speed {6} --cores {7}".format(pflock, epsilon, mu, delta, path, dataset, speed, cores)
  logging.warning(command)
  timePFLOCK = time.time()
  subprocess.call(command, shell=True)
  logging.warning("LOG_PFLOCK,{},{},{},{}".format(epsilon, mu, delta, time.time() - timePFLOCK))
  logging.warning("PFLOCK_END")
