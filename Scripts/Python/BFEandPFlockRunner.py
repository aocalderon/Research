#!/home/acald013/opt/miniconda3/bin/python

import subprocess
import argparse
import logging

## Reading arguments...
parser = argparse.ArgumentParser()
parser.add_argument("--min_epsilon", "-e1", default=30.0, help="Minimum epsilon...")
parser.add_argument("--max_epsilon", "-e2", default=50.0, help="Maximum epsilon...")
parser.add_argument("--step_epsilon", "-se", default=10.0, help="Step in epsilon...")
parser.add_argument("--min_mu", "-m1", default=3, help="Minimum mu...")
parser.add_argument("--max_mu", "-m2", default=5, help="Maximum mu...")
parser.add_argument("--step_mu", "-sm", default=1, help="Step in mu...")
parser.add_argument("--min_delta", "-d1", default=3, help="Minimum delta...")
parser.add_argument("--max_delta", "-d2", default=5, help="Maximum delta...")
parser.add_argument("--step_delta", "-sd", default=1, help="Step in delta...")
parser.add_argument("--bfe_dataset", "-f", default="/home/and/Documents/PhD/Research/Datasets/Berlin/berlin0-10.tsv", help="BFE dataset")
parser.add_argument("--pflock_path", "-k", default="Datasets/Berlin/", help="PFlock path")
parser.add_argument("--pflock_dataset", "-g", default="berlin0-10", help="PFlock dataset")
args = parser.parse_args()

## Setting variables...
logging.basicConfig(format="%(asctime)s -> %(message)s")
min_epsilon = int(args.min_epsilon)
max_epsilon = int(args.max_epsilon)
step_epsilon = int(args.step_epsilon)
min_mu = int(args.min_mu)
max_mu = int(args.max_mu)
step_mu = int(args.step_mu)
min_delta = int(args.min_delta)
max_delta = int(args.max_delta)
step_delta = int(args.step_delta)
runner_script = "~/Research/Scripts/Python/runBFEandPFlock.py"

for delta in range(min_delta, max_delta + 1, step_delta):
  for epsilon in range(min_epsilon, max_epsilon + 1, step_epsilon):
    for mu in range(min_mu, max_mu + 1, step_mu):
      logging.warning("Iteration Epsilon={0}, Mu={1} and Delta={2} has started...".format(epsilon, mu, delta))
      command = "{0} -f {1} -g {2} -k {3} -e {4} -m {5} -d {6}".format(runner_script
      , args.bfe_dataset
      , args.pflock_dataset
      , args.pflock_path
      , epsilon
      , mu
      , delta)
      #print(command)
      subprocess.call(command, shell=True)
      logging.warning("Iteration Epsilon={0}, Mu={1} and Delta={2} has ended...".format(epsilon, mu, delta))
      logging.warning("***")
      logging.warning("***")
      logging.warning("***")
print("DONE!!!")

