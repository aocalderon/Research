import subprocess
import argparse
import logging

## Reading arguments...
parser = argparse.ArgumentParser()
parser.add_argument("--min_epsilon", "-e1", default=100.0, help="Minimum epsilon...")
parser.add_argument("--max_epsilon", "-e2", default=100.0, help="Maximum epsilon...")
parser.add_argument("--step_epsilon", "-se", default=5.0, help="Step in epsilon...")
parser.add_argument("--min_mu", "-m1", default=10, help="Minimum mu...")
parser.add_argument("--max_mu", "-m2", default=10, help="Maximum mu...")
parser.add_argument("--step_mu", "-sm", default=1, help="Step in mu...")
parser.add_argument("--min_delta", "-d1", default=7, help="Minimum delta...")
parser.add_argument("--max_delta", "-d2", default=7, help="Maximum delta...")
parser.add_argument("--step_delta", "-sd", default=1, help="Step in delta...")
parser.add_argument("--path", "-p", default="Datasets/Berlin/", help="URL path...")
parser.add_argument("--dataset", "-i", default="berlin0-10", help="Point dataset...")
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
runner_script = "python3 runBFEandPFlock.py"

for delta in range(min_delta, max_delta + 1, step_delta):
  for epsilon in range(min_epsilon, max_epsilon + 1, step_epsilon):
    for mu in range(min_mu, max_mu + 1, step_mu):
      logging.warning("\n\nIteration Epsilon={0}, Mu={1} and Delta={2} has started...".format(epsilon, mu, delta))
      command = "{0} -p {1} -i {2} -e {3} -m {4} -d {5}".format(runner_script
      , args.path
      , args.dataset
      , epsilon
      , mu
      , delta)
      subprocess.call(command, shell=True)
      logging.warning("\n\nIteration Epsilon={0}, Mu={1} and Delta={2} has ended...".format(epsilon, mu, delta))
print("\n\nBFEandPFlockRunner.py has finished.")
print("\nRun 'grep \" vs \" nohup.out' to see the results...")

