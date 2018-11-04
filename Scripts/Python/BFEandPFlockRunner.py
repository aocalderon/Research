import subprocess
import argparse
import logging

## Reading arguments...
parser = argparse.ArgumentParser()
parser.add_argument("--min_epsilon", "-e1", default=60.0, help="Minimum epsilon...")
parser.add_argument("--max_epsilon", "-e2", default=100.0, help="Maximum epsilon...")
parser.add_argument("--step_epsilon", "-se", default=5.0, help="Step in epsilon...")
parser.add_argument("--min_mu", "-m1", default=5, help="Minimum mu...")
parser.add_argument("--max_mu", "-m2", default=5, help="Maximum mu...")
parser.add_argument("--step_mu", "-sm", default=1, help="Step in mu...")
parser.add_argument("--min_delta", "-d1", default=5, help="Minimum delta...")
parser.add_argument("--max_delta", "-d2", default=5, help="Maximum delta...")
parser.add_argument("--step_delta", "-sd", default=1, help="Step in delta...")
parser.add_argument("--path", "-p", default="Datasets/Berlin/", help="URL path...")
parser.add_argument("--dataset", "-i", default="berlin0-10", help="Point dataset...")
parser.add_argument("--cores", "-c", default=28, help="Number of cores...")
parser.add_argument("--no_bfe", dest='bfe', action='store_false', help="Run BFE?")
parser.add_argument("--no_pflock", dest='pflock', action='store_false', help="Run PFlock?")
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
cores = int(args.cores)
no_bfe = ""
if(args.bfe):
  no_bfe = "--no_bfe" 
no_pflock = ""
if(args.pflock):
  no_pflock = "--no_pflock" 
runner_script = "python3 -u runBFEandPFlock.py"

logging.warning("BFEandPFlockRunner.py has started...")
for delta in range(min_delta, max_delta + 1, step_delta):
  for epsilon in range(min_epsilon, max_epsilon + 1, step_epsilon):
    for mu in range(min_mu, max_mu + 1, step_mu):
      logging.warning("Iteration Epsilon={0}, Mu={1} and Delta={2} has started...".format(epsilon, mu, delta))
      command = "{0} -p {1} -i {2} -e {3} -m {4} -d {5} -c {6}".format(runner_script, args.path, args.dataset, epsilon, mu, delta, cores)
      print(command)
      subprocess.call(command, shell=True)
      logging.warning("Iteration Epsilon={0}, Mu={1} and Delta={2} has ended...".format(epsilon, mu, delta))
logging.warning("BFEandPFlockRunner.py has finished.")
logging.warning("Run 'grep \" vs \" nohup.out' to see the results...")

