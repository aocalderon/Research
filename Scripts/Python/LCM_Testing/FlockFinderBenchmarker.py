import subprocess
import argparse
import logging
import os

## Reading arguments...
parser = argparse.ArgumentParser()
parser.add_argument("--min_epsilon",  "-e1",  default=10,                 help="Minimum epsilon...")
parser.add_argument("--max_epsilon",  "-e2",  default=40,                 help="Maximum epsilon...")
parser.add_argument("--step_epsilon", "-se",  default=5,                  help="Step in epsilon...")
parser.add_argument("--min_mu",       "-m1",  default=3,                  help="Minimum mu...")
parser.add_argument("--max_mu",       "-m2",  default=4,                  help="Maximum mu...")
parser.add_argument("--step_mu",      "-sm",  default=1,                  help="Step in mu...")
parser.add_argument("--min_delta",    "-d1",  default=4,                  help="Minimum delta...")
parser.add_argument("--max_delta",    "-d2",  default=5,                  help="Maximum delta...")
parser.add_argument("--step_delta",   "-sd",  default=1,                  help="Step in delta...")
parser.add_argument("--path",         "-p",   default="Datasets/Berlin/", help="URL path...")
parser.add_argument("--dataset",      "-i",   default="berlin0-10",       help="Point dataset...")
parser.add_argument("--speed",        "-s",   default=10,                 help="PFlock speed between timestamps...")
parser.add_argument("--partitions",   "-n",   default=1024,               help="PFlock number of partitions...")
parser.add_argument("--cores",        "-c",   default=8,                  help="PFlock number of cores...")
args = parser.parse_args()

## Setting variables...
logging.basicConfig(format="%(asctime)s -> %(message)s")
research_home = os.environ['RESEARCH_HOME']
pflock_jar    = "{0}{1}".format(research_home, "PFlock/target/scala-2.11/pflock_2.11-2.0.jar")

min_epsilon   = int(args.min_epsilon)
max_epsilon   = int(args.max_epsilon)
step_epsilon  = int(args.step_epsilon)
min_mu        = int(args.min_mu)
max_mu        = int(args.max_mu)
step_mu       = int(args.step_mu)
min_delta     = int(args.min_delta)
max_delta     = int(args.max_delta)
step_delta    = int(args.step_delta)

command = "spark-submit --class FlockFinderBenchmark {0} --path {1} --dataset {2} --epsilon {3} --epsilon_max {4} --epsilon_step {5} --mu {6} --mu_max {7} --mu_step {8} --delta {9} --delta_max {10} --delta_step {11} --cores {12}".format(
pflock_jar, args.path, args.dataset, min_epsilon, max_epsilon, step_epsilon, min_mu, max_mu, step_mu, min_delta, max_delta, step_delta, args.cores)
logging.warning(command)
subprocess.call(command, shell=True)
