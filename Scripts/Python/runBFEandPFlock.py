import subprocess
import argparse
import os

## Reading arguments...
parser = argparse.ArgumentParser()
parser.add_argument("--epsilon", "-e", default=10, help="Epsilon")
parser.add_argument("--mu", "-m", default=3, help="Mu")
parser.add_argument("--delta", "-d", default=3, help="Delta")
parser.add_argument("--path", "-p", default="Datasets/Berlin/", help="PFlock path")
parser.add_argument("--dataset", "-i", default="berlin0-2", help="PFlock dataset")
parser.add_argument("--extension", "-ext", default="tsv", help="PFlock dataset extension")
parser.add_argument("--no_bfe", dest='bfe', action='store_false', help="Run BFE?")
parser.add_argument("--no_pflock", dest='pflock', action='store_false', help="Run PFlock?")
parser.set_defaults(bfe=True, pflock=True)
args = parser.parse_args()

## Setting variables...
research_home = os.environ['RESEARCH_HOME']
epsilon = args.epsilon
mu = args.mu
delta = args.delta

path = args.path
dataset = args.dataset
extension = args.extension
bfe_dataset = "{0}{1}{2}.{3}".format(research_home, path, dataset, extension)
## Running BFE...
command = "bfe {0} {1} {2} {3}".format(bfe_dataset, epsilon, mu, delta)
print(command)
if(args.bfe):
  subprocess.call(command, shell=True)

pflock = "{0}{1}".format(research_home, "PFlock/target/scala-2.11/pflock_2.11-2.0.jar")
## Running PFlock...
command = "spark-submit --class FlockFinderMergeLast {0} --epsilon {1} --epsilon_max {1} --mu {2} --mu_max {2} --delta {3} --delta_max {3} --path {4} --dataset {5} --debug".format(pflock, epsilon, mu, delta, path, dataset)
print(command)
if(args.pflock):
  subprocess.call(command, shell=True)

## Sorting and comparing outputs...
bfe_output = "/tmp/BFE_E{0}_M{1}_D{2}.txt".format(epsilon, mu, delta)
pflock_output = "/tmp/PFLOCK_E{0}_M{1}_D{2}.txt".format(epsilon, mu, delta)
flock_checker = "{0}Scripts/Scala/FlockChecker/target/scala-2.11/flockchecker_2.11-0.1.jar".format(research_home)

command = "spark-submit {0} {1} {2}".format(flock_checker, pflock_output, bfe_output)
subprocess.call(command, shell=True)
