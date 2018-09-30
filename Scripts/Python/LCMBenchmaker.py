#!/home/acald013/opt/miniconda3/bin/python

import os
import argparse
from subprocess import call
import logging
from datetime import datetime as d
        
## Reading arguments...
parser = argparse.ArgumentParser()
parser.add_argument("--input_file", "-i", required=True, help="Input file...")
parser.add_argument("--debug", "-d", default=False, help="Activate debug mode.")
args = parser.parse_args()
logging.basicConfig(format="%(asctime)s -> %(message)s")

def log(run_id, method, start):
    time = d.now() - start
    secs = time.total_seconds()
    logging.warning("{},{},{}".format(run_id, method, secs))
    return secs

research_home   = os.environ['RESEARCH_HOME']
input_path      = "Validation/LCM_max/input"
input_file      = args.input_file
input_url       = "{}/{}/{}".format(research_home, input_path, input_file)
jar_file        = "PFlock/target/scala-2.11/pflock_2.11-2.0.jar"
jar_path        = "{}/{}".format(research_home, jar_file)
lcm_java_class  = "--class SPMF.LCM"
fpmax_class     = "--class SPMF.FPMax"
lcm_scala_class = "--class SPMF.ScalaLCM.LCMmax"
debug           = args.debug

logging.warning("Reading {}...".format(input_file))
tests = []
old_id = -1
test_in = open("/tmp/test", 'w')
f = open(input_url, 'r')
for line in f.readlines():
    cur_id, cur_line = line.split(",")
    cur_id = cur_id.strip()
    cur_line = cur_line.strip()
    if old_id != cur_id:
        test_in.close()
        test_name = "{}/{}/LCMinput_{}.txt".format(research_home, input_path, cur_id)
        test_in = open(test_name,'w')
        tests.append(test_name)
        old_id = cur_id
    test_in.write(cur_line + '\n')
test_in.close()
input_id = input_file.split('_')[-1].split(".")[0]
logging.warning("{} has been read...".format(input_file))

#tests = tests[0:40]
for test in tests:
    test_id = test.split('_')[-1].split(".")[0]
    run_id = "D{}-{}".format(input_id, test_id)
    # Running LCM uno version...
    start = d.now()
    logging.warning("LCMuno start...")
    fid = test.split("/")[-1].split("_")[-1].split(".")[0]
    output_path    = "Validation/LCM_max/output"
    output_file    = "LCMuno_{}_unsorted.txt".format(fid)
    output_url     = "{}/{}/{}".format(research_home, output_path, output_file)
    command        = "{} {} {} {} {}".format('lcm', '_M', test, '1', output_url)
    if(debug):
        print(command)
    call([command], shell=True)
    lcmuno_time = log(run_id, "LCMuno", start)
    
    logging.warning("Sorting LCMuno output...")
    output_unsort  = open(output_url,  'r')
    output_file    = "LCMuno_{}.txt".format(fid)
    lcmuno_url     = "{}/{}/{}".format(research_home, output_path, output_file)
    output_sort    = open(lcmuno_url, 'w')
    for pattern in output_unsort.readlines():
        p = " ".join(map(str, sorted([ int(x) for x in pattern.split(" ")])))
        output_sort.write(p + '\n')
    output_unsort.close()
    output_sort.close()
    logging.warning("Done!!!")
    
    # Running LCM java version...
    start = d.now()
    logging.warning("LCMjava start...")
    output_path    = "Validation/LCM_max/output"
    output_file    = "LCMjava_{}.txt".format(fid)
    lcmjava_out    = "{}/{}/{}".format(research_home, output_path, output_file)
    command        = "{} {} {} {} {}".format('spark-submit', lcm_java_class, jar_path, test, lcmjava_out)
    if(debug):
        print(command)
    call([command], shell=True)
    lcmjava_time = log(run_id, "LCMjava", start)
    
    # Running LCM scala version...
    start = d.now()
    logging.warning("LCMscala start...")
    output_path    = "Validation/LCM_max/output"
    output_file    = "LCMscala_{}.txt".format(fid)
    lcmscala_out   = "{}/{}/{}".format(research_home, output_path, output_file)
    command        = "{} {} {} {} {}".format('spark-submit', lcm_scala_class, jar_path, test, lcmscala_out)
    if(debug):
        print(command)
    call([command], shell=True)
    lcmscala_time = log(run_id, "LCMscala", start)

    logging.warning("Sorting files...")
    lcmscala_sorted  = "{}/{}/{}".format(research_home, output_path, "LCM_scala_sort_{}.txt".format(fid))
    call(['sort', lcmjava_out, '-o', lcmscala_sorted])
    lcmjava_sorted  = "{}/{}/{}".format(research_home, output_path, "LCM_java_sort_{}.txt".format(fid))
    call(['sort', lcmjava_out, '-o', lcmjava_sorted])
    lcmuno_sorted   = "{}/{}/{}".format(research_home, output_path, "LCM_uno_sort_{}.txt".format(fid))
    call(['sort', lcmuno_url, '-o', lcmuno_sorted])
    logging.warning("Done!!!")

    logging.warning("Applying Diff...")
    call(['diff', '-s', lcmuno_sorted, lcmjava_sorted])
    call(['diff', '-s', lcmuno_sorted, lcmscala_sorted])
    logging.warning("Done!!!")
