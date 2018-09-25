#!/home/acald013/opt/miniconda3/bin/python

import os
from subprocess import call
import logging

logging.basicConfig(format="%(asctime)s -> LOG %(message)s")
research_home  = os.environ['RESEARCH_HOME']
input_path     = "Validation/LCM_max/input"
input_file     = "Datasets_berlin0-10_110.0_5_6_10.txt" 
input_url      = "{}/{}/{}".format(research_home, input_path, input_file)
lcm_scala      = "Scripts/Scala/LCM/target/scala-2.11/lcm_2.11-0.1.jar"
lcm_path       = "{}/{}".format(research_home, lcm_scala)

logging.warning("Extracting datasets...")
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
        print("Saving {}...".format(test_name))
        test_in = open(test_name,'w')
        tests.append(test_name)
        old_id = cur_id
    test_in.write(cur_line + '\n')
test_in.close()
logging.warning("Done!!!")

tests = tests[25:75]
for test in tests:
    logging.warning("Running LCMuno...")
    fid = test.split("/")[-1].split("_")[-1].split(".")[0]
    output_path    = "Validation/LCM_max/output"
    output_file    = "LCMuno_{}_unsorted.txt".format(fid)
    output_url     = "{}/{}/{}".format(research_home, output_path, output_file)
    call(['time','lcm', '_M', test, '1', output_url])
    logging.warning("Done!!!")
    
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
    
    logging.warning("Running LCMand...")
    output_path    = "Validation/LCM_max/output"
    output_file    = "LCMand_{}.txt".format(fid)
    lcmand_url     = "{}/{}/{}".format(research_home, output_path, output_file)
    call(['time', 'scala', lcm_path, test, lcmand_url])
    logging.warning("Done!!!")
    
    logging.warning("Sorting files...")
    lcmand_sorted  = "{}/{}/{}".format(research_home, output_path, "LCM_and_sort_{}.txt".format(fid))
    lcmuno_sorted  = "{}/{}/{}".format(research_home, output_path, "LCM_uno_sort_{}.txt".format(fid))
    call(['sort', lcmuno_url, '-o', lcmuno_sorted])
    call(['sort', lcmand_url, '-o', lcmand_sorted])
    logging.warning("Done!!!")

    logging.warning("Applying Diff...")
    call(['diff', '-s', lcmuno_sorted, lcmand_sorted])
    logging.warning("Done!!!")
    
