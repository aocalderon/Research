#!/home/acald013/opt/miniconda3/bin/python

import os
from subprocess import call

research_home  = os.environ['RESEARCH_HOME']
input_path     = "Validation/LCM_max/input"
input_file     = "Datasets_berlin0-10_110.0_5_6_0.txt" 
input_url      = "{}/{}/{}".format(research_home, input_path, input_file)

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
    test_in.write(cur_line)
test_in.close()

for test in tests:
    output_path    = "Validation/LCM_max/Output"
    output_file    = "LCMuno_{}.txt".format(cur_id)
    output_url     = "{}/{}/{}".format(research_home, output_path, output_file)
    call(['lcm', 'M', test, '1', output_url])
