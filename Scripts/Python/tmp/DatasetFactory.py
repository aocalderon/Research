#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 25 18:36:13 2018

@author: and
"""
import pandas as pd

i             = 4
RESEARCH_HOME = "/home/acald013/"
RESEARCH_HOME = "/home/and/Documents/PhD/"
path          = "{}/Research/Validation/LCM_max/input/".format(RESEARCH_HOME)
input_tag     = "B_T4_E100_M5_D3_t"
output_tag    = "a"
extension     = "txt"
input_file    = "{}{}{}.{}".format(path, input_tag, i, extension)
dataset       = pd.read_csv(input_file, header=None, names=['id','items','x','y'])
results2      = "/tmp/results_sequential.txt"

D = pd.Series(dataset['items','x', 'y'])
data = "{}{}{}.txt".format(path, output_tag, i)
f = open(data, 'w')
[f.write("{}\n".format(d)) for d in D]
f.close()
