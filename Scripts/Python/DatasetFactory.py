#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 25 18:36:13 2018

@author: and
"""
import pandas as pd

i          = 0
path       = "~/Research/Validation/LCM_max/input/"
tag        = "B_T4_E100_M5_D3_t"
extension  = "txt"
input_file = "{}{}{}.{}".format(path, tag, i, extension)
dataset    = pd.read_csv(input_file, header=None, names=['id','items'])
results2   = "/tmp/results_sequential.txt"

D = pd.Series(dataset['items']).drop_duplicates()
data = "{}d{}.txt".format(path, i)
f = open(data, 'w')
[f.write("{}\n".format(d)) for d in D]
f.close()
