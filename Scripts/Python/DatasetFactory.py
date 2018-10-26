#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 25 18:36:13 2018

@author: and
"""
import pandas as pd

i = 9
path = "/home/and/Documents/PhD/Research/Validation/LCM_max/input/"
input_file = "{}Datasets_berlin0-10_110.0_5_6_{}.txt".format(path, i)
dataset = pd.read_csv(input_file, header=None, names=['id','items'])
results2 = "/tmp/results_sequential.txt"
D = pd.Series(dataset['items']).drop_duplicates()
data = "{}d{}.txt".format(path, i)
f = open(data, 'w')
[f.write("{}\n".format(d)) for d in D]
f.close()