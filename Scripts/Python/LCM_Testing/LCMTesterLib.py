#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  9 20:12:00 2018

@author: and
"""
import pandas as pd
from subprocess import check_output, call

def sortOutput(input_path):
    output_unsort  = open(input_path,  'r')
    output_temp = "/tmp/output.txt"
    output_sort = open(output_temp, 'w')
    for pattern in output_unsort.readlines():
        p = " ".join(map(str, sorted([ int(x) for x in pattern.split(" ")])))
        output_sort.write(p + '\n')
    output_unsort.close()
    output_sort.close()
    name, ext = input_path.split(".")
    output_path = "{}_sorted.{}".format(name, ext)
    call(['sort', output_temp, '-o', output_path])
    return(output_path)
    
def runLCMuno2(input_file, debug):
    dataset = pd.read_csv(input_file, header=None, names=['id','items', 'x', 'y'])
    D = pd.Series(dataset['items'])
    data = "/tmp/d.txt"
    f = open(data, 'w')
    [f.write("{}\n".format(d)) for d in D]
    f.close()
    results = "/tmp/sequential.txt"
    command = "{} {} {} {} {}".format('lcm', '_M', data, '1', results)
    if debug:
        print(command)
    call(['lcm', '_M', data, '1', results])
    
    r2 = sortOutput(results)    
    n = check_output(['wc', r2]).decode('ascii').split(" ")[2]
    return(n)    
    
def runLCMuno(input_file, debug):
    dataset = pd.read_csv(input_file, header=None, names=['id','items'])
    results2 = "/tmp/results_sequential.txt"
    D = pd.Series(dataset['items'])
    data = "/tmp/d.txt"
    f = open(data, 'w')
    [f.write("{}\n".format(d)) for d in D]
    f.close()
    command = "{} {} {} {} {}".format('lcm', '_M', data, '1', results2)
    if debug:
        print(command)
    call(['lcm', '_M', data, '1', results2])
    
    r2 = sortOutput(results2)    
    n = check_output(['wc', r2]).decode('ascii').split(" ")[2]
    return(n)

def runLCMunoTest(input_file, debug):
    dataset = pd.read_csv(input_file, header=None, names=['id','items'])
    output = []
    n = pd.Series(dataset['id']).unique()
    for i in n:
        D = pd.Series(dataset.loc[dataset['id'] == i]['items'])
        data_in = "/tmp/d{}.txt".format(i)
        f = open(data_in,'w')
        [f.write("{}\n".format(d)) for d in D]
        f.close()
        command = "{} {} {} {} {}".format('lcm', '_M', data_in, '1', '-')
        if debug:
            print(command)
        out = check_output(['lcm', '_M', data_in, '1', '-']).decode("ascii")
        output.append( out )
        print(".", end="", flush=True)
    print("")
    data_out = "/tmp/final.txt"
    g = open(data_out, "w")
    g.write("".join(output))
    g.close()
    results1 = "/tmp/results_parallel.txt"
    command = "{} {} {} {} {}".format('lcm', '_M', data_out, '1', results1)
    if debug:
        print(command)
    call(['lcm', '_M', data_out, '1', results1])
    
    results2 = "/tmp/results_sequential.txt"
    D = pd.Series(dataset['items'])
    data = "/tmp/d.txt"
    f = open(data, 'w')
    [f.write("{}\n".format(d)) for d in D]
    f.close()
    command = "{} {} {} {} {}".format('lcm', '_M', data, '1', results2)
    if debug:
        print(command)
    call(['lcm', '_M', data, '1', results2])
    
    r1 = sortOutput(results1)
    r2 = sortOutput(results2)    
    call(['diff', '-s', r1, r2])
    call(['wc', r1])