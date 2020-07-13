#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  7 20:32:25 2020

@author: and
"""
from dataclasses import dataclass

@dataclass(frozen=True)
class Task:
    host: str
    taskId: int
    launch: int
    finish: int
    duration: int
    
    def toString(self):
        return("{}\t{}\t{}\t{}\t{}\n".format(host, taskId, launch, finish, duration))

id = 17
filename = "/home/and/Research/Meetings/next/figures/pflock{}.tsv".format(id)
cores = 8

def getAvailableCore(T, time, taskId):
    try:
        core = [T[i][time] for i in range(cores)].index(0)
        return(core)
    except ValueError:
        #print("Not available core at {} for task {}".format(time, taskId))
        return(getAvailableCore(T, time + 1, taskId))

tasks_prime = []
with open(filename) as f:
    next(f)
    for line in f:
        arr = line.split("\t")
        taskId = int(arr[3])
        launch = int(arr[4])
        finish = int(arr[5])
        duration = int(arr[6])
        host = arr[9]
        task = Task(host, taskId, launch, finish, duration)
        tasks_prime.append(task)
start = min(tasks_prime, key = lambda task: task.launch).launch    
tasks = []
for t in tasks_prime:
    task = Task(t.host, t.taskId, t.launch - start, t.finish - start, t.duration)
    tasks.append(task)
length = max(tasks, key = lambda  task: task.finish).finish

f = open("/home/and/Research/Meetings/next/figures/mapper{}.tsv".format(id), "w")
f.write("coreId\thost\ttaskId\tlaunchTime\tfinishTime\tduration\n")

hosts = ["mr-01","mr-02","mr-03","mr-04","mr-05","mr-06","mr-07","mr-08","mr-09","mr-10","mr-11","mr-12"]

for host in hosts:
    print(host)
    T = [[0 for i in range(length)] for j in range(cores)]
    sample = [task for task in tasks if task.host == host]
    sample.sort(key = lambda task: task.launch)
    for t in sample:
        core = getAvailableCore(T, t.launch, taskId = t.taskId)
        line = "{}\t{}\t{}\t{}\t{}\t{}\n".format(core, 
                                                 t.host, 
                                                 t.taskId, 
                                                 t.launch+start, 
                                                 t.finish+start, 
                                                 t.duration)
        f.write(line)
        interval = [x for x in range(t.launch, t.finish)]
        for time in interval:
            T[core][time] = t.taskId
f.close()