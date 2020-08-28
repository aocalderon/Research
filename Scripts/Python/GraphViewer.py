#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 27 12:27:18 2020

@author: and
"""

from graphviz import Graph

filename = "/opt/mace22/test100.grh"
dot = Graph(engine='sfdp')

i = 0
with open(filename, 'r') as f:
    for line in f.readlines():
        print(line)
        nodes = line.split(',')
        print(nodes)
        for node in nodes:
            if node != '\n':
                dot.edge(str(i), node)
        i = i + 1
dot.view()
