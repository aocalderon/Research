#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  9 14:12:05 2018

@author: and
"""
import argparse
import logging
from LCMTesterLib import runLCMunoTest

## Reading arguments...
parser = argparse.ArgumentParser()
parser.add_argument("--input_file", "-i",  required=True,      help="Input file in 2...")
parser.add_argument("--debug",      "-d",  default=False,      help="Activate debug mode.")
args = parser.parse_args()
logging.basicConfig(format="%(asctime)s -> %(message)s")

logging.info("Starting...")
input_file    = args.input_file
if args.debug != False:
    debug = True
else:
    debug = False

runLCMunoTest(input_file)

logging.info("Done!")

