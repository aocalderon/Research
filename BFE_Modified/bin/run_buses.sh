#!/bin/bash
##########################################################################
## Script to the experiments for the dataset buses.
##
## To use it, just configure the params in the section CONFIGURATION and
## LET IT RIP :)
###########################################################################


#### CONFIGURATION ####
## INPUT ##
DATA_DIR=../flocks/data
DATA_FILE=buses_by_time.txt
DATA_ID=buses

## OUTPUT ##
LOG_DIR=logs
BUILD_DIR=build

## NOTIFICATION ##
EMAIL=true
SEND_TO='sanches.e.denis@gmail.com'
MACHINE=$(hostname -f)
MAILER="extras/notification-email/sendmail.php notification:mailer "
PHP=$(which php5)

## RUNNING PARAMETERS ##
METHODS="BFE PSI" # MAIN METHODS

# Methods available
# * BFE: BFE original
# * PSI: BFE + PS + BinSign + InvIdx

# DEFAULTS
DEF_NUM=5
DEF_DELTA=10
DEF_DIST=0.7

MIN_NUM=4
MAX_NUM=20
DIST_LIST="$(LANG=en_US seq -s' ' .3 .1 1.0)"
MIN_DELTA=4
MAX_DELTA=20

#### END - CONFIGURATION ####


############################
####  PLEASE DO NOT
####      EDIT !!!!
############################


#### START SCRIPT ####

## SELF INFO ##
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )" # Get current directory of the script

source "${DIR}/_common_run.sh"
