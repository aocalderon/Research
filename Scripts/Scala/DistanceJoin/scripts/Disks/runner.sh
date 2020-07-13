#!/bin/bash

ES=(6 8 10 12)
PS=(1000 1200 1400 1600 1800 2000)

for E in "${ES[@]}"
do
    for P in "${PS[@]}"
    do
	echo "./DisksFinder_server.sh -e $E -p $P -a 50"
    done
done

