#!/bin/bash
E=$1
M=$2

cat /tmp/BFE_E${E}_M${M}_D1.txt | cut -d"," -f3 > /tmp/bfe.txt

sort /tmp/bfe.txt -o /tmp/a.txt
sort /tmp/pflock.txt -o /tmp/b.txt

diff -s /tmp/a.txt /tmp/b.txt
