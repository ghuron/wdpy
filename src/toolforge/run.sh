#!/bin/bash
echo "Starting a new job at" $(date -u) >> ${1//_/-}.out
#rm -f ${1//_/-}.*
./pyvenv/bin/python ./src/$1.py $(< ./src/toolforge/.credentials )
