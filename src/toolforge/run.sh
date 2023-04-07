#!/bin/bash
./pyvenv/bin/python ./src/${1//-/_}.py $(< ./src/toolforge/.credentials )
mkdir --parent ./log
mv ./$1.out log/$1.out.$(date "+%Y%m%d")
mv ./$1.err log/$1.err.$(date "+%Y%m%d")