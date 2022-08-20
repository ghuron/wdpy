#!/bin/bash
./pyvenv/bin/python ./src/$1 $(< ./src/toolforge/.credentials )
