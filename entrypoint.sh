#!/bin/sh

if [ $# -eq 0 ]
  then
    echo "No arguments supplied"
    exit 1
fi

SETUP_CMD=$1

cd cadcutils 
python setup.py $SETUP_CMD

cd ../cadcdata
pip install ../cadcutils
python setup.py $SETUP_CMD
exit 0