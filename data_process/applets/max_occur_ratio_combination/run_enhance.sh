#!/usr/bin/env bash

mkdir -p logs
logfile="logs/log-$$.log"
echo "start: $(date)" | tee ${logfile}
rm -rf result
mkdir -p result/max_occur
python3 get_max_occur_ratio_combination_enhance.py | tee -a ${logfile}
echo "finish: $(date)" | tee -a ${logfile}
