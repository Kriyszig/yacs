#!/bin/bash

if [ -f "util/pid.txt" ] ; then
  pids=`python3 util/list_pid.py`
  iterator=${pids//;/$'\n'}
  for pid_d in $iterator
  do
    kill "$pid_d"
  done
fi