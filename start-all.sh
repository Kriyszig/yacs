#!/bin/bash

if [ -f "util/pid.txt" ] ; then
    rm "util/pid.txt"
fi

touch "util/pid.txt"
ports=`python3 util/list_port.py`
iterator=${ports//;/$'\n'}
for port in $iterator
do
  echo "$port"
  python3 worker.py "$port"&
  processid=$!
  echo "$processid" >> util/pid.txt
done