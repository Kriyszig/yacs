#!/bin/bash

reqsize="5;10;15;20;25"
iterator_req=${reqsize//;/$'\n'}
sleep_val=10
for sizes in $iterator_req
do
  algorithms="round-robin;least-loaded;random"
  iterator_alg=${algorithms//;/$'\n'}
  for alg in $iterator_alg
  do
    ./start-all.sh
    echo "$alg"
    python3 master.py -sa="$alg"&
    processid=$!
    echo "$processid" >> util/pid.txt
    python3 requests.py "$sizes"
    sleep "$sleep_val"
    echo "Done one iteration"
    ./stop-all.sh
  done
  sleep_val="$(($sleep_val+10))"
done