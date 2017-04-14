#!/bin/sh

#for t in {0..2}; do
#  for i in {5..10}; do
#    proc=$((($t * 6) + ($i - 5)))
#    ssh proj$i "ulimit -c unlimited && cd /data/zzxx/husky-h4 && ls -la > /dev/null && ls -la conf && perf stat -e cache-misses ./$@ --proc_id $proc" &
#  done
#done

proc=0
grep "^[^#]" $2 | grep info= | awk -F'=' '{print $2}' | awk -F':' '{print $1}' | sed s/ib/worker/ | while read machine
do
# ssh $machine "ulimit -c unlimited && cd /data/zzxx/husky-h4 && ls -la > /dev/null && ls -la conf && perf stat -e cache-misses ./$1 -C $2 --proc_id $proc" &
  ssh $machine "cd /data/zzxx/husky-h4 && ls -la > /dev/null && ls -la conf && ./$1 -C $2 --proc_id $proc" &
  proc=$(($proc+1))
  sleep 0.1
done
