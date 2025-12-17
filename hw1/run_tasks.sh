#!/bin/bash

set -e

print () {
  echo
  echo ">>> $*"
  echo "-------"
}

print "hdfs dfs -mkdir /createme"
hdfs dfs -mkdir /createme

print "hdfs dfs -rm -r -f /delme"
hdfs dfs -rm -r -f /delme

print "echo \"Hadoop client is working\" | hdfs dfs -put - /nonnull.txt"
echo "Hadoop client is working" | hdfs dfs -put - /nonnull.txt

print "hdfs dfs -put /tmp/shadow.txt /shadow.txt"
hdfs dfs -put /tmp/shadow.txt /shadow.txt

print "hadoop jar hadoop-mapreduce-examples wordcount"
hadoop jar \
/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar \
wordcount \
/shadow.txt \
/wordcount_out

print "hdfs dfs -cat /wordcount_out/part-r-00000"
hdfs dfs -cat /wordcount_out/part-r-00000

print "grep Innsmouth count"
hdfs dfs -cat /wordcount_out/part-r-00000 | grep "^Innsmouth"

print "write result to /whataboutinsmouth.txt"
hdfs dfs -cat /wordcount_out/part-r-00000 \
  | grep "^Innsmouth" \
  | awk '{print $2}' \
  | hdfs dfs -put - /whataboutinsmouth.txt

print "hdfs dfs -cat /whataboutinsmouth.txt"
hdfs dfs -cat /whataboutinsmouth.txt

