#!/bin/bash
echo "This script include commands to run mapreduce jobs using hadoop streaming to index documents"

echo "Input file is :"
echo $1


hdfs dfs -ls /

# Pipeline Stage 1: Term extraction
echo "Running Pipeline Stage 1: Term extraction"
hdfs dfs -test -d /tmp/index/stage1 && hdfs dfs -rm -r -f /tmp/index/stage1
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -input /index/data \
    -output /tmp/index/stage1 \
    -mapper "/usr/bin/python3 mapreduce/mapper1.py" \
    -reducer "/usr/bin/python3 mapreduce/reducer1.py" \
    -file mapreduce/mapper1.py \
    -file mapreduce/reducer1.py


# Pipeline Stage 2: Cassandra storage
echo "Running Pipeline Stage 2: Cassandra storage"
hdfs dfs -test -d /tmp/index/stage2 && hdfs dfs -rm -r -f /tmp/index/stage2
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -input /tmp/index/stage1 \
    -output /tmp/index/stage2 \
    -mapper "/usr/bin/python3 mapreduce/mapper2.py" \
    -reducer "/usr/bin/python3 mapreduce/reducer2.py" \
    -file mapreduce/mapper2.py \
    -file mapreduce/reducer2.py


echo "MapReduce pipeline completed successfully"