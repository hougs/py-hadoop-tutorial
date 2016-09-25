#!/bin/bash

export PYSPARK_DRIVER_PYTHON=ipython
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --NotebookApp.ip='*' --NotebookApp.open_browser=False --NotebookApp.port=8880"

export HADOOP_CONF_DIR=/etc/hive/conf
export HIVE_CP=/opt/cloudera/parcels/CDH/lib/hive/lib/

pyspark --master local[2] --deploy-mode client --driver-memory 2G \
    --driver-class-path $HIVE_CP \
    --conf spark.executor.extraClassPath=$HIVE_CP \
    --conf spark.yarn.executor.memoryOverhead=2048



