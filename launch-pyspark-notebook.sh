#!/bin/bash

export PYSPARK_DRIVER_PYTHON=ipython
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --NotebookApp.ip='*' --NotebookApp.open_browser=False --NotebookApp.port=8880"

export HADOOP_CONF_DIR=/etc/hive/conf
export HIVE_CP=/opt/cloudera/parcels/CDH/lib/hive/lib/

pyspark --master yarn --deploy-mode client --driver-memory 2g \
--num-executors 1 --executor-memory 8g --executor-cores 3

