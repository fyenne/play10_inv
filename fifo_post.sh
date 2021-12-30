#!/bin/bash
# pyspark python3 py2 
export PYSPARK_PYTHON="/app/anaconda3/bin/python"
export PYSPARK_DRIVER_PYTHON="/app/anaconda3/bin/python"
spark-submit --master yarn --deploy-mode client --queue root.dsc --conf spark.executor.memoryOverhead=2048 --conf spark.driver.memoryOverhead=2048 --executor-memory 4G --driver-memory 4G ./fifo_post.py  --env ${env} --day_of_week ${day_of_week}