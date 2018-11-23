#!/bin/bash


export PROJECT_ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export SUBROOT_PATH="$PROJECT_ROOT_PATH/spark"
cd $SUBROOT_PATH


if [ -z "$SPARK_HOME" ]; then
    export SPARK_HOME="$HOME/apps/spark"
fi

export PYSPARK_PYTHON=python3


$SPARK_HOME/bin/spark-submit \
    --packages org.apache.hadoop:hadoop-aws:2.7.3 \
    --py-files sequencers.py,functions/*.py,settings.py \
    --master local[*] \
    main.py &

echo $! > spark.pid
