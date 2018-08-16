#!/bin/bash


export SUBROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd $SUBROOT_PATH

export PROJECT_ROOT_PATH="$SUBROOT_PATH/.."


if [ -z "$SPARK_HOME" ]; then
    export SPARK_HOME="$HOME/apps/spark"
fi

export PYSPARK_PYTHON=python3


OPTIONS="$1"


$SPARK_HOME/bin/spark-submit \
    --packages org.apache.hadoop:hadoop-aws:2.7.3 \
    --master local[*] \
    main.py "$OPTIONS"
