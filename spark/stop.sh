#!/bin/bash

export PROJECT_ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd $PROJECT_ROOT_PATH

export SUBROOT_PATH="$PROJECT_ROOT_PATH/spark"


kill -9 $(cat spark.pid)
rm spark.pid
