#!/bin/bash


export SUBROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd $SUBROOT_PATH


# DIR2DELETE="$1"


kill -9 $(cat spark.pid)
rm spark.pid
# rm -Rf "$DIR2DELETE"
