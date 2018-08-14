#!/bin/bash


export SUBROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd $SUBROOT_PATH

export PROJECT_ROOT_PATH="$SUBROOT_PATH/.."


pip install -r python.reqs


gunicorn app.main
