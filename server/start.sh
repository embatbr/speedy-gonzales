#!/bin/bash


export PROJECT_ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export SUBROOT_PATH="$PROJECT_ROOT_PATH/server"
cd $SUBROOT_PATH


pip install -r requirements.txt --upgrade


gunicorn -b 0.0.0.0:8000 app.main
