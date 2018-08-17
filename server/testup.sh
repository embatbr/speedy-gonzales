#!/bin/bash


export SUBROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd $SUBROOT_PATH

export PROJECT_ROOT_PATH="$SUBROOT_PATH/.."

export AWS_ACCESS_KEY_ID="$(jq '.aws_access_key_id' -r $HOME/.aws/creds.json)"
export AWS_SECRET_ACCESS_KEY="$(jq '.aws_secret_access_key' -r $HOME/.aws/creds.json)"


pip install -r python-reqs/speedy-gonzales-test --upgrade


python test/main.py
