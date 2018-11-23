#!/bin/bash


export PROJECT_ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd $PROJECT_ROOT_PATH


USER="$1"
INSTANCE_IP="$2"
USER_HOME="/home/$USER"


rm -Rf server/app/__pycache__/
rm -Rf spark/__pycache__/
rm -Rf spark/functions/__pycache__/


echo "ssh $USER@$INSTANCE_IP 'rm -Rf $USER_HOME/speedy-gonzales'"
ssh $USER@$INSTANCE_IP 'rm -Rf $USER_HOME/speedy-gonzales'

echo "scp -r server/ $USER@$INSTANCE_IP:$USER_HOME/speedy-gonzales"
scp -r server/ $USER@$INSTANCE_IP:$USER_HOME/speedy-gonzales
echo "scp -r spark/ $USER@$INSTANCE_IP:$USER_HOME/speedy-gonzales"
scp -r spark/ $USER@$INSTANCE_IP:$USER_HOME/speedy-gonzales

echo "ssh $USER@$INSTANCE_IP"
ssh $USER@$INSTANCE_IP
