#!/bin/bash

# Work around boto3's lack of support for AWS_REGION
mkdir -p ~/.aws
echo "[default]" > ~/.aws/config
echo "region=$AWS_REGION" >> ~/.aws/config

exec /usr/local/bin/python -B /opt/ecs-rollover/rollover.py $@
