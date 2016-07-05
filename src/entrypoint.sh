#!/bin/bash

# Work around boto3's lack of support for AWS_REGION
if [ -z "$AWS_REGION" ] ; then
    echo "Warning: AWS_REGION is not set. Defaulting to 'us-west-1'"
    export AWS_REGION="us-west-1"
fi
mkdir -p ~/.aws
echo "[default]" > ~/.aws/config
echo "region=$AWS_REGION" >> ~/.aws/config

exec /usr/local/bin/python -B /opt/ecs-rollover/rollover.py $@
