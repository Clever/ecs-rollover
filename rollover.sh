#!/bin/bash

docker run -it \
    -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    -e AWS_REGION=$AWS_REGION \
    ecs-rollover:local $@
