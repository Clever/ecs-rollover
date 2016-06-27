#!/bin/bash

docker run -it \
    -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    -e AWS_REGION=$AWS_REGION \
    -v $SSH_AUTH_SOCK:/ssh-agent \
    -e "SSH_AUTH_SOCK=/ssh-agent" \
    $USER/ecs-rollover:local $@ 
