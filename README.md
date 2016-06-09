# ecs-rollover

This script is used to safely rollover or scale down ECS nodes. 

**NOTE:** This script assumes all _tasks_ that need to be migrated belong to a _service_.

## How it works
1. Accepts command line options:
  * rollover vs. scale down
  * `docker stop` timeout
  * cluster name
  * auto scaling group name
2. Queries all container instances in the cluster
3. Ask the user which instances to rollover
  * Warns on imbalanced availability zones
4. One by one for each instance:
  1. Queries all services running in the cluster to track state
  2. Detach the instance from the scaling group
    * Wait for replacement to come online (if rollover)
  3. De-registers the instance and waits for it to be inactive
  4. Wait for tasks to be rescheduled on other instances and in `steady state`
  5. If service has an ELB, it will detach the old container instance
  6. ssh into the instance and `docker stop` each container
    * Uses the configurable stop timeout
  7. Stop & terminate the instance

## Dependencies

```
pip install virtualenvwrapper
source /usr/local/bin/virtualenvwrapper.sh
mkvirtualenv ecs-rollover
make install_deps
```


You will need AWS credentials configured before running this script. Configuring them is the same as for the ECS command line interface. See [ECS CLI Configuration docs](http://docs.aws.amazon.com/AmazonECS/latest/developerguide/ECS_CLI_Configuration.html) for details.


## rollover.py

The most basic usage only requires a cluster name and scaling group:
```
./rollover <cluster_name> <asg name>
```

To scaledown instead, add the `-s` option: 
```
./rollover -s <cluster_name> <asg name>
```

See `--help` for additional options and usage.

## Other Tools

In case the rollover or scale down process fails, there are some utilities to make recovering/continuing easier.

### elb.py

You can remove an ec2 instance from a specific elb or from all of them using the elb script:

```
./elb.py detach <ec2_id> [elb_name [elb_name ...]]
```

### ec2.py

The ec2 script allows you to stop or terminate ec2 instances:
```
./ec2.py {stop,terminate} ec2_id [ec2_id ...]
```

