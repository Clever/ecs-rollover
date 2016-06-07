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
4. Queries all services running in the cluster to track state
5. Detaches the selected instances from the scaling group
  * Wait for replacements to come online (if rollover)
6. One by one for each instance:
  1. Detaches the selected instances from the scaling group
    * Wait for replacements to come online (if rollover) 
  2. De-registers the instance and waits for it to be inactive
  3. Wait for tasks to be rescheduled onto other instances and in `steady state`
  4. If service has an ELB, it will detach the old container instance
  5. ssh into the instance and `docker stop` each container
    * Uses the configurable stop timeout
  6. Stops & terminates the instance

## Dependencies

```
pip install virtualenvwrapper
source /usr/local/bin/virtualenvwrapper.sh
mkvirtualenv ecs-rollover
make install_deps
```

## running the script

You will need AWS credentials configured before running this script. Configuring them is the same as for the ECS command line interface. See [ECS CLI Configuration docs](http://docs.aws.amazon.com/AmazonECS/latest/developerguide/ECS_CLI_Configuration.html) for details.

```
workon ecs-rollover
./rollover <cluster_name> <asg name>
```
