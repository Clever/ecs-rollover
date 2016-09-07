#! /usr/bin/env python

import argparse
import datetime
import fnmatch
import itertools
from operator import itemgetter
import os
import sys
import time
import math

# local imports
import ec2
import elb
import ecs
import scaling
import utils

import boto3

SERVICE_ACTIVE = "ACTIVE"

# S3 bucket for EC2 Run Command output
EC2_RUN_OUTPUT_S3_BUCKET = 'clever-test'

class ECSInstance(object):
    """
    properties:
      - ecs_id
      - ec2_id
      - availability_zone
      - ip_address
      - cpu_utilized (percent)
      - mem_utilized (percent)
      - launch_time
    """
    def __init__(self, ecs_client, ec2_client, ecs_id):
        self.ecs_client = ecs_client
        self.ec2_client = ec2_client
        self.ecs_id = ecs_id

        self._populate_ecs_info()
        self._populate_ec2_info()

    def _populate_ecs_info(self):
        info = self.ecs_client.describe_instances([self.ecs_id])
        self.ec2_id = info[self.ecs_id]['ec2InstanceId']

        # look up registered and remaining resources
        cpu_registered = -1
        cpu_remaining = -1
        mem_registered = -1
        mem_remaining = -1
        for r in info[self.ecs_id]['registeredResources']:
            if r['name'] == 'CPU':
                cpu_registered = r['integerValue']
            if r['name'] == 'MEMORY':
                mem_registered = r['integerValue']
        for r in info[self.ecs_id]['remainingResources']:
            if r['name'] == 'CPU':
                cpu_remaining = r['integerValue']
            if r['name'] == 'MEMORY':
                mem_remaining = r['integerValue']

        # compute utilization %, rounding up
        self.cpu_utilized = math.ceil(100 * (1 - float(cpu_remaining) / cpu_registered))
        self.mem_utilized = math.ceil(100 * (1 - float(mem_remaining) / mem_registered))

    def _populate_ec2_info(self):
        info = self.ec2_client.describe_instances([self.ec2_id])
        self.availability_zone = info[self.ec2_id]['Placement']['AvailabilityZone']
        self.ip_address = info[self.ec2_id]['PrivateIpAddress']
        self.launch_time = info[self.ec2_id]['LaunchTime']

    def __cmp__(self, other):
        return cmp(self.ecs_id, other.ecs_id)

    def __repr__(self):
        return "{} ({} - {}) [{:3.0f}% cpu, {:3.0f}% mem] -- {}".format(self.ecs_id, self.ec2_id, self.availability_zone, self.cpu_utilized, self.mem_utilized, self.launch_time)


def prompt_for_instances(ecs_instances, asg_contents, scale_down=False, sort_by="launch_time"):
    """
    sorts the instances into an order that tries not to cause an AZ imbalance
    when removing instances. Also, prompts the user if there are issues.
    @param ecs_instances: list of ECSInstance() objects
    @param asg_contents: dictionary of ec2_ids to availability zones in the ASG
    @param scale_down: bool if scale down or rollover
    @param sort_by: string ("launch_time" or "utilization") for how to sort printed instances
    @return: sorted list of ECSInstance() objects to remove
    """
    all_azs = set([az for az in asg_contents.values()])

    # ask the user which instances to remove:
    if scale_down:
        print "Which instances do you want to remove?"
    else:
        print "Which instances do you want to rollover?"

    # allow sorting by launch_time or utilization
    sorts = dict(
        utilization=dict(key=lambda i: i.cpu_utilized + i.mem_utilized, reverse=True),
        launch_time=dict(key=lambda i: i.launch_time, reverse=False),
    )
    sort_type = sorts[sort_by]
    ecs_instances.sort(key=sort_type["key"], reverse=sort_type["reverse"])
    for x, instance in enumerate(ecs_instances):
        print "%d\t - %s" % (x, instance)
    selections = raw_input('Specify the indices - comma-separated (ex. "1,2,4") or inclusive range (ex. "7-11"): ').split(',')
    selected_instances = []
    for selection in selections:
        if '-' in selection:
            start, end = selection.split('-')
            start = int(start)
            end = int(end)
            selected_instances += ecs_instances[start:end+1]
        else:
            index = int(selection)
            selected_instances.append(ecs_instances[index])

    # remove the selected instances to determine the remaining AZ balance
    to_remove = {}
    for ecs_instance in selected_instances:
        if ecs_instance.ec2_id in asg_contents:
            del asg_contents[ecs_instance.ec2_id]
        else:
            print "WARNING: %s is not in the AutoScalingGroup. It will not be replaced" % (ecs_instance.ec2_id)

        to_remove.setdefault(ecs_instance.availability_zone, [])
        to_remove[ecs_instance.availability_zone].append(ecs_instance)

    remaining_instances_by_az = {}
    for ecs_instance in ecs_instances:
        if ecs_instance.ec2_id in asg_contents:
            remaining_instances_by_az.setdefault(ecs_instance.availability_zone, []).append(ecs_instance)
    remaining_instances = [i for sublist in remaining_instances_by_az.values() for i in sublist]

    #
    # check the Availability Zone balance
    #
    az_balance = {}
    for k, v in asg_contents.iteritems():
        az_balance.setdefault(v, []).append(k)

    max_diff = 0
    for a, b in itertools.combinations(az_balance.keys(), 2):
        max_diff = max(max_diff, abs(len(az_balance[a]) - len(az_balance[b])))

    remaining = sum([len(i) for i in to_remove.values()])
    ordered_instances = []
    # order by zone with the most instances first
    for az in all_azs:
        remaining_instances_by_az.setdefault(az, [])

    zone_counts = sorted(remaining_instances_by_az.items(),
                         key=itemgetter(1),
                         cmp=lambda a, b: cmp(len(a), len(b)),
                         reverse=True)
    for az in itertools.cycle([z[0] for z in zone_counts]):
        if to_remove.get(az, []):
            ordered_instances.append(to_remove[az].pop(0))
            remaining -= 1
        if remaining == 0:
            break

    print "About to remove the following instances:"
    for instance in ordered_instances:
        print instance

    asg_zones = set([az for az in asg_contents.values()])
    if scale_down and (max_diff > 1 or len(asg_zones) == 1):
        print "WARNING: The instances you selected will cause the auto scaling" \
              " group to rebalance instances across availability zones. This" \
              " may result in a destructive operation."

    confirm = raw_input("Do you want to continue [y/N]? ")
    if confirm.lower() != 'y':
        return [], remaining_instances

    return ordered_instances, remaining_instances


def map_service_events(service_descriptions):
    """
    Creates a mapping of service ids to their events
    @param service_descriptions: dictionary of service ids to descriptions
    @return: dictionary of service_ids to lists of events
    """
    service_events = {}
    for service_id, desc in service_descriptions.items():
        events = sorted(desc['events'], key=itemgetter('createdAt'))
        service_events[service_id] = events
    return service_events


def map_instance_services(service_descriptions, task_descriptions):
    """
    Creates a mapping of the services that are running on each ECS instance
    @param service_descriptions: dictionary of service ids to descriptions
    @param task_descriptions: dictionary of task arns to descriptions
    @return: dictionary of ecs_ids to list of service ids
    """
    defs_to_services = {}
    instance_services = {}

    for service_id, desc in service_descriptions.items():
        def_arn = desc['taskDefinition']
        defs_to_services[def_arn] = service_id

    for task_arn, task in task_descriptions.items():
        def_arn = task['taskDefinitionArn']
        if def_arn in defs_to_services:
            # only look at tasks with services (ignore instance startup tasks)
            ecs_id = utils.pull_instance_id(task['containerInstanceArn'])
            instance_services.setdefault(ecs_id, [])
            if defs_to_services[def_arn] not in instance_services[ecs_id]:
                instance_services[ecs_id].append(defs_to_services[def_arn])

    return instance_services


def get_added_asg_instances(old_instances, new_instances):
    """
    diff two lists of asg instance dicts
    @param old_instances: list of asg instance dictionaries
    @param new_instances: list of asg instance dictionaries
    @return: list of new instances (ec2 ids) in the asg
    """
    old_ids = [i['InstanceId'] for i in old_instances]
    new_ids = [i['InstanceId'] for i in new_instances]
    return [i for i in new_ids if i not in old_ids]


def wait_for_all_services(ecs_client, services_on_instance, service_events):
    """
    Wait for all services on an instance to reach steady state
    @param ecs_client: ecs client object
    @param services_on_instance: list of service ids
    @param service_events: map of services to lists of events
    @return: list of service_ids that never completed
    """
    failed = []
    for service_id in services_on_instance:
        last_event = service_events[service_id][-1]
        completed, event = ecs_client.wait_for_service_steady_state(service_id,
                                                                    last_event)
        # push the new event into the list for that service so
        # that the next instance doesn't confuse this event for
        # its own
        service_events[service_id].append(event)

        if not completed:
            failed.append(service_id)
    return failed


def get_matching_tasks_by_hosts(ecs_client, ec2_client, match_expr):
    """
    Get all the ECS instances that are running tasks that match the match_expr
    @param ecs_client: ecs client object
    @param ec2_client: ec2 client object
    @param match_expr: string to match task definitions against
    @return: dictionary of all ecs_ids to list of matching task definitions
    """
    task_ids = ecs_client.list_tasks()
    task_descriptions = ecs_client.describe_tasks(task_ids)
    running_map = {}
    for task in task_descriptions.values():
        task_def = utils.pull_task_definition_name(task['taskDefinitionArn'])
        ecs_id = utils.pull_instance_id(task['containerInstanceArn'])
        running_map.setdefault(ecs_id, [])
        if fnmatch.fnmatch(task_def, match_expr):
            running_map[ecs_id].append(task_def)
    return running_map


def main_rollover(args):
    """
    Main entry point for rollover and scaledown commands
    """
    if args.dry_run:
        print "############## DRY RUN MODE ##############"
        print

    #
    # Create AWS connections
    #
    ecs_client = ecs.ECSClient(args.cluster)
    ec2_client = ec2.EC2Client()
    asg = scaling.AutoScalingGroup(args.asg)

    # get all the ecs instances and their necessary metadata
    all_ecs_instances = []
    for instance in ecs_client.list_container_instances():
        ecs_instance = ECSInstance(ecs_client, ec2_client, instance)
        all_ecs_instances.append(ecs_instance)

    # get all the ec2 instances in the ASG and their availability zones
    asg_instances = asg.describe_instances()
    asg_contents = {}
    for asg_instance in asg_instances:
        ec2_id = asg_instance['InstanceId']
        asg_contents[ec2_id] = asg_instance['AvailabilityZone']

    # Prompt the user for the instances to adjust
    selected_ecs_instances, remaining_instances = prompt_for_instances(all_ecs_instances,
                                                                       asg_contents,
                                                                       args.scale_down,
                                                                       args.sort)
    if not selected_ecs_instances:
        return True

    #
    # Verify that Cluster services are healthy first
    #
    service_ids = ecs_client.list_services()
    service_descriptions = ecs_client.describe_services(service_ids)
    service_issues = []
    for service in service_descriptions.values():
        if service['status'] != SERVICE_ACTIVE:
            serivce_issues.append(service['serviceName'])

    if service_issues:
        print "ERROR: Not all services are active: %s" % ", ".join(service_issues)
        return False

    #
    # Verify that Cluster size is >= largest service count
    #
    if args.scale_down and service_ids:
        counts_to_service = {}
        for service in service_descriptions.values():
            counts_to_service.setdefault(service['desiredCount'], []).append(service['serviceName'])

        max_count = sorted(counts_to_service.keys(), reverse=True)[0]
        if max_count > len(remaining_instances):
            print "ERROR: New cluster size (%d) is smaller than largest services: %s (%d)" % (len(remaining_instances), ", ".join(counts_to_service[max_count]), max_count)
            return False

    #
    # Iterate through each instance
    #
    skipped_shutdown = []
    return_value = True
    for ecs_instance in selected_ecs_instances:
        print "Preparing to remove %s" % (ecs_instance)

        #
        # Remove ECS instance from scaling group
        #
        if args.scale_down:
            sys.stdout.write("Remove EC2 instance from scaling group...")
        else:
            sys.stdout.write("Removing EC2 instance from scaling group and waiting for replacement...")
        sys.stdout.flush()
        if not args.dry_run:
            if args.scale_down:
                asg.detach_instances([ecs_instance.ec2_id], scale_down=True)
            else:
                asg.detach_instances_and_wait([ecs_instance.ec2_id])
        print "done"

        #
        # Wait for new ec2 instance to join the ECS cluster
        #
        if not args.scale_down and not args.dry_run:
            new_asg_instances = asg.describe_instances()
            new_ec2_id = get_added_asg_instances(asg_instances,
                                                 new_asg_instances)[0]
            asg_instances = new_asg_instances
            sys.stdout.write("Waiting for replacement EC2 instance %s to join ECS..." % (new_ec2_id))
            sys.stdout.flush()
            while new_ec2_id not in ecs_client.list_active_ec2_instances():
                time.sleep(10)
            print "done"

        #
        # Query services and tasks just before calling
        #
        # NOTE: If a deployment is made and scheduled to the machine being
        # removed after the services and tasks are queried, but before
        # deregister_container_instance() is called, then it wont be tracked
        # and removed during the rollover. The following calls are grouped
        # together as closely as possible to minimize this risk.
        #
        service_ids = ecs_client.list_services()
        service_descriptions = ecs_client.describe_services(service_ids)
        service_events = map_service_events(service_descriptions)

        task_ids = ecs_client.list_tasks()
        task_descriptions = ecs_client.describe_tasks(task_ids)
        ecs_instance_services = map_instance_services(service_descriptions,
                                                      task_descriptions)

        #
        # De-register instances from ECS
        #
        sys.stdout.write("De-registering instance from ECS ...")
        sys.stdout.flush()
        if not args.dry_run:
            ecs_client.deregister_container_instance(ecs_instance.ecs_id)
        print "done"

        #
        # Wait for task migrations
        #
        services_on_instance = ecs_instance_services.get(ecs_instance.ecs_id, [])
        if services_on_instance:
            sys.stdout.write("Rolling over services ...")
            sys.stdout.flush()
            if not args.dry_run:
                failed_services = wait_for_all_services(ecs_client,
                                                        services_on_instance,
                                                        service_events)
                if failed_services:
                    return_value = False
                    service_names = [service_descriptions[sid]['serviceName'] for sid in failed_services]
                    print "ERROR: Timeout while waiting for %s to reach steady state" % (service_names)
                    break
            print "done"

            sys.stdout.write("Removing instance from any service ELBs ...")
            sys.stdout.flush()
            if not args.dry_run:
                for service_id in services_on_instance:
                    # remove the current instance from the ELB if there is one
                    # defined
                    service = service_descriptions[service_id]
                    for balancer in service.get('loadBalancers', []):
                        elb_client = elb.ELBClient(balancer['loadBalancerName'])
                        elb_client.deregister_instances([ecs_instance.ec2_id])
            print "done"

        #
        # stop all the docker containers on the machine
        #
        sys.stdout.write("Stopping containers on instance ...")
        sys.stdout.flush()
        ip_address = ecs_instance.ip_address
        if not args.dry_run:
            # TEST DOCKER IS RUNNING
            if not run_with_timeout(ecs_instance.ec2_id, 'docker ps -a -q', 10):
                print "FAILED to run `docker ps`. Skipping shutdown for %s" % (ecs_instance)
                skipped_shutdown.append(ecs_instance)
                continue

            # STOP ALL DOCKER CONTAINERS
            command = 'docker stop -t %d $(docker ps -a -q)' % args.timeout
            if not run_with_timeout(ecs_instance.ec2_id, command, args.timeout):
                print "FAILED"
                print "WARNING: Failed to stop all containers"
        print "done stopping docker containers"

        #
        # Stop and terminate the EC2 instance
        #
        sys.stdout.write("Stopping and Terminating instance ...")
        sys.stdout.flush()
        if not args.dry_run:
            ec2_client.stop_and_wait_for_instances([ecs_instance.ec2_id])
            ec2_client.terminate_and_wait_for_instances([ecs_instance.ec2_id])
        print "done stopping and terminating instance"
        print

    # Print the instances that need to be manually shutdown
    if skipped_shutdown:
        print "#"*80
        print "The following instances could not be shutdown."
        print "They likely still have tasks running on them:"
        for instance in skipped_shutdown:
            print instance

    if not return_value:
        print "NOTE: Some errors were encountered."
    elif args.scale_down:
        print "Scale down complete!"
    else:
        print "Rollover complete!"
    return return_value


def run_with_timeout(instance_id, command, timeout):
    """
        @param instance_id str
        @param command str
        @param timeout int
        @return bool if command succeeded
    """

    client = boto3.client('ssm')
    response = client.send_command(
        InstanceIds = [instance_id],
        DocumentName = 'AWS-RunShellScript',
        TimeoutSeconds = 3600,
        Comment = '',
        Parameters = {
            'commands': ["#!/bin/bash", command]
        },
        OutputS3BucketName = EC2_RUN_OUTPUT_S3_BUCKET,
        OutputS3KeyPrefix = 'rollover-' + datetime.datetime.now().strftime('%Y%m%d')
    )

    # Error sending response
    if response.get('ResponseMetadata', {}).get('HTTPStatusCode') is not 200:
        print "ERROR sending command:", response
        return False

    command_id = response.get('Command').get('CommandId')

    # Not sure when/if this actually happens; adding as a safeguard for now.
    if not command_id:
        print "Error: could not find command ID in response", response
        return False

    invocation_response = client.list_command_invocations(CommandId=command_id, InstanceId=instance_id, Details=True)
    result = invocation_response.get('CommandInvocations')[0].get('CommandPlugins')[0]

    # Wait until command reaches final state
    while result.get('Status') in ['Pending', 'InProgress', 'Cancelling']:
        time.sleep(2)
        invocation_response = client.list_command_invocations(CommandId=command_id, InstanceId=instance_id, Details=True)
        result = invocation_response.get('CommandInvocations')[0].get('CommandPlugins')[0]

    return result.get('ResponseCode') == 0

def main_docker_stop(args):
    """
    Main entry point for the docker-stop command
    """
    command = "docker stop -t %d $(docker ps -a -q)" % args.timeout
    return run_with_timeout(args.ec2_id, command, args.timeout)

def main_check_for_task(args):
    sys.stdout.write("Querying ECS ...")
    sys.stdout.flush()

    ecs_client = ecs.ECSClient(args.cluster)
    ec2_client = ec2.EC2Client()
    instance_map = {}
    for instance in ecs_client.list_container_instances():
        ecs_instance = ECSInstance(ecs_client, ec2_client, instance)
        instance_map[ecs_instance.ecs_id] = ecs_instance

    running_map = get_matching_tasks_by_hosts(ecs_client,
                                              ec2_client,
                                              args.task_name_expr)
    print "Done"

    for ecs_id in sorted(running_map):
        running_task_defs = running_map[ecs_id]
        instance = instance_map[ecs_id]

        if args.invert_match and not running_task_defs:
            print "%s (%s, %12s) - NO MATCH" % (ecs_id,
                                                instance.ec2_id,
                                                instance.ip_address)
        elif not args.invert_match and running_task_defs:
            print "%s (%s, %12s) - RUNNING %s" % (ecs_id,
                                                  instance.ec2_id,
                                                  instance.ip_address,
                                                  ", ".join(running_task_defs))

    matched = sum([1 if defs else 0 for defs in running_map.values()])
    if args.invert_match:
        no_match = len(instance_map) - matched
        print "%d of %d hosts do NOT match the pattern `%s`" % (no_match,
                                                                len(instance_map),
                                                                args.task_name_expr)
    else:
        print "%d of %d hosts have running tasks that matched the pattern `%s`" % (matched,
                                                                                   len(instance_map),
                                                                                   args.task_name_expr)


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    #
    # Rollover args
    #
    rollover_parser = subparsers.add_parser('rollover',
                                            help="rollover ECS nodes")
    rollover_parser.set_defaults(func=main_rollover, scale_down=False)

    rollover_parser.add_argument('-t',
                                 '--timeout',
                                 type=int,
                                 default=30,
                                 help="`docker stop` timeout")
    rollover_parser.add_argument('-s',
                                 '--sort',
                                 choices=['launch_time', 'utilization'],
                                 default="launch_time",
                                 help="sorts instances by 'launch_time' or 'utilization'. "
                                        "If not provided, defaults to 'launch_time'")
    rollover_parser.add_argument('--dry-run',
                                 action="store_true",
                                 default=False,
                                 help="dry run. Don't actually make changes.")
    rollover_parser.add_argument('cluster',
                                 help="fully qualified name of the cluster")
    rollover_parser.add_argument('asg',
                                 help="auto scaling group for the cluster")

    #
    # Scaledown args
    #
    scaledown_parser = subparsers.add_parser('scaledown',
                                             help="remove ECS nodes")
    scaledown_parser.set_defaults(func=main_rollover, scale_down=True)

    scaledown_parser.add_argument('-t',
                                  '--timeout',
                                  type=int,
                                  default=30,
                                  help="`docker stop` timeout")
    scaledown_parser.add_argument('-s',
                                 '--sort',
                                 choices=['launch_time', 'utilization'],
                                 default="launch_time",
                                 help="sorts instances by 'launch_time' or 'utilization'. "
                                        "If not provided, defaults to 'launch_time'")
    scaledown_parser.add_argument('--dry-run',
                                  action="store_true",
                                  default=False,
                                  help="dry run. Don't actually make changes.")
    scaledown_parser.add_argument('cluster',
                                  help="fully qualified name of the cluster")
    scaledown_parser.add_argument('asg',
                                  help="auto scaling group for the cluster")

    #
    # elb-detach args
    #
    elb_detach_parser = subparsers.add_parser('elb-detach',
                                              help="Remove an EC2 instance "
                                                   "from ELBs")
    elb_detach_parser.set_defaults(func=elb.main_detach)

    elb_detach_parser.add_argument('ec2_id',
                                   help="EC2 instance id")
    elb_detach_parser.add_argument('load_balancer_name',
                                   nargs='*',
                                   help="load balancer to detach from. "
                                        "If not provided, all will be queried")

    #
    # docker-stop args
    #
    docker_stop_parser = subparsers.add_parser('docker-stop',
                                               help="stop docker containers on"
                                                    " an ec2 instance")
    docker_stop_parser.set_defaults(func=main_docker_stop)

    docker_stop_parser.add_argument('-t',
                                    '--timeout',
                                    type=int,
                                    default=30,
                                    help="`docker stop` timeout")
    docker_stop_parser.add_argument('ec2_id',
                                    help="EC2 instance id")

    #
    # ec2-stop args
    #
    ec2_stop_parser = subparsers.add_parser('ec2-stop',
                                            help="Stop EC2 instances")
    ec2_stop_parser.set_defaults(func=ec2.main_stop)

    ec2_stop_parser.add_argument('ec2_id',
                                 nargs='+',
                                 help="EC2 instance id")

    #
    # ec2-terminate args
    #
    ec2_terminate_parser = subparsers.add_parser('ec2-terminate',
                                                 help="Terminate EC2 instances")
    ec2_terminate_parser.set_defaults(func=ec2.main_terminate)

    ec2_terminate_parser.add_argument('ec2_id',
                                      nargs='+',
                                      help="EC2 instance id")

    #
    # check for a task
    #
    check_task_parser = subparsers.add_parser('check-task',
                                              help="return a list of ECS instances running the given task")
    check_task_parser.set_defaults(func=main_check_for_task)

    check_task_parser.add_argument('-v',
                                   '--invert-match',
                                   action="store_true",
                                   default=False,
                                   help="Print the ECS instances NOT running the task")
    check_task_parser.add_argument('cluster',
                                   help="fully qualified name of the cluster")
    check_task_parser.add_argument('task_name_expr',
                                   help="task definition name (wildcards accepted)")

    args = parser.parse_args()
    if not args.func(args):
        sys.exit(1)


if __name__ == "__main__":
    main()
