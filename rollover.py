#! /usr/bin/env python

import argparse
import itertools
from operator import itemgetter
import os
import sys
import time

# local imports
import ec2
import elb
import ecs
import scaling
import ssh
import utils


def sort_and_check_availability_zones(instance_descriptions, instances_in_asg):
    """
    sorts the instances into an order that tries not to cause an AZ imbalance
    when removing instances. Also, prompts the user if there are issues.
    @param instance_descriptions: dict of ec2_ids to descriptions
    @param instances_in_asg: list of instance dicts in the asg
    @return: sorted list of instances to remove
    """
    asg_instances_to_zones = {}
    for asg_instance in instances_in_asg:
        ec2_id = asg_instance['InstanceId']
        asg_instances_to_zones[ec2_id] = asg_instance['AvailabilityZone']

    # remove the selected instances to determine the remaining AZ balance
    to_remove = {}
    for ec2_id, desc in instance_descriptions.items():
        if ec2_id in asg_instances_to_zones:
            del asg_instances_to_zones[ec2_id]
        else:
            print "WARNING: %s is not in the AutoScalingGroup. It will not be replaced" % (ec2_id)

        az = desc['Placement']['AvailabilityZone']
        to_remove.setdefault(az, [])
        to_remove[az].append(ec2_id)

    # check az balance
    max_diff = 0
    for a, b in itertools.combinations(to_remove.keys(), 2):
        max_diff = max(max_diff, abs(len(to_remove[a]) - len(to_remove[b])))

    remaining = sum([len(i) for i in to_remove.values()])
    ordered_instances = []
    # order by zone with the most instances first
    zone_counts = sorted(to_remove.items(),
                         key=itemgetter(1),
                         cmp=lambda a, b: cmp(len(a), len(b)),
                         reverse=True)
    for az in itertools.cycle([z[0] for z in zone_counts]):
        if to_remove[az]:
            ordered_instances.append(to_remove[az].pop(0))
            remaining -= 1
        if remaining == 0:
            break

    print "About to remove the following instances:"
    for instance in ordered_instances:
        print "%s - %s" % (instance, instance_descriptions[instance]['Placement']['AvailabilityZone'])

    asg_zones = set([az for az in asg_instances_to_zones.values()])
    if max_diff > 1 or len(asg_zones) == 1:
        print "WARNING: The instances you selected will cause the auto scaling" \
              " group to rebalance instances across availability zones. This" \
              " will result in a destructive operation."

    confirm = raw_input("Do you want to continue [y/N]? ")
    if confirm.lower() != 'y':
        return []

    return ordered_instances


def prompt_for_instances(instances, scale_down=False):
    """
    Ask the user to confirm the instances to remove
    @param instances: list of ecs instance ids
    @param scale_down: bool if scale down or rollover
    @return: list of instance ids, empty list means user backed out
    """
    if scale_down:
        print "Which instances do you want to remove?"
    else:
        print "Which instances do you want to rollover?"

    for x, instance in enumerate(sorted(instances)):
        print "%d\t - %s" % (x, instance)
    selections = raw_input('Specify the indices - comma-separated (ex. "1,2,4") or inclusive range (ex. "7-11"): ').split(',')

    selected_instances = []
    for selection in selections:
        if '-' in selection:
            start, end = selection.split('-')
            start = int(start)
            end = int(end)
            selected_instances += instances[start:end+1]
        else:
            index = int(selection)
            selected_instances.append(instances[index])

    # confirm selection
    if scale_down:
        print "You selected the following instances to remove:"
    else:
        print "You selected the following instances to rollover:"

    for instance in selected_instances:
        print instance
    confirm = raw_input("Is this correct [y/N]? ")
    if confirm.lower() != 'y':
        return []
    return selected_instances


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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s',
                        '--scale-down',
                        action="store_true",
                        default=False,
                        help="scale the cluster down instead of rolling over")
    parser.add_argument('-t',
                        '--timeout',
                        type=int,
                        default=30,
                        help="`docker stop` timeout")
    parser.add_argument('-n',
                        '--noop',
                        action="store_true",
                        default=False,
                        help="dry run. Don't actually make changes.")
    parser.add_argument('cluster',
                        help="fully qualified name of the cluster")
    parser.add_argument('asg',
                        help="auto scaling group for the cluster")
    args = parser.parse_args()

    if args.noop:
        print "############## NO-OP MODE ##############"
        print

    #
    # Create AWS connections
    #
    ecs_client = ecs.ECSClient(args.cluster)
    ec2_client = ec2.EC2Client()
    asg = scaling.AutoScalingGroup(args.asg)

    # Prompt the user for the instances to adjust
    cluster_instances = ecs_client.list_container_instances()
    selected_instances = prompt_for_instances(cluster_instances,
                                              args.scale_down)
    if not selected_instances:
        return

    #
    # learn about and sort the instances by availability zone
    #
    ecs_descriptions = ecs_client.describe_instances(selected_instances)
    ec2_to_ecs_id = dict([(v['ec2InstanceId'], k) for k, v in ecs_descriptions.items()])
    ec2_ids = ec2_to_ecs_id.keys()
    ec2_descriptions = ec2_client.describe_instances(ec2_ids)

    # sort and warn by availability zones
    asg_instances = asg.describe_instances()
    sorted_ec2_ids = sort_and_check_availability_zones(ec2_descriptions,
                                                       asg_instances)
    if not sorted_ec2_ids:
        return

    #
    # Iterate through each instance
    #
    asg_ids = [asg_instance['InstanceId'] for asg_instance in asg_instances]
    sorted_asg_ids = [ec2_id for ec2_id in sorted_ec2_ids if ec2_id in asg_ids]
    for ec2_id in sorted_asg_ids:
        ecs_id = ec2_to_ecs_id[ec2_id]
        print "Preparing to remove %s (%s)" % (ecs_id, ec2_id)

        #
        # Remove ECS instance from scaling group
        #
        if args.scale_down:
            sys.stdout.write("Remove EC2 instance %s from scaling group..." % (ec2_id))
        else:
            sys.stdout.write("Removing EC2 instance %s from scaling group and waiting for replacement..." % (ec2_id))
        sys.stdout.flush()
        if not args.noop:
            if args.scale_down:
                asg.detach_instances([ec2_id], scale_down=True)
            else:
                asg.detach_instances_and_wait([ec2_id])
        print "done"

        #
        # Wait for new ec2 instance to join the ECS cluster
        #
        if not args.scale_down and not args.noop:
            new_asg_instances = asg.describe_instances()
            new_ec2_id = get_added_asg_instances(asg_instances,
                                                 new_asg_instances)[0]
            asg_instances = new_asg_instances
            sys.stdout.write("Waiting for replacement instance %s to join ECS..." % (new_ec2_id))
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
        sys.stdout.write("De-registering instance %s from ECS..." % (ecs_id))
        sys.stdout.flush()
        if not args.noop:
            ecs_client.deregister_container_instance(ecs_id)
        print "done"

        #
        # Wait for task migrations
        #
        services_on_instance = ecs_instance_services.get(ecs_id, [])
        if services_on_instance:
            sys.stdout.write("Rolling over services from %s ..." % (ecs_id))
            sys.stdout.flush()
            if not args.noop:
                for service_id in services_on_instance:
                    last_event = service_events[service_id][-1]
                    completed, event = ecs_client.wait_for_service_steady_state(service_id,
                                                                                last_event)
                    if not completed:
                        print "TIMEOUT"
                        print "Timeout hit while waiting for %s to reach steady state" % (service_id)

                    # push the new event into the list for that service so
                    # that the next instance doesn't confuse this event for
                    # its own
                    service_events[service_id].append(event)
            print "done"

            sys.stdout.write("Removing %s from any service ELBs..." % (ec2_id))
            sys.stdout.flush()
            if not args.noop:
                for service_id in services_on_instance:
                    # remove the current instance from the ELB if there is one
                    # defined
                    service = service_descriptions[service_id]
                    for balancer in service.get('loadBalancers', []):
                        elb_client = elb.ELBClient(balancer['loadBalancerName'])
                        elb_client.deregister_instances([ec2_id])
            print "done"

        #
        # stop all the docker containers on the machine
        #
        sys.stdout.write("Stopping containers on %s ..." % (ec2_id))
        sys.stdout.flush()
        ip_address = ec2_descriptions[ec2_id]['PrivateIpAddress']
        if not args.noop:
            if not ssh.stop_all_containers(ip_address, args.timeout):
                print "FAILED"
                print "WARNING: Failed to stop all containers"
        print "done"

        #
        # Stop and terminate the EC2 instance
        #
        sys.stdout.write("Stopping and Terminating %s ..." % (ec2_id))
        sys.stdout.flush()
        if not args.noop:
            ec2_client.stop_and_wait_for_instances([ec2_id])
            ec2_client.terminate_and_wait_for_instances([ec2_id])
        print "done"
        print

    if args.scale_down:
        print "Scale down complete!"
    else:
        print "Rollover complete!"


if __name__ == "__main__":
    main()
