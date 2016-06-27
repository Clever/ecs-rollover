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


class ECSInstance(object):
    """
    properties:
      - ecs_id
      - ec2_id
      - availability_zone
      - ip_address
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

    def _populate_ec2_info(self):
        info = self.ec2_client.describe_instances([self.ec2_id])
        self.availability_zone = info[self.ec2_id]['Placement']['AvailabilityZone']
        self.ip_address = info[self.ec2_id]['PrivateIpAddress']

    def __cmp__(self, other):
        return cmp(self.ecs_id, other.ecs_id)

    def __repr__(self):
        return "%s (%s - %s)" % (self.ecs_id, self.ec2_id, self.availability_zone)


def prompt_for_instances(ecs_instances, asg_contents, scale_down=False):
    """
    sorts the instances into an order that tries not to cause an AZ imbalance
    when removing instances. Also, prompts the user if there are issues.
    @param ecs_instances: list of ECSInstance() objects
    @param asg_contents: dictionary of ec2_ids to availability zones in the ASG
    @param scale_down: bool if scale down or rollover
    @return: sorted list of ECSInstance() objects to remove
    """
    # ask the user which instances to remove:
    if scale_down:
        print "Which instances do you want to remove?"
    else:
        print "Which instances do you want to rollover?"

    for x, instance in enumerate(sorted(ecs_instances)):
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
        print instance

    asg_zones = set([az for az in asg_contents.values()])
    if max_diff > 1 or len(asg_zones) == 1:
        print "WARNING: The instances you selected will cause the auto scaling" \
              " group to rebalance instances across availability zones. This" \
              " will result in a destructive operation."

    confirm = raw_input("Do you want to continue [y/N]? ")
    if confirm.lower() != 'y':
        return []

    return ordered_instances


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
    selected_ecs_instances = prompt_for_instances(all_ecs_instances,
                                                  asg_contents,
                                                  args.scale_down)
    if not selected_ecs_instances:
        return True

    #
    # test ssh works before proceeding
    #
    sys.stdout.write("Testing ssh ...")
    sys.stdout.flush()
    if not ssh.test(selected_ecs_instances[0].ip_address):
        print "ERROR: Could not ssh into %s " % selected_ecs_instances[0].ec2_id
        print "You may need to configure your network and/or ssh settings to " \
              "allow for non-interactive access to your EC2 machines."
        return False
    print "done"

    #
    # Iterate through each instance
    #
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
        sys.stdout.write("De-registering instance from ECS...")
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

            sys.stdout.write("Removing instance from any service ELBs...")
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
            if not ssh.stop_all_containers(ip_address, args.timeout):
                print "FAILED"
                print "WARNING: Failed to stop all containers"
        print "done"

        #
        # Stop and terminate the EC2 instance
        #
        sys.stdout.write("Stopping and Terminating instance ...")
        sys.stdout.flush()
        if not args.dry_run:
            ec2_client.stop_and_wait_for_instances([ecs_instance.ec2_id])
            ec2_client.terminate_and_wait_for_instances([ecs_instance.ec2_id])
        print "done"
        print

    if args.scale_down:
        print "Scale down complete!"
    else:
        print "Rollover complete!"
    return True


def main_docker_stop(args):
    """
    Main entry point for the docker-stop command
    """
    ec2_client = ec2.EC2Client()
    info = ec2_client.describe_instances([args.ec2_id])
    return ssh.stop_all_containers(info[args.ec2_id]['PrivateIpAddress'],
                                   args.timeout)


def main_ssh_test(args):
    """
    Main entry point for the ssh-test command
    """
    ec2_client = ec2.EC2Client()
    info = ec2_client.describe_instances([args.ec2_id])
    return ssh.test(info[args.ec2_id]['PrivateIpAddress'])


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
    # ssh-test args
    #
    ssh_test_parser = subparsers.add_parser('ssh-test',
                                            help="Test connection to ec2 machine")
    ssh_test_parser.set_defaults(func=main_ssh_test)

    ssh_test_parser.add_argument('ec2_id',
                                 help="EC2 instance id")

    args = parser.parse_args()
    if not args.func(args):
        sys.exit(1)


if __name__ == "__main__":
    main()
