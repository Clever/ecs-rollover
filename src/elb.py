#! /usr/bin/env python
"""
module for interacting with Elastic Load Balancers (ELBs)
"""

import boto3
import sys


def load_balancers_with_instance(ec2_id):
    """
    @param ec2_id: ec2 instance id
    @return: list of elb names with the ec2 instance attached
    """
    elbs = []
    client = boto3.client('elb')
    paginator = client.get_paginator('describe_load_balancers')
    for resp in paginator.paginate():
        for elb in resp['LoadBalancerDescriptions']:
            # filter for ec2_instance
            ec2_ids = [i['InstanceId'] for i in elb['Instances']]
            if ec2_id in ec2_ids:
                elbs.append(elb['LoadBalancerName'])
    return elbs


class ELBClient(object):
    """
    Client for interacting with Elastic Load Balancers
    """
    def __init__(self, elb_name):
        self.elb_name = elb_name
        self.client = boto3.client('elb')

    def deregister_instances(self, instance_ids):
        """
        @param instance_ids: list of ec2 instance ids
        @return: list of remaining instances still attached
        """
        elb_instances = [{'InstanceId': i} for i in instance_ids]
        resp = self.client.deregister_instances_from_load_balancer(LoadBalancerName=self.elb_name,
                                                                   Instances=elb_instances)
        return [i['InstanceId'] for i in resp['Instances']]


def main_detach(args):
    """
    Main entry point for detach command
    """
    if args.load_balancer_name:
        load_balancers = args.load_balancer_name
    else:
        # query for load balancers with this ec2 instance
        load_balancers = load_balancers_with_instance(args.ec2_id)

    for load_balancer in load_balancers:
        sys.stdout.write("Detaching from %s ..." % (load_balancer))
        sys.stdout.flush()
        elb_client = ELBClient(load_balancer)
        elb_client.deregister_instances([args.ec2_id])
        print "done"
