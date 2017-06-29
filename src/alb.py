#! /usr/bin/env python
"""
module for interacting with Application Load Balancers (ALBs)
"""

import boto3
import sys


class ALBGroup(object):
    def __init__(self, arn, albs, targets):
        self.arn = arn
        self.albs = albs
        self.targets = targets

        self.client = boto3.client('elbv2')

    def deregister_targets(self, instance_ids):
        """
        @param instance_ids: list of ec2 instance ids
        @return: true if successful, false otherwise
        """
        targets = [{'Id': i} for i in instance_ids]
        resp = self.client.deregister_targets(TargetGroupArn=self.arn,
                                              Targets=targets)
        status_code = resp.get('ResponseMetadata', {}).get('HTTPStatusCode')
        return status_code == 200


class _ALBCache_(object):
    def __init__(self):
        self.target_groups = {}
        client = boto3.client('elbv2')
        paginator = client.get_paginator('describe_target_groups')
        for resp in paginator.paginate():
            for group in resp['TargetGroups']:
                health = client.describe_target_health(TargetGroupArn=group['TargetGroupArn'])
                targets = []
                for details in health['TargetHealthDescriptions']:
                    targets.append(details['Target']['Id'])

                self.target_groups[group['TargetGroupArn']] = ALBGroup(group['TargetGroupArn'], group['LoadBalancerArns'], targets)


ALBCache = None


def NewALBGroup(arn):
    global ALBCache
    if not ALBCache:
        ALBCache = _ALBCache_()

    return ALBCache.target_groups[arn]


def target_group_arns_with_instance(ec2_id):
    """
    @param ec2_id: ec2 instance id
    @return: list of alb arns with the ec2 instance attached
    """
    global ALBCache
    if not ALBCache:
        ALBCache = _ALBCache_()

    group_arns = set()
    for group in ALBCache.target_groups.values():
        if ec2_id in group.targets:
            group_arns.add(group.arn)

    return list(group_arns)


def main_detach(args):
    """
    Main entry point for detach command
    """
    global ALBCache
    if not ALBCache:
        ALBCache = _ALBCache_()

    if args.target_group_arn:
        target_group_arns = args.target_group_arn
    else:
        # query for load balancers with this ec2 instance
        target_group_arns = target_group_arns_with_instance(args.ec2_id)

    target_groups = [ALBCache.target_groups[arn] for arn in target_group_arns]

    for target_group in target_groups:
        # print target_group.arn, target_group.albs
        sys.stdout.write("Detaching from target_group %s ..." % (target_group.arn))
        sys.stdout.flush()
        target_group.deregister_targets([args.ec2_id])
        print "done"
