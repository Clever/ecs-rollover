"""
module for interacting with EC2
"""

import boto3


class EC2Client(object):
    """
    Client for interacting with EC2
    """
    def __init__(self):
        self.client = boto3.client('ec2')

    def describe_instances(self, ec2_ids):
        """
        @param ec2_ids: list of ec2 instance ids
        @return: dictionary of ec2 instance ids to description dicts
        """
        instances = {}

        paginator = self.client.get_paginator('describe_instances')
        for resp in paginator.paginate(DryRun=False, InstanceIds=ec2_ids):
            for reservation in resp['Reservations']:
                for instance in reservation['Instances']:
                    instance_id = instance['InstanceId']
                    instances[instance_id] = instance

        return instances

    def stop_and_wait_for_instances(self, ec2_ids):
        """
        @param ec2_ids: list of ec2 instance ids
        """
        self.stop_instances(ec2_ids)
        self.wait_for_stopped(ec2_ids)

    def stop_instances(self, ec2_ids):
        """
        @param ec2_ids: list of ec2 instance ids
        """
        self.client.stop_instances(DryRun=False, InstanceIds=ec2_ids)

    def terminate_and_wait_for_instances(self, ec2_ids):
        """
        @param ec2_ids: list of ec2 instance ids
        """
        self.terminate_instances(ec2_ids)
        self.wait_for_terminated(ec2_ids)

    def terminate_instances(self, ec2_ids):
        """
        @param ec2_ids: list of ec2 instance ids
        """
        self.client.terminate_instances(DryRun=False, InstanceIds=ec2_ids)

    def wait_for_stopped(self, ec2_ids):
        """
        @param ec2_ids: list of ec2 instance ids
        """
        waiter = self.client.get_waiter('instance_stopped')
        waiter.wait(DryRun=False, InstanceIds=ec2_ids)

    def wait_for_terminated(self, ec2_ids):
        """
        @param ec2_ids: list of ec2 instance ids
        """
        waiter = self.client.get_waiter('instance_terminated')
        waiter.wait(DryRun=False, InstanceIds=ec2_ids)
