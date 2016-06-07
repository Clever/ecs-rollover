"""
module for interacting with Elastic Load Balancers (ELBs)
"""

import boto3


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
