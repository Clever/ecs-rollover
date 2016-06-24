"""
module for interacting with auto scaling groups
"""

import boto3
from operator import itemgetter
import time

# local imports
import utils


class AutoScalingGroup(object):
    """
    Client for interacting with a auto scaling group
    """
    def __init__(self, scaling_group):
        self.client = boto3.client('autoscaling')
        self.scaling_group = scaling_group

    def describe_instances(self):
        """
        @return: list of attached instance dicts
        """
        info = []
        paginator = self.client.get_paginator('describe_auto_scaling_groups')
        for resp in paginator.paginate(AutoScalingGroupNames=[self.scaling_group]):
            info += resp['AutoScalingGroups']

        # Only queried one ASG
        return info[0].get('Instances', [])

    def describe_scaling_activities(self):
        """
        @return: list of recent activities
        """
        activities = []

        paginator = self.client.get_paginator('describe_scaling_activities')
        for resp in paginator.paginate(AutoScalingGroupName=self.scaling_group):
            activities += resp['Activities']
        return activities

    def detach_instances(self, instance_ids, scale_down=False):
        """
        @param instance_ids: list of ec2 instance ids to detach
        @param scale_down: if true will not replace the instance
        @return: list of activities from detaching
        """
        resp = self.client.detach_instances(AutoScalingGroupName=self.scaling_group,
                                            InstanceIds=instance_ids,
                                            ShouldDecrementDesiredCapacity=scale_down)
        return resp['Activities']

    def detach_instances_and_wait(self, instance_ids):
        """
        detach instances and wait for their replacements to become ready
        @param instance_ids: list of ec2 instance ids to detach and replace
        @return: most recent activity
        """
        activities = self.detach_instances(instance_ids)
        activities.sort(key=itemgetter('StartTime'))
        self.wait_for_instance_launch(activities[-1], len(instance_ids))

    def wait_for_instance_launch(self, last_activity, count):
        """
        blocks until the activity stream shows `count` instances become ready.
        @param last_activity: the last seen activity. All older activities will be filtered out
        @param count: number of instances to wait for
        @return: most recent activity
        """
        new_activities = []
        TIMEOUT = 300
        started = time.time()
        while len(new_activities) < count:
            if time.time() - started > TIMEOUT:
                return last_activity

            time.sleep(10)
            for activity in self.describe_scaling_activities():
                if activity['StartTime'] > last_activity['StartTime']:
                    if activity['Progress'] == 100:
                        new_activities.append(activity)

        return sorted(new_activities, key=itemgetter('StartTime'))[-1]
