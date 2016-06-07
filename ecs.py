"""
module for interacting with ECS
"""

import boto3
import time

# local imports
import utils


class ECSError(Exception):
    """ECS API Error"""
    def __init__(self, arn, reason):
        err = "Query failed for %s: %s" % (arn, reason)
        Exception.__init__(self, err)


class ECSClient(object):
    """
    Client for interacting with ECS
    """
    def __init__(self, cluster):
        self.client = boto3.client('ecs')
        self.cluster = cluster

    def describe_instances(self, instance_ids):
        """
        @param instance_ids: list of ecs instance ids
        @return: dictionary of ecs instance ids to description dicts
        """
        info = {}

        # API is limited to 10 at a time
        for batch in utils.batch_list(10, instance_ids):
            resp = self.client.describe_container_instances(cluster=self.cluster,
                                                            containerInstances=batch)
            if resp.get('failures'):
                raise ECSError(resp['arn'], resp['reason'])

            for instance in resp['containerInstances']:
                instance_id = utils.pull_instance_id(instance['containerInstanceArn'])
                info[instance_id] = instance

        return info

    def describe_services(self, service_ids):
        """
        @param service_ids: list of ecs service ids
        @return: dictionary of ecs service ids to description dicts
        """
        info = {}

        # API is limited to 10 at a time
        for batch in utils.batch_list(10, service_ids):
            resp = self.client.describe_services(cluster=self.cluster,
                                                 services=batch)
            if resp.get('failures'):
                raise ECSError(resp['arn'], resp['reason'])

            for service in resp['services']:
                service_id = utils.pull_service_id(service['serviceArn'])
                info[service_id] = service

        return info

    def describe_tasks(self, task_arns):
        """
        @param task_arns: list of ecs task arns
        @return: dictionary of ecs task arns to descriptions dicts
        """
        info = {}

        # API is limited to 10 at a time
        for batch in utils.batch_list(10, task_arns):
            resp = self.client.describe_tasks(cluster=self.cluster,
                                              tasks=task_arns)
            if resp.get('failures'):
                raise ECSError(resp['arn'], resp['reason'])

            for task in resp['tasks']:
                task_arn = task['taskArn']
                info[task_arn] = task

        return info

    def deregister_container_instance(self, instance_id):
        """
        @param instance_id: single ecs instance id
        """
        # NOTE: force=True is used so that the task become orphaned and get
        #       rescheduled across the cluster
        self.client.deregister_container_instance(cluster=self.cluster,
                                                  containerInstance=instance_id,
                                                  force=True)

    def list_cluster_instances(self):
        """
        @return: list of ecs instance ids
        """
        arns = []

        paginator = self.client.get_paginator('list_container_instances')
        for resp in paginator.paginate(cluster=self.cluster):
            arns += resp['containerInstanceArns']
        return [utils.pull_instance_id(arn) for arn in arns]

    def list_services(self):
        """
        @return: list of ecs service ids
        """
        service_arns = []

        paginator = self.client.get_paginator('list_services')
        for resp in paginator.paginate(cluster=self.cluster):
            service_arns += resp['serviceArns']
        return [utils.pull_service_id(arn) for arn in service_arns]

    def list_tasks(self):
        """
        @return: list of ecs task arns
        """
        task_arns = []

        paginator = self.client.get_paginator('list_tasks')
        for resp in paginator.paginate(cluster=self.cluster):
            task_arns += resp['taskArns']
        return task_arns

    def wait_for_service_steady_state(self, service_id, last_event):
        """
        Blocks until the event stream shows a steady state message. Events
        before last_event are filtered out. Times out after 60sec
        @param service_id: ecs service id
        @param last_event: the last seen event from the ecs service
        """
        looped = 0
        for service in self.describe_services([service_id]).values():
            if looped > 6:
                # TIMEOUT
                break
            looped += 1

            time.sleep(10)
            for event in service['events']:
                if event['createdAt'] > last_event['createdAt']:
                    if "has reached a steady state" in event['message']:
                        return event
        return last_event
