"""
Generic helper utils
"""
import itertools


def batch_list(size, items):
    """
    chops a list into batches of `size`
    @param size: max size of each batch
    @param items: list of things to batch
    @return: list of batched lists
    """
    batches = []
    args = [iter(items)] * size
    for t in itertools.izip_longest(*args):
        batches.append([e for e in t if e is not None])
    return batches


def pull_instance_id(arn):
    """
    pulls the ecs instance id from the full arn
    """
    return arn.split('container-instance/', 1)[-1]


def pull_service_id(arn):
    """
    pulls the ecs service id from the full arn
    """
    return arn.split('service/', 1)[-1]


def pull_task_definition_name(arn):
    """
    pulls the ecs task definition name from the full arn
    """
    return arn.split('task-definition/', 1)[-1]
