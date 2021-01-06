from fireworks.utilities.fw_utilities import get_fw_logger


def calc_wastage(workflow, algo):
    """
    Calculate the resource wastage after clustering

    Args:
        workflow (DAG): Clustered workflow DAG
        algo (String): name of the clustering algorithm ('wpa' or 'hrab'/'rac')
    """

    total_wastage = 0
    clusters = workflow.get_nodes()
    for cluster in clusters.values():
        tasks = cluster.get_cluster_tasks()
        if len(tasks) > 0:
            max_cores = get_max_cores(tasks)
            for task in tasks:
                total_wastage += task.get_exec_time() * (max_cores - task.get_num_cores())

    logger = get_fw_logger(name='wastage', stream_level='INFO')
    logger.info('Total Resource Wastage in {}: {}'.format(algo.upper(), total_wastage))


def get_max_cores(tasks):
    max_cores = 0
    if len(tasks) > 0:
        for task in tasks:
            task_cores = task.get_num_cores()
            if task_cores > max_cores:
                max_cores = task_cores
    return max_cores
