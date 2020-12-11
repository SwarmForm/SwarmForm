from swarmform.core.swarm_dag import Node, DAG
from swarmform.util.resource_wastage import calc_wastage


def get_tasks_at_level(workflow, level):
    """
    return the tasks at given level

    Args:
        workflow (DAG): DAG object of workflow
        level (int): level which output tasks needed to be

    Returns:
        list(Node)
    """
    tasks = []
    for task in workflow.get_nodes():
        if workflow.get_nodes()[task].get_level() == level:
            tasks.append(workflow.get_nodes()[task])
    return tasks


def sort_tasks_by_exec_time_in_decreasing(tasks):
    """
    Sort tasks in descending order by the execution time

    Args:
        tasks (list(Node)

    Returns:
        list(Node)
    """
    n = len(tasks)
    for i in range(n):
        for j in range(0, n - i - 1):
            if tasks[j].get_exec_time() < tasks[j + 1].get_exec_time():
                tasks[j], tasks[j + 1] = tasks[j + 1], tasks[j]
    return tasks


def get_sum_of_exec_time(cluster):
    """
    Returns the sum of execution time from a given list of tasks

    Args:
        cluster (list(Node))

    Returns:
        int
    """
    exec_sum = 0
    for task in cluster:
        exec_sum += task.get_exec_time()
    return exec_sum


def cluster_wf_in_hrab(workflow, cluster_num):
    """
        Returns the clustered workflow using HRAB algorithm

        Args:
            cluster_num: number of cluster in a level
            workflow (DAG)

        Returns:
            workflow (DAG)
    """
    for level in range(1, workflow.get_height() + 1):
        tasks = get_tasks_at_level(workflow, level)
        if len(tasks) > cluster_num:
            clusters = []
            for i in range(1, cluster_num + 1):
                clusters.append(Node(-i * level, level, {}, parents=[], children=[]))
            cluster_size = int(len(tasks) / cluster_num)
            if (cluster_size * cluster_num) < len(tasks):
                cluster_size += 1
            sort_tasks_by_exec_time_in_decreasing(tasks)
            normalize_cores(tasks)
            normalize_runtime(tasks)
            avg_runtime = get_average_runtime(tasks)
            avg_cores = get_average_cores(tasks)
            for task in tasks:
                candidate_task = get_candidate_cluster(clusters, task, cluster_size, avg_cores, avg_runtime)
                candidate_task.set_cluster_node(task)
            for cluster in clusters:
                update_cluster_info(cluster)
                update_fw_info(cluster)
            update_parent_child_relationships(clusters)
            for cluster in clusters:
                sequential_ids = []
                for task in cluster.get_cluster_tasks():
                    workflow.delete_node(task.get_fw_id())
                    sequential_ids.append(task.get_fw_id())
                cluster.set_sequential_ids(sequential_ids)
                cluster.set_fw_id(task.get_fw_id())
                workflow.add_node(task.get_fw_id(), cluster)
    workflow.update_links()

    calc_wastage(workflow, 'hrab')
    return workflow


def get_max_cores(tasks):
    max_cores = 0
    for task in tasks:
        if task.get_num_cores() > max_cores:
            max_cores = task.get_num_cores()
    return max_cores


def get_min_cores(tasks):
    min_cores = tasks[0].get_num_cores()
    for task in tasks:
        if task.get_num_cores() < min_cores:
            min_cores = task.get_num_cores()
    return min_cores


def get_max_runtime(tasks):
    max_runtime = 0
    for task in tasks:
        if task.get_exec_time() > max_runtime:
            max_runtime = task.get_exec_time()
    return max_runtime


def get_min_runtime(tasks):
    min_runtime = tasks[0].get_exec_time()
    for task in tasks:
        if task.get_exec_time() < min_runtime:
            min_runtime = task.get_exec_time()
    return min_runtime


def normalize_cores(tasks):
    max_cores = get_max_cores(tasks)
    min_cores = get_min_cores(tasks)
    for task in tasks:
        normalized_cores = (task.get_num_cores() - min_cores) / ((max_cores - min_cores) + 0.0001)
        task.set_normalized_cores(normalized_cores)


def normalize_runtime(tasks):
    max_runtime = get_max_runtime(tasks)
    min_runtime = get_min_runtime(tasks)
    for task in tasks:
        normalized_runtime = (task.get_exec_time() - min_runtime) / ((max_runtime - min_runtime) + 0.0001)
        task.set_normalized_runtime(normalized_runtime)


def get_average_runtime(tasks):
    total = 0
    for task in tasks:
        total += task.get_normalized_runtime()
    return total / len(tasks)


def get_average_cores(tasks):
    total = 0
    for task in tasks:
        total += task.get_normalized_cores()
    return total / len(tasks)


def get_total_runtime(cluster, avg_runtime):
    total_runtime = avg_runtime
    if bool(cluster.get_cluster_tasks()):
        total_runtime = 0
        for task in cluster.get_cluster_tasks():
            total_runtime += task.get_normalized_runtime()
    return total_runtime


def get_max_cores_in_cluster(cluster, avg_cores):
    max_cores = avg_cores
    if bool(cluster.get_cluster_tasks()):
        max_cores = 0
        for task in cluster.get_cluster_tasks():
            if task.get_normalized_cores() > max_cores:
                max_cores = task.get_normalized_cores()
    return max_cores


def get_clustering_factors(clusters, task, avg_cores, avg_runtime):
    factor_map = {}
    for cluster in clusters:
        factor = (0.1 + abs(task.get_normalized_cores() - get_max_cores_in_cluster(cluster, avg_cores))) * (
                0.1 + get_total_runtime(cluster, avg_runtime))
        if factor not in factor_map:
            factor_map[factor] = []
        c_list = factor_map.get(factor)
        if cluster not in c_list:
            c_list.append(cluster)
    return factor_map


def get_candidate_cluster(clusters, task, cluster_size, avg_cores, avg_runtime):
    factors = get_clustering_factors(clusters, task, avg_cores, avg_runtime)
    fac_values = list(factors.keys())
    fac_values.sort()
    for fac_value in fac_values:
        potential_clusters = factors[fac_value]
        for potential_cluster in potential_clusters:
            if len(potential_cluster.get_cluster_tasks()) < cluster_size:
                return potential_cluster


def update_parent_child_of_a_task(cluster, task):
    # update parents of task
    if bool(task.get_parents()):
        for parent in task.get_parents():
            if not is_parent_already_assigned(cluster, parent.get_fw_id()):
                cluster.add_parent(parent)
            parent.remove_child(task.get_fw_id())
            if not is_child_already_assigned(parent, cluster.get_fw_id()):
                parent.add_child(cluster)

    # update children of task
    if bool(task.get_children()):
        for child in task.get_children():
            if not is_child_already_assigned(cluster, child.get_fw_id()):
                cluster.add_child(child)
            child.remove_parent(task.get_fw_id())
            if not is_parent_already_assigned(child, cluster.get_fw_id()):
                child.add_parent(cluster)


def update_parent_child_relationships(clusters):
    for cluster in clusters:
        if len(cluster.get_cluster_tasks()) > 0:
            for task in cluster.get_cluster_tasks():
                update_parent_child_of_a_task(cluster, task)


def update_cluster_info(cluster):
    cluster_info = {}
    for task in cluster.get_cluster_tasks():
        cores = task.get_num_cores()
        runtime = task.get_exec_time()
        fw_info = {'exec_time': runtime, 'cores': cores}
        cluster_info[task.get_fw_id()] = fw_info
    cluster.set_cluster_info(cluster_info)


def update_fw_info(cluster):
    cores = 0
    runtime = 0
    for task in cluster.get_cluster_tasks():
        runtime += task.get_exec_time()
        if cores < task.get_num_cores():
            cores = task.get_num_cores()
    cluster.set_fw_info(runtime, cores)


def is_parent_already_assigned(task, parent_id):
    """
    Checks whether the given node is already assigned as parent or not

    Args:
        task (Node)
        parent_id (fw_id)

    Returns:
        int
    """
    parents = task.get_parents()
    for parent in parents:
        if parent.get_fw_id() == parent_id:
            return True
    return False


def is_child_already_assigned(task, child_id):
    """
    Checks whether the given node is already assigned as child or not

    Args:
        task (Node)
        child_id (fw_id)

    Returns:
        int
    """
    children = task.get_children()
    for child in children:
        if child.get_fw_id() == child_id:
            return True
    return False

