from swarmform.core.swarm_dag import Node, DAG
from random import randrange
from swarmform.util.resource_wastage import calc_wastage
from copy import deepcopy


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


def get_longest_parent(task):

    """
    Returns the parent of the given task which has the longest execution time

    Args:
        task (Node)

    Returns:
        Node
    """
    longest_parent = task.get_parents()[0]
    for parent in task.get_parents():
        if longest_parent.get_exec_time() < parent.get_exec_time():
            longest_parent = parent
    return longest_parent


def sort_tasks_by_longest_parent(tasks):

    """
    Sort the list of tasks considering the longest parents of the tasks

    Args:
        tasks (list(Node))

    Returns:
        list(Node)
    """
    n = len(tasks)
    if len(tasks) == 1:
        return tasks
    for i in range(n):
        for j in range(0, n - i - 1):
            if get_longest_parent(tasks[j]).get_exec_time() > get_longest_parent(tasks[j + 1]).get_exec_time():
                tasks[j], tasks[j + 1] = tasks[j + 1], tasks[j]
    return tasks


def sort_tasks_by_exec_time(tasks):

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
            if tasks[j].get_exec_time() < tasks[j+1].get_exec_time():
                tasks[j], tasks[j + 1] = tasks[j + 1], tasks[j]
    return tasks


def get_unassigned_parents(tasks):

    """
    Returns a list of unassigned tasks from a given list of tasks

    Args:
        tasks (list(Node))

    Returns:
        list(Node)
    """
    parents = []
    for parent in tasks.get_parents():
        if not parent.get_is_assigned():
            parents.append(parent)
    return parents


def get_sum_0f_exec_time(cluster):

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


def assign_parent_to_clusters(task):

    """
    Assign parent tasks of a given node to clusters

    Args:
        task (Node)

    Returns:
        list of clustered Nodes
    """
    cls = []
    c = 0
    # Keep the nodes which are about to cluster
    cluster_c = []
    # Longest parent of the node
    longest_parent = get_longest_parent(task)
    # Level of the nodes
    c_level = longest_parent.get_level()
    max_run_time = 0
    # Check the longest parent is clustered or not. If it is not clustered set the maximum runtime
    if not longest_parent.get_is_assigned():
        c += 1
        cluster_c.append(longest_parent)
        max_run_time = longest_parent.get_exec_time()
        longest_parent._is_assigned = True
    # Get the unassigned parents of node
    par_list = get_unassigned_parents(task)
    if len(par_list) > 0:
        # Sort the unassigned parent tasks in descending order
        par_list = sort_tasks_by_exec_time(par_list)
        c += 1
        cluster_c = []
        # keep the information of the cluster
        cls_info = {}
        # iterate all the unassigned tasks to cluster
        while len(par_list) > 0:
            task = par_list.pop()
            # Check the sum of execution time current cluster and current node is less than or
            # equal to the maximum run time. # If it gets true assign the current task to the cluster
            if (get_sum_0f_exec_time(cluster_c) + task.get_exec_time() <= max_run_time) and len(cluster_c) < 2:
                cluster_c.append(task)
                cls_info[task.get_fw_id()] = {'exec_time': task.get_exec_time(), 'cores': task.get_num_cores()}
                # Mark the task as assigned
                task.isAssigned = True
            else:
                # If the sum of execution time current cluster and current node is larger than the maximum run time.
                # Stop adding node to the current cluster.
                if (len(cls_info)) > 1:  # check the current cluster has more than one node
                    # Create a DAG Node object embedding the current clustered nodes.
                    # Set the fw_id of the first node as the fw_id of the clustered node and
                    # set the sum of execution times of all the nodes as cluster execution time and
                    # maximum number of cores as the cluster required cores.
                    core_space = 0
                    sequential_ids = []
                    first_cluster_node = cluster_c[0]
                    second_cluster_node = cluster_c[1]
                    # Set the task which has maximum number of nodes to the begging of the sequential list
                    if first_cluster_node.get_num_cores() >= second_cluster_node.get_num_cores():
                        core_space = first_cluster_node.get_num_cores()-second_cluster_node.get_num_cores()
                        sequential_ids = [first_cluster_node.get_fw_id(), second_cluster_node.get_fw_id()]
                    elif first_cluster_node.get_num_cores() < second_cluster_node.get_num_cores():
                        core_space = second_cluster_node.get_num_cores() - first_cluster_node.get_num_cores()
                        sequential_ids = [second_cluster_node.get_fw_id(), first_cluster_node.get_fw_id()]

                    cluster = Node(fw_id=sequential_ids[0], level=c_level,
                                   fw_info={'exec_time': get_sum_0f_exec_time(cluster_c),
                                            'cores': cls_info[sequential_ids[0]]['cores']},
                                   parents=[], children=[], assigned=True)
                    cluster.set_cluster_info(cls_info)
                    cluster.set_sequential_ids(sequential_ids)
                    # Set the information about the cluster space available in the cluster to fit other nodes
                    # to reduce the resource utilization.
                    cluster.set_cluster_space([cls_info[cluster.get_fw_ids_to_cluster_sequentially()[1]]['exec_time'],
                                               core_space])
                    # Add the children to new clustered node
                    for tsk in cluster_c:
                        cluster.set_cluster_node(tsk)
                        children = tsk.get_children()
                        if children is None:
                            continue
                        for child in children:
                            if not is_child_already_assigned(cluster, child.get_fw_id()):
                                cluster.add_child(child)
                    # Add the parents to the new clustered node
                    for tsk in cluster_c:
                        parents = tsk.get_parents()
                        if parents is None:
                            continue
                        for parent in parents:
                            if not is_parent_already_assigned(cluster, parent.get_fw_id()):
                                cluster.add_parent(parent)
                    # Add clustered node as the parent of the children of the clustered tasks
                    for tsk in cluster_c:
                        children = tsk.get_children()
                        if children is None:
                            continue
                        for child in children:
                            child.remove_parent(tsk.get_fw_id())
                            if not is_parent_already_assigned(child, cluster.get_fw_id()):
                                child.add_parent(cluster)
                    # Add clustered node as the children of the parents of the clustered tasks
                    for tsk in cluster_c:
                        parents = tsk.get_parents()
                        if parents is None:
                            continue
                        for parent in parents:
                            parent.remove_child(tsk.get_fw_id())
                            if not is_child_already_assigned(parent, cluster.get_fw_id()):
                                parent.add_child(cluster)
                    cls.append(cluster)
                    # Sets a new cluster
                    cluster_c = []
                    cls_info = {}
                    c += 1
                    cluster_c.append(task)
                    cls_info[task.get_fw_id()] = {'exec_time': task.get_exec_time(), 'cores': task.get_num_cores()}
                    task.isAssigned = True
            # Handle the last task of the list
            if len(par_list) == 0:
                if (len(cls_info)) > 1:
                    core_space = 0
                    sequential_ids = []
                    first_cluster_node = cluster_c[0]
                    second_cluster_node = cluster_c[1]
                    # Set the task which has maximum number of nodes to the begging of the sequential list
                    if first_cluster_node.get_num_cores() >= second_cluster_node.get_num_cores():
                        core_space = first_cluster_node.get_num_cores() - second_cluster_node.get_num_cores()
                        sequential_ids = [first_cluster_node.get_fw_id(), second_cluster_node.get_fw_id()]
                    elif first_cluster_node.get_num_cores() < second_cluster_node.get_num_cores():
                        core_space = second_cluster_node.get_num_cores() - first_cluster_node.get_num_cores()
                        sequential_ids = [second_cluster_node.get_fw_id(), first_cluster_node.get_fw_id()]

                    cluster = Node(fw_id=sequential_ids[0], level=c_level,
                                   fw_info={'exec_time': get_sum_0f_exec_time(cluster_c),
                                            'cores': cls_info[sequential_ids[0]]['cores']},
                                   parents=[], children=[], assigned=True)
                    cluster.set_cluster_info(cls_info)
                    cluster.set_sequential_ids(sequential_ids)
                    # Set the information about the cluster space available in the cluster to fit other nodes to
                    # reduce the resource utilization.
                    cluster.set_cluster_space(
                        [cls_info[cluster.get_fw_ids_to_cluster_sequentially()[1]]['exec_time'], core_space])
                    # Add the children to new clustered node
                    for tsk in cluster_c:
                        cluster.set_cluster_node(tsk)
                        children = tsk.get_children()
                        if children is None:
                            continue
                        for child in children:
                            if not is_child_already_assigned(cluster, child.get_fw_id()):
                                cluster.add_child(child)
                    for tsk in cluster_c:
                        parents = tsk.get_parents()
                        if parents is None:
                            continue
                        for parent in parents:
                            if not is_parent_already_assigned(cluster, parent.get_fw_id()):
                                cluster.add_parent(parent)
                    for tsk in cluster_c:
                        children = tsk.get_children()
                        if children is None:
                            continue
                        for child in children:
                            child.remove_parent(tsk.get_fw_id())
                            if not is_parent_already_assigned(child, cluster.get_fw_id()):
                                child.add_parent(cluster)
                    for tsk in cluster_c:
                        parents = tsk.get_parents()
                        if parents is None:
                            continue
                        for parent in parents:
                            parent.remove_child(tsk.get_fw_id())
                            if not is_child_already_assigned(parent, cluster.get_fw_id()):
                                parent.add_child(cluster)
                    cls.append(cluster)
                break
    return cls


def resource_balance(clusters_at_level, tasks, wf):

    """
    Perform resource balancing based on available resource on clustered nodes and update the workflow

    Args:
        wf (DAG)
        clusters_at_level (list(Node): Clustered nodes at a given level
        tasks (list(Node)): Nodes at a given level

    """
    # Iterate the task in level with the all the clustered nodes in the level and
    # check whether is there any available space to fit in the clustered nodes
    for task in tasks:
        parents = task.get_parents()
        for parent in parents:
            # If the parent task is already clustered. Skip
            if len(parent.get_cluster_info()) > 1:
                continue
            # If the clustered task is already filled. Skip
            for cluster in clusters_at_level:
                if bool(cluster.get_fw_ids_to_cluster_parallely()):
                    continue
                # Check the clustered node has space to fit a unclustered task.
                # Task should have less or equal number of core requirement and execution time.
                if cluster.get_cluster_space()[0] >= parent.get_exec_time() and cluster.get_cluster_space()[1] >= parent.get_num_cores():
                    p_parents = parent.get_parents()
                    p_children = parent.get_children()
                    for p_parent in p_parents:
                        # remove the node as child from node's parent node
                        p_parent.remove_child(parent.get_fw_id())
                        # remove the parents from node
                        parent.remove_parent(p_parent.get_fw_id())
                        # Assign the parents of the nodes to the new clustered nodes
                        if not is_parent_already_assigned(cluster, p_parent.get_fw_id()):
                            cluster.add_parent(p_parent)
                        # Assign new cluster as the child of parent nodes of the task
                        if not is_child_already_assigned(p_parent, cluster.get_fw_id()):
                            p_parent.add_child(cluster)
                    for p_child in p_children:
                        # remove the node as parent from node's child node
                        p_child.remove_parent(parent.get_fw_id())
                        # remove the children from node
                        parent.remove_child(p_child.get_fw_id())
                        # Assign the children of the nodes to the new clustered nodes
                        if not is_child_already_assigned(cluster, p_child.get_fw_id()):
                            cluster.add_child(p_child)
                        # Assign new cluster as the parent of child nodes of the task
                        if not is_parent_already_assigned(p_child, cluster.get_fw_id()):
                            p_child.add_parent(cluster)
                    # Delete the node from the WK
                    wf.delete_node(parent.get_fw_id())
                    # Assign a minus key to the refer the parallel running jobs
                    key = -1*randrange(1, 100)
                    # Set parallel nodes to the cluster
                    cluster.set_parallel_ids([cluster.get_fw_ids_to_cluster_sequentially()[1],parent.get_fw_id()], key)
                    # Set sequentially running nodes to the cluster
                    cluster.set_sequential_ids([cluster.get_fw_ids_to_cluster_sequentially()[0], key])


def wpa_clustering(workflow):

    """
    WPA clustering alogirthm

    Args:
        workflow (DAG)

    Returns:
        Clustered workflow DAG (DAG)
    """
    # Iterate the WF level by level
    for level in range(workflow.get_height(), 1, -1):
        cls_at_level = []
        # Get the tasks at level
        tasks_at_level = get_tasks_at_level(workflow, level)
        # Sort the tasks by longest parent of the tasks
        tasks_at_level_sorted = sort_tasks_by_longest_parent(tasks_at_level)
        # Iterate the tasks of the level
        for task in tasks_at_level_sorted:
            # Assign parents of the task to the clusters
            cls = assign_parent_to_clusters(task)
            for cluster in cls:
                cls_info = cluster.get_cluster_info()
                temp_wfid = cluster.get_fw_id()
                for wfid in cls_info:
                    # Delete the tasks which are clustered from the WF
                    workflow.delete_node(wfid)
                # Add new clustered node to the WF
                workflow.add_node(temp_wfid, cluster)
            for cl in cls:
                cls_at_level.append(cl)
        # Resource balance
        resource_balance(cls_at_level, tasks_at_level_sorted, workflow)
    workflow.update_links()

    calc_wastage(workflow, 'wpa')
    return workflow


def cluster_vertically(workflow):
    for level in range(1, workflow.get_height()):
        tsk_at_level = get_tasks_at_level(workflow, level)
        for task in tsk_at_level:
            m_task = task
            cluster_c = []
            cluster_c.append(task)
            cls_info = {}
            cls_info[task.get_fw_id()] = {'exec_time': task.get_exec_time(), 'cores': task.get_num_cores()}
            while(True):
                if task.get_children() is None:
                    break
                if len(task.get_children()) == 1:
                    child = task.get_children()[0]
                    if len(child.get_parents()) == 1:
                        cls_info[child.get_fw_id()] = {'exec_time': child.get_exec_time(), 'cores': child.get_num_cores()}
                        cluster_c.append(child)
                        task = child
                    else:
                        break
                else:
                    break
            if len(cls_info) > 1:
                # print(cls_info)
                sequential_id = []
                cluster = Node(fw_id=m_task.get_fw_id(), level=level,
                               fw_info={'exec_time': get_sum_0f_exec_time(cluster_c),
                                        'cores': cls_info[task.get_fw_id()]['cores']},
                               parents=[], children=[], assigned=False)
                cluster.set_cluster_info(cls_info)
                for cls in cluster_c:
                    sequential_id.append(cls.get_fw_id())
                    cluster.set_cluster_node(cls)
                cluster.set_sequential_ids(sequential_id)
                if not m_task.get_parents() is None:
                    for parent in m_task.get_parents():
                        cluster.add_parent(parent)
                if not cluster_c[-1].get_children() is None:
                    for child in cluster_c[-1].get_children():
                        child.remove_parent(cluster_c[-1].get_fw_id())
                        child.add_parent(cluster)
                        cluster.add_child(child)
                for tsk in cluster_c:
                    workflow.delete_node(tsk.get_fw_id())
                workflow.add_node(m_task.get_fw_id(), cluster)
                workflow.update_links()
    return workflow

