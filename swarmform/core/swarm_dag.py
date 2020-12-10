class Node:

    def __init__(self, fw_id, level, fw_info, assigned=False, parents=None, children=None):

        """
        Args:
            fw_id (int): an identification number for this Firework.
            level (int): depth of the node from the entry node.
            fw_info (dict): contains info like execution time in seconds, required number of cores of a node(job).
                            eg: fw_info = {'exec_time': 10, 'cores': 5}
            assigned (boolean): indicates whether this node is assigned to a cluster or not
            parents (Node or [Node]): list of parent nodes this node depends on.
            children (Node or [Node]): list of children nodes depend on this node.
        """

        self._fw_id = fw_id
        self._level = level
        self._fw_info = fw_info
        self._is_assigned = assigned
        self._parents = parents
        self._children = children
        self._cluster_info = {fw_id: fw_info}
        self._sequential_ids = []
        self._parallel_ids = {}
        self._cluster_space = []
        self._normalized_runtime = 0
        self._normalized_cores = 0
        self._cluster_tasks = []

    def set_cluster_node(self, task):
        self._cluster_tasks.append(task)

    def set_normalized_runtime(self, runtime):
        self._normalized_runtime = runtime

    def set_normalized_cores(self, cores):
        self._normalized_cores = cores

    def set_cluster_space(self, space):
        self._cluster_space = space

    def get_cluster_space(self):
        return self._cluster_space

    def set_sequential_ids(self, id_list):
        self._sequential_ids = id_list

    def get_fw_ids_to_cluster_sequentially(self):
        return self._sequential_ids

    def set_parallel_ids(self, id_list, key):
        self._parallel_ids[key] = id_list

    def set_fw_id(self, fw_id):
        self._fw_id = fw_id

    def get_fw_ids_to_cluster_parallely(self):
        return self._parallel_ids

    def get_fw_id(self):
        return self._fw_id

    def get_level(self):
        return self._level

    def get_normalized_cores(self):
        return self._normalized_cores

    def get_normalized_runtime(self):
        return self._normalized_runtime

    def get_exec_time(self):
        if self._fw_info:
            return self._fw_info['exec_time']
        else:
            return None

    def get_num_cores(self):
        if self._fw_info:
            return self._fw_info['cores']
        else:
            return None

    def get_is_assigned(self):
        return self._is_assigned

    def get_parents(self):
        return self._parents

    def get_children(self):
        return self._children

    def get_cluster_info(self):
        return self._cluster_info

    def get_cluster_tasks(self):
        return self._cluster_tasks

    def set_cluster_info(self, c_info):
        """
        Args:
            c_info (dict): a dictionary of cluster information containing {fw_id: fw_info}
        """
        self._cluster_info = c_info

    def get_fw_ids_to_cluster(self):
        fw_ids_to_cluster = self._cluster_info.keys()
        return list(fw_ids_to_cluster)

    def add_parent(self, parent):

        if not isinstance(self._parents, list):
            self._parents = []

        self._parents.append(parent)

    def add_child(self, child):

        if not isinstance(self._children, list):
            self._children = []

        self._children.append(child)

    def remove_parent(self, parent_id):

        for parent in self._parents:
            if parent.get_fw_id() == parent_id:
                self._parents.remove(parent)
                break

    def remove_child(self, child_id):

        for child in self._children:
            if child.get_fw_id() == child_id:
                self._children.remove(child)
                break

    def set_fw_info(self, exec_time, cores):
        self._fw_info = {'exec_time': exec_time, 'cores': cores}


class DAG:

    def __init__(self, sf):

        """
        Args:
            sf(SwarmFlow): SwarmFlow object
        """

        self._dag_id = sf.sf_id
        self._dag_name = sf.name or "Unnamed-SF"
        self._nodes = {}  # dictionary in format of {fw_id: Node}
        parents_dict = {}

        fireworks = sf.fws
        self._links = sf.links  # dictionary in the format of {parent_id:[child_ids]}
        metadata = sf.metadata  # dictionary in the format of {fw_id: [exec_time, cores]}
        self._costs = {}  # dictionary in the format of {fw_id: [param1, param2]}
        # Set parameters in _queueadapter as cost parameters
        for fw in fireworks:
            if fw.spec.get('_queueadapter', None):
                params = fw.spec['_queueadapter']
                self._costs.update({str(fw.fw_id): params})

        # creating Nodes and adding to the _nodes dictionary
        self._height = 0
        for fw in fireworks:
            fw_id = fw.fw_id
            level = self.find_node_level(fw_id)
            fw_info = self._costs[str(fw_id)] if str(fw_id) in self._costs else {}
            node = Node(fw_id=fw_id, level=level, fw_info=fw_info)
            if fw_id in self._nodes or fw_id in parents_dict:
                raise ValueError('FW ids must be unique!')
            self._nodes[fw_id] = node
            if level > self._height:
                self._height = level

        # setting parents and children
        for parent_id in self._links:
            parent_node = self._nodes[parent_id]
            children = self._links[parent_id]
            for child_id in children:
                child_node = self._nodes[child_id]
                parent_node.add_child(child_node)
                child_node.add_parent(parent_node)

    def get_dag_id(self):
        return self._dag_id

    def get_dag_name(self):
        return self._dag_name

    def get_height(self):
        return self._height

    def get_nodes(self):
        return self._nodes

    def get_costs(self):
        return self._costs

    def get_parent_child_relationships(self):
        return self._links

    def find_all_paths(self, start, end, path=[]):
        """
        Args:
            start (int): starting node id
            end (int): ending node id
            path (list): list of node ids from starting node to ending node

        Returns:
            paths (list[list]): 2D list containing all the paths to ending node
        """
        graph = self._links
        path = path + [start]
        if start == end:
            return [path]
        if start not in graph.keys():
            return []
        paths = []
        for node in graph[start]:
            if node not in path:
                new_paths = self.find_all_paths(node, end, path)
                for new_path in new_paths:
                    paths.append(new_path)

        return paths

    def all_paths_from_roots(self, node_id):
        """
        Args:
            node_id (int): id of the node which needs to be traversed

        Returns:
            paths (list[list]): 2D list containing all the paths from all root nodes to given node
        """
        graph = self._links
        paths = []
        for key in graph.keys():
            if all(key not in val for val in graph.values()):
                paths += self.find_all_paths(key, node_id)

        return paths

    def find_node_level(self, node_id):
        """
        Args:
            node_id (int): node id of the node of level required

        Returns:
            level (int)
        """
        paths = self.all_paths_from_roots(node_id)

        return max([len(x) for x in paths])

    def add_node(self, fw_id, node):
        """
        Args:
            fw_id (int): firework id of the node
            node (Node): node object needs to be added
        """
        if fw_id in self._nodes:
            raise ValueError('FW ids must be unique!')
        self._nodes[fw_id] = node

    def delete_node(self, fw_id):
        """
        Args:
            fw_id (int): firework id of the node to be deleted
        """
        if fw_id not in self._nodes:
            raise KeyError('FW id not exists')
        del self._nodes[fw_id]

    def update_links(self):
        """
            Reset the links attribute and update with new parent-children relationships.
            This method should be called after overriding the DAG object.
        """
        links = {}
        for fw_id in self._nodes:
            node = self._nodes[fw_id]
            children = node.get_children()
            if children:
                links[node.get_fw_id()] = []
                for child in children:
                    links[node.get_fw_id()].append(child.get_fw_id())
        self._links = links

    def update_height(self):
        """
        Update the height attribute with updated links
        This method should be called after overriding the DAG object if the height is changed
        """
        self.update_links()
        height = 0
        for fw_id in self._nodes:
            level = self.find_node_level(fw_id)
            if level > height:
                height = level

        self._height = height
