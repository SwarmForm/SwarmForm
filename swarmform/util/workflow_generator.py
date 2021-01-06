import datetime
import os
import yaml
import xml.etree.ElementTree as ET

from fireworks import Firework, ScriptTask
from swarmform.core.swarmwork import SwarmFlow


class WorkflowGenerator:

    @classmethod
    # Read input from DAX file
    def read_input_dax(cls, filename):
        try:
            with open(filename, 'r') as f:
                input_xml = ET.parse(filename)
                return input_xml
        except IOError:
            print("Error while opening the DAX file")

    @classmethod
    def parse_dax(cls, xml_tree):
        root = xml_tree.getroot()
        workflow_dict = {}
        id_map = {}
        fw_id = 1

        job_count = root.attrib['jobCount']
        namespace = root[0].attrib['namespace']
        swarmflow_name = namespace + "_" + job_count
        for child in root:
            if child.tag.endswith('job'):
                # Set execution time as the first element of the firework
                # Set cores to zero as it is considered only in parallel task clustering
                # Set empty list to add children
                if (child.attrib.get('runtime',None)):
                    runtime = round((float(child.attrib['runtime'])/float(10)), 3)
                else:
                    runtime = 0
                if (child.attrib.get('cores',None)):
                    cores = int(child.attrib['cores'])
                else:
                    cores = 0
                firework = [runtime, cores]
                workflow_dict.update({fw_id: firework})

                # Create a mapping between dax job id and firework id
                id_map.update({child.attrib['id']: fw_id})
                fw_id += 1

        dependency_dict = cls.get_parent_child_relationships(id_map, root)
        updated_dependency_dict = cls.replace_job_id_with_fw_id(id_map, dependency_dict)
        for fw_id in workflow_dict:
            fw = workflow_dict[fw_id]
            fw.append(updated_dependency_dict[fw_id])

        return workflow_dict, swarmflow_name

    @classmethod
    # When the DAX is given, output a dict with children of each parent
    # { parent_id : [child_id] }
    def get_parent_child_relationships(cls, id_map, dax):
        job_ids = id_map.keys()
        dependency_dict = {}
        for job in job_ids:
            # Get the child tag in the dict and set it as the parent
            parent = job
            children = []
            # Traverse the DAX to find children with parent tags equal to parent
            # If found, set the child tag(one level up in the xml tree) as a child of the considered parent
            for child in dax:
                if child.tag.endswith('child'):
                    for parent_id in child:
                        if parent_id.attrib['ref'] == parent:
                            children.append(child.attrib['ref'])
            dependency_dict.update({parent: children})
        return dependency_dict

    @classmethod
    def replace_job_id_with_fw_id(cls, id_map,dependency_dict):

        # Enumerate dependency_dict and replace its children_ids with the fw_ids of the children
        for parent_id in id_map:
            child_id_list = dependency_dict[parent_id]
            for index, child_id in enumerate(child_id_list):
                if child_id in id_map:
                    child_id_list[index] = id_map[child_id]

            # Enumerate dependency_dict and replace its parent ids with their fw_ids
            if parent_id in id_map:
                dependency_dict[id_map[parent_id]] = dependency_dict.pop(parent_id)

        return dependency_dict

    @classmethod
    # Read input from YAML file
    def read_input_yaml(cls, filename):
        try:
            with open(filename, 'r') as f:
                input_yaml = yaml.load(f, Loader=yaml.FullLoader)
            jobs = input_yaml['fireworks']
            swarmflow_name = input_yaml['swarmflow_name']
            return jobs, swarmflow_name
        except IOError:
            print("Error while opening the DAX file")

    @classmethod
    # Generate script for each shell script
    def gen_script(cls, job_id, job_exec_time):
        script = """
                    start=$(date +"%T.%3N");
                    echo "##########task {} start time ${{start}}";
                    sleep {};
                    end=$(date +"%T.%3N");
                    echo "##########task {} end time ${{end}}";
                    exit;
                    """
        script = script.format(job_id, job_exec_time, job_id)
        return script

    @classmethod
    # Create directory for the swarmflow
    def create_directory(cls, swarmflow_name):
        dir_name = swarmflow_name + "-" + str(int(datetime.datetime.now().timestamp()))
        os.mkdir(dir_name)
        return dir_name

    @classmethod
    # Create firework for each shell script
    def create_firework(cls, filename,spec):
        cur_dir = os.getcwd()
        task_path = cur_dir + "/" + filename
        command = "sh " + task_path
        task = ScriptTask.from_str(command)
        firework = Firework(task, spec=spec)
        return firework

    @classmethod
    # Create shell script file for each firework
    def create_scripts(cls, dir_name, jobs):
        fws = []

        # eg: filename = task1.sh
        for job_id in jobs:
            filename = dir_name + "/task" + str(job_id) + ".sh"
            job_exec_time = jobs[job_id][0]
            cores = jobs[job_id][1]
            script = cls.gen_script(job_id, job_exec_time)

            with open(filename, 'w+') as f:
                f.write(script)

            spec = {
                "_queueadapter": {
                    "exec_time":job_exec_time,
                    "cores":cores
                }
            }
        # Create firework for each job given
            firework = cls.create_firework(filename,spec)
            fws.append(firework)

        return fws

    @classmethod
    # Map the firework ids to its list positions
    def map_list_positions(cls, child_id):
        index = child_id - 1
        return index

    @classmethod
    # Create the dependency dictionary using the given parent-child relationships
    def create_dependencies(cls, jobs, fireworks):
        dependencies = {}

        for job_id in jobs:
            # 3rd element in jobs dictionary represent children of the firework
            children = jobs[job_id][2]

            # Get the position of firework in the fireworks list, relevant to the considered firework
            parent_fw_position = job_id - 1
            if children is not None and children != []:
                children_fw_positions = map(cls.map_list_positions, children)

                # Create a children fireworks list for each firework and add it to dependencies dictionary
                child_list = []
                for position in children_fw_positions:
                    child_list.append(fireworks[position])
            else:
                child_list = []
            parent_fw = fireworks[parent_fw_position]

            dependencies.update({parent_fw: child_list})

        return dependencies

    @classmethod
    # Save the swarmflow to a YAML file
    def dump_swarmflow(cls, swarmflow, dir_name, swarmflow_name):
        swarmflow_output_filename = dir_name + "/" + swarmflow_name + ".yaml"
        swarmflow.to_file(swarmflow_output_filename, "yaml")
        return None

    @classmethod
    def generate_workflow(cls, input_file):

        if input_file.endswith('yaml') or input_file.endswith('yml'):
            jobs, swarmflow_name = cls.read_input_yaml(input_file)
        elif input_file.endswith('xml'):
            xml_tree = cls.read_input_dax(input_file)
            jobs, swarmflow_name = cls.parse_dax(xml_tree)
        else:
            raise IOError('Input file format not recognized. Only YAML and DAX formats are supported')
        dir_name = cls.create_directory(swarmflow_name)
        fireworks = cls.create_scripts(dir_name, jobs)
        dependencies = cls.create_dependencies(jobs, fireworks)
        swarmflow = SwarmFlow(fireworks=fireworks, links_dict=dependencies, name=swarmflow_name)
        cls.dump_swarmflow(swarmflow, dir_name, swarmflow_name)
        return swarmflow
