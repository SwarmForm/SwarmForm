import datetime
import os
import yaml

from fireworks import Firework, ScriptTask
from swarmform.core.swarmwork import Swarmflow


# Read input from YAML file
def read_input(filename):
    try:
        if filename.endswith('yaml') or filename.endswith('yml'):
            with open(filename, 'r') as f:
                input_yaml = yaml.load(f, Loader=yaml.FullLoader)
            jobs = input_yaml['fireworks']
            swarmflow_name = input_yaml['swarmflow_name']
            return jobs, swarmflow_name
    except IOError:
        print("Error with the input file")


# Generate script for each shell script
def gen_script(job_id, job_exec_time):
    script = """
                start=$(date);
                echo "##########task {} start time ${{start}}";
                sleep {};
                end=$(date);
                echo "##########task {} end time ${{end}}";
                exit;
                """
    script = script.format(job_id, job_exec_time, job_id)
    return script


# Create directory for the swarmflow
def create_directory(swarmflow_name):
    dir_name = swarmflow_name + "-" + str(int(datetime.datetime.now().timestamp()))
    os.mkdir(dir_name)
    return dir_name


# Create firework for each shell script
def create_firework(filename):
    cur_dir = os.getcwd()
    task_path = cur_dir + "/" + filename
    command = "sh " + task_path
    task = ScriptTask.from_str(command)
    firework = Firework(task)
    return firework


# Create shell script file for each firework
def create_scripts(dir_name, jobs):
    fws = []

    # eg: filename = task1.sh
    for job_id in jobs:
        filename = dir_name + "/task" + str(job_id) + ".sh"
        job_exec_time = jobs[job_id][0]
        script = gen_script(job_id, job_exec_time)

        with open(filename, 'w+') as f:
            f.write(script)

    # Create firework for each job given
        firework = create_firework(filename)
        fws.append(firework)

    return fws


# Map the firework ids to its list positions
def map_list_positions(child_id):
    index = child_id - 1
    return index


# Create metadata for each firework
def create_metadata(jobs, fireworks):

    # output format { 'costs' : {fw_id : {'exec_time' : x , 'cores' : y }}}
    metadata = {'costs': {}}
    for job_id in jobs:
        # 1st element in jobs dictionary represent execution time of the firework
        # 2nd element in jobs dictionary represent cores required for the firework
        exec_time = jobs[job_id][0]
        cores = jobs[job_id][1]

        # Get the position of firework in the fireworks list
        parent_fw_position = job_id - 1
        metadata_dict = {'exec_time': exec_time, 'cores': cores}
        fw_id = fireworks[parent_fw_position].fw_id

        # Create metadata dictionary for each firework and add it to metadata dictionary
        metadata['costs'].update({fw_id: metadata_dict})

    return metadata


# Create the dependency dictionary using the given parent-child relationships
def create_dependencies(jobs, fireworks):
    dependencies = {}

    for job_id in jobs:
        # 3rd element in jobs dictionary represent children of the firework
        children = jobs[job_id][2]

        # Get the position of firework in the fireworks list, relevant to the considered firework
        parent_fw_position = job_id - 1
        if children is not None and children != []:
            children_fw_positions = map(map_list_positions, children)

            # Create a children fireworks list for each firework and add it to dependencies dictionary
            child_list = []
            for position in children_fw_positions:
                child_list.append(fireworks[position])
        else:
            child_list = []
        parent_fw = fireworks[parent_fw_position]

        dependencies.update({parent_fw: child_list})

    return dependencies


# Save the swarmflow to a YAML file
def dump_swarmflow(swarmflow, dir_name, swarmflow_name):
    swarmflow_output_filename = dir_name + "/" + swarmflow_name + ".yaml"
    swarmflow.to_file(swarmflow_output_filename, "yaml")
    return None


def main():

    swarmflow_definition_file = "/home/kalana/fyp/fireworks/examples/cluster_examples/custom-jobs-25-fail.yaml"
    jobs, swarmflow_name = read_input(swarmflow_definition_file)
    dir_name = create_directory(swarmflow_name)
    fireworks = create_scripts(dir_name, jobs)
    dependencies = create_dependencies(jobs, fireworks)
    metadata = create_metadata(jobs, fireworks)
    swarmflow = Swarmflow(fireworks=fireworks, links_dict=dependencies, metadata=metadata, name=swarmflow_name)
    dump_swarmflow(swarmflow, dir_name, swarmflow_name)


if __name__ == "__main__":
    main()
