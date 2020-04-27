from swarmform import SwarmPad, WorkflowGenerator


def main():
	# set up the LaunchPad and reset it
	swarmpad = SwarmPad()
	swarmpad.reset('', require_password=False)
	workflow_yaml = "/home/kalana/fyp-new/SwarmForm/swarmform/util/workflows/yaml/custom-jobs-25.yaml"
	dax_swarmflow = WorkflowGenerator.generate_workflow(workflow_yaml)
	swarmpad.add_sf(dax_swarmflow)


if __name__ == "__main__":
	main()
