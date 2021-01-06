from swarmform import SwarmPad, WorkflowGenerator


def main():
	# set up the LaunchPad and reset it
	swarmpad = SwarmPad()
	swarmpad.reset('', require_password=False)
	workflow_dax = "/home/ayesh/SwarmForm/swarmform/util/workflows/dax/cores/Inspiral_100.xml"
	dax_swarmflow = WorkflowGenerator.generate_workflow(workflow_dax)
	swarmpad.add_sf(dax_swarmflow)


if __name__ == "__main__":
	main()
