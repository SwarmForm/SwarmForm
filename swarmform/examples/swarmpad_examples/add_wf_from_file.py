from swarmform import SwarmPad
from swarmform.core.swarmwork import SwarmFlow
from fireworks import Firework, ScriptTask, FWorker
from fireworks.core.rocket_launcher import launch_rocket

if __name__ == "__main__":
	# set up the SwarmPad and reset it
	swarmpad = SwarmPad()
	#swarmpad.reset('', require_password=False)

	# create the Firework consisting of a custom "Addition" task
	sf = SwarmFlow.from_file('/home/kalana/fyp-new/SwarmForm/swarmform/custom-25-jobs-correct-1587484561/custom-25-jobs-correct.yaml')

	# store workflow
	swarmpad.add_sf(sf)
