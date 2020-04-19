from swarmform import SwarmPad
from fireworks import Firework, ScriptTask, FWorker
from fireworks.core.rocket_launcher import launch_rocket

# set up the SwarmPad and reset it
swarmpad = SwarmPad()
swarmpad.reset('', require_password=False)

# create the Firework consisting of a custom "Addition" task
firework = Firework(ScriptTask.from_str('echo "hello'))

# store workflow and launch it locally
swarmpad.add_sf(firework)
launch_rocket(swarmpad, FWorker())