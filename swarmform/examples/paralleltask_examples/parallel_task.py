from fireworks.core.rocket_launcher import rapidfire
from fireworks import Firework, FWorker, ScriptTask

from swarmform import SwarmPad
from swarmform.user_objects.firetasks.parallel_tasks import ParallelTask


def main():
	# set up the LaunchPad and reset it
	swarmpad = SwarmPad()
	swarmpad.reset('', require_password=False)

	firetask1 = ScriptTask.from_str('echo "starting"; sleep 30; echo "ending"')
	firetask2 = ScriptTask.from_str('echo "hello from BACKGROUND thread #1"')
	firetask3 = ScriptTask.from_str('echo "hello from BACKGROUND thread #2"')

	# Combine [firetasks] parallely to form a ParallelTask
	par_task = ParallelTask.from_firetasks([firetask1, firetask2])

	# Combine the par_task and firetask3 sequentially
	firework = Firework([par_task, firetask3])

	# store workflow and launch it locally
	swarmpad.add_wf(firework)
	rapidfire(swarmpad, FWorker())


if __name__ == "__main__":
	main()
