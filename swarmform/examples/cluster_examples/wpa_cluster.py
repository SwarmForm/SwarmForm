from swarmform import SwarmPad
from swarmform.core.swarmwork import SwarmFlow
from swarmform.core.cluster import cluster_sf

if __name__ == "__main__":
	# set up the SwarmPad and reset it
	swarmpad = SwarmPad()
	swarmpad.reset('', require_password=False)

	filename = "/home/ayesh/SwarmForm/swarmform/examples/workflow_generator_examples/LIGO_100-1607683547/LIGO_100.yaml"
	# create the Firework consisting of a custom "Addition" task
	unclustered_sf = SwarmFlow.from_file(filename)

	# store workflow
	swarmpad.add_sf(unclustered_sf)

	# Cluster the SwarmFlow
	clustered_sf = cluster_sf(swarmpad, unclustered_sf.sf_id, "wpa")

	# Archive unclustered SwarmFlow
	unclustered_sf_fw_id = unclustered_sf.fws[0].fw_id

	swarmpad.archive_wf(unclustered_sf_fw_id)

	swarmpad.add_sf(clustered_sf)
