import datetime

from fireworks import LaunchPad, Firework
from fireworks.core.launchpad import LazyFirework
from fireworks.fw_config import GRIDFS_FALLBACK_COLLECTION
from swarmform.core.swarmwork import Swarmflow


class SwarmPad(LaunchPad):

	def reset(self, password, require_password=True, max_reset_wo_password=25):
		"""
		Create a new SwarmForm database. This will overwrite the existing SwarmForm database! To
		safeguard against accidentally erasing an existing database, a password must be entered.
		Args:
			password (str): A String representing today's date, e.g. '2012-12-31'
			require_password (bool): Whether a password is required to reset the DB. Setting to
				false is dangerous because running code unintentionally could clear your DB - use
				max_reset_wo_password to minimize risk.
			max_reset_wo_password (int): A failsafe; when require_password is set to False,
				FWS will not clear DBs that contain more Swarmflows than this parameter
		"""
		m_password = datetime.datetime.now().strftime('%Y-%m-%d')

		if password == m_password or (
				not require_password and self.workflows.count() <= max_reset_wo_password):
			self.fireworks.delete_many({})
			self.launches.delete_many({})
			self.workflows.delete_many({})
			self.offline_runs.delete_many({})
			self._restart_ids(1, 1, 1)
			if self.gridfs_fallback is not None:
				self.db.drop_collection(
					"{}.chunks".format(GRIDFS_FALLBACK_COLLECTION))
				self.db.drop_collection(
					"{}.files".format(GRIDFS_FALLBACK_COLLECTION))
			self.tuneup()
			self.m_logger.info('SwarmPad was RESET.')
		elif not require_password:
			raise ValueError(
				"Password check cannot be overridden since the size of DB ({} workflows) "
				"is greater than the max_reset_wo_password parameter ({}).".format(
					self.fireworks.count(),
					max_reset_wo_password))
		else:
			raise ValueError(
				"Invalid password! Password is today's date: {}".format(
					m_password))

	def add_sf(self, sf, reassign_all=True):
		"""
		Add Swarmflow(or firework) to the SwarmPad. The firework ids will be reassigned.
		Args:
			sf (Swarmflow/Firework)
			reassign_all(bool): Reassign Firework ids
		Returns:
			dict: mapping between old and new Firework ids
		"""

		# Set a new workflow Id, if an id is not provided
		if not hasattr(sf, 'sf_id'):
			sf.sf_id = self.get_new_sf_id()
			self.m_logger.info('Create a new workflow with workflow_id: {}'.format(sf.sf_id))
		else:
			self.m_logger.info('Created a new workflow with workflow_id: {}'.format(sf.sf_id))

		# Check whether the passed workflow is an individual firework
		if isinstance(sf, Firework):
			sf = Swarmflow.from_Firework(fw=sf, sf_id=sf.sf_id)

		# sets the root FWs as READY
		# prefer to wf.refresh() for speed reasons w/many root FWs
		for fw_id in sf.root_fw_ids:
			sf.id_fw[fw_id].state = 'READY'
			sf.fw_states[fw_id] = 'READY'
		# insert the FireWorks and get back mapping of old to new ids
		old_new = self._upsert_fws(list(sf.id_fw.values()),
								   reassign_all=reassign_all)
		# update the Workflow with the new ids
		sf._reassign_ids(old_new)
		# insert the WFLinks
		self.workflows.insert_one(sf.to_db_dict())
		self.m_logger.info('Added a workflow. id_map: {}'.format(old_new))
		return old_new

	def get_new_sf_id(self, quantity=1):
		"""
		Checkout the next Swarmflow id
		Args:
			quantity (int): optionally ask for many ids, otherwise defaults to 1
							this then returns the *first* wf_id in that range
		"""
		try:
			return self.fw_id_assigner.find_one_and_update({}, {
				'$inc': {'next_sf_id': quantity}})['next_sf_id']
		except Exception:
			raise ValueError(
				"Could not get next Swarmflow id! If you have not yet initialized the database,"
				" please do so by performing a database reset (e.g., swarmpad reset)")

	def _restart_ids(self, next_fw_id, next_launch_id, next_sf_id):
		"""
		internal method used to reset firework id counters.
		Args:
			next_fw_id (int): id to give next Firework
			next_launch_id (int): id to give next Launch
			next_sf_id (int): id to give next Swarmflow
		"""
		self.fw_id_assigner.delete_many({})
		self.fw_id_assigner.find_one_and_replace({'_id': -1},
												 {'next_fw_id': next_fw_id,
												  'next_launch_id': next_launch_id,
												  'next_sf_id': next_sf_id},
												 upsert=True)
		self.m_logger.debug(
			'RESTARTED fw_id, launch_id to ({}, {}, {})'.format(next_fw_id,
																next_launch_id, next_sf_id))

	def get_sf_by_fw_id(self, fw_id):
		"""
		Given a Firework id, give back the Swarmflow containing that Firework.

		Args:
			fw_id (int)

		Returns:
			A Swarmflow object
		"""
		links_dict = self.workflows.find_one({'nodes': fw_id})
		if not links_dict:
			raise ValueError(
				"Could not find a Workflow with fw_id: {}".format(fw_id))
		fws = map(self.get_fw_by_id, links_dict["nodes"])
		return Swarmflow(fws, links_dict['links'], links_dict['name'],
						links_dict['metadata'], links_dict['created_on'],
						links_dict['updated_on'], None, links_dict['sf_id'])

	def get_sf_by_fw_id_lzyfw(self, fw_id):
		"""
		Given a FireWork id, give back the Swarmflow containing that FireWork.

		Args:
			fw_id (int)

		Returns:
			A Swarmflow object
		"""
		links_dict = self.workflows.find_one({'nodes': fw_id})
		if not links_dict:
			raise ValueError(
				"Could not find a Workflow with fw_id: {}".format(fw_id))

		fws = []
		for fw_id in links_dict['nodes']:
			fws.append(LazyFirework(fw_id, self.fireworks, self.launches,
									self.gridfs_fallback))
		# Check for fw_states in links_dict to conform with pre-optimized workflows
		if 'fw_states' in links_dict:
			fw_states = dict(
				[(int(k), v) for (k, v) in links_dict['fw_states'].items()])
		else:
			fw_states = None

		return Swarmflow(fws, links_dict['links'], links_dict['name'],
						links_dict['metadata'], links_dict['created_on'],
						links_dict['updated_on'], fw_states, links_dict['sf_id'])

	def get_sf_by_id(self, sf_id):
		"""
		Given a Swarmflow id, give back the Swarmflow.
		Args:
			sf_id (int)
		Returns:
			A Swarmflow object
		"""
		links_dict = self.workflows.find_one({'sf_id': sf_id})

		if not links_dict:
			raise ValueError(
				"Could not find a Workflow with sf_id: {}".format(sf_id))

		fws = map(self.get_fw_by_id, links_dict["nodes"])
		return Swarmflow(fws, links_dict['links'], links_dict['name'],
						links_dict['metadata'], links_dict['created_on'],
						links_dict['updated_on'], None, links_dict['sf_id'])

	def get_sf_by_name(self, wf_name):
		"""
		Given a Swarmflow name, give back the Swarmflow.
		Args:
			wf_name (string)
		Returns:
			A Swarmflow Object
		"""

		links_dict = self.workflows.find_one({'name': wf_name})

		if not links_dict:
			raise ValueError(
				"Could not find a Swarmflow with wf_name: {}".format(wf_name))

		fws = map(self.get_fw_by_id, links_dict["nodes"])
		return Swarmflow(fws, links_dict['links'], links_dict['name'],
						links_dict['metadata'], links_dict['created_on'],
						links_dict['updated_on'], None, links_dict['sf_id'])
