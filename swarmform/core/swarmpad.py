import datetime

from fireworks import LaunchPad, Firework
from fireworks.fw_config import GRIDFS_FALLBACK_COLLECTION

from swarmform.core.swarmwork import SwarmFlow


class SwarmPad(LaunchPad):

	@classmethod
	def from_dict(cls, d):
		port = d.get('port', None)
		name = d.get('name', None)
		username = d.get('username', None)
		password = d.get('password', None)
		logdir = d.get('logdir', None)
		strm_lvl = d.get('strm_lvl', None)
		user_indices = d.get('user_indices', [])
		wf_user_indices = d.get('wf_user_indices', [])
		ssl = d.get('ssl', False)
		ssl_ca_certs = d.get('ssl_ca_certs',
							 d.get('ssl_ca_file',
								   None))  # ssl_ca_file was the old notation for FWS < 1.5.5
		ssl_certfile = d.get('ssl_certfile', None)
		ssl_keyfile = d.get('ssl_keyfile', None)
		ssl_pem_passphrase = d.get('ssl_pem_passphrase', None)
		authsource = d.get('authsource', None)
		uri_mode = d.get('uri_mode', False)
		mongoclient_kwargs = d.get('mongoclient_kwargs', None)
		return SwarmPad(d['host'], port, name, username, password,
						logdir, strm_lvl, user_indices, wf_user_indices, ssl,
						ssl_ca_certs, ssl_certfile, ssl_keyfile,
						ssl_pem_passphrase,
						authsource, uri_mode, mongoclient_kwargs)

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
				FWS will not clear DBs that contain more SwarmFlows than this parameter
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
		Add SwarmFlow(or firework) to the SwarmPad. The firework ids will be reassigned.
		Args:
			sf (SwarmFlow/Firework)
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
			sf = SwarmFlow.from_Firework(fw=sf, sf_id=sf.sf_id)

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
		Checkout the next SwarmFlow id
		Args:
			quantity (int): optionally ask for many ids, otherwise defaults to 1
							this then returns the *first* sf_id in that range
		"""
		try:
			return self.fw_id_assigner.find_one_and_update({}, {
				'$inc': {'next_sf_id': quantity}})['next_sf_id']
		except Exception:
			raise ValueError(
				"Could not get next SwarmFlow id! If you have not yet initialized the database,"
				" please do so by performing a database reset (e.g., swarmpad reset)")

	def _restart_ids(self, next_fw_id, next_launch_id, next_sf_id):
		"""
		internal method used to reset firework id counters.
		Args:
			next_fw_id (int): id to give next Firework
			next_launch_id (int): id to give next Launch
			next_sf_id (int): id to give next SwarmFlow
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

	def get_sf_by_id(self, sf_id):
		"""
		Given a SwarmFlow id, give back the SwarmFlow.
		Args:
			sf_id (int)
		Returns:
			A SwarmFlow object
		"""
		links_dict = self.workflows.find_one({'sf_id': sf_id})

		if not links_dict:
			raise ValueError(
				"Could not find a Workflow with sf_id: {}".format(sf_id))

		fws = map(self.get_fw_by_id, links_dict["nodes"])
		return SwarmFlow(fws, links_dict['links'], links_dict['name'],
						 links_dict['metadata'], links_dict['created_on'],
						 links_dict['updated_on'], None, links_dict['sf_id'])

	def get_sf_by_name(self, sf_name):
		"""
		Given a SwarmFlow name, give back the SwarmFlow.
		If multiple SwarmFlows with the same name are found, first SwarmFlow is returned
		Args:
			sf_name (string)
		Returns:
			A SwarmFlow Object
		"""

		links_dict = self.workflows.find_one({'name': sf_name})

		if not links_dict:
			raise ValueError(
				"Could not find a SwarmFlow with sf_name: {}".format(sf_name))

		fws = map(self.get_fw_by_id, links_dict["nodes"])
		return SwarmFlow(fws, links_dict['links'], links_dict['name'],
						 links_dict['metadata'], links_dict['created_on'],
						 links_dict['updated_on'], None, links_dict['sf_id'])
