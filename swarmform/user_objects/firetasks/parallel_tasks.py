# coding: utf-8

from __future__ import unicode_literals
from fireworks import ScriptTask, explicit_serialize


@explicit_serialize
class ParallelTask(ScriptTask):
	""" Combines multiple user defined ScriptTasks parallely"""
	required_params = ['script']

	@classmethod
	def from_firetasks(cls, firetasks, parameters=None):
		firetasks = firetasks if isinstance(firetasks, (list, tuple)) else [firetasks]
		parameters = parameters if parameters else {}
		script = ''
		for task in firetasks:
			script += task['script'][0] + ' & '
		script += 'wait'
		parameters['script'] = [script]
		parameters['use_shell'] = True
		return cls(parameters)
