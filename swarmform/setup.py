#!/usr/bin/env python

from setuptools import setup, find_packages
import os

module_dir = os.path.dirname(os.path.abspath(__file__))

if __name__ == "__main__":
    setup(
        name='SwarmForm',
        version="0.1",
        description='SwarmForm Workflow Management Software',
        long_description=open(os.path.join(module_dir, 'README.md')).read(),
        url='https://github.com/SwarmForm/SwarmForm',
        author='Kalana Wijethunga, Randika Jayasekara, Ayesh Weerasinghe',
        author_email='kalana.16@cse.mrt.ac.lk, rpjayaseka.16@cse.mrt.ac.lk, ayeshw.16@cse.mrt.ac.lk',
        packages=find_packages(),
        install_requires=['FireWorks=1.9.5'],
        entry_points={
            'console_scripts': [
                'sform = swarmform.scripts.sform_run:sform'
            ]
        }
    )
