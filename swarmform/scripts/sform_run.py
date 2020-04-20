import datetime
import os
import sys
import traceback
from argparse import ArgumentParser

from fireworks.fw_config import LAUNCHPAD_LOC

from swarmform import SwarmPad
from swarmform.core.swarmwork import SwarmFlow

DEFAULT_LPAD_YAML = "my_launchpad.yaml"


def handle_interrupt(signum):
    sys.stderr.write("Interrupted by signal {:d}\n".format(signum))
    sys.exit(1)


def get_sp(args):
    try:
        if not args.launchpad_file:
            if os.path.exists(os.path.join(args.config_dir, DEFAULT_LPAD_YAML)):
                args.launchpad_file = os.path.join(args.config_dir, DEFAULT_LPAD_YAML)
            else:
                args.launchpad_file = LAUNCHPAD_LOC

        if args.launchpad_file:
            return SwarmPad.from_file(args.launchpad_file)
        else:
            args.loglvl = 'CRITICAL' if args.silencer else args.loglvl
            return SwarmPad(logdir=args.logdir, strm_lvl=args.loglvl)
    except Exception:
        traceback.print_exc()
        err_message = 'SwarmForm was not able to connect to MongoDB. Is the server running? ' \
                      'The database file specified was {}.'.format(args.launchpad_file)
        if not args.launchpad_file:
            err_message += ' Type "lpad init" if you would like to set up a file that specifies ' \
                           'location and credentials of your Mongo database (otherwise use default ' \
                           'localhost configuration).'
        raise ValueError(err_message)


def reset(args):
    sp = get_sp(args)
    if not args.password:
        if input('Are you sure? This will RESET {} workflows and all data. (Y/N)'.format(
                sp.workflows.count()))[0].upper() == 'Y':
            args.password = datetime.datetime.now().strftime('%Y-%m-%d')
        else:
            raise ValueError('Operation aborted by user.')
    sp.reset(args.password)


def add_sf(args):
    sp = get_sp(args)
    if args.dir:
        files = []
        for f in args.wf_file:
            files.extend([os.path.join(f, i) for i in os.listdir(f)])
    else:
        files = args.wf_file
    for f in files:
        fwf = SwarmFlow.from_file(f)
        if args.check:
            from fireworks.utilities.dagflow import DAGFlow
            DAGFlow.from_fireworks(fwf)
        sp.add_sf(fwf)


def get_sf(args):
    if args.id:
        get_sf_by_id(args)
    if args.name:
        get_sf_by_name(args)


def get_sf_by_id(args):
    sp = get_sp(args)

    if isinstance(args.sf_id, int):
        swarmflow = sp.get_sf_by_id(args.sf_id)
        args.outupt(swarmflow)
        #print(swarmflow)
    else:
        raise ValueError('Incorrect id type {}.'.format(type(args.sf_id)))


def get_sf_by_name(args):
    sp = get_sp(args)
    swarmflow = sp.get_sf_by_name(args.sf_name)
    args.outupt(swarmflow)
    #print (swarmflow)


def sform():
    m_description = 'A command line interface to SwarmForm. For more help on a specific command, ' \
                    'type "sform <command> -h".'

    parser = ArgumentParser(description=m_description)
    parser.add_argument('-o', '--output', choices=["json", "yaml"],
                        default='json', type=lambda s: s.lower(),
                        help='Set output display format to either json or YAML. '
                             'YAML is easier to read for long documents. JSON is the default.')

    subparsers = parser.add_subparsers(help='command', dest='command')

    addsf_parser = subparsers.add_parser('add', help='insert a SwarmFlow from file')
    addsf_parser.add_argument('-sf', '--sf_file', nargs='+',
                              help='Path to a Firework or SwarmFlow file')
    addsf_parser.set_defaults(func=add_sf, check=False)

    getsf_parser = subparsers.add_parser('get_sf', help='Get SwarmFlow from SwarmPad')
    getsf_parser.add_argument('-id', '--sf-id', help='SwarmFlow id')
    getsf_parser.add_argument('-n', '--name', help='SwarmFlow name')
    getsf_parser.set_defaults(func=get_sf)


if __name__ == '__main__':
    sform()
