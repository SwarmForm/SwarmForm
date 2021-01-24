import datetime
import os
import sys
import signal
import traceback
import six
from argparse import ArgumentParser

from fireworks.scripts.lpad_run import init_yaml, get_output_func

from swarmform import SwarmPad
from swarmform.core.swarmwork import SwarmFlow
from swarmform.core.cluster import cluster_sf
from swarmform.sf_config import LAUNCHPAD_LOC, CONFIG_FILE_DIR

DEFAULT_LPAD_YAML = "my_swarmpad.yaml"


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
            err_message += ' Type "sform init" if you would like to set up a file that specifies ' \
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
        for f in args.sf_file:
            files.extend([os.path.join(f, i) for i in os.listdir(f)])
    else:
        files = args.sf_file
    for f in files:
        fwf = SwarmFlow.from_file(f)
        if args.check:
            from fireworks.utilities.dagflow import DAGFlow
            DAGFlow.from_fireworks(fwf)
        sp.add_sf(fwf)


def get_sf(args):
    if args.sf_id:
        sf = get_sf_by_id(args)
    if args.name:
        sf = get_sf_by_name(args)
    # TODO: Use get_wf_summary_dict function and print formatted output
    print(args.output(sf))


def get_sf_by_id(args):
    sp = get_sp(args)

    if isinstance(args.sf_id, int):
        swarmflow = sp.get_sf_by_id(args.sf_id)
        return swarmflow
    else:
        raise ValueError('Incorrect id type {}.'.format(type(args.sf_id)))


def get_sf_by_name(args):
    sp = get_sp(args)
    swarmflow = sp.get_sf_by_name(args.sf_name)
    return swarmflow


# Cluster the jobs in a workflow
def cluster_workflow(args):
    sp = get_sp(args)
    unclustered_sf = sp.get_sf_by_id(args.sf_id)
    unclustered_sf_fw_id = unclustered_sf.fws[0].fw_id
    clustered_workflow = cluster_sf(sp, args.sf_id, args.algo, args.cc)
    sp.add_sf(clustered_workflow)
    sp.archive_wf(unclustered_sf_fw_id)
    sp.m_logger.info('Workflow with id {} clustered succesfully'.format(args.sf_id))


def sform():
    m_description = 'A command line interface to SwarmForm. For more help on a specific command, ' \
                    'type "sform <command> -h".'

    parser = ArgumentParser(description=m_description)
    parser.add_argument('-o', '--output', choices=["json", "yaml"],
                        default='json', type=lambda s: s.lower(),
                        help='Set output display format to either json or YAML. '
                             'YAML is easier to read for long documents. JSON is the default.')

    parser.add_argument('-l', '--launchpad_file', help='path to LaunchPad file containing '
                                                       'central DB connection info',
                        default=None)
    parser.add_argument('-c', '--config_dir',
                        help='path to a directory containing the LaunchPad file (used if -l unspecified)',
                        default=CONFIG_FILE_DIR)
    parser.add_argument('--logdir', help='path to a directory for logging')
    parser.add_argument('--loglvl', help='level to print log messages', default='INFO')
    parser.add_argument('-s', '--silencer', help='shortcut to mute log messages', action='store_true')
    parser.add_argument('-sf', '--sf_id', help='Id of the SwarmFlow to cluster', default=None, type=int)
    parser.add_argument('-a', '--algo', help='Clustering algorithm (wpa or rac)', default='rac', type=str)
    parser.add_argument('-cc', help='Cluster count per horizontal level', default=5, type=int)

    subparsers = parser.add_subparsers(help='command', dest='command')

    init_parser = subparsers.add_parser(
        'init', help='Initialize a SwarmForm launchpad YAML file.')
    init_parser.add_argument('-u', '--uri_mode',
                             action="store_true",
                             help="Connect via a URI, see: "
                                  "https://docs.mongodb.com/manual/reference/connection-string/")
    init_parser.add_argument('--config-file', default=DEFAULT_LPAD_YAML,
                             type=str,
                             help="Filename to write to.")
    init_parser.set_defaults(func=init_yaml)

    reset_parser = subparsers.add_parser('reset', help='Reset and re-initialize the SwarmForm database')
    reset_parser.add_argument('--password', help="Today's date,  e.g. 2012-02-25. "
                                                 "Password or positive response to input prompt "
                                                 "required to protect against accidental reset.")
    reset_parser.set_defaults(func=reset)

    addsf_parser = subparsers.add_parser('add', help='Insert a SwarmFlow from file')
    addsf_parser.add_argument('-sf', '--sf_file', nargs='+',
                              help='Path to a Firework or SwarmFlow file')
    addsf_parser.add_argument('-d', '--dir',
                              action="store_true",
                              help="Directory mode. Finds all files in the "
                                   "paths given by wf_file.")
    addsf_parser.set_defaults(func=add_sf, check=False)

    getsf_parser = subparsers.add_parser('get_sf', help='Get SwarmFlow from SwarmPad')
    getsf_parser.add_argument('-id', '--sf-id', type=int, help='SwarmFlow id')
    getsf_parser.add_argument('-n', '--name', type=str, help='SwarmFlow name')
    getsf_parser.set_defaults(func=get_sf)

    cluster_wf_parser = subparsers.add_parser('cluster',
                                              help='Cluster the fireworks in the SwarmFlow and save the new '
                                                   'SwarmFlow to the database')
    cluster_wf_parser.add_argument('-sf', '--sf_id', help='Id of the SwarmFlow to cluster', default=None, type=int)
    cluster_wf_parser.add_argument('-a', '--algo', help='Clustering algorithm (wpa or rac)', default='rac', type=str)
    cluster_wf_parser.add_argument('-cc', help='Number of clusters per horizontal level', default=5, type=int)
    cluster_wf_parser.set_defaults(func=cluster_workflow)

    args = parser.parse_args()

    args.output = get_output_func(args.output)

    if args.command is None:
        # if no command supplied, print help
        parser.print_help()
    else:
        if hasattr(args, "fw_id") and args.fw_id is not None and \
                isinstance(args.fw_id, six.string_types):
            if "," in args.fw_id:
                args.fw_id = [int(x) for x in args.fw_id.split(",")]
            else:
                args.fw_id = [int(args.fw_id)]

        args.func(args)

    signal.signal(signal.SIGINT, handle_interrupt)  # graceful exit on ^C


if __name__ == '__main__':
    sform()
