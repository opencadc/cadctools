# -*- coding: utf-8 -*-

# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2018.                            (c) 2018.
#  Government of Canada                 Gouvernement du Canada
#  National Research Council            Conseil national de recherches
#  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
#  All rights reserved                  Tous droits réservés
#
#  NRC disclaims any warranties,        Le CNRC dénie toute garantie
#  expressed, implied, or               énoncée, implicite ou légale,
#  statutory, of any kind with          de quelque nature que ce
#  respect to the software,             soit, concernant le logiciel,
#  including without limitation         y compris sans restriction
#  any warranty of merchantability      toute garantie de valeur
#  or fitness for a particular          marchande ou de pertinence
#  purpose. NRC shall not be            pour un usage particulier.
#  liable in any event for any          Le CNRC ne pourra en aucun cas
#  damages, whether direct or           être tenu responsable de tout
#  indirect, special or general,        dommage, direct ou indirect,
#  consequential or incidental,         particulier ou général,
#  arising from the use of the          accessoire ou fortuit, résultant
#  software.  Neither the name          de l'utilisation du logiciel. Ni
#  of the National Research             le nom du Conseil National de
#  Council of Canada nor the            Recherches du Canada ni les noms
#  names of its contributors may        de ses  participants ne peuvent
#  be used to endorse or promote        être utilisés pour approuver ou
#  products derived from this           promouvoir les produits dérivés
#  software without specific prior      de ce logiciel sans autorisation
#  written permission.                  préalable et particulière
#                                       par écrit.
#
#  This file is part of the             Ce fichier fait partie du projet
#  OpenCADC project.                    OpenCADC.
#
#  OpenCADC is free software:           OpenCADC est un logiciel libre ;
#  you can redistribute it and/or       vous pouvez le redistribuer ou le
#  modify it under the terms of         modifier suivant les termes de
#  the GNU Affero General Public        la “GNU Affero General Public
#  License as published by the          License” telle que publiée
#  Free Software Foundation,            par la Free Software Foundation
#  either version 3 of the              : soit la version 3 de cette
#  License, or (at your option)         licence, soit (à votre gré)
#  any later version.                   toute version ultérieure.
#
#  OpenCADC is distributed in the       OpenCADC est distribué
#  hope that it will be useful,         dans l’espoir qu’il vous
#  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
#  without even the implied             GARANTIE : sans même la garantie
#  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
#  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
#  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
#  General Public License for           Générale Publique GNU Affero
#  more details.                        pour plus de détails.
#
#  You should have received             Vous devriez avoir reçu une
#  a copy of the GNU Affero             copie de la Licence Générale
#  General Public License along         Publique GNU Affero avec
#  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
#  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
#                                       <http://www.gnu.org/licenses/>.
#
#
# ***********************************************************************

#
# Acknowledgment: This code is based on the code developed by the
# East Asian Observatory for the JCMT Science Archive
#

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from collections import namedtuple

from configparser import ConfigParser
from datetime import datetime, timedelta
import logging
import os
import tempfile
import sys
import json
import glob

from termcolor import colored
from .data_verify import check_valid
from .utils import etrans_config, fetch_cadc_file_info, put_cadc_file
from .namecheck import check_file_name
import vos

from cadcutils import net, util
from cadcetrans import version
from .utils import CommandError, ProcError, TransferFailure,\
    TRANS_ROOT_LOGNAME, TransferException, _get_last_week_logs,\
    _get_transfer_log
import cadcetrans.utils as utils

APP_NAME = 'cadc-etrans'

logger = logging.getLogger(__name__)

FileInfo = namedtuple('FileInfo', 'name stream_name')

allowed_streams = ('new', 'replace', 'any')

__all__ = ['transfer']

# names of processing timestamp files
PROC_TIMESTAMP_FILE = 'transfer.ini'
# name of the etransfer section
ETRANS_SECTION = 'etransfer'
# minimum staging time in minutes. Only files older than MIN_STAGE_TIME are
# being processed to avoid picking up files before they are fully transferred
# into the input directory
MIN_STAGE_TIME = 5


def transfer(trans_dir, stream_name=None, dry_run=False,
             subject=None, namecheck_file=None):
    """
    Attempt to put files into the archive at CADC.

    This function is controlled by the configuration file entry
    etransfer.maxfiles. It looks in the "new" and
    "replace" directories inside "transdir" for at most  "max_files" files.
    The files are moved to a temporary processing directory and then either
    moved to a reject directory or deleted on completion.  In the event of
    failure to transfer, the files are put back in either the "new" or
    "replace" directory.

    The stream name argument can be given to select only files in the "new",
    "replace"  or any directory.  It must be given in the dry_run case since
    then no "proc" directory is created.

    :param trans_dir -- Directory where files to be transferred are located
    :param stream_name -- Stream to work with
    :param dry_run -- If True the last step (actual transfer of file) not
    executed
    :param subject -- Subject that runs the command
    :param namecheck_file -- File containing the rules for file names or None
    if there are no name checks

    """

    max_files = int(etrans_config.get(ETRANS_SECTION, 'max_files'))

    files = []
    n_err = 0

    # Select transfer stream names.
    stream_names = allowed_streams
    if stream_name is None:
        if dry_run:
            raise CommandError('Stream name must be specified in dry run mode')
    else:
        if stream_name not in stream_names:
            raise CommandError('Unknown stream name {0}'.format(stream_name))

        stream_names = (stream_name,)

    files = _get_files_to_transfer(stream_names, trans_dir)

    if not files:
        logger.info('No files found for transfer')
        return

    if dry_run:
        # Work in the stream name directory.
        proc = files[:max_files]
        proc_dir = os.path.join(trans_dir, stream_name)
        use_sub_dir = False
        stamp_file = None
    else:
        # Create working directory.
        proc = []
        pd = os.path.join(trans_dir, 'proc')
        if not os.path.exists(pd):
            os.makedirs(pd)
        proc_dir = tempfile.mkdtemp(prefix='proc', dir=pd)
        logger.info('Working directory: {}'.format(proc_dir))

        # Create stream name-based subdirectories.
        use_sub_dir = True
        for stream_name in stream_names:
            os.mkdir(os.path.join(proc_dir, stream_name))

        stamp_file = _write_proc_info(proc_dir)
        # Move some files into the working directory to prevent
        # multiple transfer processes trying to transfer them
        # simultaneously.
        for file in files:
            try:
                os.rename(
                    os.path.join(trans_dir, file.stream_name, file.name),
                    os.path.join(proc_dir, file.stream_name, file.name))
                proc.append(file)
                logger.debug('Processing file {}'.format(file.name))

            except Exception:
                # Another process may have started processing the file,
                # so skip it.
                logger.debug('Cannot move file {}, skipping'.format(file.name))

            # Did we get enough files already?
            if len(proc) >= max_files:
                break

    # Attempt to process all the files in our working directory.
    for file in proc:
        # Determine path to the directory containing the file and the
        # file itself.
        if use_sub_dir:
            proc_sub_dir = os.path.join(proc_dir, file.stream_name)
        else:
            proc_sub_dir = proc_dir

        proc_file = os.path.join(proc_sub_dir, file.name)

        try:
            # Check the file.
            transfer_check(proc_sub_dir, file.name, file.stream_name, subject,
                           namecheck_file)

            if dry_run:
                logger.info('Accepted file {} (DRY RUN)'.format(file.name))

            else:
                type, encoding = _get_mime(file.name)
                # Transfer the file.
                put_cadc_file(os.path.join(proc_sub_dir, file.name),
                              None, subject, mime_type=type,
                              mime_encoding=encoding)

                # On success, delete the file.
                logger.info('Transferred file {}'.
                            format(file.name))
                os.unlink(proc_file)

        except TransferException as e:
            # In the event of an error generated by one of the pre-transfer
            # checks, move the file into a reject directory.
            n_err += 1
            code = e.reject_code
            logger.error('Rejecting file {} ({})'.format(file.name, code))

            if not dry_run:
                reject_dir = os.path.join(trans_dir, 'reject', code)
                if not os.path.exists(reject_dir):
                    logger.debug(
                        'Making reject directory: {}'.format(reject_dir))
                    os.makedirs(reject_dir)

                logger.debug('Moving file to: {}'.format(reject_dir))
                os.rename(proc_file, os.path.join(reject_dir, file.name))

        except TransferFailure as e:
            # In the event of failure to transfer, put the file back into
            # its original stream name directory.
            n_err += 1
            logger.error(
                'Failed to transfer file {} ({})'.format(file.name, str(e)))

            if not dry_run:
                os.rename(
                    proc_file,
                    os.path.join(trans_dir, file.stream_name, file.name))

        except Exception as e:
            # Catch any other exception and also put the file back.
            n_err += 1
            logger.exception(
                'Error while transferring file {} : {}'.
                format(file.name, str(e)))

            if not dry_run:
                os.rename(
                    proc_file,
                    os.path.join(trans_dir, file.stream_name, file.name))

    # Finally clean up the processing directory.  It should have nothing
    # left in it by this point other than the stream subdirectories and
    # stamp file.
    if not dry_run:
        os.unlink(stamp_file)

        for stream_name in stream_names:
            os.rmdir(os.path.join(proc_dir, stream_name))

        os.rmdir(proc_dir)

    # If errors occurred, exit with bad status.
    if n_err:
        raise CommandError('Errors occurred during transfer'
                           ' ({0} error(s))'.format(n_err))


def transfer_check(proc_dir, filename, stream_name, subject,
                   namecheck_file=None):
    """Check if a file is suitable for transfer to CADC.

    Given the directory, file name and stream ("new", "replace", "any"),
    determine if a file is acceptable.  This function aims to replicate the
    checks which would have been made by the CADC e-transfer process.
    Checking for decompressibility is not implemented as it is not expected
    that we will be transferring compressed files.

    Raises a TransferException (including a rejection code) if a problem
    is detected.  No changes to the filesystem should be made, so this
    function should be safe to call in dry run mode.

    """

    proc_file = os.path.join(proc_dir, filename)
    proc_file = os.path.join(proc_dir, filename)

    # Check for permission to read the file.
    if not os.access(proc_file, os.R_OK):
        raise TransferException('permission')

    # Check if file size is zero.
    if os.stat(proc_file).st_size == 0:
        raise TransferException('empty')

    # Name-check.
    if namecheck_file:
        namecheck_section = check_file_name(namecheck_file, filename, True)
        if namecheck_section is None:
            raise TransferException('name')

    # Check correct new/replacement/any stream name.
    try:
        cadc_file_info = fetch_cadc_file_info(filename, subject)
    except ProcError:
        raise TransferFailure('Unable to check CADC file info')

    if stream_name == 'new':
        if cadc_file_info is not None:
            raise TransferException('not_new')

    elif stream_name == 'replace':
        if cadc_file_info is None:
            raise TransferException('not_replace')
    elif stream_name == 'any':
        # nothing to do
        pass

    check_valid(proc_file)


def _get_files_to_transfer(stream_names, trans_dir):
    # Search for files to transfer. Files updated more recently than
    # MIN_STAGE_TIME are ignored - this is to prevent processing files
    # not transferred completely
    files = []
    for stream_name in stream_names:
        if os.path.isdir(os.path.join(trans_dir, stream_name)):
            dir = os.path.join(trans_dir, stream_name)
            now = int(datetime.now().strftime('%s'))  # now in seconds
            tmp = [(f, os.path.getctime(os.path.join(dir, f))) for f
                   in os.listdir(dir) if
                   (now - os.path.getctime(os.path.join(dir, f))) >
                   MIN_STAGE_TIME * 60]
            if tmp:
                tmp.sort(key=lambda x: x[1])
                for file in tmp:
                    logger.debug('Found file {} ({})'.format(file[0],
                                                             stream_name))
                    files.append(FileInfo(file[0], stream_name))
    return files


def _write_proc_info(proc_dir):
    # Write stamp file to allow automatic clean-up.
    stamp_file = os.path.join(proc_dir, PROC_TIMESTAMP_FILE)
    config = ConfigParser()
    config.add_section('transfer')
    config.set('transfer', 'pid', str(os.getpid()))
    config.set('transfer', 'start',
               datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
    with open(stamp_file, 'w') as f:
        config.write(f)

    return stamp_file


def _get_proc_info(proc_dir):
    # Check for and read the timestamp file.
    stamp_file = os.path.join(proc_dir, PROC_TIMESTAMP_FILE)
    config = ConfigParser()
    config_files_read = config.read(stamp_file)
    if not config_files_read:
        logger.debug('Directory {} has no stamp file'.format(proc_dir))
        return None
    return config, stamp_file


def clean_up(trans_dir, dry_run=False):
    """Attempt to clean up orphaned tranfer "proc" directories.

    :param trans_dir -- the source directory
    :param dry_run -- perform checks only in dry_run mode
-    """

    # Determine latest start time for which we will consider cleaning up
    # a proc directory.
    start_limit = datetime.utcnow() - timedelta(
        minutes=int(etrans_config.get(ETRANS_SECTION, 'cleanup_minutes')))

    # Look for proc directories.
    proc_base_dir = os.path.join(trans_dir, 'proc')

    if not os.path.isdir(proc_base_dir):
        return

    for dir_ in os.listdir(proc_base_dir):
        # Consider only directories with the expected name prefix.
        proc_dir = os.path.join(proc_base_dir, dir_)
        if not (dir_.startswith('proc') and os.path.isdir(proc_dir)):
            continue

        logger.debug('Directory {} found'.format(dir_))
        timestamp_file, config_filename = _get_proc_info(proc_dir)
        if not timestamp_file:
            continue

        # Check if the transfer started too recently to consider.
        start = datetime.strptime(timestamp_file.get('transfer', 'start'),
                                  '%Y-%m-%d %H:%M:%S')

        if start > start_limit:
            logger.debug('Directory {} is too recent to clean up'.format(dir_))
            continue

        # Check if the transfer process is still running (by PID).
        pid = int(timestamp_file.get('transfer', 'pid'))
        is_running = True
        try:
            os.kill(pid, 0)
        except OSError:
            is_running = False

        if is_running:
            logger.debug('Directory {} corresponds to running process ({})'.
                         format(dir_, pid))

        # All checks are complete: move the files back to their initial
        # stream name directories.
        n_moved = 0
        n_skipped = 0

        for stream_name in allowed_streams:
            stream_has_skipped_files = False

            proc_stream_dir = os.path.join(proc_dir, stream_name)
            if not os.path.exists(proc_stream_dir):
                continue

            orig_stream_dir = os.path.join(trans_dir, stream_name)
            if (not os.path.exists(orig_stream_dir)) and (not dry_run):
                os.mkdir(orig_stream_dir)

            for file_ in os.listdir(proc_stream_dir):
                logger.debug('Directory {} has file {} ({})'.
                             format(dir_, file_, stream_name))

                proc_file = os.path.join(proc_stream_dir, file_)
                orig_file = os.path.join(orig_stream_dir, file_)

                if os.path.exists(orig_file):
                    logger.warning(
                        'File {} present in {} and {} directories'.
                        format(file_, dir_, stream_name))
                    n_skipped += 1
                    stream_has_skipped_files = True

                else:
                    if dry_run:
                        logger.info('Would move {} {} back to {} (DRY RUN)'.
                                    format(dir_, file_, stream_name))
                    else:
                        os.rename(proc_file, orig_file)
                    n_moved += 1

            if (not stream_has_skipped_files) and (not dry_run):
                os.rmdir(proc_stream_dir)

        logger.info(
            'Proc directory {}: {} file(s) cleaned up, {} skipped'.
            format(dir_, n_moved, n_skipped))

        # If we didn't skip any files, remove the stamp file and now-empty
        # proc directory.  (Unless in dry run mode.)
        if n_skipped or dry_run:
            continue

        os.unlink(config_filename)
        os.rmdir(proc_dir)


def get_rejected_files(dirname):
    result = {}
    if os.path.isdir(os.path.join(dirname, 'reject')):
        for d in os.listdir(os.path.join(dirname, 'reject')):
            dir = os.path.join(dirname, 'reject', d)
            if os.path.isdir(dir):
                if d.endswith('verify'):
                    result[d.split()[0]] = os.listdir(dir)
                else:
                    result[d] = os.listdir(dir)
            else:
                logger.error('Unexpected file in the rejected dir: {}'.
                             format(dir))
    return result


def get_trans_files(dirname):
    """
    Checks a directory for files that are being processed ('proc/proc*')
    and gets the relevant information (pid, number of files, etc.)
    :param dirname: directory name to check
    :return: list with one entry per found directory in proc with each element
    of the list detailing the pid, time since start and number of remaining
    files or error message if sub directory not recognized as a
    processing one.
    """
    result = []
    proc_dir = os.path.join(dirname, 'proc')
    if os.path.isdir(proc_dir):
        for d in os.listdir(proc_dir):
            dir = os.path.join(proc_dir, d)
            if d.startswith('proc') and os.path.isdir(dir):
                procinfo = _get_proc_info(dir)[0]
                pid = int(procinfo.get('transfer', 'pid'))
                start = procinfo.get('transfer', 'start')
                proc = {'pid': pid, 'started': start}
                # count the files
                for input in allowed_streams:
                    dd = os.path.join(dir, input)
                    if os.path.isdir(dd) and os.listdir(dd):
                        proc['files'] = len(os.listdir(dd))
                result.append(proc)
            else:
                msg = 'Unknown file/dir in proc directory: {}'.format(d)
                result.append({'error': msg})
    else:
        result = [{'error': 'No proc directory'}]
    return result


def print_status(dirname):
    """
    Prints the status of the system using the information from the root (input)
    directory and the log directory.
    :param dirname: directory to use
    """
    print('\nRejected files:')
    rfiles = get_rejected_files(dirname)
    if not rfiles:
        print('\tNone')
    else:
        types = list(rfiles.keys())
        types.sort()
        for f in types:
            print('\t {:6} -'.format(f),
                  colored('{:8d}'.format(len(rfiles[f])), 'red'))

    print('\nTransferring files:')
    tfiles = get_trans_files(dirname)
    if not tfiles:
        print('\tNone')
    else:
        errors = []
        for f in tfiles:
            if 'error' in f:
                errors.append(f)
            else:
                print('\tpid {:7} - {:6} (started at {:15})'.
                      format(f['pid'], f['files'], f['started']))
        if errors:
            print('\tErrors:')
            for e in errors:
                print(colored('\t\t{}'.format(e['error']), 'red'))

    print('\nTransferred files*:')
    lasth = [0, 0]
    last24h = [0, 0]
    last7d = [0, 0]
    now = datetime.utcnow()
    logs = _get_last_week_logs()
    for l in logs:
        with open(l) as f:
            for r in f:
                if 'put_cadc_file' in r:  # TODO - make it more robust
                    fields = r.split('put_cadc_file -')
                    if len(fields) > 1:
                        logdate = datetime.strptime(
                            fields[0].split('[')[0].strip(),
                            '%Y-%m-%d %H:%M:%S,%f')
                        timediff = now - logdate
                        index = 1
                        data = json.loads(fields[1].strip())
                        if ('success' in data) and data['success']:
                            index = 0
                        if timediff.total_seconds()/60.0/60.0 < 1:
                            lasth[index] += 1
                        if timediff.total_seconds()/60.0/60.0 < 24:
                            last24h[index] += 1
                        if timediff.total_seconds()/60.0/60.0 < 24 * 7:
                            last7d[index] += 1
    print('\tLast hour success -', colored('{:6}'.format(lasth[0]), 'green'),
          ' error -', colored('{:8d}'.format(lasth[1]), 'red'))
    print('\tLast day  success -', colored('{:6}'.format(last24h[0]), 'green'),
          ' error -', colored('{:8d}'.format(last24h[1]), 'red'))
    print('\tLast week success -', colored('{:6}'.format(last7d[0]), 'green'),
          ' error -', colored('{:8d}'.format(last7d[1]), 'red'))
    print('* - details in {}'.format(', '.join(logs)))


def update_backup(subject, dirname):
    """
    Logs an update of the status of processing and rejecting files and
    transfers the latest logging files to the configured (vos) backup
    :param subject:
    :param dirname:
    :return:
    """
    backup_dir = etrans_config.get(ETRANS_SECTION, 'backup_dir')
    # this should come from the config instance
    config_file = os.path.join(utils._CONFIG_PATH, 'cadc-etrans-config')
    if not backup_dir:
        raise RuntimeError('backup_dir must be specified in {}'.
                           format(config_file))
    if not backup_dir.startswith('vos:'):
        raise RuntimeError(not 'Only back_dir of form vos: supported in {}'.
                           format(config_file))
    rfiles = get_rejected_files(dirname)
    tfiles = get_trans_files(dirname)
    _get_transfer_log().info('{} - {}'.
                             format(utils.LOG_STATUS_LABEL,
                                    json.dumps({'rejected': rfiles,
                                                'transferring': tfiles})))
    logger.debug('Rsyncing the transfer logs')
    translog = etrans_config.get(ETRANS_SECTION, 'transfer_log_dir')
    client = vos.Client(vospace_certfile=subject.certificate,
                        transfer_shortcut=True)
    for f in glob.glob(
            '{}*'.format(os.path.join(translog, TRANS_ROOT_LOGNAME))):
        client.copy(f, '{}/{}'.format(backup_dir, os.path.basename(f)))


def _get_mime(filename):
    """
    Gets mime time of a file according based on its extension and according
    to the configuration
    :param filename:
    :return:
    """
    _, file_extension = os.path.splitext(filename)
    try:
        type = etrans_config.get('mime-types', file_extension.strip('.'))
    except KeyError:
        type = None
    try:
        encoding = \
            etrans_config.get('mime-encodings', file_extension.strip('.'))
    except KeyError:
        encoding = None
    return type, encoding


def main_app():
    parser = util.get_base_parser(version=version.version,
                                  default_resource_id=False)

    parser.description = (
        'Application for transferring data and metadata electronically '
        'to the Canadian Astronomy Data Centre.\n'
        'It uses the config information in '
        '~/.config/cadc-etrans to get the execution context '
        'and configuration.')

    subparsers = parser.add_subparsers(
        dest='cmd',
        help='Supported commands. Use the -h|--help argument of a command '
             'for more details')
    data_parser = subparsers.add_parser(
        'data', description='Transfer data to a CADC archive',
        help='Transfer data to a CADC archive.')
    data_parser.add_argument('-c', '--check-filename',
                             help='Namecheck file to check file names against',
                             required=False)
    data_parser.add_argument(
        '-s', '--streamname', choices=allowed_streams,
        help='Process only files in this input stream [new, replace, any]')
    data_parser.add_argument(
        '--dryrun', help=('Perform all steps with the exception of the actual '
                          'file transfer to the CADC'),
        action='store_true', required=False)
    data_parser.add_argument('source',
                             help='Source directory where the files are '
                             'located.')
    data_parser.epilog = (
        'Note:\nTo ensure that a file is fully received before attempting to\n'
        'transfer it, it must spend a minimum amount of time (5min) in the\n'
        'input directory without being modified/updated prior to its\n'
        'processing.\n\n'
        'Examples:\n'
        '- Use default netrc file ($HOME/.netrc) to transfer FITS files in\n'
        '        the "current" dir: \n'
        '        cadc-etrans data -v -n current\n'
        '- Use a different netrc file transfer the files in dryrun mode:\n'
        '        cadc-etrans data -d --netrc ~/mynetrc --dryrun workdir ')

    status_parser = subparsers.\
        add_parser('status',
                   description='Displays the status of the system',
                   help='Display the status of the system')
    status_parser.add_argument('-b', '--backup',
                               help='sends status and local logs to a backup '
                                    'location specified in the config file',
                               action='store_true', required=False)
    status_parser.add_argument('source',
                               help='Source directory where the files are '
                               'located.')

    # handle errors
    errors = [0]

    def handle_error(msg, exit_after=True):
        """
        Prints error message and exit (by default)
        :param msg: error message to print
        :param exit_after: True if log error message and exit,
        False if log error message and return
        :return:
        """

        errors[0] += 1
        logger.error(msg)
        if exit_after:
            sys.exit(-1)  # TODO use different error codes?

    args = parser.parse_args()
    if len(sys.argv) < 2:
        parser.print_usage(file=sys.stderr)
        sys.stderr.write("{}: error: too few arguments\n".format(APP_NAME))
        sys.exit(-1)
    if args.verbose:
        logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    elif args.debug:
        logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
    else:
        if (args.cmd != 'status') and args.dryrun:
            logging.basicConfig(level=logging.INFO, stream=sys.stdout)
        else:
            logging.basicConfig(level=logging.WARN, stream=sys.stderr)

    if args.cmd == 'meta':
        raise NotImplementedError('meta command not implemented yet')
    subject = net.Subject.from_cmd_line_args(args)
    if args.cmd == 'status':
        if args.backup:
            update_backup(subject, args.source)
        else:
            print_status(args.source)
    elif args.cmd == 'data':
        try:
            clean_up(args.source, dry_run=args.dryrun)
            transfer(args.source, stream_name=args.streamname,
                     dry_run=args.dryrun, subject=subject,
                     namecheck_file=args.check_filename)
        except CommandError as e:
            logger.error('{}'.format(str(e)))
    else:
        print('ERROR - unknown command')
        sys.exit(-1)

    print('DONE')
