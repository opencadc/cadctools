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

from .data_verify import get_md5sum, is_valid_fits, is_valid_png
from .utils import etrans_config, fetch_cadc_file_info, put_cadc_file
from .namecheck import check_file_name

from cadcutils import net, util
from cadcetrans import version
from .utils import CommandError, ProcError, TransferFailure

APP_NAME = 'cadc-etrans'

logger = logging.getLogger(__name__)

FileInfo = namedtuple('FileInfo', 'name stream')

allowed_streams = ('new', 'replace')


class TransferException(Exception):
    """Class for transfer exceptions.

    Objects in this class have a "reject_code" attribute corresponding
    to the p-transfer reject subdirectory the file should be moved into.
    """

    def __init__(self, code):
        Exception.__init__(
            self, 'file rejected for p-transfer ({0})'.format(code))
        self.reject_code = code


def transfer(stream=None, dry_run=False, subject=None, namecheck_file=None):
    """
    Attempt to put files into the archive at CADC.

    This function is controlled by the configuration file entries
    etransfer.transdir and etransfer.maxfiles. It looks in the "new" and
    "replace" directories inside "transdir" for at most  "max_files" files.
    The files are moved to a temporary processing directory and then either
    moved to a reject directory or deleted on completion.  In the event of
    failure to transfer, the files are put back in either the "new" or
    "replace" directory.

    The stream argument can be given to select only files in the "new" or
    "replace" directory.  It must be given in the dry_run case since then no
    "proc" directory is created.

    :param stream -- Stream to work with
    :param dry_run -- If True the last step (actual transfer of file) not
    executed
    :param subject -- Subject that runs the command
    :param namecheck_file -- File containing the rules for file names or None
    if there are no name checks

    """

    trans_dir = etrans_config.get('etransfer', 'transdir')
    max_files = int(etrans_config.get('etransfer', 'max_files'))

    files = []
    n_err = 0

    # Select transfer streams.
    streams = allowed_streams
    if stream is None:
        if dry_run:
            raise CommandError('Stream must be specified in dry run mode')
    else:
        if stream not in streams:
            raise CommandError('Unknown stream {0}'.format(stream))

        streams = (stream,)

    # Search for files to transfer.
    for stream in streams:
        for file in os.listdir(os.path.join(trans_dir, stream)):
            logger.debug('Found file {} ({})'.format(file, stream))
            files.append(FileInfo(file, stream))

    if not files:
        logger.info('No files found for transfer')
        return

    if dry_run:
        # Work in the stream directory.
        proc = files[:max_files]
        proc_dir = os.path.join(trans_dir, stream)
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

        # Create stream-based subdirectories.
        use_sub_dir = True
        for stream in streams:
            os.mkdir(os.path.join(proc_dir, stream))

        # Write stamp file to allow automatic clean-up.
        stamp_file = os.path.join(proc_dir, 'transfer.ini')

        config = ConfigParser()
        config.add_section('transfer')
        config.set('transfer', 'pid', str(os.getpid()))
        config.set('transfer', 'start',
                   datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))

        with open(stamp_file, 'w') as f:
            config.write(f)

        # Move some files into the working directory to prevent
        # multiple transfer processes trying to transfer them
        # simultaneously.
        for file in files:
            try:
                os.rename(
                    os.path.join(trans_dir, file.stream, file.name),
                    os.path.join(proc_dir, file.stream, file.name))
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
            proc_sub_dir = os.path.join(proc_dir, file.stream)
        else:
            proc_sub_dir = proc_dir

        proc_file = os.path.join(proc_sub_dir, file.name)

        try:
            # Check the file.
            md5sum = get_md5sum(proc_file)
            ad_stream = transfer_check(
                proc_sub_dir, file.name, file.stream, md5sum, subject,
                namecheck_file)

            if dry_run:
                logger.info('Accepted file {} ({}) (DRY RUN)'.
                            format(file.name, ad_stream))

            else:
                # Transfer the file.
                put_cadc_file(os.path.join(proc_sub_dir, file.name),
                              ad_stream, subject)

                # # Check it was transferred correctly.
                # try:
                #     cadc_file_info = fetch_cadc_file_info(file.name)
                # except ProcError:
                #     raise TransferFailure('Unable to check CADC file info')
                #
                # if cadc_file_info is None:
                #     # File doesn't seem to be there?
                #     logger.error('File transferred but has no info')
                #     raise TransferFailure('No file info')
                #
                # elif md5sum != cadc_file_info['md5sum']:
                #     # File corrupted on transfer?  Put it back but in
                #     # the replace directory for later re-transfer.
                #     logger.error('File transferred but MD5 sum wrong')
                #     file = file._replace(stream='replace')
                #     raise TransferFailure('MD5 sum wrong')

                # On success, delete the file.
                logger.info('Transferred file {} ({})'.format(file.name,
                                                              ad_stream))
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
            # its original stream directory.
            n_err += 1
            logger.error(
                'Failed to transfer file {} ({})'.format(file.name, str(e)))

            if not dry_run:
                os.rename(
                    proc_file,
                    os.path.join(trans_dir, file.stream, file.name))

        except Exception as e:
            # Catch any other exception and also put the file back.
            n_err += 1
            logger.exception(
                'Error while transferring file {}'.format(file.name))

            if not dry_run:
                os.rename(
                    proc_file,
                    os.path.join(trans_dir, file.stream, file.name))

    # Finally clean up the processing directory.  It should have nothing
    # left in it by this point other than the stream subdirectories and
    # stamp file.
    if not dry_run:
        os.unlink(stamp_file)

        for stream in streams:
            os.rmdir(os.path.join(proc_dir, stream))

        os.rmdir(proc_dir)

    # If errors occurred, exit with bad status.
    if n_err:
        raise CommandError('Errors occurred during transfer'
                           ' ({0} error(s))'.format(n_err))


def transfer_check(proc_dir, filename, stream, md5sum, subject,
                   namecheck_file=None):
    """Check if a file is suitable for transfer to CADC.

    Given the directory, file name and stream ("new" or "replace"), determine
    if a file is acceptable.  This function aims to replicate the checks which
    would have been made by the CADC e-transfer process.  Checking for
    decompressibility is not implemented as it is not expected that we will be
    transferring compressed files.

    Raises a TransferException (including a rejection code) if a problem
    is detected.  No changes to the filesystem should be made, so this
    function should be safe to call in dry run mode.

    Returns the CADC AD stream to be used for the file.  This is determined by
    a mapping from namecheck section to stream name in the configuration file
    entry etransfer.ad_stream.
    """

    ad_streams = dict(map(
        lambda x: x.split(':'),
        etrans_config.get('etransfer', 'ad_stream').split(' ')))

    proc_file = os.path.join(proc_dir, filename)

    # Check for permission to read the file.
    if not os.access(proc_file, os.R_OK):
        raise TransferException('permission')

    # Check if file size is zero.
    if os.stat(proc_file).st_size == 0:
        raise TransferException('empty')

    # Check extension and validity.
    (root, ext) = os.path.splitext(filename)
    # if ext == '.sdf':
    #    if not valid_hds(proc_file):
    #        raise TransferException('corrupt')

    if ext == '.fits':
        if not is_valid_fits(proc_file):
            raise TransferException('fitsverify')

    elif ext == '.png':
        if not is_valid_png(proc_file):
            raise TransferException('corrupt')

    else:
        raise TransferException('filetype')

    # Name-check.
    if namecheck_file:
        namecheck_section = check_file_name(namecheck_file, filename, True)
        if namecheck_section is None:
            raise TransferException('name')
        if namecheck_section in ad_streams:
            ad_stream = ad_streams[namecheck_section]
        else:
            raise TransferException('stream')
    else:
        ad_stream = None
    # Check correct new/replacement stream.
    try:
        cadc_file_info = fetch_cadc_file_info(filename, subject)
    except ProcError:
        raise TransferFailure('Unable to check CADC file info')

    if stream == 'new':
        if cadc_file_info is not None:
            raise TransferException('not_new')

    elif stream == 'replace':
        if cadc_file_info is None:
            raise TransferException('not_replace')
        elif md5sum == cadc_file_info['md5sum']:
            raise TransferException('unchanged')

    else:
        raise Exception('unknown stream {0}'.format(stream))

    return ad_stream


def clean_up(dry_run=False):
    """Attempt to clean up orphaned p-tranfer "proc" directories.
    """

    trans_dir = etrans_config.get('etransfer', 'transdir')

    # Determine latest start time for which we will consider cleaning up
    # a proc directory.
    start_limit = datetime.utcnow() - timedelta(
        minutes=int(etrans_config.get('etransfer', 'cleanup_minutes')))

    # Look for proc directories.
    proc_base_dir = os.path.join(trans_dir, 'proc')

    for dir_ in os.listdir(proc_base_dir):
        # Consider only directories with the expected name prefix.
        proc_dir = os.path.join(proc_base_dir, dir_)
        if not (dir_.startswith('proc') and os.path.isdir(proc_dir)):
            continue

        logger.debug('Directory {} found'.format(dir_))

        # Check for and read the stamp file.
        stamp_file = os.path.join(proc_dir, 'transfer.ini')
        config = ConfigParser()
        config_files_read = config.read(stamp_file)
        if not config_files_read:
            logger.debug('Directory {} has no stamp file'.format(dir_))
            continue

        # Check if the transfer started too recently to consider.
        start = datetime.strptime(config.get('transfer', 'start'),
                                  '%Y-%m-%d %H:%M:%S')

        if start > start_limit:
            logger.debug('Directory {} is too recent to clean up'.format(dir_))
            continue

        # Check if the transfer process is still running (by PID).
        pid = int(config.get('transfer', 'pid'))
        is_running = True
        try:
            os.kill(pid, 0)
        except OSError:
            is_running = False

        if is_running:
            logger.debug('Directory {} corresponds to running process ({})'.
                         format(dir_, pid))

        # All checks are complete: move the files back to their initial
        # stream directories.
        n_moved = 0
        n_skipped = 0

        for stream in allowed_streams:
            stream_has_skipped_files = False

            proc_stream_dir = os.path.join(proc_dir, stream)
            if not os.path.exists(proc_stream_dir):
                continue

            orig_stream_dir = os.path.join(trans_dir, stream)
            if (not os.path.exists(orig_stream_dir)) and (not dry_run):
                os.mkdir(orig_stream_dir)

            for file_ in os.listdir(proc_stream_dir):
                logger.debug('Directory {} has file {} ({})'.
                             format(dir_, file_, stream))

                proc_file = os.path.join(proc_stream_dir, file_)
                orig_file = os.path.join(orig_stream_dir, file_)

                if os.path.exists(orig_file):
                    logger.warning(
                        'File {} present in {} and {} directories'.
                        format(file_, dir_, stream))
                    n_skipped += 1
                    stream_has_skipped_files = True

                else:
                    if dry_run:
                        logger.info('Would move {} {} back to {} (DRY RUN)'.
                                    format(dir_, file_, stream))
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

        os.unlink(stamp_file)
        os.rmdir(proc_dir)


def main_app():
    parser = util.get_base_parser(version=version.version)

    parser.description = (
        'Application for transferring data and metadata electronically '
        'to the Canadian Astronomy Data Centre.\n'
        'It uses the config information in '
        '$HOME/.config/cadc/cadc-etrans-config to get the execution context '
        'and configuration.')

    subparsers = parser.add_subparsers(
        dest='cmd',
        help='Supported commands. Use the -h|--help argument of a command '
             'for more details')
    get_parser = subparsers.add_parser(
        'data', description='Transfer data to a CADC archive',
        help='Transfer data to a CADC archive')
    get_parser.add_argument('-c', '--check-filename',
                            help='Namecheck file to check file names against',
                            required=False)
    get_parser.add_argument(
        '-s', '--stream',
        help='Process only files in this stream [new, replace]')
    #  TODO limit to range of stream
    get_parser.add_argument(
        '--dryrun', help=('Perform all steps with the exception of the actual '
                          'file transfer to the CADC'),
        action='store_true', required=False)
    get_parser.epilog = (
        'Examples:\n'
        '- Use default netrc file ($HOME/.netrc) to transfer FITS files in'
        '        the "current" dir: \n'
        '        cadc-etrans data -v -n current\n'
        '- Use a different netrc file transfer the files in dryrun mode:\n'
        '        cadc-etrans data -d --netrc ~/mynetrc --dryrun workdir ')

    subparsers.add_parser(
        'meta',
        description='Transfer metadata observation files to a CADC archive',
        help='Transfer metadata file to a CADC archive')

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
        logging.basicConfig(level=logging.WARN, stream=sys.stdout)

    if args.cmd == 'meta':
        raise NotImplementedError('meta command not implemented yet')

    subject = net.Subject.from_cmd_line_args(args)

    clean_up(dry_run=args.dryrun)
    transfer(stream=args.stream, dry_run=args.dryrun, subject=subject,
             namecheck_file=args.check_filename)
