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

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os
from cadcutils import util, exceptions
from cadcdata import CadcDataClient
import time
import logging

logger = logging.getLogger(__name__)

_ROOT = os.path.abspath(os.path.dirname(__file__))
_DEFAULT_CONFIG_PATH = os.path.join(_ROOT, 'data', 'default-cadcetrans-config')
_CONFIG_PATH = os.path.expanduser("~") + '/.config/cadc/cadc-etrans-config'

try:
    etrans_config = util.Config(_CONFIG_PATH)
except IOError:
    # Assume this is the first invocation and the config file has not been
    # created yet => create it
    util.Config.write_config(_CONFIG_PATH, _DEFAULT_CONFIG_PATH)
    # now read parse it again
    etrans_config = util.Config(_CONFIG_PATH)


class TransferFailure(Exception):
    """
    Class for p-transfer failures.

    Objects in this class represent a failure to transfer a file to
    CADC.  The transfer should be re-tried later.
    """
    pass


class TransferException(Exception):
    """Class for transfer exceptions.

    Objects in this class have a "reject_code" attribute corresponding
    to the p-transfer reject subdirectory the file should be moved into.
    """

    def __init__(self, code):
        Exception.__init__(
            self, 'file rejected for p-transfer ({0})'.format(code))
        self.reject_code = code


class CommandError(Exception):
    """
    Class for errors detected running a command.
    """
    pass


class ProcError(Exception):
    """
    Base Class to handle errors in this module.
    """
    pass


def fetch_cadc_file_info(filename, subject):
    """
    Retrieve information about a file at the CADC.
    :param filename -- name of the file
    :param subject -- subject (type cadcutils.net.Subject) executing the
    command.
    """
    try:
        archive = etrans_config.get('etransfer', 'archive')
        if not archive:
            raise RuntimeError('Name of archive not found')

        data_client = CadcDataClient(subject)

        return data_client.get_file_info(archive, filename)
    except exceptions.NotFoundException:
        return None
    except Exception as e:
        raise ProcError('Error fetching CADC file info: ' + str(e))


def put_cadc_file(filename, stream, subject, mime_type=None,
                  mime_encoding=None):
    """
    Transfers a file to the CADC archive
    :param filename -- name of the file
    :param stream -- the name of archive stream at the CADC
    :param subject -- subject (type cadcutils.net.Subject) executing the
    command.
    :param mime_type -- file MIME type
    :param mime_encoding - file MIME encoding
    """
    try:
        archive = etrans_config.get('etransfer', 'archive')
        if not archive:
            raise RuntimeError('Name of archive not found')

        size = os.stat(filename).st_size
        start = time.time()
        data_client = CadcDataClient(subject)

        data_client.put_file(archive, filename, archive_stream=stream,
                             mime_type=mime_type,
                             mime_encoding=mime_encoding)
        duration = time.time() - start
        _get_transfer_log().info('Success {} in {}s, avg. speed {}MB/s'.
                                 format(filename, round(duration, 2),
                                        round(size/1024/1024/duration, 2)))
    except Exception as e:
        _get_transfer_log().info('Error {}: {}'.format(filename, str(e)))
        raise ProcError('Error transferring file: ' + str(e))


def _get_transfer_log():
    logdir = etrans_config.get('etransfer', 'transfer_log_dir')
    trans_log = os.path.join(logdir, 'cadcetrans.log')
    if not _get_transfer_log.logger:
        _get_transfer_log.logger = logging.getLogger('cadcetrans')
        fh = logging.FileHandler(trans_log)
        fh.setFormatter(logging.Formatter("%(asctime)s --- %(message)s"))
        _get_transfer_log.logger.addHandler(fh)
        _get_transfer_log.logger.setLevel(logging.INFO)
    return _get_transfer_log.logger


_get_transfer_log.logger = None


def get_reverse_translog():
    logdir = etrans_config.get('etransfer', 'transfer_log_dir')
    trans_log = os.path.join(logdir, 'cadcetrans.log')
    buf_size = 8192
    with open(trans_log) as fh:
        segment = None
        offset = 0
        fh.seek(0, os.SEEK_END)
        file_size = remaining_size = fh.tell()
        while remaining_size > 0:
            offset = min(file_size, offset + buf_size)
            fh.seek(file_size - offset)
            buffer = fh.read(min(remaining_size, buf_size))
            remaining_size -= buf_size
            lines = buffer.split('\n')
            # the first line of the buffer is probably not a complete line so
            # we'll save it and append it to the last line of the next buffer
            # we read
            if segment is not None:
                # if the previous chunk starts right from the beginning of line
                # do not concact the segment to the last line of new chunk
                # instead, yield the segment first
                if buffer[-1] is not '\n':
                    lines[-1] += segment
                else:
                    yield segment
            segment = lines[0]
            for index in range(len(lines) - 1, 0, -1):
                if len(lines[index]):
                    yield lines[index]
        # Don't yield None if the file was empty
        if segment is not None:
            yield segment
