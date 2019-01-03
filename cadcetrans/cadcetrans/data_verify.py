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

from .utils import etrans_config
import logging
import subprocess
import re
from PIL import Image
import os
import tarfile
import tempfile
from cadcetrans.utils import TransferException

logger = logging.getLogger(__name__)

fitsverify_output = re.compile(r' (\d+) warnings and (\d+) errors')


def check_valid(filename, allow_warnings=True):
    _, file_extension = os.path.splitext(filename)

    method_name = etrans_config.get('verifiers', file_extension.strip('.'))
    if not method_name:
        logger.warning('No verifier found for file {}'.format(filename))
        return
    try:
        method = globals()[method_name]
    except KeyError:
        logger.warning('Configured verifier for file extension {} not found. '
                       'Check your config file.'.format(file_extension))
        return
    method(filename, allow_warnings)


def check_valid_fits(filename, allow_warnings=True):
    """
    Check whether a given file is a valid FITS file.

    This uses fitsverify with the -q option to determine the number
    of errors and warnings.  Returns True unless there are errors,
    or there are warnings and the allow_warning option is not set.
    :param filename -- The name of the file
    :param allow_warnings -- True if fitsverify warnings are allowed to pass
    """

    # Fitsverify exits with bad status even if there are warnings, so we
    # can't just use subprocess.check_output.
    logger.debug('Running fitsverify on file {}'.format(filename))
    process = subprocess.Popen(['fitsverify', '-q', filename],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    process.wait()
    stdout, stderr = process.communicate()

    out = stdout.decode('utf-8')
    logger.debug(out.rstrip())

    if out.startswith('verification OK'):
        return True

    elif not allow_warnings:
        raise TransferException('fits verify')
    m = fitsverify_output.search(out)
    if not m:
        logger.error('fitsverify output did not match expected pattern')
        raise TransferException('fits verify')
    # warnings = int(m.group(1))
    errors = int(m.group(2))

    # Already know we are in "allow_warnings" mode, so just check the
    # number of actual errors.
    if errors:
        raise TransferException('fits verify')


def check_valid_png(filename, allow_warnings=False):
    """
    Determine whether a PNG file is valid.
    :param filename -- name of the file
    :param allow_warnings -- True for lenient validity check, False otherwise
    """
    try:
        im = Image.open(filename)
        assert im.format == 'PNG'
        im.verify()
        return
    except Exception as e:
        logger.debug('{} not valid png file: {}'.format(filename, str(e)))
        raise TransferException('png verify')


def check_valid_jpeg(filename, allow_warnings=False):
    """
    Determine whether a PNG file is valid.
    :param filename -- name of the file
    :param allow_warnings -- True for lenient validity check, False otherwise
    """
    try:
        im = Image.open(filename)
        assert im.format == 'JPEG'
        im.verify()
        return
    except Exception as e:
        logger.debug('{} not valid jpeg file: {}'.format(filename, str(e)))
        raise TransferException('jpeg verify')


def check_valid_tar(filename, allow_warnings=False):
    """
        Determine whether a tar, gz or bz2 file is valid.
        :param filename -- name of the file
        """
    try:
        if not tarfile.is_tarfile(filename):
            raise Exception()
    except Exception as e:
        logger.debug('{} not valid tar file: {}'.format(filename, str(e)))
        raise TransferException('tar verify')


def check_valid_tar_and_content(filename, allow_warnings=False):
    """
    Determine whether a tar, gz or bz2 file is valid. Unlike check_valid_tar,
    this method also checks the validity of the content files.
    :param filename -- name of the file
    :param allow_warnings -- True for lenient validity check, False otherwise
    """
    try:
        tar_file = tarfile.open(filename)
    except Exception as e:
        logger.debug('{} not valid tar file: {}'.format(filename, str(e)))
        raise TransferException('tar verify')

    logger.debug("Verify content of {}".format(filename))
    tmpdir = tempfile.mkdtemp(prefix='etrans-tarverify')
    tar_file.extractall(path=tmpdir)
    tar_file.close()
    for root, dirs, files in os.walk(tmpdir):
        for f in files:
            logger.debug('Checking file {} in ({})'.format(f, filename))
            check_valid(os.path.join(root, f), allow_warnings)


def check_valid_hds(filename, allow_warnings=False):
    """
    Checks to see if a given file is a valid hds file.

    This uses hdstrace, and assumes if it can provide a return
    code of 0 then the file is valid.
    It runs hdstrace from the starlink build defined in the
    run_job.starpath section of the config file.

    parameter:
    :param filename string
    full filename including path and suffix.

    returns Boolean
    True: file is valid hds
    False: file is not valid hds.

    NOTE: This is used by the JCMT folks only and depends on the STARLINK
    software. Hence it is not part of the Travis tests.
    """

    # Path to hdstrace.
    starpath = etrans_config.get('job_run', 'starpath')
    com_path = os.path.join(starpath, 'bin', 'hdstrace')

    # Environmental variables.
    myenv = os.environ.copy()
    myenv['ADAM_NOPROMPT'] = '1'
    myenv['ADAM_EXIT'] = '1'
    myenv['LD_LIBRARY_PATH'] = os.path.join(starpath, 'lib')

    # Run hdstrace.
    returncode = subprocess.call([com_path, filename, 'QUIET'],
                                 env=myenv,
                                 stderr=subprocess.STDOUT,
                                 shell=False)

    # Status is True for returncode=0, False otherwise.
    return returncode == 0
