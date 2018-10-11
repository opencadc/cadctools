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
import pytest
from cadcutils.net import Subject
from cadcetrans.etrans_core import transfer, main_app
from cadcetrans.utils import CommandError
import tempfile
import shutil
from mock import patch, Mock
from six import StringIO
import sys

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')
TESTDATA_INPUT_DIR = os.path.join(TESTDATA_DIR, 'input')

# create a work directory
PROC_DIR = tempfile.mkdtemp(prefix='cadcetrans')
dest = os.path.join(PROC_DIR, 'new')
os.mkdir(dest)


class MyExitError(Exception):
    pass


def config_get(section, key):
    # this is a mock of the
    config = {'transdir': PROC_DIR, 'max_files': 10,
              'ad_stream': 'RAW:raw PROCESSED:product'}
    return config[key]


@patch('cadcetrans.etrans_core.etrans_config')
def test_transfer(config_mock):
    config_mock.get = config_get

    # no files to transfer
    transfer('new', True, Subject())

    # copy all the files including the invalid ones:
    src_files = os.listdir(TESTDATA_INPUT_DIR)
    for file_name in src_files:
        full_file_name = os.path.join(TESTDATA_INPUT_DIR, file_name)
        if (os.path.isfile(full_file_name)):
            shutil.copy(full_file_name, dest)

    invalid_files = [f for f in os.listdir(dest) if 'invalid' in f]

    with pytest.raises(CommandError) as e:
        transfer('new', True, Subject())
    assert 'Errors occurred during transfer ({} error(s))'\
           .format(len(invalid_files)) in str(e)

    # remove the "invalid" files
    for f in invalid_files:
        os.unlink(os.path.join(dest, f))

    transfer('new', True, Subject())

    with patch('cadcetrans.etrans_core.put_cadc_file'):
        transfer('new', False, Subject())  # no more errors
    # all files processed
    assert not os.listdir(dest)

    # stream required in dryrun mode
    with pytest.raises(CommandError):
        transfer(None, True, Subject())


@patch('sys.exit', Mock(side_effect=[MyExitError, MyExitError, MyExitError,
                                     MyExitError, MyExitError, MyExitError,
                                     MyExitError, MyExitError]))
def test_help():
    """ Tests the helper displays for commands and subcommands in main"""

    # expected helper messages
    with open(os.path.join(TESTDATA_DIR, 'help.txt'), 'r') as myfile:
        usage = myfile.read()
    with open(
            os.path.join(TESTDATA_DIR, 'data_help.txt'), 'r') as myfile:
        data_usage = myfile.read()
    with open(os.path.join(TESTDATA_DIR, 'meta_help.txt'), 'r') as myfile:
        meta_usage = myfile.read()

    # maxDiff = None  # Display the entire difference
    # --help
    with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
        sys.argv = ["cadc-etrans", "--help"]
        with pytest.raises(MyExitError):
            main_app()
        assert usage == stdout_mock.getvalue()

    with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
        sys.argv = ["cadc-etrans", "data", "--help"]
        with pytest.raises(MyExitError):
            main_app()
        assert data_usage == stdout_mock.getvalue()

    with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
        sys.argv = ["cadc-etrans", "meta", "--help"]
        with pytest.raises(MyExitError):
            main_app()
        assert meta_usage == stdout_mock.getvalue()
