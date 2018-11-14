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
from mock import Mock, patch
from cadcetrans.utils import put_cadc_file, fetch_cadc_file_info,\
    ProcError
from cadcutils.net import Subject
from cadcutils.exceptions import NotFoundException
import tempfile

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')


def config_get(section, key):
    # this is a mock of the
    config = {'archive': 'TESTPUT',
              'transfer_log_dir': '/tmp'}
    return config[key]


@patch('cadcetrans.utils.etrans_config')
def test_info(config_mock):
    config_mock.get.return_value = 'TESTINFO'
    with patch('cadcetrans.utils.CadcDataClient') as data_mock:
        # file found
        info_mock = Mock(return_value={'md5sum': '0xbeef'})
        data_mock.return_value.get_file_info = info_mock

        result = fetch_cadc_file_info('foo.fits', Subject())
        assert result
        assert '0xbeef' == result['md5sum']
        info_mock.assert_called_with('TESTINFO', 'foo.fits')

        # file not found
        data_mock.return_value.get_file_info.side_effect = NotFoundException()
        assert not fetch_cadc_file_info('foo.fits', Subject())

        # calling error
        with pytest.raises(ProcError):
            data_mock.return_value.get_file_info.side_effect = RuntimeError()
            fetch_cadc_file_info('foo.fits', Subject())


@patch('cadcetrans.utils.etrans_config')
def test_put(config_mock):
    config_mock.get = config_get
    with patch('cadcetrans.utils.CadcDataClient') as data_mock:
        # success
        put_mock = Mock(return_value=None)
        file = tempfile.NamedTemporaryFile(suffix='.fits')
        data_mock.return_value.put_file = put_mock
        put_cadc_file(file.name, None, Subject())
        put_mock.assert_called_with('TESTPUT', file.name, archive_stream=None,
                                    mime_type=None, mime_encoding=None)

        # to stream
        put_mock = Mock(return_value=None)
        data_mock.return_value.put_file = put_mock
        put_cadc_file(file.name, 'raw', Subject())
        put_mock.assert_called_with('TESTPUT', file.name,
                                    archive_stream='raw',
                                    mime_type=None, mime_encoding=None)

        # MIME types and encoding
        put_mock = Mock(return_value=None)
        data_mock.return_value.put_file = put_mock
        put_cadc_file(file.name, 'raw', Subject(), 'application/fits', 'gzip')
        put_mock.assert_called_with('TESTPUT', file.name,
                                    archive_stream='raw',
                                    mime_type='application/fits',
                                    mime_encoding='gzip')

        put_mock = Mock(return_value=None)
        data_mock.return_value.put_file = put_mock
        put_cadc_file(file.name, 'raw', Subject(), '', None)
        put_mock.assert_called_with('TESTPUT', file.name,
                                    archive_stream='raw',
                                    mime_type='',
                                    mime_encoding=None)

        # calling error
        with pytest.raises(ProcError):
            data_mock.return_value.put_file.side_effect = RuntimeError()
            put_cadc_file(file.name, 'raw', Subject())
