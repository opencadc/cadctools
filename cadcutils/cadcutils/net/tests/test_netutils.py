# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2020.                            (c) 2020.
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

import pytest
from mock import patch, Mock, MagicMock, call
from requests.utils import CaseInsensitiveDict
import base64

from cadcutils.net import get_header_filename, extract_md5, Transfer


def test_get_header_filename():
    assert 'example.txt' == get_header_filename(
        {'content-disposition': 'Attachment; filename=example.txt'})

    assert 'an example.txt' == get_header_filename(
        {'content-disposition': 'INLINE; FILENAME= "an example.txt" '})

    assert '€ rates.txt' == get_header_filename(
        {'content-disposition':
         "attachment;filename*= UTF-8''%e2%82%ac%20rates.txt"})

    assert '€ rates.txt' == get_header_filename(
        {'content-disposition': "attachment; filename=\"EURO rates.txt\";"
                                "filename*=utf-8''%e2%82%ac%20rates.txt"})


def test_extract_md5():
    md5checksum = '0x12345'
    headers = CaseInsensitiveDict()
    headers['digest'] = 'md5={}'.format(
        base64.b64encode(md5checksum.encode('ascii')).decode('ascii'))
    assert md5checksum == extract_md5(headers=headers)


# patch sleep to stop the test from sleeping and slowing down execution
@patch('cadcutils.net.netutils.time.sleep', MagicMock(), create=True)
def test_transfer_error():
    session = Mock()
    testservice_url = 'https://sometestservice.server/testservice'

    session.get.side_effect = [Mock(text='COMPLETED'),
                               Mock(text='COMPLETED')]
    test_target = Transfer(session=session)

    # job successfully completed
    assert not test_target.get_transfer_error(
        testservice_url + '/results/transferDetails', 'vos://testservice')
    session.get.assert_called_with(testservice_url + '/phase',
                                   allow_redirects=True)

    # job suspended
    session.reset_mock()
    session.get.side_effect = [Mock(text='COMPLETED'),
                               Mock(text='ABORTED')]
    with pytest.raises(OSError):
        test_target.get_transfer_error(
            testservice_url + '/results/transferDetails', 'vos://testservice')
    # check arguments for session.get calls

    session.get.assert_has_calls(
        [call(testservice_url + '/phase', allow_redirects=True),
         call(testservice_url + '/phase', allow_redirects=True)])

    # job encountered an internal error
    session.reset_mock()
    session.get.side_effect = [Mock(text='COMPLETED'),
                               Mock(text='ERROR'),
                               Mock(text='InternalFault')]
    with pytest.raises(OSError):
        test_target.get_transfer_error(
            testservice_url + '/results/transferDetails', 'vos://testservice')
    session.get.assert_has_calls(
        [call(testservice_url + '/phase', allow_redirects=True),
         call(testservice_url + '/phase', allow_redirects=True),
         call(testservice_url + '/error')])

    # job encountered an unsupported link error
    session.reset_mock()
    link_file = 'testlink.fits'
    session.get.side_effect = [Mock(text='COMPLETED'),
                               Mock(text='ERROR'),
                               Mock(
                                   text="Unsupported link target: " +
                                        link_file)]
    assert link_file == test_target.get_transfer_error(
        testservice_url + '/results/transferDetails', 'vos://testservice')
    session.get.assert_has_calls(
        [call(testservice_url + '/phase', allow_redirects=True),
         call(testservice_url + '/phase', allow_redirects=True),
         call(testservice_url + '/error')])


def test_transfer():
    session = Mock()
    redirect_response = Mock()
    redirect_response.status_code = 303
    redirect_response.headers = \
        {'Location': 'https://transfer.host/transfer'}
    response = Mock()
    response.status_code = 200
    response.text = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<vos:transfer xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.0" '
        'version="2.1">'
        '<vos:target>vos://some.host~vault/abc</vos:target>'
        '<vos:direction>pullFromVoSpace</vos:direction>'
        '<vos:protocol uri="ivo://ivoa.net/vospace/core#httpsget">'
        '<vos:endpoint>https://transfer.host/transfer/abc</vos:endpoint>'
        '<vos:securityMethod '
        'uri="ivo://ivoa.net/sso#tls-with-certificate" />'
        '</vos:protocol>'
        '<vos:keepBytes>true</vos:keepBytes>'
        '</vos:transfer>')
    session.post.return_value = redirect_response
    session.get.return_value = response
    test_transfer = Transfer(session=session)
    test_transfer.get_transfer_error = Mock()  # not transfer error
    protocols = test_transfer.transfer(
        'https://some.host/service', 'vos://abc', 'pullFromVoSpace')
    assert protocols == ['https://transfer.host/transfer/abc']

    session.reset_mock()
    session.post.return_value = Mock(status_code=404)
    with pytest.raises(OSError) as e:
        test_transfer.transfer(
            'https://some.host/service', 'vos://abc',
            'pullFromVoSpace')
        assert 'File not found: vos://abc' == str(e)

    session.reset_mock()
    session.post.return_value = Mock(status_code=500)
    with pytest.raises(OSError) as e:
        test_transfer.transfer(
            'https://some.host/service', 'vos://abc',
            'pullFromVoSpace')
        assert 'Failed to get transfer service response.' == str(e)
