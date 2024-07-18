# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2024.                            (c) 2024.
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

import os
import unittest

import requests
from unittest.mock import Mock, patch, call, ANY
from io import StringIO
from urllib.parse import urlparse
import tempfile
import pytest
import hashlib
from tempfile import NamedTemporaryFile, TemporaryDirectory

from cadcutils import exceptions
from cadcutils import net
from cadcutils.net import ws, auth
from cadcutils.net.ws import DEFAULT_RETRY_DELAY, MAX_RETRY_DELAY, \
    MAX_NUM_RETRIES, SERVICE_RETRY, _check_server_version

# The following is a temporary workaround for Python issue
# 25532 (https://bugs.python.org/issue25532)
call.__wrapped__ = None

# Content type header
CONTENT_TYPE = 'Content-Type'
TEXT_TYPE = 'text/plain'


def test_check_server_version():
    assert _check_server_version(None, None) is None
    assert _check_server_version({}, None) is None
    assert _check_server_version({'server1': '1.3'}, None) is None
    assert _check_server_version(None, 'OpenCADC/cadc-rest + cadc/server1-1.2.3') is None

    with pytest.raises(RuntimeError):
        _check_server_version({'server1': '1.1'},
                              'OpenCADC/cadc-rest + cadc/server1-1.2.3')

    # not trigger cases
    # same version
    _check_server_version({'server1': '1.2'},
                          'OpenCADC/cadc-rest + cadc/server1-1.2')

    # patch version
    _check_server_version({'server1': '1.2'}, 'OpenCADC/cadc-rest + cadc/server1-1.2.3')

    # client ahead
    _check_server_version({'server1': '1.5'},
                          'OpenCADC/cadc-rest + cadc/server1-0.7')

    _check_server_version({'server1': '1.5'},
                          'OpenCADC/cadc-rest + cadc/server1-1.3.4')

    # client works with multiple server APIs
    _check_server_version({'server0': '0.1', 'server1': '1.2'},
                          'OpenCADC/cadc-rest + cadc/server1-1.2.3')

    with pytest.raises(RuntimeError):
        _check_server_version({'server0': '2.3', 'server1': '1.1'},
                              'OpenCADC/cadc-rest + cadc/server1-1.2.3')

    # error cases
    with pytest.raises(ValueError):
        _check_server_version({'server1': '1.1.1'},
                              'OpenCADC/cadc-rest + cadc/server1-1.2.3')

    with pytest.raises(ValueError):
        _check_server_version({'server1': '1.1a'},
                              'OpenCADC/cadc-rest + cadc/server1-1.2.3')
    with pytest.raises(ValueError):
        _check_server_version({'server1': '1.1'},
                              'OpenCADC/cadc-rest + cadc/server1-1.2a')


class TestListResources(unittest.TestCase):
    @patch('cadcutils.net.ws.requests.get')
    def test_list_resources(self, get_mock):
        response_caps = Mock()
        response_caps.text = (
            '# This is just a test\n'
            'ivo://cadc.nrc.ca/serv1 = '
            'http://www.cadc.nrc.gc.ca/serv1/capabilities\n'
            'ivo://cadc.nrc.ca/serv2 = '
            'http://www.cadc.nrc.gc.ca/serv2/capabilities\n')
        response_serv1 = Mock()
        response_serv1.text = (
            '<vosi:capabilities xmlns:vosi='
            '"http://www.ivoa.net/xml/VOSICapabilities/v1.0" '
            'xmlns:vs="http://www.ivoa.net/xml/VODataService/v1.1" '
            'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">\n'
            '<capability standardID="ivo://ivoa.net/std/VOSI#capabilities">\n'
            '<interface xsi:type="vs:ParamHTTP" role="std">\n'
            '<accessURL use="full">'
            'http://www.cadc.hia.nrcgc..ca/serv1/capabilities</accessURL>\n'
            '</interface>\n'
            '</capability>\n'
            '<capability standardID="ivo://ivoa.net/std/VOSI#availability">\n'
            '<interface xsi:type="vs:ParamHTTP" role="std">\n'
            '<accessURL use="full">'
            'http://www.cadc.nrc.gc.ca/serv1/availability</accessURL>\n'
            '</interface>\n'
            '</capability>\n'
            '</vosi:capabilities>\n')
        response_serv2 = Mock()
        response_serv2.text = (
            '<vosi:capabilities xmlns:vosi='
            '"http://www.ivoa.net/xml/VOSICapabilities/v1.0" '
            'xmlns:vs="http://www.ivoa.net/xml/VODataService/v1.1" '
            'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">\n'
            '<capability standardID="ivo://ivoa.net/std/VOSI#capabilities">\n'
            '<interface xsi:type="vs:ParamHTTP" role="std">\n'
            '<accessURL use="full">'
            'http://www.cadc.hia.nrcgc..ca/serv2/capabilities</accessURL>\n'
            '</interface>\n'
            '</capability>\n'
            '<capability standardID="ivo://ivoa.net/std/VOSI#tables-1.1">\n'
            '<interface xsi:type="vs:ParamHTTP" role="std">\n'
            '<accessURL use="full">'
            'http://www.cadc.nrc.gc.ca/serv2/availability</accessURL>\n'
            '</interface>\n'
            '</capability>\n'
            '</vosi:capabilities>\n')
        get_mock.side_effect = [response_caps, response_serv1, response_serv2]
        self.maxDiff = None
        usage = \
            ('ivo://cadc.nrc.ca/serv1 '
             '(http://www.cadc.nrc.gc.ca/serv1/capabilities) - '
             'Capabilities: ivo://ivoa.net/std/VOSI#availability, '
             'ivo://ivoa.net/std/VOSI#capabilities\n\n'
             'ivo://cadc.nrc.ca/serv2 '
             '(http://www.cadc.nrc.gc.ca/serv2/capabilities) - '
             'Capabilities: ivo://ivoa.net/std/VOSI#capabilities, '
             'ivo://ivoa.net/std/VOSI#tables-1.1')
        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            ws.list_resources()
            self.assertEqual(usage, stdout_mock.getvalue().strip())

    @patch('cadcutils.net.ws.WsCapabilities')
    @patch('cadcutils.net.auth.os.path.isfile', Mock())
    @patch('cadcutils.net.auth.netrclib')
    @patch('cadcutils.net.ws.RetrySession.put')
    @patch('cadcutils.net.ws.RetrySession.head')
    @patch('cadcutils.net.ws.RetrySession.delete')
    @patch('cadcutils.net.ws.RetrySession.get')
    @patch('cadcutils.net.ws.RetrySession.post')
    def test_ops(self, post_mock, get_mock, delete_mock, head_mock, put_mock,
                 netrclib_mock, caps_mock):
        anon_subject = auth.Subject()
        with self.assertRaises(ValueError):
            ws.BaseWsClient(None, anon_subject, "TestApp")
        resource = 'aresource'
        service = 'myservice'
        resource_id = 'ivo://www.canfar.net/{}'.format(service)
        # test anonymous access
        cm = Mock()
        cm.get_access_url.return_value = "http://host/availability"
        caps_mock.return_value = cm
        client = ws.BaseWsClient(resource_id, anon_subject, 'TestApp')
        resource_uri = urlparse(resource_id)
        base_url = 'http://{}{}/pub'.format(resource_uri.netloc,
                                            resource_uri.path)
        resource_url = 'http://{}{}/{}'.format(resource_uri.netloc, base_url,
                                               resource)
        self.assertEqual(anon_subject, client.subject)
        self.assertTrue(client.retry)
        self.assertEqual('TestApp', client.agent)
        self.assertTrue(client.retry)
        self.assertEqual(None, client._session)  # lazy initialization
        client.get(resource_url)
        get_mock.assert_called_with(resource_url, params=None, verify=True)
        params = {'arg1': 'abc', 'arg2': 123, 'arg3': True}
        client.post(resource_url, **params)
        post_mock.assert_called_with(resource_url, verify=True, **params)
        client.delete(resource_url)
        delete_mock.assert_called_with(resource_url, verify=True)
        client.head(resource_url)
        head_mock.assert_called_with(resource_url, verify=True)
        client.put(resource_url, **params)
        put_mock.assert_called_with(resource_url, verify=True, **params)
        self.assertTrue(isinstance(client._session, ws.RetrySession))

        # test basic authentication access
        post_mock.reset_mock()
        get_mock.reset_mock()
        put_mock.reset_mock()
        delete_mock.reset_mock()
        head_mock.reset_mock()
        host = 'www.different.org'
        subject = auth.Subject(netrc='somecert')
        client = ws.BaseWsClient(resource_id, subject, 'TestApp', retry=False,
                                 host=host)
        base_url = 'http://{}{}/auth'.format(host, resource_uri.path)
        resource_url = '{}/{}'.format(base_url, resource)
        self.assertEqual('TestApp', client.agent)
        self.assertFalse(client.retry)
        client.get(resource_url)
        get_mock.assert_called_with(resource_url, params=None, verify=True)
        params = {'arg1': 'abc', 'arg2': 123, 'arg3': True}
        client.post(resource_url, **params)
        post_mock.assert_called_with(resource_url, verify=True, **params)
        client.delete(resource_url)
        delete_mock.assert_called_with(resource_url, verify=True)
        client.head(resource_url)
        head_mock.assert_called_with(resource_url, verify=True)
        client.put(resource_url, **params)
        put_mock.assert_called_with(resource_url, verify=True, **params)
        self.assertTrue(isinstance(client._session, ws.RetrySession))

        # test cert authentication
        post_mock.reset_mock()
        get_mock.reset_mock()
        put_mock.reset_mock()
        delete_mock.reset_mock()
        head_mock.reset_mock()
        certfile = 'some/certfile.pem'
        subject = auth.Subject(certificate=certfile)
        client = ws.BaseWsClient(resource_id, subject, 'TestApp')
        base_url = 'https://{}{}/pub'.format(resource_uri.netloc,
                                             resource_uri.path)
        resource_url = '{}/{}'.format(base_url, resource)
        self.assertEqual('TestApp', client.agent)
        self.assertTrue(client.retry)
        client.get(resource_url)
        get_mock.assert_called_with(resource_url, params=None, verify=True)
        params = {'arg1': 'abc', 'arg2': 123, 'arg3': True}
        client.post(resource_url, **params)
        post_mock.assert_called_with(resource_url, verify=True, **params)
        client.delete(resource_url)
        delete_mock.assert_called_with(resource_url, verify=True)
        client.head(resource_url)
        head_mock.assert_called_with(resource_url, verify=True)
        client.put(resource_url, **params)
        put_mock.assert_called_with(resource_url, verify=True, **params)
        self.assertTrue(isinstance(client._session, ws.RetrySession))
        self.assertEqual((certfile, certfile), client._session.cert)

        # repeat above tests test with the temporary sc2repo test
        resource = 'aresource'
        service = 'sc2repo'
        resource_id = 'ivo://www.canfar.phys.uvic.ca/{}'.format(service)
        # test anonymous access
        client = ws.BaseWsClient(resource_id, auth.Subject(), 'TestApp')
        resource_uri = urlparse(resource_id)
        base_url = 'http://{}{}/observations'.format(resource_uri.netloc,
                                                     resource_uri.path)
        resource_url = '{}/{}'.format(base_url, resource)
        self.assertTrue(client.retry)
        self.assertEqual('TestApp', client.agent)
        self.assertTrue(client.retry)
        self.assertEqual(None, client._session)  # lazy initialization
        client.get(resource_url)
        get_mock.assert_called_with(resource_url, params=None, verify=True)
        params = {'arg1': 'abc', 'arg2': 123, 'arg3': True}
        client.post(resource_url, **params)
        post_mock.assert_called_with(resource_url, verify=True, **params)
        client.delete(resource_url)
        delete_mock.assert_called_with(resource_url, verify=True)
        client.head(resource_url)
        head_mock.assert_called_with(resource_url, verify=True)
        client.put(resource_url, **params)
        put_mock.assert_called_with(resource_url, verify=True, **params)
        self.assertTrue(isinstance(client._session, ws.RetrySession))

        # test basic authentication access
        post_mock.reset_mock()
        get_mock.reset_mock()
        put_mock.reset_mock()
        delete_mock.reset_mock()
        head_mock.reset_mock()
        host = 'caom2workshop.canfar.net'
        subject = auth.Subject(netrc=True)
        subject._hosts_auth[host] = ('auser', 'apasswd')
        client = ws.BaseWsClient(resource_id, subject, 'TestApp', retry=False,
                                 host=host)
        base_url = 'http://{}{}/auth-observations'.format(host,
                                                          resource_uri.path)
        resource_url = '{}/{}'.format(base_url, resource)
        self.assertEqual('TestApp', client.agent)
        self.assertFalse(client.retry)
        client.get(resource_url)
        get_mock.assert_called_with(resource_url, params=None, verify=True)
        params = {'arg1': 'abc', 'arg2': 123, 'arg3': True}
        client.post(resource_url, **params)
        post_mock.assert_called_with(resource_url, verify=True, **params)
        client.delete(resource_url)
        delete_mock.assert_called_with(resource_url, verify=True)
        client.head(resource_url)
        head_mock.assert_called_with(resource_url, verify=True)
        client.put(resource_url, **params)
        put_mock.assert_called_with(resource_url, verify=True, **params)
        self.assertTrue(isinstance(client._session, ws.RetrySession))

        # test cert authentication
        post_mock.reset_mock()
        get_mock.reset_mock()
        put_mock.reset_mock()
        delete_mock.reset_mock()
        head_mock.reset_mock()
        certfile = 'some/certfile.pem'
        client = ws.BaseWsClient(resource_id,
                                 auth.Subject(certificate=certfile), 'TestApp')
        base_url = 'https://{}{}/observations'.format(resource_uri.netloc,
                                                      resource_uri.path)
        resource_url = '{}/{}'.format(base_url, resource)
        self.assertEqual('TestApp', client.agent)
        self.assertTrue(client.retry)
        client.get(resource_url)
        get_mock.assert_called_with(resource_url, params=None, verify=True)
        params = {'arg1': 'abc', 'arg2': 123, 'arg3': True}
        client.post(resource_url, **params)
        post_mock.assert_called_with(resource_url, verify=True, **params)
        client.delete(resource_url)
        delete_mock.assert_called_with(resource_url, verify=True)
        client.head(resource_url)
        head_mock.assert_called_with(resource_url, verify=True)
        client.put(resource_url, **params)
        put_mock.assert_called_with(resource_url, verify=True, **params)
        self.assertTrue(isinstance(client._session, ws.RetrySession))
        self.assertEqual((certfile, certfile), client._session.cert)

        # test cookie authentication
        post_mock.reset_mock()
        get_mock.reset_mock()
        put_mock.reset_mock()
        delete_mock.reset_mock()
        head_mock.reset_mock()
        subject = auth.Subject()
        subject.cookies.append(auth.CookieInfo(resource_uri.netloc,
                                               'MyTestCookie', 'cookievalue'))
        client = ws.BaseWsClient(resource_id, subject, 'TestApp',
                                 insecure=True)
        base_url = 'https://{}{}/observations'.format(resource_uri.netloc,
                                                      resource_uri.path)
        resource_url = '{}/{}'.format(base_url, resource)
        self.assertEqual('TestApp', client.agent)
        self.assertTrue(client.retry)
        client.get(resource_url)
        get_mock.assert_called_with(resource_url, params=None, verify=False)
        params = {'arg1': 'abc', 'arg2': 123, 'arg3': True}
        client.post(resource_url, **params)
        post_mock.assert_called_with(resource_url, verify=False, **params)
        client.delete(resource_url)
        delete_mock.assert_called_with(resource_url, verify=False)
        client.head(resource_url)
        head_mock.assert_called_with(resource_url, verify=False)
        client.put(resource_url, **params)
        put_mock.assert_called_with(resource_url, verify=False, **params)
        self.assertTrue(isinstance(client._session, ws.RetrySession))
        self.assertEqual(1, len(client._session.cookies))
        self.assertEqual('cookievalue',
                         client._session.cookies['MyTestCookie'])

    @patch('cadcutils.net.ws.util.Md5File')
    @patch('cadcutils.net.ws.WsCapabilities')
    def test_upload_file_no_put_txn(self, caps_mock, md5_file_mock):
        anon_subject = auth.Subject()
        target_url = 'https://someurl/path/file'
        cm = Mock()
        cm.get_access_url.return_value = "http://host/availability"
        caps_mock.return_value = cm
        response = Mock()
        response.status_code = requests.codes.ok
        response.headers = {}
        session = Mock()
        session.put.return_value = response
        md5_file_mock_obj = Mock()
        md5_file_mock.return_value.__enter__.return_value = md5_file_mock_obj
        client = ws.BaseDataClient(resource_id='ivo://cadc.nrc.ca/resourceid',
                                   subject=anon_subject, agent='TestApp')
        # upload small file (md5 checksum is pre-computed)
        client._get_session = Mock(return_value=session)
        content = 'this is some content for the test'
        md5 = hashlib.md5()
        md5.update(str.encode(content))
        content_md5 = md5.hexdigest()
        src = tempfile.NamedTemporaryFile()
        with open(src.name, 'w') as f:
            f.write(content)
        md5_file_mock_obj.md5_checksum = content_md5
        net.add_md5_header(headers=response.headers, md5_checksum=content_md5)
        md5_file_mock_obj.md5_checksum = content_md5
        # add caller headers and test they are passed through
        caller_header = {CONTENT_TYPE: TEXT_TYPE}
        client.upload_file(url=target_url, src=src.name, headers=caller_header)
        session.put.assert_called_once()
        put_headers = {}
        net.add_md5_header(headers=put_headers, md5_checksum=content_md5)
        put_headers[ws.HTTP_LENGTH] = str(len(content))
        net.add_md5_header(headers=put_headers, md5_checksum=content_md5)
        expected_headers = caller_header
        expected_headers.update(put_headers)
        session.put.assert_called_with(target_url, headers=expected_headers,
                                       data=ANY, verify=True)

        # pass the md5 in update small file
        session.put.reset_mock()
        rsp = client.upload_file(url=target_url, src=src.name,
                                 md5_checksum=content_md5)
        assert ('file', content_md5, len(content)) == rsp
        session.put.assert_called_once()
        session.put.assert_called_with(target_url, headers=put_headers,
                                       data=ANY, verify=True)

        # force calculate md5 on the fly
        orig_max_md5_compute_size = ws.MAX_MD5_COMPUTE_SIZE
        session.put.reset_mock()
        try:
            ws.MAX_MD5_COMPUTE_SIZE = 3
            rsp = client.upload_file(url=target_url, src=src.name)
            assert ('file', content_md5, len(content)) == rsp
            session.put.assert_called_once()
            session.put.assert_called_with(
                target_url,
                headers={ws.HTTP_LENGTH: str(len(content)), ws.PUT_TXN_OP: ws.PUT_TXN_START},
                data=ANY, verify=True)
        except Exception as e:
            ws.MAX_MD5_COMPUTE_SIZE = orig_max_md5_compute_size
            raise e

        # mimic large file that requires to be split in segments
        # force calculate md5 on the fly
        orig_max_md5_compute_size = ws.MAX_MD5_COMPUTE_SIZE
        orig_file_segment_threshold = ws.FILE_SEGMENT_THRESHOLD
        session.put.reset_mock()
        try:
            ws.MAX_MD5_COMPUTE_SIZE = 3
            ws.FILE_SEGMENT_THRESHOLD = 3
            rsp = client.upload_file(url=target_url, src=src.name)
            assert ('file', content_md5, len(content)) == rsp
            assert len(session.put.mock_calls) == 2
            # 2 calls - one to start transaction and the other one to
            # do the transfer since the response does not contain a
            # transaction id (usually a sign that the server doesn't support it
            assert [call(target_url, verify=True,
                         headers={ws.HTTP_LENGTH: '0',
                                  ws.PUT_TXN_TOTAL_LENGTH: str(len(content)),
                                  ws.PUT_TXN_OP: ws.PUT_TXN_START}),
                    call(target_url,
                         headers={ws.HTTP_LENGTH: str(len(content))},
                         data=ANY, verify=True)] == session.put.mock_calls
        finally:
            ws.MAX_MD5_COMPUTE_SIZE = orig_max_md5_compute_size
            ws.FILE_SEGMENT_THRESHOLD = orig_file_segment_threshold

        # make it fail the first attempt but succeed on the next
        session.put.reset_mock()
        session.put.side_effect = [exceptions.PreconditionFailedException,
                                   response]
        rsp = client.upload_file(url=target_url, src=src.name,
                                 md5_checksum=content_md5)
        assert ('file', content_md5, len(content)) == rsp
        assert session.put.mock_calls == [
            call(target_url, headers=put_headers, data=ANY, verify=True),
            call(target_url, headers=put_headers, data=ANY, verify=True)]

        # fail on repeated BadRequests
        session.put.reset_mock()
        session.put.side_effect = [exceptions.PreconditionFailedException] * 4
        with pytest.raises(exceptions.PreconditionFailedException):
            client.upload_file(url=target_url, src=src.name,
                               md5_checksum=content_md5)

        # fail on other errors
        session.put.reset_mock()
        session.put.side_effect = [exceptions.UnexpectedException]
        with pytest.raises(exceptions.UnexpectedException):
            client.upload_file(url=target_url, src=src.name,
                               md5_checksum=content_md5)

    @patch('cadcutils.net.ws.util.Md5File')
    @patch('cadcutils.net.ws.WsCapabilities')
    def test_upload_file_put_txn(self, caps_mock, md5_file_mock):
        anon_subject = auth.Subject()
        target_url = 'https://someurl/path/file'
        cm = Mock()
        cm.get_access_url.return_value = "http://host/availability"
        caps_mock.return_value = cm
        response = Mock()
        response.status_code = requests.codes.ok
        response.headers = {}
        session = Mock()
        session.put.return_value = response
        md5_file_mock_obj = Mock()
        md5_file_mock.return_value.__enter__.return_value = md5_file_mock_obj
        client = ws.BaseDataClient(resource_id='ivo://cadc.nrc.ca/resourceid',
                                   subject=anon_subject, agent='TestApp')
        client._get_session = Mock(return_value=session)
        content = 'this is some content for the test'
        md5 = hashlib.md5()
        md5.update(str.encode(content))
        content_md5 = md5.hexdigest()
        src = tempfile.NamedTemporaryFile()
        with open(src.name, 'w') as f:
            f.write(content)
        md5_file_mock_obj.md5_checksum = content_md5
        net.add_md5_header(headers=response.headers, md5_checksum=content_md5)
        md5_file_mock_obj.md5_checksum = content_md5
        orig_max_md5_compute_size = ws.MAX_MD5_COMPUTE_SIZE
        try:
            # put larger one segment files
            session.put.reset_mock()
            response_headers = {ws.PUT_TXN_ID: '123',
                                ws.HTTP_LENGTH: str(len(content))}
            net.add_md5_header(response_headers, content_md5)
            session.put.return_value = Mock(headers=response_headers)
            # add caller headers and test they are passed through
            caller_header = {CONTENT_TYPE: TEXT_TYPE}
            # lower the threshold for "large" files so that the current test
            # files becomes large
            ws.MAX_MD5_COMPUTE_SIZE = 10
            # PUT headers do not contain the md5 anymore
            rsp = client.upload_file(url=target_url, src=src.name,
                                     headers=caller_header)
            assert ('file', content_md5, len(content)) == rsp
            put_headers = {ws.HTTP_LENGTH: str(len(content)),
                           ws.PUT_TXN_OP: ws.PUT_TXN_START}
            commit_headers = {ws.PUT_TXN_ID: '123',
                              ws.PUT_TXN_OP: ws.PUT_TXN_COMMIT,
                              ws.HTTP_LENGTH: '0'}
            expected_put_headers = dict(caller_header)
            expected_put_headers.update(put_headers)
            expected_commit_headers = dict(caller_header)
            expected_commit_headers.update(commit_headers)
            assert session.put.mock_calls == \
                [call(target_url, data=ANY, verify=True,
                      headers=expected_put_headers),
                 call(target_url, verify=True,
                      headers=expected_commit_headers)]

            # repeat but provide the checksum as argument to upload_file so
            # no transaction is required
            del put_headers[ws.PUT_TXN_OP]
            session.put.reset_mock()
            rsp = client.upload_file(
                url=target_url, src=src.name, md5_checksum=content_md5)
            net.add_md5_header(headers=put_headers, md5_checksum=content_md5)
            session.put.assert_called_with(target_url, headers=put_headers,
                                           data=ANY, verify=True)
            assert ('file', content_md5, len(content)) == rsp

            # mimic a replacement where source and destination are identical
            session.put.reset_mock()
            head_headers = {}
            net.add_md5_header(head_headers, content_md5)
            head_response = Mock(headers=head_headers)
            session.head.return_value = head_response
            rsp = client.upload_file(url=target_url, src=src.name, md5_checksum=content_md5)
            session.put.assert_not_called()
            assert ('file', content_md5, len(content)) == rsp

            # mimic md5 mismatch
            session.put.reset_mock()
            response_headers = {ws.PUT_TXN_ID: '123',
                                ws.HTTP_LENGTH: str(len(content))}
            net.add_md5_header(response_headers, 'd41d8cd98f00b204e9800998ecf8427e')
            session.put.return_value = Mock(headers=response_headers)

            with pytest.raises(exceptions.TransferException):
                client.upload_file(url=target_url, src=src.name)

        finally:
            ws.MAX_MD5_COMPUTE_SIZE = orig_max_md5_compute_size

        # empty file
        empty_file = tempfile.NamedTemporaryFile()
        with open(empty_file.name, 'w') as f:
            f.write('')
        with pytest.raises(ValueError):
            client.upload_file(url=target_url,
                               src=empty_file.name)

    @patch('cadcutils.net.ws.WsCapabilities')
    def test_upload_file_put_txn_append(self, caps_mock):
        anon_subject = auth.Subject()
        target_url = 'https://someurl/path/file'
        cm = Mock()
        cm.get_access_url.return_value = "http://host/availability"
        caps_mock.return_value = cm
        response = Mock()
        response.status_code = requests.codes.ok
        response.headers = {}
        session = Mock()
        session.put.return_value = response
        client = ws.BaseDataClient(resource_id='ivo://cadc.nrc.ca/resourceid',
                                   subject=anon_subject, agent='TestApp')
        client._get_session = Mock(return_value=session)

        orig_max_md5_compute_size = ws.MAX_MD5_COMPUTE_SIZE
        orig_max_file_segment_size = ws.FILE_SEGMENT_THRESHOLD
        try:
            ws.MAX_MD5_COMPUTE_SIZE = 5  # force transaction
            ws.FILE_SEGMENT_THRESHOLD = 10  # force segments
            segments = [b'segment1', b'segment2', b'end3']
            src = tempfile.NamedTemporaryFile()
            start_txn_headers = {ws.PUT_TXN_ID: '123',
                                 ws.PUT_TXN_MIN_SEGMENT: '1',
                                 ws.PUT_TXN_MAX_SEGMENT: len(segments[0])}

            def _create_put_responses():
                # this function creates the content of the file according
                # to the segments and also generates the responses to
                # PUT commands with corresponding headers
                md5 = hashlib.md5()
                seg_md5s = []
                with open(src.name, 'wb') as f:
                    for seg in segments:
                        f.write(seg)
                        md5.update(seg)
                        seg_md5s.append(md5.hexdigest())
                commit_txn_headers = {ws.HTTP_LENGTH: '0'}
                net.add_md5_header(commit_txn_headers, seg_md5s[-1])
                responses = [Mock(headers=start_txn_headers)]
                for seg_md5 in seg_md5s:
                    segment_txn_headers = {ws.PUT_TXN_ID: '123',
                                           ws.HTTP_LENGTH: '0'}
                    net.add_md5_header(segment_txn_headers, seg_md5)
                    responses.append(
                        Mock(headers=segment_txn_headers))

                responses.append(Mock(headers=commit_txn_headers))
                return responses

            put_responses = _create_put_responses()
            file_size = os.stat(src.name).st_size

            def put_mock(url, data=None, **kwargs):
                # this is a "semi" mock of the PUT function. The PUT request
                # is mocked but this function consumes from the file (data)
                # when available
                headers = kwargs['headers']
                assert url == target_url
                if put_mock.put_num:
                    # except the first PUT that starts transaction,
                    # all the other ones need to include the trans id
                    assert headers[ws.PUT_TXN_ID] == '123'
                    if put_mock.put_num in [1, 2, 3]:
                        if put_mock.exception is not None and \
                                put_mock.exception == put_mock.put_num:
                            # raise a Transfer exception for the segment
                            session.head.return_value = put_responses[
                                put_mock.exception]
                            session.post.return_value = \
                                put_responses[put_mock.exception - 1]
                            put_mock.exception = None
                            raise exceptions.TransferException('Test')
                        if put_mock.wrong_md5 is not None and \
                                (put_mock.put_num == put_mock.wrong_md5):
                            # return the wrong md5 for the segment
                            # set the response for the revert POST to be
                            # similar to the previous (successful) PUT
                            session.post.return_value = \
                                put_responses[put_mock.wrong_md5 - 1]
                            if put_mock.wrong_md5 == 1:
                                # first segment - return start txn response
                                rsp = put_responses[0]
                            else:
                                # return a mismatch md5 response
                                tmp_hd = {ws.PUT_TXN_ID: '123',
                                          ws.HTTP_LENGTH: '0'}
                                net.add_md5_header(tmp_hd, 'beef'*8)
                                rsp = Mock(headers=tmp_hd)
                            put_mock.wrong_md5 = None
                            return rsp

                        # check length of segments
                        assert headers[ws.HTTP_LENGTH] == \
                            str(len(segments[put_mock.put_num-1]))
                        assert headers[CONTENT_TYPE] == TEXT_TYPE
                    else:
                        assert headers[ws.PUT_TXN_OP] == ws.PUT_TXN_COMMIT
                        assert headers[ws.HTTP_LENGTH] == '0'
                        assert headers[CONTENT_TYPE] == TEXT_TYPE
                else:
                    assert headers[ws.PUT_TXN_OP] == ws.PUT_TXN_START
                    assert headers[ws.HTTP_LENGTH] == '0'
                    assert headers[ws.PUT_TXN_TOTAL_LENGTH] == str(file_size)
                    assert headers[CONTENT_TYPE] == TEXT_TYPE
                if data:
                    data.read(100)
                rsp = put_responses[put_mock.put_num]
                put_mock.put_num += 1
                return rsp
            put_mock.exception = None
            put_mock.put_num = 0
            put_mock.wrong_md5 = None
            session.put = put_mock
            # add caller headers and test they are passed through
            caller_header = {CONTENT_TYPE: TEXT_TYPE}
            client.upload_file(url=target_url, src=src.name,
                               headers=caller_header)
            # check all puts were called
            assert len(put_responses) == put_mock.put_num

            # redo the tests but have the file size multiple of its segments
            # reset the put_mock "mock" function first

            put_mock.exception = None
            put_mock.put_num = 0
            put_mock.wrong_md5 = None
            segments = [b'segment1', b'segment2', b'endsize3']
            put_responses = _create_put_responses()
            file_size = os.stat(src.name).st_size

            client.upload_file(url=target_url, src=src.name,
                               headers=caller_header)
            # check all puts were called
            assert len(put_responses) == put_mock.put_num

            # redo the test but have an exception thrown in PUT for segment 2
            put_mock.exception = 2
            put_mock.put_num = 0
            client.upload_file(url=target_url, src=src.name,
                               headers=caller_header)
            # check all puts were called
            assert len(put_responses) == put_mock.put_num

            # redo the test but have a md5 mismatch for segment 1
            put_mock.wrong_md5 = 1
            put_mock.put_num = 0
            client.upload_file(url=target_url, src=src.name,
                               headers=caller_header)
            # check all puts were called
            assert len(put_responses) == put_mock.put_num

            # repeat the test but have a md5 mismatch for segment 2
            put_mock.wrong_md5 = 2
            put_mock.put_num = 0
            client.upload_file(url=target_url, src=src.name,
                               headers=caller_header)
            # check all puts were called
            assert len(put_responses) == put_mock.put_num

            # permanent Transfer error
            session.put = Mock(headers=start_txn_headers,
                               side_effect=[exceptions.TransferException] * 3)
            with pytest.raises(exceptions.TransferException):
                client.upload_file(url=target_url, src=src.name)

            # permanent Transfer error including in HEAD
            session.put = Mock(side_effect=[Mock(headers=start_txn_headers),
                                            exceptions.TransferException])
            head_exception = exceptions.UnexpectedException
            session.head = Mock(side_effect=head_exception)
            with pytest.raises(head_exception):
                client.upload_file(url=target_url, src=src.name)
        finally:
            ws.MAX_MD5_COMPUTE_SIZE = orig_max_md5_compute_size
            ws.PUT_TXN_MAX_SEGMENT = orig_max_file_segment_size

    def test_get_segment_size(self):
        get_segment = ws.BaseDataClient._get_segment_size  # shortcut
        # file size > preferred segment size
        assert ws.PREFERRED_SEGMENT_SIZE == \
            get_segment(ws.PREFERRED_SEGMENT_SIZE+1, None, None)
        # file size < preferred segment size
        assert ws.PREFERRED_SEGMENT_SIZE - 1 == \
            get_segment(ws.PREFERRED_SEGMENT_SIZE-1, None, None)
        # minimum size > file_size and preferred size
        assert ws.PREFERRED_SEGMENT_SIZE + 2 == \
            get_segment(ws.PREFERRED_SEGMENT_SIZE,
                        ws.PREFERRED_SEGMENT_SIZE+2, None)
        # max size < file_size and preferred size
        assert ws.PREFERRED_SEGMENT_SIZE - 1 == \
            get_segment(ws.PREFERRED_SEGMENT_SIZE,
                        None, ws.PREFERRED_SEGMENT_SIZE-1)


class TestRetrySession(unittest.TestCase):
    """ Class for testing retry session """

    @patch('time.sleep')
    @patch('cadcutils.net.ws.requests.Session.send')
    @patch('cadcutils.net.ws.requests.Session.merge_environment_settings',
           Mock(return_value={}))
    def test_retry(self, send_mock, time_mock):
        request = Mock()
        send_mock.return_value = Mock()
        rs = ws.RetrySession(False)
        rs.send(request)
        send_mock.assert_called_with(request, timeout=120)

        # retry to user defined timeout
        send_mock.return_value = Mock()
        rs = ws.RetrySession(False)
        rs.send(request, timeout=77)
        send_mock.assert_called_with(request, timeout=77)

        # mock delays for connect timeout
        send_mock.reset_mock()
        rs = ws.RetrySession()
        # connection error that triggers retries
        cte = requests.exceptions.ConnectTimeout()
        response = requests.Response()
        response.status_code = requests.codes.ok
        send_mock.side_effect = [cte, response]
        rs.send(request)
        time_mock.assert_called_with(DEFAULT_RETRY_DELAY)

        # mock delays for read timeout
        send_mock.reset_mock()
        rs = ws.RetrySession()
        # connection error that triggers retries
        rte = requests.exceptions.ReadTimeout()
        send_mock.side_effect = [rte]
        with self.assertRaises(exceptions.TransferException):
            rs.send(request)

        # mock Connection errors
        send_mock.reset_mock()
        rs = ws.RetrySession()
        # connection error that triggers retries
        ce = requests.exceptions.ConnectionError('Some error')
        send_mock.side_effect = [ce]
        with self.assertRaises(exceptions.HttpException):
            rs.send(request)

        # mock reset by peer error
        # mock Connection errors
        send_mock.reset_mock()
        rs = ws.RetrySession()
        # connection error that triggers retries
        ce = requests.exceptions.ConnectionError('Connection reset by peer')
        send_mock.side_effect = [ce]
        with self.assertRaises(exceptions.TransferException):
            rs.send(request)

        # two connection error delay = DEFAULT_RETRY_DELAY
        send_mock.reset_mock()
        time_mock.reset_mock()
        rs = ws.RetrySession()
        # connection error that triggers retries
        ce = requests.exceptions.ConnectTimeout()
        response = requests.Response()
        response.status_code = requests.codes.ok
        send_mock.side_effect = [ce, ce, response]  # two connection errors
        rs.send(request)
        calls = [call(DEFAULT_RETRY_DELAY), call(DEFAULT_RETRY_DELAY * 2)]
        time_mock.assert_has_calls(calls)

        # set the start retry to a large number and see how it is capped
        # to MAX_RETRY_DELAY
        send_mock.reset_mock()
        time_mock.reset_mock()
        rs = ws.RetrySession(start_delay=MAX_RETRY_DELAY / 2 + 1)
        # connection error that triggers retries
        ce = requests.exceptions.ConnectTimeout()
        response = requests.Response()
        response.status_code = requests.codes.ok
        send_mock.side_effect = [ce, ce, response]  # two connection errors
        rs.send(request)
        calls = (call(MAX_RETRY_DELAY / 2 + 1), call(MAX_RETRY_DELAY))
        time_mock.assert_has_calls(calls)

        # return the error all the time
        send_mock.reset_mock()
        time_mock.reset_mock()
        rs = ws.RetrySession(start_delay=MAX_RETRY_DELAY / 2 + 1)
        # connection error that triggers retries
        ce = requests.exceptions.ConnectTimeout()
        # make sure the mock returns more errors than the maximum number
        # of retries allowed
        http_errors = []
        i = 0
        while i <= MAX_NUM_RETRIES:
            http_errors.append(ce)
            i += 1
        send_mock.side_effect = http_errors
        with self.assertRaises(exceptions.HttpException):
            rs.send(request)

        # return HttpError 503 with Retry-After
        send_mock.reset_mock()
        time_mock.reset_mock()
        rs = ws.RetrySession()
        server_delay = 5
        # connection error that triggers retries
        he = requests.exceptions.HTTPError()
        he.response = requests.Response()
        he.response.status_code = requests.codes.unavailable
        he.response.headers[SERVICE_RETRY] = server_delay
        response = requests.Response()
        response.status_code = requests.codes.ok
        send_mock.side_effect = [he, response]
        rs.send(request)
        calls = [call(server_delay)]
        time_mock.assert_has_calls(calls)

        # return HttpError 503 with Retry-After with an invalid value
        send_mock.reset_mock()
        time_mock.reset_mock()
        start_delay = 66
        rs = ws.RetrySession(start_delay=start_delay)
        server_delay = 'notnumber'
        # connection error that triggers retries
        he = requests.exceptions.HTTPError()
        he.response = requests.Response()
        he.response.status_code = requests.codes.unavailable
        he.response.headers[SERVICE_RETRY] = server_delay
        response = requests.Response()
        response.status_code = requests.codes.ok
        send_mock.side_effect = [he, response]
        rs.send(request)
        calls = [call(start_delay)]  # uses the default delay
        time_mock.assert_has_calls(calls)

        # return HttpError 503 with no Retry-After
        send_mock.reset_mock()
        time_mock.reset_mock()
        start_delay = 66
        rs = ws.RetrySession(start_delay=start_delay)
        he = requests.exceptions.HTTPError()
        he.response = requests.Response()
        he.response.status_code = requests.codes.unavailable
        response = requests.Response()
        response.status_code = requests.codes.ok
        send_mock.side_effect = [he, response]
        rs.send(request)
        calls = [call(start_delay)]  # uses the default delay
        time_mock.assert_has_calls(calls)

        # tests non-transient errors
        send_mock.reset_mock()
        time_mock.reset_mock()
        rs = ws.RetrySession()
        he = requests.exceptions.HTTPError()
        he.response = requests.Response()
        he.response.status_code = requests.codes.internal_server_error
        send_mock.side_effect = he
        with self.assertRaises(exceptions.HttpException):
            rs.send(request)

    def test_config_file_location(self):
        # test the location of the config file when the host is specified
        myhost = 'myhost.ca'
        service = 'myservice'
        resource_id = 'ivo://canfar.phys.uvic.ca/{}'.format(service)
        client = Mock(resource_id=resource_id)
        with patch('cadcutils.net.ws.os.makedirs') as makedirs_mock:
            wscap = ws.WsCapabilities(client, host=myhost)
        expected_location = os.path.join(ws.CACHE_LOCATION, 'alt-domains',
                                         myhost)
        makedirs_mock.check_called_with(expected_location)
        self.assertEqual(os.path.join(expected_location, 'resource-caps'),
                         wscap.reg_file)
        self.assertEqual(
            os.path.join(expected_location, urlparse(resource_id).netloc,
                         '.{}'.format(service)),
            wscap.caps_file)

    def test_misc(self):
        """
        Tests miscellaneous functions
        """
        service_url = 'http://somehost.com/service'
        with patch('cadcutils.net.ws.WsCapabilities') as caps_mock:
            caps_mock.return_value.get_service_host.return_value =\
                'somehost.com'
            caps_mock.return_value.get_access_url.return_value = service_url
            client = ws.BaseWsClient("someresourceID", auth.Subject(),
                                     'TestApp')
            self.assertEqual('{}'.format(service_url),
                             client._get_url(('myfeature', None)))
            caps_mock.return_value.get_access_url.assert_called_with(
                'myfeature')
            self.assertEqual('{}'.format(service_url),
                             client._get_url(('myfeature', '')))

        test_host = 'testhost.com'
        with patch('cadcutils.net.ws.os.makedirs'):
            client = ws.BaseWsClient("someresourceID", auth.Subject(),
                                     'TestApp', host=test_host)
        self.assertEqual(test_host, client.host)

        # test with resource as url
        with patch('cadcutils.net.ws.WsCapabilities') as caps_mock:
            caps_mock.return_value.get_service_host.return_value =\
                'somehost.com'
            cm = Mock()
            cm.get_access_url.return_value = "http://host/availability"
            caps_mock.return_value = cm
            client = ws.BaseWsClient("someresourceID", auth.Subject(),
                                     'TestApp')
            resource_url = 'http://someurl.com/path/'
            self.assertEqual(resource_url, client._get_url(resource_url))
            # repeat with overriden host name
            client = ws.BaseWsClient("someresourceID", auth.Subject(),
                                     'TestApp', host=test_host)
            # same result
            self.assertEqual(resource_url, client._get_url(resource_url))

            # test exceptions with different status in the response
            session = ws.RetrySession()
            response = requests.Response()
            response.status_code = requests.codes.not_found
            with self.assertRaises(exceptions.NotFoundException):
                session.check_status(response)
            response.status_code = requests.codes.unauthorized
            with self.assertRaises(exceptions.UnauthorizedException):
                session.check_status(response)
            response.status_code = requests.codes.forbidden
            with self.assertRaises(exceptions.ForbiddenException):
                session.check_status(response)
            response.status_code = requests.codes.bad_request
            with self.assertRaises(exceptions.BadRequestException):
                session.check_status(response)
            response.status_code = requests.codes.conflict
            with self.assertRaises(exceptions.AlreadyExistsException):
                session.check_status(response)
            response.status_code = requests.codes.internal_server_error
            with self.assertRaises(exceptions.InternalServerException):
                session.check_status(response)
            response.status_code = requests.codes.unavailable
            with self.assertRaises(requests.HTTPError):
                session.check_status(response)
            response.status_code = requests.codes.not_extended
            with self.assertRaises(exceptions.UnexpectedException):
                session.check_status(response)

    @patch('time.sleep')
    @patch('cadcutils.net.ws.requests.Session.send')
    def test_idempotent(self, send_mock, time_mock):
        # mock delays for connect timeout
        send_mock.reset_mock()
        rs = ws.RetrySession()
        # GET, PUT, DELETE, HEAD are idempotent operations so re-tries on
        # connection timeouts happen automatically
        cte = requests.exceptions.ConnectTimeout()
        response = requests.Response()
        response.status_code = requests.codes.ok
        # GET
        send_mock.side_effect = [cte, response]
        rs.get('https://someurl')
        time_mock.assert_called_with(DEFAULT_RETRY_DELAY)
        # PUT
        send_mock.reset_mock()
        time_mock.reset_mock()
        send_mock.side_effect = [cte, response]
        rs.put('https://someurl')
        time_mock.assert_called_with(DEFAULT_RETRY_DELAY)
        # DELETE
        send_mock.reset_mock()
        time_mock.reset_mock()
        send_mock.side_effect = [cte, response]
        rs.delete('https://someurl')
        time_mock.assert_called_with(DEFAULT_RETRY_DELAY)
        # HEAD
        send_mock.reset_mock()
        time_mock.reset_mock()
        send_mock.side_effect = [cte, response]
        rs.head('https://someurl')
        time_mock.assert_called_with(DEFAULT_RETRY_DELAY)

        # POST is not idempotent by default so re-try does not happen
        send_mock.reset_mock()
        time_mock.reset_mock()
        send_mock.side_effect = [cte, response]
        with pytest.raises(requests.exceptions.ConnectTimeout):
            rs.post('https://someurl')

        # POST with service unavailable response. Note the different exception
        # when exception is generated from response
        unavailable = requests.Response()
        unavailable.status_code = requests.codes.unavailable
        send_mock.side_effect = [unavailable]
        with pytest.raises(exceptions.UnexpectedException):
            rs.post('https://someurl')

        # Create session with idempotent POSTs to enable retries
        send_mock.reset_mock()
        time_mock.reset_mock()
        send_mock.side_effect = [cte, response]
        rs = ws.RetrySession(idempotent_posts=True)
        rs.post('https://someurl')
        time_mock.assert_called_with(DEFAULT_RETRY_DELAY)


capabilities_content = \
    """
    <vosi:capabilities
    xmlns:vosi="http://www.ivoa.net/xml/VOSICapabilities/v1.0"
    xmlns:vs="http://www.ivoa.net/xml/VODataService/v1.1"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        <capability standardID="ivo://ivoa.net/std/VOSI#capabilities">
            <interface xsi:type="vs:ParamHTTP" role="std">
                <accessURL use="full">
                    http://WS_URL/capabilities
                </accessURL>
            </interface>
        </capability>
        <capability standardID="ivo://ivoa.net/std/VOSI#availability">
            <interface xsi:type="vs:ParamHTTP" role="std">
                <accessURL use="full">
                    http://WS_URL/availability
                </accessURL>
            </interface>
        </capability>
        <capability standardID="vos://cadc.nrc.ca~service/CADC/mystnd01">
            <interface xsi:type="vs:ParamHTTP" role="std" version="1.0">
                <accessURL use="base">
                    http://WS_URL/pub
                </accessURL>
            </interface>
            <interface xsi:type="vs:ParamHTTP" role="std" version="1.0">
                <accessURL use="base">
                    http://WS_URL/auth
                </accessURL>
                <securityMethod
                standardID=
                "ivo://ivoa.net/sso#BasicAA"/>
            </interface>
            <interface xsi:type="vs:ParamHTTP" role="std" version="1.0">
                <accessURL use="base">
                    https://WS_URL
                </accessURL>
                <securityMethod
                standardID="ivo://ivoa.net/sso#tls-with-certificate"/>
            </interface>
        </capability>
    </vosi:capabilities>
    """


class TestWsCapabilities(unittest.TestCase):
    """Class for testing the webservie client"""

    @patch('cadcutils.net.ws.util.get_url_content')
    def test_get_reg(self, get_content_mock):
        """
        Tests the registry part of WsCapabilities
        """
        # test when registry information is read from the server
        # (cache is outdated)
        service = 'myservice'
        resource_id = 'ivo://canfar.phys.uvic.ca/{}'.format(service)
        resource_cap_url = 'http://www.canfar.net/myservice'
        cadcreg_content = ('#test content\n {} = {} \n'
                           'ivo://some.provider/service = '
                           'http://providerurl.test/service'). \
            format(resource_id, resource_cap_url)
        get_content_mock.return_value = cadcreg_content
        client = Mock(resource_id=resource_id)
        caps = ws.WsCapabilities(client)
        self.assertEqual(os.path.join(ws.CACHE_LOCATION, ws.REGISTRY_FILE),
                         caps.reg_file)
        self.assertEqual(
            os.path.join(ws.CACHE_LOCATION,
                         'canfar.phys.uvic.ca', '.{}'.format(service)),
            caps.caps_file)
        self.assertEqual(resource_cap_url, caps._get_capability_url())

    @patch('cadcutils.net.ws.util.get_url_content')
    def test_get_caps(self, get_content_mock):
        """
        Tests the capabilities part of WsCapabilities
        """
        # test when registry information is read from the server
        # (cache is outdated)
        service = 'myservice'
        resource_id = 'ivo://canfar.phys.uvic.ca/{}'.format(service)
        resource_cap_url = 'www.canfar.net/myservice'
        expected_content = capabilities_content.replace('WS_URL',
                                                        resource_cap_url)
        get_content_mock.return_value = expected_content
        caps = ws.WsCapabilities(Mock(resource_id=resource_id,
                                      subject=auth.Subject()))

        # mock _get_capability_url to return some url without attempting
        # to access the server
        def get_url():
            return 'https://some.url/capabilities'

        caps._get_capability_url = get_url
        # remove the cached file if exists
        if os.path.isfile(caps.caps_file):
            os.remove(caps.caps_file)
        caps.caps_urls[service] = '{}/capabilities'.format(resource_cap_url)
        self.assertEqual('http://{}/capabilities'.format(resource_cap_url),
                         caps.get_access_url(
                             'ivo://ivoa.net/std/VOSI#capabilities'))
        self.assertEqual('http://{}/availability'.format(resource_cap_url),
                         caps.get_access_url(
                             'ivo://ivoa.net/std/VOSI#availability'))
        self.assertEqual('http://{}/pub'.format(resource_cap_url),
                         caps.get_access_url(
                             'vos://cadc.nrc.ca~service/CADC/mystnd01'))

        # mock _get_capability_url to return a subservice
        service = 'myservice/mysubservice'
        resource_id = 'ivo://canfar.phys.uvic.ca/{}'.format(service)
        resource_cap_url = 'www.canfar.net/myservice/mysubservice'
        expected_content = capabilities_content.replace('WS_URL',
                                                        resource_cap_url)
        get_content_mock.return_value = expected_content
        caps = ws.WsCapabilities(Mock(resource_id=resource_id,
                                      subject=auth.Subject()))
        # remove the cached file if exists
        if os.path.isfile(caps.caps_file):
            os.remove(caps.caps_file)
        caps._get_capability_url = get_url
        caps.caps_urls[service] = '{}/capabilities'.format(resource_cap_url)
        self.assertEqual('http://{}/capabilities'.format(resource_cap_url),
                         caps.get_access_url(
                             'ivo://ivoa.net/std/VOSI#capabilities'))
        self.assertEqual('http://{}/availability'.format(resource_cap_url),
                         caps.get_access_url(
                             'ivo://ivoa.net/std/VOSI#availability'))
        self.assertEqual('http://{}/pub'.format(resource_cap_url),
                         caps.get_access_url(
                             'vos://cadc.nrc.ca~service/CADC/mystnd01'))


class TestWsOutsideCalls(unittest.TestCase):
    """ Class to test Ws with calls to outside sites"""

    @patch('time.sleep')
    def testCalls(self, time_mock):
        client = ws.BaseWsClient('https://httpbin.org', net.Subject(), 'FOO')
        response = client.get('https://httpbin.org')
        self.assertEqual(response.status_code, requests.codes.ok)

        with self.assertRaises(exceptions.InternalServerException):
            client.get('https://httpbin.org/status/500')

        time_mock.reset_mock()
        with self.assertRaises(exceptions.HttpException):
            client.get('https://httpbin.org/status/503')

        calls = [call(DEFAULT_RETRY_DELAY),
                 call(min(DEFAULT_RETRY_DELAY*2, MAX_RETRY_DELAY)),
                 call(min(DEFAULT_RETRY_DELAY * 4, MAX_RETRY_DELAY)),
                 call(min(DEFAULT_RETRY_DELAY * 8, MAX_RETRY_DELAY)),
                 call(min(DEFAULT_RETRY_DELAY * 16, MAX_RETRY_DELAY)),
                 call(min(DEFAULT_RETRY_DELAY * 32, MAX_RETRY_DELAY))]

        time_mock.assert_has_calls(calls)


def test_resolve_name():
    client = ws.BaseDataClient('https://httpbin.org', net.Subject(), 'FOO')
    dest = NamedTemporaryFile()
    dest_dir = os.path.dirname(dest.name)
    file_name = os.path.basename(dest.name)
    tmp_file = os.path.join(dest_dir, '{}-beef.part'.format(file_name))
    # file name provided
    final_dest, temp_dest = client._resolve_destination_file(
        dest.name, 'beef', None)
    assert dest.name == final_dest
    assert tmp_file == temp_dest

    # directory provided
    dest_dir = os.path.dirname(dest.name)
    final_dest, temp_dest = client._resolve_destination_file(
        dest_dir, 'beef', os.path.basename(dest.name))
    assert dest.name == final_dest
    assert tmp_file == temp_dest

    # no md5 - no temporary file/resume
    final_dest, temp_dest = client._resolve_destination_file(
        dest_dir, None, os.path.basename(dest.name))
    assert dest.name == final_dest == temp_dest

    with pytest.raises(ValueError):
        client._resolve_destination_file(None, 'beef', None)


def test_download_file_method():
    client = ws.BaseDataClient('https://httpbin.org', net.Subject(), 'FOO')
    # mock the get
    response = Mock()
    file_name = 'filename.jpg'
    md5 = 'ab56b4d92b40713acc5af89985d4b786'
    response.status_code = requests.codes.ok
    response.headers = \
        {'digest': 'md5=YWI1NmI0ZDkyYjQwNzEzYWNjNWFmODk5ODVkNGI3ODY=',
         'Content-Length': '5',
         'content-disposition': 'attachment; filename={}'.format(file_name)}
    response.raw.read.side_effect = [b'abc', b'de']
    client.get = Mock(return_value=response)
    temp_dir = TemporaryDirectory()
    # proxy _save_bytes through a mock to check when it's called
    client._save_bytes = Mock(side_effect=client._save_bytes)
    rsp = client.download_file('https://dataservice/path/file', temp_dir.name)
    dest, temp_dest = client._resolve_destination_file(
        temp_dir.name, src_md5=md5, default_file_name=file_name)
    assert os.path.isfile(dest)
    assert not os.path.isfile(temp_dest)
    assert client._save_bytes.called
    client.get.assert_called_once_with('https://dataservice/path/file', stream=True)
    assert file_name == rsp[0]
    assert md5 == rsp[1]

    # calling it the second time does not cause another get
    client.get.reset_mock()
    client._save_bytes.reset_mock()
    client.get = Mock(return_value=response)
    rsp = client.download_file('https://dataservice/path/file', temp_dir.name)
    assert not client._save_bytes.called
    client.get.assert_called_once_with('https://dataservice/path/file', stream=True)
    assert file_name == rsp[0]
    assert md5 == rsp[1]

    # specify destination file name
    client.get.reset_mock()
    response.raw.read.side_effect = [b'abc', b'de']
    client._save_bytes.reset_mock()
    client.get = Mock(return_value=response)
    # override file name
    dest_file = 'myfile'
    target_dest = os.path.join(temp_dir.name, dest_file)
    rsp = client.download_file('https://dataservice/path/file', target_dest)
    dest, temp_dest = client._resolve_destination_file(
        target_dest, src_md5=md5, default_file_name=file_name)
    assert os.path.isfile(dest)
    assert not os.path.isfile(temp_dest)
    assert client._save_bytes.called
    client.get.assert_called_once_with('https://dataservice/path/file', stream=True)
    assert dest_file == rsp[0]
    assert md5 == rsp[1]

    # make temporary file larger (BUG case). Operation should succeed
    client.get.reset_mock()
    os.rename(dest, temp_dest)
    open(temp_dest, 'ab').write(b'ghi')
    assert 5 < os.stat(temp_dest).st_size
    response.raw.read.side_effect = [b'abc', b'de']
    rsp = client.download_file('https://dataservice/path/file', temp_dir.name)
    dest, temp_dest = client._resolve_destination_file(
        temp_dir.name, src_md5=md5, default_file_name=file_name)
    assert os.path.isfile(dest)
    assert not os.path.isfile(temp_dest)
    assert client._save_bytes.called
    client.get.assert_called_once_with('https://dataservice/path/file', stream=True)
    assert file_name == rsp[0]
    assert md5 == rsp[1]

    # calling it when incomplete temporary file exits
    # truncate the last 3 bytes but the service does not support
    # ranges
    client.get.reset_mock()
    os.rename(dest, temp_dest)
    with open(temp_dest, 'r+') as f:
        f.seek(0, os.SEEK_END)
        f.seek(f.tell() - 3, os.SEEK_SET)
        f.truncate()
    response.raw.read.side_effect = [b'abc', b'de']
    client.get = Mock(return_value=response)
    rsp = client.download_file('https://dataservice/path/file', temp_dir.name)
    assert os.path.isfile(dest)
    assert not os.path.isfile(temp_dest)
    assert client._save_bytes.called
    client.get.assert_called_once_with('https://dataservice/path/file', stream=True)
    assert file_name == rsp[0]
    assert md5 == rsp[1]

    # repeat the test when the service supports ranges
    client.get.reset_mock()
    os.rename(dest, temp_dest)
    with open(temp_dest, 'r+') as f:
        f.seek(0, os.SEEK_END)
        f.seek(f.tell() - 3, os.SEEK_SET)
        f.truncate()
    response.status_code = requests.codes.partial_content
    response.headers['Accept-Ranges'] = 'bytes '
    response.raw.read.side_effect = [b'cde']
    client.get = Mock(return_value=response)
    rsp = client.download_file('https://dataservice/path/file', temp_dir.name)
    assert os.path.isfile(dest)
    assert not os.path.isfile(temp_dest)
    assert client._save_bytes.called
    assert 2 == client.get.call_count
    # second get call with Range header expected
    assert [call('https://dataservice/path/file', stream=True),
            call('https://dataservice/path/file', stream=True,
                 headers={'Range': 'bytes=2-'})] in client.get.mock_calls
    assert file_name == rsp[0]
    assert md5 == rsp[1]


def test_save_bytes():
    client = ws.BaseDataClient('https://httpbin.org', net.Subject(), 'FOO')
    dest = NamedTemporaryFile()
    outer = {'bytes_count': 0}

    def count_bytes(bytes):
        outer['bytes_count'] += len(bytes)

    response = Mock()
    response.raw.read.side_effect = [b'abc', b'de']
    response.headers = \
        {'digest': 'md5=YWI1NmI0ZDkyYjQwNzEzYWNjNWFmODk5ODVkNGI3ODY='}
    client._save_bytes(response=response, src_length=5, dest_file=dest.name,
                       process_bytes=count_bytes)
    assert 5 == outer['bytes_count']
    assert 5 == os.stat(dest.name).st_size
    assert b'abcde' == open(dest.name, 'rb').read()

    # repeat the test but make szie of source and destination mismatch
    dest = NamedTemporaryFile()
    response = Mock()
    response.raw.read.side_effect = [b'aaa', b'aa']  # different content
    response.headers = \
        {'digest': 'md5=YWI1NmI0ZDkyYjQwNzEzYWNjNWFmODk5ODVkNGI3ODY='}
    with pytest.raises(exceptions.TransferException):
        client._save_bytes(response=response, src_length=7,
                           dest_file=dest.name,
                           process_bytes=count_bytes)

    # repeat the test but make md5s of source and destination mismatch
    dest = NamedTemporaryFile()
    response = Mock()
    response.raw.read.side_effect = [b'aaa', b'aa']  # different content
    response.headers = \
        {'digest': 'md5=YWI1NmI0ZDkyYjQwNzEzYWNjNWFmODk5ODVkNGI3ODY='}
    with pytest.raises(exceptions.TransferException):
        client._save_bytes(response=response, src_length=5,
                           dest_file=dest.name,
                           process_bytes=count_bytes)
