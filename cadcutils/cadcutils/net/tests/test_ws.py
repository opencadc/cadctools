# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2016.                            (c) 2016.
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

import unittest

import os
import requests
import time
from mock import Mock, patch, call, mock_open
from six.moves.urllib.parse import urlparse
from six import StringIO

from cadcutils.net import ws, auth
from cadcutils.net.ws import DEFAULT_RETRY_DELAY, MAX_RETRY_DELAY, MAX_NUM_RETRIES, SERVICE_RETRY
from cadcutils import exceptions

# The following is a temporary workaround for Python issue 25532 (https://bugs.python.org/issue25532)
call.__wrapped__ = None


class TestListResources(unittest.TestCase):

    @patch('cadcutils.net.ws.requests.get')
    def test_list_resources(self, get_mock):
        response_caps = Mock()
        response_caps.text = ('# This is just a test\n'
                                 'ivo://cadc.nrc.ca/serv1 = http://www.cadc.nrc.gc.ca/serv1/capabilities\n'
                                 'ivo://cadc.nrc.ca/serv2 = http://www.cadc.nrc.gc.ca/serv2/capabilities\n')
        response_serv1 = Mock()
        response_serv1.text = ('<vosi:capabilities xmlns:vosi="http://www.ivoa.net/xml/VOSICapabilities/v1.0" '
                                  'xmlns:vs="http://www.ivoa.net/xml/VODataService/v1.1" '
                                  'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">\n'
                                  '<capability standardID="ivo://ivoa.net/std/VOSI#capabilities">\n'
                                  '<interface xsi:type="vs:ParamHTTP" role="std">\n'
                                  '<accessURL use="full">http://www.cadc.hia.nrcgc..ca/serv1/capabilities</accessURL>\n'
                                  '</interface>\n'
                                  '</capability>\n'
                                  '<capability standardID="ivo://ivoa.net/std/VOSI#availability">\n'
                                  '<interface xsi:type="vs:ParamHTTP" role="std">\n'
                                  '<accessURL use="full">http://www.cadc.nrc.gc.ca/serv1/availability</accessURL>\n'
                                  '</interface>\n'
                                  '</capability>\n'
                                  '</vosi:capabilities>\n')
        response_serv2 = Mock()
        response_serv2.text = ('<vosi:capabilities xmlns:vosi="http://www.ivoa.net/xml/VOSICapabilities/v1.0" '
                                  'xmlns:vs="http://www.ivoa.net/xml/VODataService/v1.1" '
                                  'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">\n'
                                 '<capability standardID="ivo://ivoa.net/std/VOSI#capabilities">\n'
                                 '<interface xsi:type="vs:ParamHTTP" role="std">\n'
                                 '<accessURL use="full">http://www.cadc.hia.nrcgc..ca/serv2/capabilities</accessURL>\n'
                                 '</interface>\n'
                                 '</capability>\n'
                                 '<capability standardID="ivo://ivoa.net/std/VOSI#tables-1.1">\n'
                                 '<interface xsi:type="vs:ParamHTTP" role="std">\n'
                                 '<accessURL use="full">http://www.cadc.nrc.gc.ca/serv2/availability</accessURL>\n'
                                 '</interface>\n'
                                 '</capability>\n'
                                 '</vosi:capabilities>\n')
        get_mock.side_effect = [response_caps, response_serv1, response_serv2]
        self.maxDiff = None
        usage = \
'''ivo://cadc.nrc.ca/serv1 (http://www.cadc.nrc.gc.ca/serv1/capabilities) - Capabilities: ivo://ivoa.net/std/VOSI#availability, ivo://ivoa.net/std/VOSI#capabilities

ivo://cadc.nrc.ca/serv2 (http://www.cadc.nrc.gc.ca/serv2/capabilities) - Capabilities: ivo://ivoa.net/std/VOSI#capabilities, ivo://ivoa.net/std/VOSI#tables-1.1

'''
        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            ws.list_resources()
            self.assertEqual(usage, stdout_mock.getvalue())


class TestWs(unittest.TestCase):

    """Class for testing the webservie client"""
    @patch('cadcutils.net.ws.WsCapabilities')
    @patch('cadcutils.net.auth.os.path.isfile', Mock())
    @patch('cadcutils.net.auth.netrclib')
    @patch('cadcutils.net.ws.RetrySession.put')
    @patch('cadcutils.net.ws.RetrySession.head')
    @patch('cadcutils.net.ws.RetrySession.delete')
    @patch('cadcutils.net.ws.RetrySession.get')
    @patch('cadcutils.net.ws.RetrySession.post')
    def test_ops(self, post_mock, get_mock, delete_mock, head_mock, put_mock, netrclib_mock, caps_mock):
        anon_subject = auth.Subject()
        with self.assertRaises(ValueError):
            ws.BaseWsClient(None, anon_subject, "TestApp")
        resource = 'aresource'
        service = 'myservice'
        resource_id = 'ivo://www.canfar.phys.uvic.ca/{}'.format(service)
        # test anonymous access
        caps_mock.get_service_host.return_value = 'http://host/myservice'
        client = ws.BaseWsClient(resource_id, anon_subject, 'TestApp')
        resource_uri = urlparse(resource_id)
        base_url = 'http://{}{}/pub'.format(resource_uri.netloc, resource_uri.path)
        resource_url = 'http://{}{}/{}'.format(resource_uri.netloc, base_url, resource)
        self.assertEqual(anon_subject, client.subject)
        self.assertTrue(client.retry)
        self.assertEqual('TestApp', client.agent)
        self.assertTrue(client.retry)
        self.assertEqual(None, client._session)  # lazy initialization
        client.get(resource_url)
        get_mock.assert_called_with(resource_url, params=None)
        params = {'arg1': 'abc', 'arg2': 123, 'arg3': True}
        client.post(resource_url, **params)
        post_mock.assert_called_with(resource_url, **params)
        client.delete(resource_url)
        delete_mock.assert_called_with(resource_url)
        client.head(resource_url)
        head_mock.assert_called_with(resource_url)
        client.put(resource_url, **params)
        put_mock.assert_called_with(resource_url, **params)
        self.assertTrue(isinstance(client._session, ws.RetrySession))

        # test basic authentication access
        post_mock.reset_mock()
        get_mock.reset_mock()
        put_mock.reset_mock()
        delete_mock.reset_mock()
        head_mock.reset_mock()
        host = 'www.different.org'
        subject = auth.Subject(netrc='somecert')
        client = ws.BaseWsClient(resource_id, subject, 'TestApp', retry=False, host=host)
        base_url = 'http://{}{}/auth'.format(host, resource_uri.path)
        resource_url = '{}/{}'.format(base_url, resource)
        self.assertEqual('TestApp', client.agent)
        self.assertFalse(client.retry)
        client.get(resource_url)
        get_mock.assert_called_with(resource_url, params=None)
        params = {'arg1': 'abc', 'arg2': 123, 'arg3': True}
        client.post(resource_url, **params)
        post_mock.assert_called_with(resource_url, **params)
        client.delete(resource_url)
        delete_mock.assert_called_with(resource_url)
        client.head(resource_url)
        head_mock.assert_called_with(resource_url)
        client.put(resource_url, **params)
        put_mock.assert_called_with(resource_url, **params)
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
        base_url = 'https://{}{}/pub'.format(resource_uri.netloc, resource_uri.path)
        resource_url = '{}/{}'.format(base_url, resource)
        self.assertEqual('TestApp', client.agent)
        self.assertTrue(client.retry)
        client.get(resource_url)
        get_mock.assert_called_with(resource_url, params=None)
        params = {'arg1': 'abc', 'arg2': 123, 'arg3': True}
        client.post(resource_url, **params)
        post_mock.assert_called_with(resource_url, **params)
        client.delete(resource_url)
        delete_mock.assert_called_with(resource_url)
        client.head(resource_url)
        head_mock.assert_called_with(resource_url)
        client.put(resource_url, **params)
        put_mock.assert_called_with(resource_url, **params)
        self.assertTrue(isinstance(client._session, ws.RetrySession))
        self.assertEqual((certfile, certfile), client._session.cert)

        # repeat above tests test with the temporary sc2repo test
        resource = 'aresource'
        service = 'sc2repo'
        resource_id = 'ivo://www.canfar.phys.uvic.ca/{}'.format(service)
        # test anonymous access
        client = ws.BaseWsClient(resource_id, auth.Subject(), 'TestApp')
        resource_uri = urlparse(resource_id)
        base_url = 'http://{}{}/observations'.format(resource_uri.netloc, resource_uri.path)
        resource_url = '{}/{}'.format(base_url, resource)
        self.assertTrue(client.retry)
        self.assertEqual('TestApp', client.agent)
        self.assertTrue(client.retry)
        self.assertEqual(None, client._session)  # lazy initialization
        client.get(resource_url)
        get_mock.assert_called_with(resource_url, params=None)
        params = {'arg1': 'abc', 'arg2': 123, 'arg3': True}
        client.post(resource_url, **params)
        post_mock.assert_called_with(resource_url, **params)
        client.delete(resource_url)
        delete_mock.assert_called_with(resource_url)
        client.head(resource_url)
        head_mock.assert_called_with(resource_url)
        client.put(resource_url, **params)
        put_mock.assert_called_with(resource_url, **params)
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
        client = ws.BaseWsClient(resource_id, subject, 'TestApp', retry=False, host=host)
        base_url = 'http://{}{}/auth-observations'.format(host, resource_uri.path)
        resource_url = '{}/{}'.format(base_url, resource)
        self.assertEqual('TestApp', client.agent)
        self.assertFalse(client.retry)
        client.get(resource_url)
        get_mock.assert_called_with(resource_url, params=None)
        params = {'arg1': 'abc', 'arg2': 123, 'arg3': True}
        client.post(resource_url, **params)
        post_mock.assert_called_with(resource_url, **params)
        client.delete(resource_url)
        delete_mock.assert_called_with(resource_url)
        client.head(resource_url)
        head_mock.assert_called_with(resource_url)
        client.put(resource_url, **params)
        put_mock.assert_called_with(resource_url, **params)
        self.assertTrue(isinstance(client._session, ws.RetrySession))

        # test cert authentication
        post_mock.reset_mock()
        get_mock.reset_mock()
        put_mock.reset_mock()
        delete_mock.reset_mock()
        head_mock.reset_mock()
        certfile = 'some/certfile.pem'
        client = ws.BaseWsClient(resource_id, auth.Subject(certificate=certfile), 'TestApp')
        base_url = 'https://{}{}/observations'.format(resource_uri.netloc, resource_uri.path)
        resource_url = '{}/{}'.format(base_url, resource)
        self.assertEqual('TestApp', client.agent)
        self.assertTrue(client.retry)
        client.get(resource_url)
        get_mock.assert_called_with(resource_url, params=None)
        params = {'arg1': 'abc', 'arg2': 123, 'arg3': True}
        client.post(resource_url, **params)
        post_mock.assert_called_with(resource_url, **params)
        client.delete(resource_url)
        delete_mock.assert_called_with(resource_url)
        client.head(resource_url)
        head_mock.assert_called_with(resource_url)
        client.put(resource_url, **params)
        put_mock.assert_called_with(resource_url, **params)
        self.assertTrue(isinstance(client._session, ws.RetrySession))
        self.assertEqual((certfile, certfile), client._session.cert)

class TestRetrySession(unittest.TestCase):

    """ Class for testing retry session """

    @patch('time.sleep')
    @patch('cadcutils.net.ws.requests.Session.send')
    @patch('cadcutils.net.ws.requests.Session.merge_environment_settings', Mock(return_value={}))
    def test_retry(self, send_mock, time_mock):
        request = Mock()
        send_mock.return_value = Mock()
        rs = ws.RetrySession(False)
        rs.send(request)
        send_mock.assert_called_with(request)

        # mock delays for the 'Connection reset by peer error'
        # one connection error delay = DEFAULT_RETRY_DELAY
        send_mock.reset_mock()
        rs = ws.RetrySession()
        ce = requests.exceptions.ConnectionError()  # connection error that triggers retries
        ce.errno = 104
        response = requests.Response()
        response.status_code = requests.codes.ok
        send_mock.side_effect = [ce, response]
        rs.send(request)
        time_mock.assert_called_with(DEFAULT_RETRY_DELAY)

        # two connection error delay = DEFAULT_RETRY_DELAY
        send_mock.reset_mock()
        time_mock.reset_mock()
        rs = ws.RetrySession()
        ce = requests.exceptions.ConnectionError()  # connection error that triggers retries
        ce.errno = 104
        response = requests.Response()
        response.status_code = requests.codes.ok
        send_mock.side_effect = [ce, ce, response]  # two connection errors
        rs.send(request)
        calls = [call(DEFAULT_RETRY_DELAY), call(DEFAULT_RETRY_DELAY*2)]
        time_mock.assert_has_calls(calls)

        # set the start retry to a large number and see how it is capped to MAX_RETRY_DELAY
        send_mock.reset_mock()
        time_mock.reset_mock()
        rs = ws.RetrySession(start_delay=MAX_RETRY_DELAY/2 + 1)
        ce = requests.exceptions.ConnectionError()  # connection error that triggers retries
        ce.errno = 104
        response = requests.Response()
        response.status_code = requests.codes.ok
        send_mock.side_effect = [ce, ce, response]  # two connection errors
        rs.send(request)
        calls = (call(MAX_RETRY_DELAY/2 + 1), call(MAX_RETRY_DELAY))
        time_mock.assert_has_calls(calls)

        # return the error all the time
        send_mock.reset_mock()
        time_mock.reset_mock()
        rs = ws.RetrySession(start_delay=MAX_RETRY_DELAY/2 + 1)
        ce = requests.exceptions.ConnectionError()  # connection error that triggers retries
        ce.errno = 104
        # make sure the mock returns more errors than the maximum number of retries allowed
        http_errors = []
        i = 0
        while i <= MAX_NUM_RETRIES:
            http_errors.append(ce)
            i += 1
        send_mock.side_effect = http_errors
        with self.assertRaises(exceptions.HttpException):
            rs.send(request)

        # return the connection error other than 104 - connection reset by peer
        send_mock.reset_mock()
        time_mock.reset_mock()
        rs = ws.RetrySession(start_delay=MAX_RETRY_DELAY / 2 + 1)
        ce = requests.exceptions.ConnectionError()  # connection error that triggers retries
        ce.errno = 105
        send_mock.side_effect = ce
        with self.assertRaises(exceptions.HttpException):
            rs.send(request)

        # return HttpError 503 with Retry-After
        send_mock.reset_mock()
        time_mock.reset_mock()
        rs = ws.RetrySession()
        server_delay = 5
        he = requests.exceptions.HTTPError()  # connection error that triggers retries
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
        he = requests.exceptions.HTTPError()  # connection error that triggers retries
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
        he = requests.exceptions.HTTPError()  # connection error that triggers retries
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
        he = requests.exceptions.HTTPError()  # connection error that triggers retries
        he.response = requests.Response()
        he.response.status_code = requests.codes.internal_server_error
        send_mock.side_effect = he
        with self.assertRaises(exceptions.HttpException):
            rs.send(request)


    def test_config_file_location(self):
        # test the location of the config file when the host is specified
        myhost = 'myhost.ca'
        resource = 'aresource'
        service = 'myservice'
        resource_id = 'ivo://canfar.phys.uvic.ca/{}'.format(service)
        client = Mock(resource_id=resource_id)
        with patch('cadcutils.net.ws.os.makedirs') as makedirs_mock:
            wscap = ws.WsCapabilities(client, host=myhost)
        expected_location = os.path.join(ws.CACHE_LOCATION, 'alt-domains', myhost)
        makedirs_mock.check_called_with(expected_location)
        self.assertEqual(os.path.join(expected_location, 'resource-caps'), wscap.reg_file)
        self.assertEqual(os.path.join(expected_location, urlparse(resource_id).netloc, service),
                         wscap.caps_file)

    def test_misc(self):
        """
        Tests miscellaneous functions
        """
        service_url = 'http://somehost.com/service'
        with patch('cadcutils.net.ws.WsCapabilities') as caps_mock:
            caps_mock.return_value.get_service_host.return_value = 'somehost.com'
            caps_mock.return_value.get_access_url.return_value = service_url
            client = ws.BaseWsClient("someresourceID", auth.Subject(), 'TestApp')
            self.assertEqual('{}'.format(service_url), client._get_url(('myfeature', None)))
            caps_mock.return_value.get_access_url.assert_called_once_with('myfeature')
            self.assertEqual('{}'.format(service_url), client._get_url(('myfeature', '')))
            
            
        test_host = 'testhost.com'
        with patch('cadcutils.net.ws.os.makedirs'):
            client = ws.BaseWsClient("someresourceID", auth.Subject(), 'TestApp', host=test_host)
        self.assertEqual(test_host, client.host)

        # test with resource as url
        with patch('cadcutils.net.ws.WsCapabilities') as caps_mock:
            caps_mock.return_value.get_service_host.return_value = 'somehost.com'
            client = ws.BaseWsClient("someresourceID", auth.Subject(), 'TestApp')
            resource_url = 'http://someurl.com/path/'
            self.assertEqual(resource_url, client._get_url(resource_url))
            # repeat with overriden host name
            client = ws.BaseWsClient("someresourceID", auth.Subject(), 'TestApp', host=test_host)
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



capabilities__content = \
"""
<vosi:capabilities xmlns:vosi="http://www.ivoa.net/xml/VOSICapabilities/v1.0" xmlns:vs="http://www.ivoa.net/xml/VODataService/v1.1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
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
            <securityMethod standardID="http://www.w3.org/Protocols/HTTP/1.0/spec.html#BasicAA"/>
        </interface>
        <interface xsi:type="vs:ParamHTTP" role="std" version="1.0">
            <accessURL use="base">
                https://WS_URL
            </accessURL>
            <securityMethod standardID="ivo://ivoa.net/sso#tls-with-certificate"/>
        </interface>
    </capability>
</vosi:capabilities>
"""

class TestWsCapabilities(unittest.TestCase):

    """Class for testing the webservie client"""
    @patch('cadcutils.net.ws.os.path.getmtime')
    @patch('cadcutils.net.ws.open', mock=mock_open())
    @patch('cadcutils.net.ws.BaseWsClient.get')
    def test_get_reg(self, get_mock, file_mock, file_modtime_mock):
        """
        Tests the registry part of WsCapabilities
        """
        # test when registry information is read from the server (cache is outdated)
        resource = 'aresource'
        service = 'myservice'
        resource_id = 'ivo://canfar.phys.uvic.ca/{}'.format(service)
        resource_cap_url = 'http://www.canfar.net/myservice'
        cadcreg_content = ('#test content\n {} = {} \n'
                           'ivo://some.provider/service = http://providerurl.test/service').\
            format(resource_id, resource_cap_url)
        response = Mock(text=cadcreg_content)
        get_mock.return_value = response
        # set the modified time of the cache file to 0 to make sure the info is retrieved from server
        file_modtime_mock.return_value = 0
        anon_subject = auth.Subject()
        # test anonymous access
        fh_mock = Mock()
        file_mock.write = fh_mock
        client = Mock(resource_id=resource_id)
        response = Mock()
        response.text = cadcreg_content
        client.get.return_value = response
        caps = ws.WsCapabilities(client)
        self.assertEqual(os.path.join(ws.CACHE_LOCATION, ws.REGISTRY_FILE), caps.reg_file)
        self.assertEqual(os.path.join(ws.CACHE_LOCATION, 'canfar.phys.uvic.ca', service), caps.caps_file)
        self.assertEqual(resource_cap_url, caps._get_capability_url())
        file_mock.assert_called_once_with(os.path.join(ws.CACHE_LOCATION, ws.REGISTRY_FILE), 'w')
        # TODO not sure why need to access write this way
        file_mock().__enter__.return_value.write.assert_called_once_with(cadcreg_content)

        # test when registry information is retrieved from the cache file
        get_mock.reset_mock()
        get_mock.return_value = None
        file_modtime_mock.reset_mock()
        file_mock.reset_mock()
        resource_cap_url2 = 'http://www.canfar.net/myservice2'
        cache_content2 = ('#test content\n {} = {} \n'
                           'ivo://some.provider/service = http://providerurl.test/service').\
            format(resource_id, resource_cap_url2)
        file_modtime_mock.return_value = time.time()
        file_mock().__enter__.return_value.read.return_value = cache_content2
        caps = ws.WsCapabilities(client)
        self.assertEqual(resource_cap_url2, caps._get_capability_url())

        # test when registry information is outdated but there are errors retrieving it from the CADC registry
        # so in the end go back and use the cache version
        file_modtime_mock.reset_mock()
        file_mock.reset_mock()
        file_modtime_mock.return_value = 0
        file_mock().__enter__.return_value.read.return_value = cache_content2
        get_mock.side_effect = [exceptions.HttpException()]
        client.get.side_effect = [exceptions.HttpException]
        caps = ws.WsCapabilities(client)
        self.assertEqual(resource_cap_url2, caps._get_capability_url())

    @patch('cadcutils.net.ws.os.path.getmtime')
    @patch('cadcutils.net.ws.open', mock=mock_open())
    @patch('cadcutils.net.ws.BaseWsClient.get')
    def test_get_caps(self, get_mock, file_mock, file_modtime_mock):
        """
        Tests the capabilities part of WsCapabilities
        """
        # test when registry information is read from the server (cache is outdated)
        resource = 'aresource'
        service = 'myservice'
        resource_id = 'ivo://canfar.phys.uvic.ca/{}'.format(service)
        resource_cap_url = 'www.canfar.net/myservice'
        cadcreg_content = ('#test content\n {} = {} \n'
                           'ivo://some.provider/service = http://providerurl.test/service'). \
            format(resource_id, resource_cap_url)
        # set the modified time of the cache file to 0 to make sure the info is retrieved from server
        file_modtime_mock.return_value = 0
        # test anonymous access
        fh_mock = Mock()
        file_mock.write = fh_mock
        client = Mock(resource_id=resource_id, subject=auth.Subject())
        response = Mock(text=capabilities__content.replace('WS_URL', resource_cap_url))
        client.get.return_value = response
        caps = ws.WsCapabilities(client)
        # mock _get_capability_url to return some url without attempting to access the server
        def get_url():
            return 'http://some.url/capabilities'
        caps._get_capability_url = get_url
        caps.caps_urls[service] = '{}/capabilities'.format(resource_cap_url)
        self.assertEqual('http://{}/capabilities'.format(resource_cap_url),
                          caps.get_access_url('ivo://ivoa.net/std/VOSI#capabilities'))
        self.assertEqual('http://{}/availability'.format(resource_cap_url),
                          caps.get_access_url('ivo://ivoa.net/std/VOSI#availability'))
        self.assertEqual('http://{}/pub'.format(resource_cap_url),
                          caps.get_access_url('vos://cadc.nrc.ca~service/CADC/mystnd01'))
        resource_url = urlparse(resource_id)
        file_mock.assert_called_once_with(os.path.join(ws.CACHE_LOCATION,
                                                       resource_url.netloc, resource_url.path.strip('/')), 'w')
        # TODO not sure why need to access write this way
        file_mock().__enter__.return_value.write.assert_called_once_with(
            capabilities__content.replace('WS_URL', resource_cap_url))

        # repeat for basic auth subject. Mock the netrc library to prevent a lookup for $HOME/.netrc
        with patch('cadcutils.net.auth.netrclib') as netrclib_mock:
            client = Mock(resource_id=resource_id, subject=auth.Subject(netrc=True))
        client.get.return_value = response
        caps = ws.WsCapabilities(client)
        caps._get_capability_url = get_url
        # capabilities works even if it has only one anonymous interface
        self.assertEqual('http://{}/capabilities'.format(resource_cap_url),
                            caps.get_access_url('ivo://ivoa.net/std/VOSI#capabilities'))
        # same for availability
        self.assertEqual('http://{}/availability'.format(resource_cap_url),
                            caps.get_access_url('ivo://ivoa.net/std/VOSI#availability'))

        # repeat for https
        with patch('os.path.isfile') as p:
            client = Mock(resource_id=resource_id, subject=auth.Subject(certificate='somecert.pem'))
        client.get.return_value = response
        caps = ws.WsCapabilities(client)
        caps._get_capability_url = get_url
        # capabilities works even if it has only one anonymous interface
        self.assertEqual('http://{}/capabilities'.format(resource_cap_url),
                            caps.get_access_url('ivo://ivoa.net/std/VOSI#capabilities'))
        # same for availability
        self.assertEqual('http://{}/availability'.format(resource_cap_url),
                            caps.get_access_url('ivo://ivoa.net/std/VOSI#availability'))
        self.assertEqual('https://{}'.format(resource_cap_url),
                          caps.get_access_url('vos://cadc.nrc.ca~service/CADC/mystnd01'))

        # test when capabilities information is retrieved from the cache file
        get_mock.reset_mock()
        get_mock.return_value = None
        file_modtime_mock.reset_mock()
        file_mock.reset_mock()
        resource_cap_url2 = 'http://www.canfar.net/myservice2'
        cache_content2 = capabilities__content.replace('WS_URL', resource_cap_url2)
        file_modtime_mock.return_value = time.time()
        file_mock().__enter__.return_value.read.return_value = cache_content2
        client = Mock(resource_id=resource_id, subject=auth.Subject())
        caps = ws.WsCapabilities(client)
        caps._get_capability_url = get_url
        caps.caps_urls[service] = '{}/capabilities'.format(resource_cap_url2)
        self.assertEqual('http://{}/capabilities'.format(resource_cap_url2),
                          caps.get_access_url('ivo://ivoa.net/std/VOSI#capabilities'))
        self.assertEqual('http://{}/availability'.format(resource_cap_url2),
                          caps.get_access_url('ivo://ivoa.net/std/VOSI#availability'))
        self.assertEqual('http://{}/pub'.format(resource_cap_url2),
                          caps.get_access_url('vos://cadc.nrc.ca~service/CADC/mystnd01'))
        resource_url = urlparse(resource_id)
        file_mock().__enter__.return_value.read.return_value = \
            capabilities__content.replace('WS_URL', resource_cap_url2)

        # repeat for basic auth subject. Mock the netrc library to prevent a lookup for $HOME/.netrc
        with patch('cadcutils.net.auth.netrclib') as netrclib_mock:
            client = Mock(resource_id=resource_id, subject=auth.Subject(netrc=True))
        caps = ws.WsCapabilities(client)
        caps._get_capability_url = get_url
        # does not work with user-password of the subject set above
        self.assertEqual('http://{}/capabilities'.format(resource_cap_url2),
                          caps.get_access_url('ivo://ivoa.net/std/VOSI#capabilities'))
        self.assertEqual('http://{}/availability'.format(resource_cap_url2),
                          caps.get_access_url('ivo://ivoa.net/std/VOSI#availability'))
        self.assertEqual('http://{}/auth'.format(resource_cap_url2),
                          caps.get_access_url('vos://cadc.nrc.ca~service/CADC/mystnd01'))

        # repeat for https
        with patch('os.path.isfile') as p:
            client = Mock(resource_id=resource_id, subject=auth.Subject(certificate='somecert.pem'))
        caps = ws.WsCapabilities(client)
        caps._get_capability_url = get_url
        # does not work with user-password of the subject set above
        self.assertEqual('http://{}/capabilities'.format(resource_cap_url2),
                          caps.get_access_url('ivo://ivoa.net/std/VOSI#capabilities'))
        self.assertEqual('http://{}/availability'.format(resource_cap_url2),
                          caps.get_access_url('ivo://ivoa.net/std/VOSI#availability'))
        self.assertEqual('https://{}'.format(resource_cap_url2),
                          caps.get_access_url('vos://cadc.nrc.ca~service/CADC/mystnd01'))
        

# TODO By default, internet tests fail. They only succeed when test with --remote-data flag.
# Need to figure out a way to skip the tests unless that flag is present.
# class TestWsOutsideCalls(unittest.TestCase):
#     """ Class to test Ws with calls to outside sites"""
#
#     @patch('time.sleep')
#     def testCalls(self, time_mock):
#         client = ws.BaseWsClient('httpbin.org')
#         response = client.get('')
#         self.assertEqual(response.status_code, requests.codes.ok)
#
#         with self.assertRaises(requests.HTTPError):
#             client.get('status/500')
#
#         time_mock.reset_mock()
#         with self.assertRaises(requests.HTTPError):
#             client.get('status/503')
#
#         calls = [call(DEFAULT_RETRY_DELAY),
#                  call(min(DEFAULT_RETRY_DELAY*2, MAX_RETRY_DELAY)),
#                  call(min(DEFAULT_RETRY_DELAY * 4, MAX_RETRY_DELAY)),
#                  call(min(DEFAULT_RETRY_DELAY * 8, MAX_RETRY_DELAY)),
#                  call(min(DEFAULT_RETRY_DELAY * 16, MAX_RETRY_DELAY)),
#                  call(min(DEFAULT_RETRY_DELAY * 32, MAX_RETRY_DELAY))]
#
#         time_mock.assert_has_calls(calls)
