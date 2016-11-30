#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2014.                            (c) 2014.
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
from mock import Mock, patch, MagicMock, call, mock_open
from cadcutils.net import ws
from cadcutils.net.ws import DEFAULT_RETRY_DELAY, MAX_RETRY_DELAY, MAX_NUM_RETRIES, SERVICE_RETRY
import requests



class TestWs(unittest.TestCase):

    """Class for testing the webservie client"""
    @patch('cadcutils.net.ws.os.path.isfile', Mock())
    @patch('cadcutils.net.ws.auth.get_user_password', Mock(return_value=('usr', 'passwd')))
    @patch('cadcutils.net.ws.RetrySession.put')
    @patch('cadcutils.net.ws.RetrySession.head')
    @patch('cadcutils.net.ws.RetrySession.delete')
    @patch('cadcutils.net.ws.RetrySession.get')
    @patch('cadcutils.net.ws.RetrySession.post')
    def test_ops(self, post_mock, get_mock, delete_mock, head_mock, put_mock):
        with self.assertRaises(ValueError):
            ws.BaseWsClient(None, "TestApp")
        resource = 'aresource'
        service = 'www.canfar.phys.uvic.ca/myservice'
        # test anonymous access
        client = ws.BaseWsClient(service, 'TestApp')
        self.assertTrue(client.anon)
        self.assertTrue(client.retry)
        self.assertEqual('http://www.canfar.phys.uvic.ca/myservice', client.base_url)
        self.assertEqual('TestApp', client.agent)
        self.assertEqual(None, client.certificate_file_location)
        self.assertEqual(None, client.basic_auth)
        self.assertTrue(client.retry)
        self.assertEquals(None, client._session) #lazy initialization
        client.get(resource)
        get_mock.assert_called_with('http://{}/{}'.format(service, resource), params=None)
        params = {'arg1':'abc', 'arg2':123, 'arg3':True}
        client.post(resource, **params)
        post_mock.assert_called_with('http://{}/{}'.format(service, resource), **params)
        client.delete(resource)
        delete_mock.assert_called_with('http://{}/{}'.format(service, resource))
        client.head(resource)
        head_mock.assert_called_with('http://{}/{}'.format(service, resource))
        client.put(resource, **params)
        put_mock.assert_called_with('http://{}/{}'.format(service, resource), **params)
        self.assertTrue(isinstance(client._session, ws.RetrySession))

        # test basic authentication access
        post_mock.reset_mock()
        get_mock.reset_mock()
        put_mock.reset_mock()
        delete_mock.reset_mock()
        head_mock.reset_mock()
        client = ws.BaseWsClient(service, 'TestApp', anon=False, retry=False)
        self.assertFalse(client.anon)
        self.assertEquals(('usr', 'passwd'), client.basic_auth) #as per the get_user_password patch
        self.assertEquals('http://www.canfar.phys.uvic.ca/myservice/auth', client.base_url)
        self.assertEquals('TestApp', client.agent)
        self.assertFalse(client.retry)
        self.assertEqual(None, client.certificate_file_location)
        client.get(resource)
        get_mock.assert_called_with('http://{}/auth/{}'.format(service, resource), params=None)
        params = {'arg1': 'abc', 'arg2': 123, 'arg3': True}
        client.post(resource, **params)
        post_mock.assert_called_with('http://{}/auth/{}'.format(service, resource), **params)
        client.delete(resource)
        delete_mock.assert_called_with('http://{}/auth/{}'.format(service, resource))
        client.head(resource)
        head_mock.assert_called_with('http://{}/auth/{}'.format(service, resource))
        client.put(resource, **params)
        put_mock.assert_called_with('http://{}/auth/{}'.format(service, resource), **params)
        self.assertTrue(isinstance(client._session, ws.RetrySession))


        # test cert authentication
        post_mock.reset_mock()
        get_mock.reset_mock()
        put_mock.reset_mock()
        delete_mock.reset_mock()
        head_mock.reset_mock()
        certfile = 'some/certfile.pem'
        client = ws.BaseWsClient(service, 'TestApp', anon=False, cert_file=certfile)
        self.assertFalse(client.anon)
        self.assertEquals(None, client.basic_auth)
        self.assertEquals('https://www.canfar.phys.uvic.ca/myservice/pub', client.base_url)
        self.assertEquals('TestApp', client.agent)
        self.assertTrue(client.retry)
        self.assertEqual(certfile, client.certificate_file_location)
        client.get(resource)
        get_mock.assert_called_with('https://{}/pub/{}'.format(service, resource), params=None)
        params = {'arg1': 'abc', 'arg2': 123, 'arg3': True}
        client.post(resource, **params)
        post_mock.assert_called_with('https://{}/pub/{}'.format(service, resource), **params)
        client.delete(resource)
        delete_mock.assert_called_with('https://{}/pub/{}'.format(service, resource))
        client.head(resource)
        head_mock.assert_called_with('https://{}/pub/{}'.format(service, resource))
        client.put(resource, **params)
        put_mock.assert_called_with('https://{}/pub/{}'.format(service, resource), **params)
        self.assertTrue(isinstance(client._session, ws.RetrySession))
        self.assertEquals((certfile, certfile), client._session.cert)


class TestRetrySession(unittest.TestCase):

    """ Class for testing retry session """

    @patch('time.sleep')
    @patch('cadcutils.net.ws.requests.Session.send')
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
        ce = requests.exceptions.ConnectionError() #connection error that triggers retries
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
        ce = requests.exceptions.ConnectionError() #connection error that triggers retries
        ce.errno = 104
        response = requests.Response()
        response.status_code = requests.codes.ok
        send_mock.side_effect = [ce, ce, response] # two connection errors
        rs.send(request)
        calls = [call(DEFAULT_RETRY_DELAY), call(DEFAULT_RETRY_DELAY*2)]
        time_mock.assert_has_calls(calls)

        # set the start retry to a large number and see how it is capped to MAX_RETRY_DELAY
        send_mock.reset_mock()
        time_mock.reset_mock()
        rs = ws.RetrySession(start_delay = MAX_RETRY_DELAY/2 + 1)
        ce = requests.exceptions.ConnectionError() #connection error that triggers retries
        ce.errno = 104
        response = requests.Response()
        response.status_code = requests.codes.ok
        send_mock.side_effect = [ce, ce, response] # two connection errors
        rs.send(request)
        calls = (call(MAX_RETRY_DELAY/2 + 1), call(MAX_RETRY_DELAY))
        time_mock.assert_has_calls(calls)

        # return the error all the time
        send_mock.reset_mock()
        time_mock.reset_mock()
        rs = ws.RetrySession(start_delay = MAX_RETRY_DELAY/2 + 1)
        ce = requests.exceptions.ConnectionError() #connection error that triggers retries
        ce.errno = 104
        # make sure the mock returns more errors than the maximum number of retries allowed
        http_errors = []
        i = 0
        while i <= MAX_NUM_RETRIES:
            http_errors.append(ce)
            i += 1
        send_mock.side_effect = http_errors
        with self.assertRaises(requests.ConnectionError):
            rs.send(request)

        # return the connection error other than 104 - connection reset by peer
        send_mock.reset_mock()
        time_mock.reset_mock()
        rs = ws.RetrySession(start_delay=MAX_RETRY_DELAY / 2 + 1)
        ce = requests.exceptions.ConnectionError()  # connection error that triggers retries
        ce.errno = 105
        send_mock.side_effect = ce
        with self.assertRaises(requests.ConnectionError):
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
        calls = [call(start_delay)] # uses the default delay
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
        calls = [call(start_delay)] # uses the default delay
        time_mock.assert_has_calls(calls)

        # tests non-transient errors
        send_mock.reset_mock()
        time_mock.reset_mock()
        rs = ws.RetrySession()
        he = requests.exceptions.HTTPError()  # connection error that triggers retries
        he.response = requests.Response()
        he.response.status_code = requests.codes.internal_server_error
        send_mock.side_effect = he
        with self.assertRaises(requests.HTTPError):
            rs.send(request)

#TODO By default, internet tests fail. They only succeed when test with --remote-data flag.
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
