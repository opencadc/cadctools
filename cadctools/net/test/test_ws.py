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

import unittest
from mock import Mock, patch, MagicMock, call, mock_open
from cadctools.net import ws
import requests

class TestWs(unittest.TestCase):

    ''' Class for testing the webservie client'''
    @patch('cadctools.net.ws.os.path.isfile', Mock())
    @patch('cadctools.net.ws.auth.get_user_password', Mock(return_value=['usr', 'passwd']))
    @patch('cadctools.net.ws.RetrySession.put')
    @patch('cadctools.net.ws.RetrySession.head')
    @patch('cadctools.net.ws.RetrySession.delete')
    @patch('cadctools.net.ws.RetrySession.get')
    @patch('cadctools.net.ws.RetrySession.post')
    def test_ops(self, post_mock, get_mock, delete_mock, head_mock, put_mock):
        with self.assertRaises(ValueError) as context:
            client = ws.BaseWsClient(None)
        resource = 'aresource'
        service = 'www.canfar.phys.uvic.ca/myservice'
        # test anonymous access
        client = ws.BaseWsClient(service)
        self.assertTrue(client.anon)
        self.assertTrue(client.retry)
        self.assertEqual('http://www.canfar.phys.uvic.ca/myservice', client.base_url)
        self.assertEqual(None, client.agent)
        self.assertEqual(None, client.certificate_file_location)
        self.assertEqual(None, client.basic_auth)
        self.assertTrue(client.retry)
        self.assertEquals(None, client._session) #lazy initialization
        response = client.get(resource)
        get_mock.assert_called_with('http://{}/{}'.format(service, resource), params=None)
        params = {'arg1':'abc', 'arg2':123, 'arg3':True}
        response = client.post(resource, **params)
        post_mock.assert_called_with('http://{}/{}'.format(service, resource), **params)
        response = client.delete(resource)
        delete_mock.assert_called_with('http://{}/{}'.format(service, resource))
        response = client.head(resource)
        head_mock.assert_called_with('http://{}/{}'.format(service, resource))
        response = client.put(resource, **params)
        put_mock.assert_called_with('http://{}/{}'.format(service, resource), **params)
        self.assertTrue(isinstance(client._session, ws.RetrySession))

        # test basic authentication access
        post_mock.reset_mock()
        get_mock.reset_mock()
        put_mock.reset_mock()
        delete_mock.reset_mock()
        head_mock.reset_mock()
        client = ws.BaseWsClient(service, anon=False, retry=False)
        self.assertFalse(client.anon)
        self.assertEquals(['usr', 'passwd'], client.basic_auth) #as per the get_user_password patch
        self.assertEquals('http://www.canfar.phys.uvic.ca/myservice/auth', client.base_url)
        self.assertEquals(None, client.agent)
        self.assertFalse(client.retry)
        self.assertEqual(None, client.certificate_file_location)
        response = client.get(resource)
        get_mock.assert_called_with('http://{}/auth/{}'.format(service, resource), params=None)
        params = {'arg1': 'abc', 'arg2': 123, 'arg3': True}
        response = client.post(resource, **params)
        post_mock.assert_called_with('http://{}/auth/{}'.format(service, resource), **params)
        response = client.delete(resource)
        delete_mock.assert_called_with('http://{}/auth/{}'.format(service, resource))
        response = client.head(resource)
        head_mock.assert_called_with('http://{}/auth/{}'.format(service, resource))
        response = client.put(resource, **params)
        put_mock.assert_called_with('http://{}/auth/{}'.format(service, resource), **params)
        self.assertTrue(isinstance(client._session, ws.RetrySession))


        # test cert authentication
        post_mock.reset_mock()
        get_mock.reset_mock()
        put_mock.reset_mock()
        delete_mock.reset_mock()
        head_mock.reset_mock()
        certfile = 'some/certfile.pem'
        client = ws.BaseWsClient(service, anon=False, cert_file=certfile)
        self.assertFalse(client.anon)
        self.assertEquals(None, client.basic_auth)
        self.assertEquals('https://www.canfar.phys.uvic.ca/myservice/pub', client.base_url)
        self.assertEquals(None, client.agent)
        self.assertTrue(client.retry)
        self.assertEqual(certfile, client.certificate_file_location)
        response = client.get(resource)
        get_mock.assert_called_with('https://{}/pub/{}'.format(service, resource), params=None)
        params = {'arg1': 'abc', 'arg2': 123, 'arg3': True}
        response = client.post(resource, **params)
        post_mock.assert_called_with('https://{}/pub/{}'.format(service, resource), **params)
        response = client.delete(resource)
        delete_mock.assert_called_with('https://{}/pub/{}'.format(service, resource))
        response = client.head(resource)
        head_mock.assert_called_with('https://{}/pub/{}'.format(service, resource))
        response = client.put(resource, **params)
        put_mock.assert_called_with('https://{}/pub/{}'.format(service, resource), **params)
        self.assertTrue(isinstance(client._session, ws.RetrySession))
        self.assertEquals((certfile, certfile), client._session.cert)

class TestRetrySession(unittest.TestCase):

    ''' Class for testing retry session '''

    @patch('cadctools.net.ws.requests.Session.get')
    def test_retry(self, get_mock):
        request = Mock()
        get_mock.send.return_value = Mock()
        rs = ws.RetrySession(False)
        rs.send(request)
        get_mock.assert_called_with(request)

        # mock delays
        get_mock.reset_mock()
        request = Mock()
        rs.ws.RetrySession()
        get_mock.side_effect = requests.exceptions.ConnectionError()
        rs.send(request)

