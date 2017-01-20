# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2017.                            (c) 2017.
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
from mock import Mock, patch, mock_open
from six.moves.urllib.parse import urlparse
import time

from cadcutils.net import ws, auth, wsreg
from cadcutils import exceptions


class TestWsCapabilities(unittest.TestCase):

    """Class for testing the webservie client"""
    @patch('cadcutils.net.wsreg.os.path.getmtime')
    @patch('cadcutils.net.wsreg.open', mock=mock_open())
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
        response = Mock(content=cadcreg_content)
        get_mock.return_value = response
        # set the modified time of the cache file to 0 to make sure the info is retrieved from server
        file_modtime_mock.return_value = 0
        anon_subject = auth.Subject()
        # test anonymous access
        fh_mock = Mock()
        file_mock.write = fh_mock
        client = ws.BaseWsClient(resource_id, anon_subject, 'TestApp')
        caps = wsreg.WsCapabilities(client)
        self.assertEquals(os.path.join(wsreg.CACHE_LOCATION, wsreg.REGISTRY_FILE), caps.reg_file)
        self.assertEquals(os.path.join(wsreg.CACHE_LOCATION, 'canfar.phys.uvic.ca', service), caps.caps_file)
        self.assertEquals(resource_cap_url, caps._get_capability_url())
        file_mock.assert_called_once_with(os.path.join(wsreg.CACHE_LOCATION, wsreg.REGISTRY_FILE), 'wb')
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
        caps = wsreg.WsCapabilities(client)
        self.assertEquals(resource_cap_url2, caps._get_capability_url())

        # test when registry information is outdated but there are errors retrieving it from the CADC registry
        # so in the end go back and use the cache version
        file_modtime_mock.reset_mock()
        file_mock.reset_mock()
        file_modtime_mock.return_value = 0
        file_mock().__enter__.return_value.read.return_value = cache_content2
        get_mock.side_effect = [exceptions.HttpException()]
        caps = wsreg.WsCapabilities(client)
        self.assertEquals(resource_cap_url2, caps._get_capability_url())

    @patch('cadcutils.net.wsreg.os.path.getmtime')
    @patch('cadcutils.net.wsreg.open', mock=mock_open())
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
        response = Mock(content=capabilities__content.replace('WS_URL', resource_cap_url))
        get_mock.return_value = response
        # set the modified time of the cache file to 0 to make sure the info is retrieved from server
        file_modtime_mock.return_value = 0
        # test anonymous access
        fh_mock = Mock()
        file_mock.write = fh_mock
        client = ws.BaseWsClient(resource_id, auth.Subject(), 'TestApp')
        caps = wsreg.WsCapabilities(client)
        # mock _get_capability_url to return some url without attempting to access the server
        def get_url():
            return 'http://some.url/capabilities'
        caps._get_capability_url = get_url
        caps.caps_urls[service] = '{}/capabilities'.format(resource_cap_url)
        self.assertEquals('http://{}/capabilities'.format(resource_cap_url),
                          caps.get_access_url('ivo://ivoa.net/std/VOSI#capabilities'))
        self.assertEquals('http://{}/availability'.format(resource_cap_url),
                          caps.get_access_url('ivo://ivoa.net/std/VOSI#availability'))
        self.assertEquals('http://{}/pub'.format(resource_cap_url),
                          caps.get_access_url('vos://cadc.nrc.ca~service/CADC/mystnd01'))
        resource_url = urlparse(resource_id)
        file_mock.assert_called_once_with(os.path.join(wsreg.CACHE_LOCATION,
                                                       resource_url.netloc, resource_url.path.strip('/')), 'wb')
        # TODO not sure why need to access write this way
        file_mock().__enter__.return_value.write.assert_called_once_with(
            capabilities__content.replace('WS_URL', resource_cap_url))

        # repeat for basic auth subject
        client = ws.BaseWsClient(resource_id, auth.Subject(netrc=True), 'TestApp')
        caps = wsreg.WsCapabilities(client)
        caps._get_capability_url = get_url
        # does not work with user-password of the subject set above
        with self.assertRaises(ValueError):
            self.assertEquals('http://{}/capabilities'.format(resource_cap_url),
                            caps.get_access_url('ivo://ivoa.net/std/VOSI#capabilities'))
        # but it works anonymously
        self.assertEquals('http://{}/capabilities'.format(resource_cap_url),
                            caps.get_access_url('ivo://ivoa.net/std/VOSI#capabilities',
                                                security_method=None))
        # same for availability
        with self.assertRaises(ValueError):
            self.assertEquals('http://{}/availability'.format(resource_cap_url),
                            caps.get_access_url('ivo://ivoa.net/std/VOSI#availability'))
        self.assertEquals('http://{}/availability'.format(resource_cap_url),
                            caps.get_access_url('ivo://ivoa.net/std/VOSI#availability', None))
        self.assertEquals('http://{}/auth'.format(resource_cap_url),
                          caps.get_access_url('vos://cadc.nrc.ca~service/CADC/mystnd01'))

        # repeat for https
        with patch('os.path.isfile') as p:
            client = ws.BaseWsClient(resource_id, auth.Subject(certificate='somecert.pem'), 'TestApp')
        caps = wsreg.WsCapabilities(client)
        caps._get_capability_url = get_url
        # does not work with user-password of the subject set above
        with self.assertRaises(ValueError):
            self.assertEquals('http://{}/capabilities'.format(resource_cap_url),
                            caps.get_access_url('ivo://ivoa.net/std/VOSI#capabilities'))
        # but it works anonymously
        self.assertEquals('http://{}/capabilities'.format(resource_cap_url),
                            caps.get_access_url('ivo://ivoa.net/std/VOSI#capabilities',
                                                security_method=None))
        # same for availability
        with self.assertRaises(ValueError):
            self.assertEquals('http://{}/availability'.format(resource_cap_url),
                            caps.get_access_url('ivo://ivoa.net/std/VOSI#availability'))
        self.assertEquals('http://{}/availability'.format(resource_cap_url),
                            caps.get_access_url('ivo://ivoa.net/std/VOSI#availability', None))
        self.assertEquals('https://{}'.format(resource_cap_url),
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
        client = ws.BaseWsClient(resource_id, auth.Subject(), 'TestApp')
        caps = wsreg.WsCapabilities(client)
        caps._get_capability_url = get_url
        caps.caps_urls[service] = '{}/capabilities'.format(resource_cap_url2)
        self.assertEquals('http://{}/capabilities'.format(resource_cap_url2),
                          caps.get_access_url('ivo://ivoa.net/std/VOSI#capabilities'))
        self.assertEquals('http://{}/availability'.format(resource_cap_url2),
                          caps.get_access_url('ivo://ivoa.net/std/VOSI#availability'))
        self.assertEquals('http://{}/pub'.format(resource_cap_url2),
                          caps.get_access_url('vos://cadc.nrc.ca~service/CADC/mystnd01'))
        resource_url = urlparse(resource_id)
        file_mock().__enter__.return_value.read.return_value = \
            capabilities__content.replace('WS_URL', resource_cap_url2)

        # repeat for basic auth subject
        client = ws.BaseWsClient(resource_id, auth.Subject(netrc=True), 'TestApp')
        caps = wsreg.WsCapabilities(client)
        caps._get_capability_url = get_url
        # does not work with user-password of the subject set above
        with self.assertRaises(ValueError):
            self.assertEquals('http://{}/capabilities'.format(resource_cap_url2),
                              caps.get_access_url('ivo://ivoa.net/std/VOSI#capabilities'))
        # but it works anonymously
        self.assertEquals('http://{}/capabilities'.format(resource_cap_url2),
                          caps.get_access_url('ivo://ivoa.net/std/VOSI#capabilities',
                                              security_method=None))
        # same for availability
        with self.assertRaises(ValueError):
            self.assertEquals('http://{}/availability'.format(resource_cap_url2),
                              caps.get_access_url('ivo://ivoa.net/std/VOSI#availability'))
        self.assertEquals('http://{}/availability'.format(resource_cap_url2),
                          caps.get_access_url('ivo://ivoa.net/std/VOSI#availability', None))
        self.assertEquals('http://{}/auth'.format(resource_cap_url2),
                          caps.get_access_url('vos://cadc.nrc.ca~service/CADC/mystnd01'))

        # repeat for https
        with patch('os.path.isfile') as p:
            client = ws.BaseWsClient(resource_id, auth.Subject(certificate='somecert.pem'), 'TestApp')
        caps = wsreg.WsCapabilities(client)
        caps._get_capability_url = get_url
        # does not work with user-password of the subject set above
        with self.assertRaises(ValueError):
            self.assertEquals('http://{}/capabilities'.format(resource_cap_url2),
                              caps.get_access_url('ivo://ivoa.net/std/VOSI#capabilities'))
        # but it works anonymously
        self.assertEquals('http://{}/capabilities'.format(resource_cap_url2),
                          caps.get_access_url('ivo://ivoa.net/std/VOSI#capabilities',
                                              security_method=None))
        # same for availability
        with self.assertRaises(ValueError):
            self.assertEquals('http://{}/availability'.format(resource_cap_url2),
                              caps.get_access_url('ivo://ivoa.net/std/VOSI#availability'))
        self.assertEquals('http://{}/availability'.format(resource_cap_url2),
                          caps.get_access_url('ivo://ivoa.net/std/VOSI#availability', None))
        self.assertEquals('https://{}'.format(resource_cap_url2),
                          caps.get_access_url('vos://cadc.nrc.ca~service/CADC/mystnd01'))


    def test_errors(self):
        # tests some error cases
        cr = wsreg.CapabilitiesReader()
        with self.assertRaises(ValueError):
            cr.parsexml('blah')

        with self.assertRaisesRegexp(ValueError, 'Error parsing capabilities document: No capabilities found'):
            cr.parsexml('<capabilities></capabilities>')

        with self.assertRaisesRegexp(ValueError,
                                     'Error parsing capabilities document. '
                                     'Capability standard ID is invalid URL: None'):
            cr.parsexml('<capabilities><capability></capability></capabilities>')

        with self.assertRaisesRegexp(ValueError,
                                     'Error parsing capabilities document. '
                                     'Capability standard ID is invalid URL: abc'):
            cr.parsexml('<capabilities><capability standardID="abc"></capability></capabilities>')

        with self.assertRaisesRegexp(ValueError, 'Error parsing capabilities document. '
                                                 'No interfaces found for capability ivo://provider/service'):
            cr.parsexml('<capabilities><capability standardID="ivo://provider/service"></capability></capabilities>')

        with self.assertRaisesRegexp(ValueError, 'Error parsing capabilities document. '
                                                 'No accessURL for interface for ivo://provider/service'):
            cr.parsexml('<capabilities><capability standardID="ivo://provider/service">'
                        '<interface></interface></capability></capabilities>')

        with self.assertRaisesRegexp(ValueError, 'Error parsing capabilities document. '
                                                 'No accessURL for interface for ivo://provider/service'):
            cr.parsexml('<capabilities><capability standardID="ivo://provider/service">'
                        '<interface></interface><accessURL></accessURL></capability></capabilities>')

        with self.assertRaisesRegexp(ValueError, 'Error parsing capabilities document. '
                                                 'No accessURL for interface for ivo://provider/service'):
            cr.parsexml('<capabilities><capability standardID="ivo://provider/service">'
                        '<interface><accessURL standardID="abc"></accessURL></interface></capability></capabilities>')

        # simplest capabilities document that parses successfully
        cr.parsexml('<capabilities><capability standardID="ivo://provider/service">'
                    '<interface><accessURL>http://someurl/somepath'
                    '</accessURL></interface></capability></capabilities>')

        # add security method
        with self.assertRaisesRegexp(ValueError, 'Error parsing capabilities document. '
                                                 'Invalid security method None for URL '
                                                 'http://someurl/somepath of capability ivo://provider/service'):
            cr.parsexml('<capabilities><capability standardID="ivo://provider/service">'
                        '<interface><accessURL>http://someurl/somepath'
                        '</accessURL><securityMethod></securityMethod></interface></capability></capabilities>')

        with self.assertRaisesRegexp(ValueError, 'Error parsing capabilities document. '
                                                 'Invalid URL in access URL \(http://someurl/somepath\) or '
                                                 'security method \(mymethod\)'):
            cr.parsexml('<capabilities><capability standardID="ivo://provider/service">'
                        '<interface><accessURL>http://someurl/somepath'
                        '</accessURL><securityMethod standardID="mymethod"></securityMethod></interface></capability></capabilities>')

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