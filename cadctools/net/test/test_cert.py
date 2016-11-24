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
from cadctools.net import auth

class TestAuth(unittest.TestCase):

    ''' Class for testing networking authorization functionality'''


    @patch('cadctools.net.auth.os', Mock())
    @patch('cadctools.net.auth.sys.stdout', Mock())
    @patch('cadctools.net.auth.getpass')
    @patch('cadctools.net.auth.sys.stdin')
    @patch('cadctools.net.auth.netrc')
    def test_user_password(self, netrc_mock, stdin_mock, getpass_mock):
        ''' Test get-cert functionality'''

        # .netrc first
        netrc_mock.netrc.return_value.authenticators.return_value = ['usr', 'account', 'passwd']
        realm = "www.canfar.phys.uvic.ca"
        self.assertEqual(('usr', 'passwd'), auth.get_user_password(realm))

        # prompt
        netrc_mock.netrc.return_value.authenticators.return_value = False
        stdin_mock.readline.return_value = 'promptusr\n'
        getpass_mock.getpass.return_value = 'promptpasswd\n'
        self.assertEqual(('promptusr', 'promptpasswd'), auth.get_user_password(realm))



    @patch('cadctools.net.auth.get_user_password', Mock(return_value=['usr', 'passwd']))
    @patch('cadctools.net.auth.requests')
    def test_get_cert(self, requests_mock):
        ''' Test get_cert functionality '''
        response = Mock()
        response.content = 'CERT CONTENT'
        requests_mock.get.return_value = response

        self.assertEqual(response.content, auth.get_cert())



    @patch('cadctools.net.auth.get_user_password', Mock(return_value='CERT'))
    def test_get_cert_main(self):
        ''' Test the cadc-get-cert app'''
        #TODO
        pass