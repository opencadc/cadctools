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

import os
import sys
import unittest

from mock import Mock, patch, mock_open
from six import StringIO

from cadcutils.net import auth


class MyExitError(Exception):
    pass


class TestAuth(unittest.TestCase):

    """ Class for testing networking authorization functionality """

    @patch('cadcutils.net.auth.os', Mock())
    @patch('cadcutils.net.auth.sys.stdout', Mock())
    @patch('cadcutils.net.auth.getpass')
    @patch('cadcutils.net.auth.sys.stdin')
    @patch('cadcutils.net.auth.netrc')
    def test_user_password(self, netrc_mock, stdin_mock, getpass_mock):
        """ Test get-cert functionality """

        # .netrc first
        netrc_mock.netrc.return_value.authenticators.return_value = ['usr', 'account', 'passwd']
        realm = "www.canfar.phys.uvic.ca"
        self.assertEqual(('usr', 'passwd'), auth.get_user_password(realm))

        # prompt
        netrc_mock.netrc.return_value.authenticators.return_value = False
        stdin_mock.readline.return_value = 'promptusr\n'
        getpass_mock.getpass.return_value = 'promptpasswd\n'
        self.assertEqual(('promptusr', 'promptpasswd'), auth.get_user_password(realm))

    @patch('cadcutils.net.auth.get_user_password', Mock(return_value=['usr', 'passwd']))
    @patch('cadcutils.net.auth.requests')
    def test_get_cert(self, requests_mock):
        """ Test get_cert functionality """
        response = Mock()
        response.content = 'CERT CONTENT'
        requests_mock.get.return_value = response

        self.assertEqual(response.content, auth.get_cert())

    @patch('cadcutils.net.auth.get_cert', Mock(return_value='CERTVALUE'))
    @patch('sys.exit', Mock(side_effect=[MyExitError]))
    def test_get_cert_main(self):
        """ Test the help option of the cadc-get-cert app """

        value = "CERTVALUE"

        # get certificate default location
        m = mock_open()
        with patch('six.moves.builtins.open', m, create=True):
            sys.argv = ["cadc-get-cert"]
            auth.get_cert_main()
        m.assert_called_once_with(os.path.join(os.getenv('HOME', '/tmp'), '.ssl/cadcproxy.pem'), 'w')
        handle = m()
        handle.write.assert_called_once_with(value)

        # save certificate in a file
        certfile = '/tmp/testcertfile'
        try:
            os.remove(certfile)
        except OSError as ex:
            pass
        sys.argv = ["cadc-get-cert", "--cert-filename", certfile]
        auth.get_cert_main()
        with open(certfile, 'r') as f:
            self.assertEqual(value, f.read())

        # test error when the directory of the cert file is in fact an existing file..
        errmsg = """[Errno 17] File exists: '{}' : {}
Expected /tmp/testcertfile to be a directory.
""".format(certfile, certfile)
        sys.argv = ["cadc-get-cert", "--cert-filename", certfile + "/cert"]
        with self.assertRaises(MyExitError):
            with patch('sys.stderr', new_callable=StringIO) as stderr_mock:
                auth.get_cert_main()
        self.assertEqual(errmsg, stderr_mock.getvalue())
        os.remove(certfile)


    @patch('sys.exit', Mock(side_effect=[MyExitError]))
    def test_get_cert_main_help(self):
        """ Test the help option of the cadc-get-cert app """

        usage =\
"""usage: cadc-get-cert [-h] [--daysValid DAYSVALID]
                     [--cert-filename CERT_FILENAME]
                     [--cert-server CERT_SERVER]

Retrieve a security certificate for interaction with a Web service such as
VOSpace. Certificate will be valid for daysValid and stored as local file
cert_filename. First looks for an entry in the users .netrc matching the realm
www.canfar.phys.uvic.ca, the user is prompted for a username and password if
no entry is found.

optional arguments:
  -h, --help            show this help message and exit
  --daysValid DAYSVALID
                        Number of days the certificate should be valid.
                        (default: 10)
  --cert-filename CERT_FILENAME
                        Filesystem location to store the proxy certificate.
                        (default: {})
  --cert-server CERT_SERVER
                        Certificate server network address. (default:
                        www.canfar.phys.uvic.ca)
""".format(os.path.join(os.getenv('HOME', '/tmp'), '.ssl/cadcproxy.pem'))
        # --help
        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            sys.argv = ["cadc-get-cert", "--help"]
            with self.assertRaises(MyExitError):
                auth.get_cert_main()
            self.assertEqual(usage, stdout_mock.getvalue())
