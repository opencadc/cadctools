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
from cadcutils.util import get_base_parser


class MyExitError(Exception):
    pass


class TestAuth(unittest.TestCase):

    """ Class for testing networking authorization functionality """


    @patch('cadcutils.net.auth.get_cert', Mock(return_value='CERTVALUE'))
    @patch('sys.exit', Mock(side_effect=[MyExitError]))
    def test_get_cert_main(self):
        """ Test the cert_main function """

        value = "CERTVALUE"

        # get certificate default location
        m = mock_open()
        with patch('six.moves.builtins.open', m, create=True):
            sys.argv = ["cadc-get-cert"]
            auth.get_cert_main()
        m.assert_called_with(os.path.join(os.getenv('HOME', '/tmp'), '.ssl/cadcproxy.pem'), 'w')
        handle = m()
        handle.write.assert_called_once_with(value)

        # save certificate in a file
        certfile = '/tmp/testcertfile'
        try:
            os.remove(certfile)
        except OSError as ex:
            pass
        sys.argv = ["cadc-get-cert", "--cert-filename", certfile]
        self.assertEquals(None, auth.get_cert_main())
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
"""usage: cadc-get-cert [-h]
                     [--cert CERT | -n | --netrc-file NETRC_FILE | -u USER]
                     [--host HOST] [--resource-id RESOURCE_ID] [-d | -q | -v]
                     [-V] [--cert-filename CERT_FILENAME]
                     [--days-valid DAYS_VALID]

Retrieve a security certificate for interaction with a Web service such as VOSpace. Certificate will be valid for days-valid and stored as local file cert_filename.

optional arguments:
  --cert CERT           location of your X509 certificate to use for
                        authentication (unencrypted, in PEM format)
  --cert-filename CERT_FILENAME
                        filesystem location to store the proxy certificate.
                        (default: {})
  --days-valid DAYS_VALID
                        number of days the certificate should be valid.
  -d, --debug           debug messages
  -h, --help            show this help message and exit
  --host HOST           base hostname for services - used mainly for testing
                        (default: www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca)
  -n                    use .netrc in $HOME for authentication
  --netrc-file NETRC_FILE
                        netrc file to use for authentication
  -q, --quiet           run quietly
  --resource-id RESOURCE_ID
                        resource identifier (default ivo://cadc.nrc.ca/cred)
  -u, --user USER       name of user to authenticate. Note: application
                        prompts for the corresponding password!
  -v, --verbose         verbose messages
  -V, --version         show program's version number and exit
""".format(os.path.join(os.getenv('HOME', '/tmp'), '.ssl/cadcproxy.pem'))
        # --help
        self.maxDiff = None  # Display the entire difference
        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            sys.argv = ["cadc-get-cert", "--help"]
            with self.assertRaises(MyExitError):
                auth.get_cert_main()
            self.assertEqual(usage, stdout_mock.getvalue())

    @patch('cadcutils.net.auth.os')
    def testSubject(self, os_mock):
        # anon subject
        subject = auth.Subject()
        self.assertTrue(subject.anon)
        self.assertEquals(None, subject.certificate)
        self.assertEquals({}, subject._hosts_auth)
        self.assertEquals(None, subject.get_auth('realm1'))

        # cert subject
        cert = 'somecert'
        subject = auth.Subject(certificate=cert)
        self.assertFalse(subject.anon)
        self.assertEquals(cert, subject.certificate)
        self.assertEquals({}, subject._hosts_auth)
        self.assertEquals(None, subject.get_auth('realm1'))

        # empty netrc subject
        m = mock_open()
        with patch('six.moves.builtins.open', m, create=True):
            subject = auth.Subject(netrc='somefile')
        self.assertFalse(subject.anon)
        self.assertEquals(None, subject.certificate)
        self.assertEquals({}, subject._hosts_auth)
        self.assertEquals(None, subject.get_auth('realm1'))

        # netrc with content
        netrc_content = {'realm1':('user1', None, 'pass1'), 'realm2':('user1', None, 'pass2')}
        expected_host_auth = {'realm1':('user1', 'pass1'), 'realm2':('user1', 'pass2')}
        os_mock.path.join.return_value = '/home/myhome/.netrc'
        with patch('cadcutils.net.auth.netrclib') as netrclib_mock:
            netrclib_mock.netrc.return_value.hosts = netrc_content
            subject = auth.Subject(netrc=True)
        self.assertFalse(subject.anon)
        self.assertEquals(None, subject.certificate)
        self.assertEquals('/home/myhome/.netrc', subject.netrc)
        self.assertEquals(expected_host_auth, subject._hosts_auth)
        self.assertEquals(('user1', 'pass1'), subject.get_auth('realm1'))
        self.assertEquals(('user1', 'pass2'), subject.get_auth('realm2'))
        self.assertEquals(None, subject.get_auth('realm3'))

        # subject with username
        username = 'user1'
        passwd = 'passwd1'
        subject = auth.Subject(username=username)
        self.assertFalse(subject.anon)
        self.assertEquals(None, subject.certificate)
        self.assertEquals({}, subject._hosts_auth)
        with patch('cadcutils.net.auth.getpass') as getpass_mock:
            getpass_mock.getpass.return_value = passwd
            self.assertEquals((username, passwd), subject.get_auth('realm1'))


        parser = get_base_parser(subparsers=False)
        args = parser.parse_args(['--resource-id', 'blah'])
        subject = auth.Subject.from_cmd_line_args(args)
        self.assertTrue(subject.anon)

        sys.argv = ['cadc-client', '--resource-id', 'blah', '--cert', 'mycert.pem']
        args = parser.parse_args()
        subject = auth.Subject.from_cmd_line_args(args)
        self.assertFalse(subject.anon)
        self.assertEquals('mycert.pem', subject.certificate)