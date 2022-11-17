# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2022.                            (c) 2022.
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
from argparse import ArgumentError

from mock import Mock, patch
from six import StringIO
from six.moves.urllib.parse import urlparse
import tempfile
import os
import sys
import logging
import hashlib
from cadcutils.util import date2ivoa, str2ivoa, get_base_parser, \
    get_log_level, get_logger, Md5File
import pytest
import json

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')


class MyExitError(Exception):
    pass


class UtilTests(unittest.TestCase):
    """ Class for testing cadc utilities """

    def test_ivoa_dates(self):
        """ Test the ivoa date formats functions date2ivoa and str2ivoa """

        expect_date = '2016-11-07T11:22:33.123'
        self.assertEqual(expect_date, date2ivoa(str2ivoa(expect_date)))

        with self.assertRaises(ValueError):
            str2ivoa('2011-01-01')

        # handling of None arguments
        self.assertEqual(None, date2ivoa(None))
        self.assertEqual(None, str2ivoa(None))

    def test_get_log_level(self):
        """ Test the handling of logging level control from
        command line arguments """

        parser = get_base_parser(subparsers=False)
        args = parser.parse_args(
            ["--debug", "--resource-id", "www.some.resource"])
        self.assertEqual(logging.DEBUG, get_log_level(args))

        parser = get_base_parser(
            subparsers=False,
            default_resource_id='ivo://www.some.resource/resourceID')
        args = parser.parse_args(["--verbose"])
        self.assertEqual(logging.INFO, get_log_level(args))

        args = parser.parse_args(["--quiet"])
        self.assertEqual(logging.FATAL, get_log_level(args))

        args = parser.parse_args([])
        self.assertEqual(logging.ERROR, get_log_level(args))

        print("passed log level tests")

    def test_init_logging_debug(self):
        """ Test the init_logging function """
        logger1 = get_logger('namspace1', log_level=logging.DEBUG)
        self.assertEqual(logging.DEBUG, logger1.getEffectiveLevel())
        logger2 = get_logger('namspace2')
        self.assertEqual(logging.ERROR, logger2.getEffectiveLevel())

    def test_shared_logger(self):
        """ Loggers with the same namespace share the
            same logging instances """
        logger1 = get_logger('namspace1', log_level=logging.DEBUG)
        self.assertEqual(logging.DEBUG, logger1.getEffectiveLevel())
        logger2 = get_logger('namspace1', log_level=logging.WARN)
        self.assertEqual(logging.WARN, logger1.getEffectiveLevel())
        self.assertEqual(logging.WARN, logger2.getEffectiveLevel())
        logger3 = get_logger('namspace2', log_level=logging.INFO)
        self.assertEqual(logging.INFO, logger3.getEffectiveLevel())
        self.assertEqual(logging.WARN, logger1.getEffectiveLevel())
        self.assertEqual(logging.WARN, logger2.getEffectiveLevel())

    def test_modify_log_level(self):
        logger = get_logger('test_modify_log_level', log_level=logging.INFO)
        self.assertEqual(logging.INFO, logger.getEffectiveLevel())
        logger = get_logger('test_modify_log_level', log_level=logging.DEBUG)
        self.assertEqual(logging.DEBUG, logger.getEffectiveLevel())
        logger.setLevel(logging.ERROR)
        self.assertEqual(logging.ERROR, logger.getEffectiveLevel())

    def test_modname_log_format(self):

        stdout_pointer = sys.stdout
        try:
            sys.stdout = StringIO()  # capture output
            logger = get_logger()
            logger.error('Test message')
            out = sys.stdout.getvalue()  # release output
            self.assertTrue('test_utils' in out)
        finally:
            sys.stdout.close()  # close the stream
            sys.stdout = stdout_pointer  # restore original stdout

    def test_info_log_format(self):

        stdout_pointer = sys.stdout
        try:
            sys.stdout = StringIO()  # capture output
            logger = get_logger('test_info_log_format', log_level=logging.INFO)
            logger.info('Test message')
            out = sys.stdout.getvalue()  # release output
            self.assertTrue('INFO' in out)
            self.assertTrue('test_info_log_format' in out)
            self.assertTrue('Test message' in out)
        finally:
            sys.stdout.close()  # close the stream
            sys.stdout = stdout_pointer  # restore original stdout

    def test_debug_log_format(self):

        stdout_pointer = sys.stdout
        try:
            sys.stdout = StringIO()  # capture output
            logger = get_logger('test_debug_log_format_namespace',
                                log_level=logging.DEBUG)
            logger.debug('Test message')
            out = sys.stdout.getvalue()  # release output
            self.assertTrue('DEBUG' in out)
            self.assertTrue('test_debug_log_format_namespace' in out)
            self.assertTrue('test_utils.test_debug_log_format:' in out)
            self.assertTrue('Test message' in out)
        finally:
            sys.stdout.close()  # close the stream
            sys.stdout = stdout_pointer  # restore original stdout

    @patch('sys.exit', Mock(side_effect=[ArgumentError(None, None),
                                         ArgumentError(None, None),
                                         ArgumentError(None, None),
                                         ArgumentError(None, None),
                                         ArgumentError(None, None)]))
    def test_base_parser(self):

        parser = get_base_parser(subparsers=False, service=True)
        # new service arg replacement for resourceid
        service_id = 'ivo://some.domain/resourceid'
        args = parser.parse_args(["--service", service_id])
        self.assertEqual(service_id, args.service)

        short_service_id = 'resourceid'
        args = parser.parse_args(["--service", short_service_id])
        self.assertEqual('ivo://cadc.nrc.ca/{}'.format(
            short_service_id), args.service)

        parser = get_base_parser(subparsers=False)
        resource_id = "ivo://www.some.resource/resourceid"
        args = parser.parse_args(["--resource-id", resource_id])
        self.assertEqual(urlparse(resource_id), args.resource_id)

        parser = get_base_parser(subparsers=False,
                                 default_resource_id=resource_id)
        args = parser.parse_args([])
        self.assertEqual(resource_id, args.resource_id)

        # missing resourceID
        parser = get_base_parser(subparsers=False)
        with self.assertRaises(ArgumentError):
            parser.parse_args([])

        # invalid resourceID (scheme)
        resource_id = "http://www.some.resource/resourceid"
        parser = get_base_parser(subparsers=False,
                                 default_resource_id=resource_id)
        with self.assertRaises(ArgumentError):
            args = parser.parse_args([])

        # invalid resourceID (missing resource id)
        resource_id = "ivo://www.some.resource"
        parser = get_base_parser(subparsers=False,
                                 default_resource_id=resource_id)
        with self.assertRaises(ArgumentError):
            args = parser.parse_args([])

        # create a base parser for subparsers but fail to add subparsers
        parser = get_base_parser()
        with self.assertRaises(RuntimeError):
            parser.parse_args(['-h'])

        # try to add a subparser to a base parser created for no subparsers
        parser = get_base_parser(subparsers=False)
        with self.assertRaises(RuntimeError):
            parser.add_subparsers(dest='cmd')

    @patch('sys.exit', Mock(side_effect=[MyExitError, MyExitError, MyExitError,
                                         MyExitError, MyExitError,
                                         MyExitError]))
    def test_base_parser_help(self):
        # help with a simple, no subparsers basic parser - these are
        # the default arguments
        self.maxDiff = None

        with open(os.path.join(TESTDATA_DIR, 'help.txt'), 'r') as f:
            expected_stdout = f.read()

        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            with self.assertRaises(MyExitError):
                sys.argv = ["cadc-client", "--help"]
                parser = get_base_parser(subparsers=False, version=3.3)
                parser.parse_args()
            assert expected_stdout.strip('\n') == \
                _fix_help(stdout_mock.getvalue())

        # same test but no version this time
        with open(os.path.join(TESTDATA_DIR, 'help_no_version.txt'), 'r') as f:
            expected_stdout = f.read()

        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            with self.assertRaises(MyExitError):
                sys.argv = ["cadc-client", "--help"]
                parser = get_base_parser(subparsers=False)
                parser.parse_args()
            assert expected_stdout.strip('\n') == \
                _fix_help(stdout_mock.getvalue())

        # --help with a simple parser with a few extra command line options
        with open(os.path.join(TESTDATA_DIR, 'help_extra_opt.txt'), 'r') as f:
            expected_stdout = f.read()

        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            with self.assertRaises(MyExitError):
                sys.argv = ["cadc-client", "--help"]
                parser = get_base_parser(subparsers=False)
                parser.add_argument('-x', action='store_true',
                                    help='test argument')
                parser.add_argument('fileID',
                                    help='the ID of the file in the archive',
                                    nargs='+')
                parser.parse_args()
            assert expected_stdout.strip() == _fix_help(stdout_mock.getvalue())

        # help with a parser with 2 subcommands
        parser = get_base_parser()
        subparsers = parser.add_subparsers(dest='cmd', help='My subcommands')
        parser_cmd1 = subparsers.add_parser('cmd1')
        parser_cmd1.add_argument('-x', action='store_true',
                                 help='test argument')
        parser_cmd2 = subparsers.add_parser('cmd2')
        parser_cmd2.add_argument('fileID',
                                 help='the ID of the file in the archive',
                                 nargs='+')

        with open(os.path.join(TESTDATA_DIR, 'help_subcommands.txt'),
                  'r') as f:
            expected_stdout = f.read()

        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            with self.assertRaises(MyExitError):
                sys.argv = ["cadc-client", "-h"]
                parser.parse_args()
        assert expected_stdout.strip('\n') == _fix_help(stdout_mock.getvalue())

        with open(os.path.join(TESTDATA_DIR, 'help_subcommands1.txt'),
                  'r') as f:
            expected_stdout = f.read()

        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            with self.assertRaises(MyExitError):
                sys.argv = ['cadc-client', 'cmd1', '-h']
                parser.parse_args()
        assert expected_stdout.strip('\n') == _fix_help(stdout_mock.getvalue())

        with open(os.path.join(TESTDATA_DIR, 'help_subcommands2.txt'),
                  'r') as f:
            expected_stdout = f.read()

        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            with self.assertRaises(MyExitError):
                sys.argv = ['cadc-client', 'cmd2', '-h']
                parser.parse_args()
            assert expected_stdout.strip('\n') == \
                _fix_help(stdout_mock.getvalue())

    def test_get_newer_version(self):
        # mock pypi returns version ['0.9', '0.9.1a1', '1.0', '1.0.dev20190219']
        client_v02 = get_base_parser(False, version='cadc-application 0.2')
        client_v09 = get_base_parser(False, version='cadc-application 0.9')
        client_v10 = get_base_parser(False, version='cadc-application 1.0')
        client_v11 = get_base_parser(False, version='cadc-application 1.1')
        with patch('cadcutils.util.utils.requests') as mock_requests:
            with open(os.path.join(TESTDATA_DIR, 'myapp_versions.json')) as f:
                versions_json = f.read()
            response = Mock()
            response.json.return_value = json.loads(versions_json)
            mock_requests.get.return_value = response
            assert '1.0' == client_v02._get_newer_version()
            assert '1.0' == client_v09._get_newer_version()
            assert None == client_v10._get_newer_version()
            assert None == client_v11._get_newer_version()


def _fix_help(help_txt):
    """
    Deals with incompatibilities between versions
    :param help_txt:
    :return:
    """
    # Different title in python 3.10
    return help_txt.replace('options:', 'optional arguments:').strip('\n')


class TestMd5File(unittest.TestCase):
    """Test the vos Md5File class.
    """
    def test_operations(self):
        tmpfile = tempfile.NamedTemporaryFile()
        txt = 'This is a test of the Md5File class'
        with open(tmpfile.name, 'w') as f:
            f.write(txt)

        binary_content = open(tmpfile.name, 'rb').read()
        with Md5File(tmpfile.name, 'rb') as f:
            assert binary_content == f.read(10000)
        assert f.file.closed
        hash = hashlib.md5()
        hash.update(binary_content)
        assert f.md5_checksum == hash.hexdigest()

        # read small chunks
        hash = hashlib.md5()
        with Md5File(tmpfile.name, 'rb') as f:
            content = f.read(5)
            while content:
                hash.update(content)
                content = f.read(5)
        assert f.md5_checksum == hash.hexdigest()

        # read part of the file
        with Md5File(tmpfile.name, 'rb', length=4) as f:
            assert binary_content[:4] == f.read(1000)
        hash = hashlib.md5()
        hash.update(binary_content[:4])
        assert f.md5_checksum == hash.hexdigest()

        # repeat but read from an offset
        with Md5File(tmpfile.name, 'rb', offset=4) as f:
            assert len(f) == len(txt) - 4
            assert binary_content[4:] == f.read(1000)
        hash = hashlib.md5()
        hash.update(binary_content[4:])
        assert f.md5_checksum == hash.hexdigest()

        # repeat but tread just a segment
        with Md5File(tmpfile.name, 'rb', offset=5, length=6) as f:
            assert f.len() == 6
            assert binary_content[5:11] == f.read(1000)
        hash = hashlib.md5()
        hash.update(binary_content[5:11])
        assert f.md5_checksum == hash.hexdigest()

        # repeat but tread just a segment and read smaller buffers
        with Md5File(tmpfile.name, 'rb', offset=5, length=10) as f:
            buffer = f.read(2)
            result = b''
            while buffer:
                result += buffer
                buffer = f.read(2)
            assert binary_content[5:15] == result
        hash = hashlib.md5()
        hash.update(binary_content[5:15])
        assert f.md5_checksum == hash.hexdigest()

        with pytest.raises(AttributeError):
            Md5File(tmpfile.name, 'rb', offset=1000)
