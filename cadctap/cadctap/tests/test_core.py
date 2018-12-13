# # -*- coding: utf-8 -*-
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
#  $Revision: 4 $
#
# ***********************************************************************
#
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os
import sys
import unittest

from six import StringIO
from cadctap.core import main_app
from mock import Mock, patch, call

# The following is a temporary workaround for Python issue
# 25532 (https://bugs.python.org/issue25532)
call.__wrapped__ = None

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')


class MyExitError(Exception):
    def __init__(self):
        self.message = "MyExitError"


mycontent = ''


class TestCadcTapClient(unittest.TestCase):
    """Test the CadcTapClient class"""

    @patch('sys.exit', Mock(side_effect=[MyExitError, MyExitError, MyExitError,
                                         MyExitError, MyExitError, MyExitError,
                                         MyExitError, MyExitError]))
    def test_help(self):
        """ Tests the helper displays for commands and subcommands in main"""
        self.maxDiff = None

        # help
        with open(os.path.join(TESTDATA_DIR, 'help.txt'), 'r') as myfile:
            usage = myfile.read()

        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            sys.argv = ['cadc-tap', '--help']
            with self.assertRaises(MyExitError):
                main_app()
            self.assertEqual(usage, stdout_mock.getvalue())

        usage = ('usage: cadc-tap [-h] [-V] [-s SERVICE]\n'
                 '                {schema,query,create,delete,index,load} ...'
                 '\ncadc-tap: error: too few arguments\n')

        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            with patch('sys.stderr', new_callable=StringIO) as stderr_mock:
                sys.argv = ['cadc-tap']
                with self.assertRaises(MyExitError):
                    main_app()
                self.assertEqual(usage, stderr_mock.getvalue())

        # schema -h
        with open(os.path.join(TESTDATA_DIR,
                               'help_schema.txt'), 'r') as myfile:
            usage = myfile.read()

        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            sys.argv = ['cadc-tap', 'schema', '--help']
            with self.assertRaises(MyExitError):
                main_app()
            self.assertEqual(usage, stdout_mock.getvalue())

        # query -h
        with open(os.path.join(TESTDATA_DIR,
                               'help_query.txt'), 'r') as myfile:
            usage = myfile.read()

        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            sys.argv = ['cadc-tap', 'query', '--help']
            with self.assertRaises(MyExitError):
                main_app()
            self.assertEqual(usage, stdout_mock.getvalue())

        # create -h
        with open(os.path.join(TESTDATA_DIR,
                               'help_create.txt'), 'r') as myfile:
            usage = myfile.read()

        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            sys.argv = ['cadc-tap', 'create', '--help']
            with self.assertRaises(MyExitError):
                main_app()
            self.assertEqual(usage, stdout_mock.getvalue())

        # delete -h
        with open(os.path.join(TESTDATA_DIR,
                               'help_delete.txt'), 'r') as myfile:
            usage = myfile.read()

        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            sys.argv = ['cadc-tap', 'delete', '--help']
            with self.assertRaises(MyExitError):
                main_app()
            self.assertEqual(usage, stdout_mock.getvalue())

        # index -h
        with open(os.path.join(TESTDATA_DIR,
                               'help_index.txt'), 'r') as myfile:
            usage = myfile.read()

        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            sys.argv = ['cadc-tap', 'index', '--help']
            with self.assertRaises(MyExitError):
                main_app()
            self.assertEqual(usage, stdout_mock.getvalue())

        # load -h
        with open(os.path.join(TESTDATA_DIR,
                               'help_load.txt'), 'r') as myfile:
            usage = myfile.read()

        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            sys.argv = ['cadc-tap', 'load', '--help']
            with self.assertRaises(MyExitError):
                main_app()
            self.assertEqual(usage, stdout_mock.getvalue())

    @patch('cadctap.youcat.YoucatClient.load')
    @patch('cadctap.youcat.YoucatClient.create_index')
    @patch('cadctap.youcat.YoucatClient.delete_table')
    @patch('cadctap.youcat.YoucatClient.create_table')
    @patch('cadctap.youcat.YoucatClient.query')
    @patch('cadctap.youcat.YoucatClient.schema')
    def test_main(self, schema_mock, query_mock, create_mock, delete_mock,
                  index_mock, load_mock):
        sys.argv = ['cadc-tap', 'schema']
        main_app()
        calls = [call()]
        schema_mock.assert_has_calls(calls)

        sys.argv = ['cadc-tap', 'query', 'QUERY']
        main_app()
        calls = [call('QUERY', None, 'VOTable', None)]
        query_mock.assert_has_calls(calls)

        sys.argv = ['cadc-tap', 'create', 'tablename', 'path/to/file']
        main_app()
        calls = [call('tablename', 'path/to/file', None)]
        create_mock.assert_has_calls(calls)

        sys.argv = ['cadc-tap', 'index', 'tablename', 'columnName']
        main_app()
        calls = [call('tablename', 'columnName', False)]
        index_mock.assert_has_calls(calls)

        sys.argv = ['cadc-tap', 'load', 'tablename', 'path/to/file']
        main_app()
        calls = [call('tablename', ['path/to/file'], 'tsv')]
        load_mock.assert_has_calls(calls)
