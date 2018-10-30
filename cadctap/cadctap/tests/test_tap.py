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
import logging
import shutil

from six import StringIO
from six.moves import xrange
from cadcutils.net import auth, ws, wscapabilities
from cadcutils import exceptions
from cadctap import CadcTapClient
from cadctap.core import main_app
from mock import Mock, patch, ANY, call
from astroquery.cadc import auth as tapauth
import astroquery.cadc.tap.model.taptable as taptable
import astroquery.cadc.tap.model.tapcolumn as tapcolumn

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

    @patch('cadcutils.net.wscapabilities.CapabilitiesReader')
    @patch('cadcutils.net.ws.WsCapabilities')
    @patch('cadctap.core.cadc.CadcTAP')
    @patch('cadcdata.core.net.BaseWsClient')
    @patch('cadctap.core.CadcTapClient')
    def test_get_tables(self, cadc_mock, basews_mock, cadctap_mock, ws_mock, wscap_mock):
        host='www.host.ca'
        url='http://www.host.ca/tap'
        ws_mock._get_content="document to parse"
        wscap_mock.parsexml="xml to parse"
        client=CadcTapClient(auth.Subject(), host=host)
        anon=tapauth.AnonAuthMethod()
        tables=[]
        table=taptable.TapTableMeta()
        table.set_schema('table')
        table.set_name('one')
        tables.append(table)
        table=taptable.TapTableMeta()
        table.set_schema('table')
        table.set_name('two')
        tables.append(table)
        table=taptable.TapTableMeta()
        table.set_schema('table')
        table.set_name('three')
        tables.append(table)
        cadctap_mock().get_tables.return_value=tables
        # get_tables
        with open(os.path.join(TESTDATA_DIR, 'get_tables.txt'), 'r') as myfile:
            usage = myfile.read()

        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            client.get_tables(False, anon, url) 

    @patch('cadcutils.net.wscapabilities.CapabilitiesReader')
    @patch('cadcutils.net.ws.WsCapabilities')
    @patch('cadctap.core.cadc.CadcTAP')
    @patch('cadcdata.core.net.BaseWsClient')
    @patch('cadctap.core.CadcTapClient')
    def test_get_table(self, cadc_mock, basews_mock, cadctap_mock, ws_mock, wscap_mock):
        host='www.host.ca'
        url='http://www.host.ca/tap'
        ws_mock._get_content="document to parse"
        wscap_mock.parsexml="xml to parse"
        client=CadcTapClient(auth.Subject(), host=host)
        anon=tapauth.AnonAuthMethod()
        table=taptable.TapTableMeta()
        table.set_schema('table')
        table.set_name('one')
        col=tapcolumn.TapColumn()
        col.set_name('col_one')
        table.add_column(col)
        col=tapcolumn.TapColumn()
        col.set_name('col_two')
        table.add_column(col)
        col=tapcolumn.TapColumn()
        col.set_name('col_three')
        table.add_column(col)
        cadctap_mock().get_table.return_value=table
        # get_table
        with open(os.path.join(TESTDATA_DIR, 'get_table.txt'), 'r') as myfile:
            usage = myfile.read()

        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            client.get_table('table.one', False, anon, url) 

    @patch('cadcutils.net.wscapabilities.CapabilitiesReader')
    @patch('cadcutils.net.ws.WsCapabilities')
    @patch('cadctap.core.cadc.CadcTAP')
    @patch('cadcdata.core.net.BaseWsClient')
    @patch('cadctap.core.CadcTapClient')
    def test_run_query(self, cadc_mock, basews_mock, cadctap_mock, ws_mock, wscap_mock):
        host='www.host.ca'
        url='http://www.host.ca/tap'
        ws_mock._get_content="document to parse"
        wscap_mock.parsexml="xml to parse"
        client=CadcTapClient(auth.Subject(), host=host)
        anon=tapauth.AnonAuthMethod()
        job=Mock()
        job().get_results.return_value='----------------'\
                                       'Query Results '\
                                       '----------------'\
                                       '   observationID'\
                                       '--------------------'\
                                       '             1168045'\
                                       '              760271'\
                                       '             1741728'\
                                       '             1168044'\
                                       'dao_c122_2005_012465'\
                                       '        n120307.0487'\
                                       'dao_c122_2005_012466'\
                                       '             1168043'\
                                       'dao_c122_2010_022544'\
                                       'dao_c182_2011_007348'
        cadctap_mock().run_query.return_value=job
        # run_query
        with open(os.path.join(TESTDATA_DIR, 'run_query.txt'), 'r') as myfile:
            usage = myfile.read()

        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            client.run_query(os.path.join(TESTDATA_DIR, 'run_query.txt'), 
                             False, None, 'votable', False, False, False, 
                             None, None, anon, url) 

    @patch('cadcutils.net.wscapabilities.CapabilitiesReader')
    @patch('cadcutils.net.ws.WsCapabilities')
    @patch('cadctap.core.cadc.CadcTAP')
    @patch('cadcdata.core.net.BaseWsClient')
    @patch('cadctap.core.CadcTapClient')
    def test_load_job(self, cadc_mock, basews_mock, cadctap_mock, ws_mock, wscap_mock):
        host='www.host.ca'
        url='http://www.host.ca/tap'
        ws_mock._get_content="document to parse"
        wscap_mock.parsexml="xml to parse"
        client=CadcTapClient(auth.Subject(), host=host)
        anon=tapauth.AnonAuthMethod()
        job=Mock()
        job().get_results.return_value='----------------'\
                                       'Query Results '\
                                       '----------------'\
                                       '   observationID'\
                                       '--------------------'\
                                       '             1168045'\
                                       '              760271'\
                                       '             1741728'\
                                       '             1168044'\
                                       'dao_c122_2005_012465'\
                                       '        n120307.0487'\
                                       'dao_c122_2005_012466'\
                                       '             1168043'\
                                       'dao_c122_2010_022544'\
                                       'dao_c182_2011_007348'
        cadctap_mock().load_async_job.return_value=job
        # load_async_job
        with open(os.path.join(TESTDATA_DIR, 'load_job.txt'), 'r') as myfile:
            usage = myfile.read()

        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            client.load_async_job('123', None, False, False, anon, url) 

    @patch('cadcutils.net.wscapabilities.CapabilitiesReader')
    @patch('cadcutils.net.ws.WsCapabilities')
    @patch('cadctap.core.cadc.CadcTAP')
    @patch('cadcdata.core.net.BaseWsClient')
    @patch('cadctap.core.CadcTapClient')
    def test_list_jobs(self, cadc_mock, basews_mock, cadctap_mock, ws_mock, wscap_mock):
        host='www.host.ca'
        url='http://www.host.ca/tap'
        ws_mock._get_content="document to parse"
        wscap_mock.parsexml="xml to parse"
        client=CadcTapClient(auth.Subject(), host=host)
        anon=tapauth.AnonAuthMethod()
        jobs=[]
        job=Mock()
        job().get_jobid.return_value='123'
        jobs.append(job)
        job=Mock()
        job().get_jobid.return_value='456'
        jobs.append(job)
        job=Mock()
        job().get_jobid.return_value='789'
        jobs.append(job)
        job=Mock()
        job().get_jobid.return_value='101'
        jobs.append(job)
        cadctap_mock().list_async_jobs.return_value=jobs
        # load_async_job
        with open(os.path.join(TESTDATA_DIR, 'list_jobs.txt'), 'r') as myfile:
            usage = myfile.read()

        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            client.list_async_jobs(False, None, False, anon, url) 

    @patch('sys.exit', Mock(side_effect=[MyExitError, MyExitError, MyExitError,
                                         MyExitError, MyExitError,
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

        usage=('usage: cadc-tap [-h] [-V]\n'
               '                {get-tables,get-table,run-query,load-async-job,list-async-jobs}\n'
               '                ...\n'
               'cadc-tap: error: too few arguments\n')
        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            with patch('sys.stderr', new_callable=StringIO) as stderr_mock:
                sys.argv = ['cadc-tap']
                with self.assertRaises(MyExitError):
                    main_app()
                self.assertEqual(usage, stderr_mock.getvalue())

        # get-tables -h
        with open(os.path.join(TESTDATA_DIR, 'help_get_tables.txt'), 'r') as myfile:
            usage = myfile.read()

        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            sys.argv = ['cadc-tap', 'get-tables', '--help']
            with self.assertRaises(MyExitError):
                main_app()
            self.assertEqual(usage, stdout_mock.getvalue())

        # get-table -h
        with open(os.path.join(TESTDATA_DIR, 'help_get_table.txt'), 'r') as myfile:
            usage = myfile.read()

        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            sys.argv = ['cadc-tap', 'get-table', '-h']
            with self.assertRaises(MyExitError):
                main_app()
            self.assertEqual(usage, stdout_mock.getvalue())

        # run-query -h
        with open(os.path.join(TESTDATA_DIR, 'help_run_query.txt'), 'r') as myfile:
            usage = myfile.read()

        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            sys.argv = ['cadc-tap', 'run-query', '--help']
            with self.assertRaises(MyExitError):
                main_app()
            self.assertEqual(usage, stdout_mock.getvalue())

        # load-async-job -h
        with open(os.path.join(TESTDATA_DIR, 'help_load_job.txt'), 'r') as myfile:
            usage = myfile.read()

        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            sys.argv = ['cadc-tap', 'load-async-job', '-h']
            with self.assertRaises(MyExitError):
                main_app()
            self.assertEqual(usage, stdout_mock.getvalue())

        # list-async-jobs -h
        with open(os.path.join(TESTDATA_DIR, 'help_list_jobs.txt'), 'r') as myfile:
            usage = myfile.read()

        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            sys.argv = ['cadc-tap', 'list-async-jobs', '--help']
            with self.assertRaises(MyExitError):
                main_app()
            self.assertEqual(usage, stdout_mock.getvalue())

    @patch('cadctap.core.CadcTapClient.list_async_jobs')
    @patch('cadctap.core.CadcTapClient.load_async_job')
    @patch('cadctap.core.CadcTapClient.run_query')
    @patch('cadctap.core.CadcTapClient.get_table')
    @patch('cadctap.core.CadcTapClient.get_tables')
    def test_main(self, get_tables_mock, get_table_mock, run_query_mock, load_async_mock, list_async_mock):
        url='http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/tap'
        sys.argv = ['cadc-tap', 'get-tables']
        main_app()
        calls = [call(False, ANY, url)]
        get_tables_mock.assert_has_calls(calls)

        sys.argv = ['cadc-tap', 'get-table', '-t', 'tablename']
        main_app()
        calls = [call('tablename', False, ANY, url)]
        get_table_mock.assert_has_calls(calls)

        sys.argv = ['cadc-tap', 'run-query', '-Q', 'query.sql', '-a', '-f', 'filename', '-b']
        main_app()
        calls = [call('query.sql', True, 'filename', 'votable', False, False, True, None, None, ANY, url)]
        run_query_mock.assert_has_calls(calls)

        sys.argv = ['cadc-tap', 'load-async-job', '-j', '123']
        main_app()
        calls = [call('123', None, False, False, ANY, url)]
        load_async_mock.assert_has_calls(calls)

        sys.argv = ['cadc-tap', 'list-async-jobs', '-n', '-f', 'filename', '-s']
        main_app()
        calls = [call(False, 'filename', True, ANY, url)]
        list_async_mock.assert_has_calls(calls)

if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
