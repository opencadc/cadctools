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
from cadcutils import net, exceptions

from six import StringIO, BytesIO
import cadctap
from cadctap.core import main_app
from mock import Mock, patch, call
import pytest
from cadctap import CadcTapClient
from cadctap.core import _get_subject_from_netrc,\
    _get_subject_from_certificate, _get_subject, exit_on_exception
import tempfile

from cadctap.core import TABLES_CAPABILITY_ID, ALLOWED_TB_DEF_TYPES,\
    ALLOWED_CONTENT_TYPES, TABLE_UPDATE_CAPABILITY_ID,\
    TABLE_LOAD_CAPABILITY_ID

# The following is a temporary workaround for Python issue
# 25532 (https://bugs.python.org/issue25532)
call.__wrapped__ = None

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')
BASE_URL = 'https://ws-cadc.canfar.net/youcat'


class MyExitError(Exception):
    def __init__(self):
        self.message = "MyExitError"


@patch('cadctap.core.net.BaseWsClient')
def test_cadc_client_basicaa(base_mock):
    service_realm = 'some.domain.com'
    service_url = 'https://{}'.format(service_realm)
    base_mock.return_value._get_url.return_value = service_url
    cookie_response = Mock()
    cookie_response.status_code = 200
    cookie_response.text = 'cookie val'
    subject = net.Subject()
    subject._netrc = True  # avoid checking for the real .netrc file
    base_mock.return_value.post.return_value = cookie_response
    # mock the get_auth of the subject to return user/passwd from netrc file
    get_auth = Mock(return_value=('user', 'pswd'))
    subject.get_auth = get_auth
    # after calling CadcTapClient the subject is augmented with cookie info
    CadcTapClient(subject)
    get_auth.assert_called_with(service_realm)
    assert len(subject.cookies) == len(cadctap.core.CADC_REALMS)
    for cookie in subject.cookies:
        # note "" around the value required by the cookie
        assert cookie.value == '"cookie val"'
    base_mock.return_value.post.assert_called_with(
        ('ivo://ivoa.net/std/UMS#login-0.1', None),
        data='username=user&password=pswd',
        headers={'Content-type': 'application/x-www-form-urlencoded',
                 'Accept': 'text/plain'})


def test_get_subject_from_certificate():
    orig_home = os.environ['HOME']
    try:
        # has no certificate
        os.environ['HOME'] = '/tmp'
        subject = _get_subject_from_certificate()
        assert(subject is None)
        # has certificate
        os.environ['HOME'] = TESTDATA_DIR
        subject = _get_subject_from_certificate()
        assert(subject is not None)
        assert(isinstance(subject, net.Subject))
    finally:
        os.environ['HOME'] = orig_home


@patch('cadctap.core.net.BaseWsClient')
@patch('cadctap.core.CadcTapClient')
@patch('netrc.netrc')
def test_get_subject_from_netrc(netrc_mock, client_mock, base_client_mock):
    netrc_instance = netrc_mock.return_value
    client_instance = client_mock.return_value
    # test non-CADC services first
    #  no matching domain
    client_instance._tap_client._host = 'www.some.tap.service.com'
    netrc_instance.hosts = {'no_such_host': 'my.host.ca'}
    args = Mock()
    args.service = 'tap'
    args.host = None
    subject = _get_subject_from_netrc('tap')
    assert(subject is None)
    # matches domain
    netrc_instance.hosts = {'no_such_host': 'my.host.ca',
                            'www.some.tap.service.com':
                            'machine some.tap.service.com \
                                login auser password passwd'}
    subject = _get_subject_from_netrc(args)
    assert(subject is not None)
    assert(isinstance(subject, net.Subject))

    # CADC services
    args.service = 'ivo://cadc.nrc.ca/tap'
    # no match
    base_client_mock.return_value._get_url.return_value = \
        'https://some.domain.com'
    netrc_instance.hosts = {'no_such_host': 'my.host.ca',
                            'sc2.canfar.net': 'machine www.canfar.net \
                                   login auser password passwd'}
    subject = _get_subject_from_netrc(args)
    assert (subject is None)
    # match domain
    base_client_mock.return_value._get_url.return_value = \
        'https://sc2.canfar.net/blah'
    client_instance._tap_client._host = 'sc2.canfar.net'
    netrc_instance.hosts = {'no_such_host': 'my.host.ca',
                            'sc2.canfar.net': 'machine www.canfar.net \
                                login auser password passwd'}
    subject = _get_subject_from_netrc(args)
    assert(subject is not None)
    assert(isinstance(subject, net.Subject))


@patch('cadctap.core.CadcTapClient')
@patch('netrc.netrc')
@patch('cadcutils.net.auth.Subject.from_cmd_line_args')
def test_get_subject(from_cmd_line_mock, netrc_mock, client_mock):
    args = Mock()
    args.service = 'tap'
    args.anon = None
    # authentication option specified
    netrc_subject = net.Subject(netrc=True)
    from_cmd_line_mock.return_value = netrc_subject
    ret_subject = _get_subject(args)
    assert(ret_subject is not None)
    assert(ret_subject == netrc_subject)

    # no authentication options, pick -n option
    anon_subject = net.Subject()
    netrc_subject = net.Subject(netrc=True)
    from_cmd_line_mock.return_value = anon_subject
    netrc_instance = netrc_mock.return_value
    netrc_instance.hosts = {'netrc.host': 'netrc.host.ca'}
    client_instance = client_mock.return_value
    client_instance._tap_client._host = 'netrc.host'
    ret_subject = _get_subject(args)
    assert(ret_subject is not None)
    assert(ret_subject.anon is False)
    assert(ret_subject.certificate is None)
    assert(ret_subject.netrc is not None)

    # no authentication options, pick --cert option
    orig_home = os.environ['HOME']
    try:
        # has certificate
        os.environ['HOME'] = TESTDATA_DIR
        anon_subject = net.Subject()
        netrc_subject = net.Subject(netrc=True)
        from_cmd_line_mock.return_value = anon_subject
        netrc_instance = netrc_mock.return_value
        netrc_instance.hosts = {'netrc.host': 'netrc.host.ca'}
        client_instance = client_mock.return_value
        client_instance._tap_client._host = 'no.such.host'
        ret_subject = _get_subject(args)
        assert(ret_subject is not None)
        assert(ret_subject.anon is False)
        assert(ret_subject.certificate is not None)
        assert(ret_subject.netrc is False)
    finally:
        os.environ['HOME'] = orig_home

    # no authentication options, pick --anon option
    orig_home = os.environ['HOME']
    try:
        # has no certificate
        os.environ['HOME'] = 'tmp'
        anon_subject = net.Subject()
        netrc_subject = net.Subject(netrc=True)
        from_cmd_line_mock.return_value = anon_subject
        netrc_instance = netrc_mock.return_value
        netrc_instance.hosts = {'netrc.host': 'netrc.host.ca'}
        client_instance = client_mock.return_value
        client_instance._tap_client._host = 'no.such.host'
        ret_subject = _get_subject(args)
        assert(ret_subject is not None)
        assert(ret_subject == anon_subject)
    finally:
        os.environ['HOME'] = orig_home


@patch('cadcutils.net.ws.BaseWsClient.put')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
def test_create_table(caps_get_mock, base_put_mock):
    caps_get_mock.return_value = BASE_URL
    client = CadcTapClient(net.Subject())
    # default format
    def_table = os.path.join(TESTDATA_DIR, 'createTable.vosi')
    def_table_content = open(def_table, 'rb').read()
    client.create_table('sometable', def_table, 'VOSITable')
    base_put_mock.assert_called_with(
        (TABLES_CAPABILITY_ID, 'sometable'), data=def_table_content,
        headers={'Content-Type': '{}'.format(
            ALLOWED_TB_DEF_TYPES['VOSITable'])})

    # VOTable format
    base_put_mock.reset_mock()
    client.create_table('sometable', def_table, 'VOTable')
    base_put_mock.assert_called_with(
        (TABLES_CAPABILITY_ID, 'sometable'), data=def_table_content,
        headers={'Content-Type': '{}'.format(
            ALLOWED_TB_DEF_TYPES['VOTable'])})

    # default is VOTable format
    base_put_mock.reset_mock()
    client.create_table('sometable', def_table)
    base_put_mock.assert_called_with(
        (TABLES_CAPABILITY_ID, 'sometable'), data=def_table_content,
        headers={'Content-Type': '{}'.format(
            ALLOWED_TB_DEF_TYPES['VOSITable'])})

    # error cases
    with pytest.raises(AttributeError):
        client.create_table(None, def_table)
    with pytest.raises(AttributeError):
        client.create_table('sometable', None)


@patch('cadcutils.net.ws.BaseWsClient.delete')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
def test_delete_table(caps_get_mock, base_delete_mock):
    caps_get_mock.return_value = BASE_URL
    client = CadcTapClient(net.Subject())
    client.delete_table('sometable')
    base_delete_mock.assert_called_with((TABLES_CAPABILITY_ID,
                                        'sometable'))

    # error case
    with pytest.raises(AttributeError):
        client.delete(None)


@patch('cadcutils.net.ws.BaseWsClient.post')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
def test_load_table(caps_get_mock, base_put_mock):
    caps_get_mock.return_value = BASE_URL
    client = CadcTapClient(net.Subject())
    test_load_tb = os.path.join(TESTDATA_DIR, 'loadTable.txt')

    # default format (tsv) using stdin
    with open(test_load_tb, 'rb') as fh:
        sys.stdin = fh
        with patch('cadctap.core.open') as open_mock:
            open_mock.return_value = fh
            client.load('schema.sometable', '-')
    base_put_mock.assert_called_with(
        (TABLE_LOAD_CAPABILITY_ID, 'schema.sometable'), data=fh,
        headers={'Content-Type': str(ALLOWED_CONTENT_TYPES['tsv'])})

    # default format (tsv)
    with open(test_load_tb, 'rb') as fh:
        with patch('cadctap.core.open') as open_mock:
            open_mock.return_value = fh
            client.load('schema.sometable', [test_load_tb])
    base_put_mock.assert_called_with(
        (TABLE_LOAD_CAPABILITY_ID, 'schema.sometable'), data=fh,
        headers={'Content-Type': str(ALLOWED_CONTENT_TYPES['tsv'])})

    # tsv format
    with open(test_load_tb, 'rb') as fh:
        with patch('cadctap.core.open') as open_mock:
            open_mock.return_value = fh
            client.load('schema.sometable', [test_load_tb], fformat='tsv')
    base_put_mock.assert_called_with(
        (TABLE_LOAD_CAPABILITY_ID, 'schema.sometable'), data=fh,
        headers={'Content-Type': str(ALLOWED_CONTENT_TYPES['tsv'])})

    # csv format
    with open(test_load_tb, 'rb') as fh:
        with patch('cadctap.core.open') as open_mock:
            open_mock.return_value = fh
            client.load('schema.sometable', [test_load_tb], fformat='csv')
    base_put_mock.assert_called_with(
        (TABLE_LOAD_CAPABILITY_ID, 'schema.sometable'), data=fh,
        headers={'Content-Type': str(ALLOWED_CONTENT_TYPES['csv'])})

    # FITS table format
    with open(test_load_tb, 'rb') as fh:
        with patch('cadctap.core.open') as open_mock:
            open_mock.return_value = fh
            client.load('schema.sometable', [test_load_tb],
                        fformat='FITSTable')
    base_put_mock.assert_called_with(
        (TABLE_LOAD_CAPABILITY_ID, 'schema.sometable'), data=fh,
        headers={'Content-Type': str(ALLOWED_CONTENT_TYPES['FITSTable'])})

    # error cases
    with pytest.raises(AttributeError):
        client.load(None, [test_load_table])
    with pytest.raises(AttributeError):
        client.load('sometable', None)
    with pytest.raises(AttributeError):
        client.load('sometable', [])


@patch('cadcutils.net.ws.BaseWsClient.post')
@patch('cadcutils.net.ws.BaseWsClient.get')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
def test_create_index(caps_get_mock, base_get_mock, base_post_mock):
    caps_get_mock.return_value = BASE_URL
    client = CadcTapClient(net.Subject())
    response1 = Mock()
    response1.status_code = 303
    job_location = 'http://go.here'
    response1.headers = {'Location': job_location}
    base_post_mock.return_value = response1
    response2 = Mock()
    response2.status_code = 200
    response2.text = "EXECUTING"
    base_get_mock.side_effect = [response2]
    response3 = Mock()
    response3.status_code = 200
    response3.text = "COMPLETED"
    base_get_mock.side_effect = [response2, response3]
    client.create_index('schema.sometable', 'col1', unique=True)

    # expected post calls
    post_calls = [call((TABLE_UPDATE_CAPABILITY_ID, None),
                  allow_redirects=False,
                  data={'table': 'schema.sometable',
                        'uniquer': True,
                        'index': 'col1'}),
                  call('{}/phase'.format(job_location),
                  data={'PHASE': 'RUN'})]
    base_post_mock.assert_has_calls(post_calls)

    # expected get calls
    get_calls = [call('{}/phase'.format(job_location), data={'WAIT': 1}),
                 call('{}/phase'.format(job_location), data={'WAIT': 1})]
    base_get_mock.assert_has_calls(get_calls)

    # error cases
    with pytest.raises(AttributeError):
        client.create_index(None, 'col1')
    with pytest.raises(AttributeError):
        client.create_index('sometable', None)
    response4 = Mock()
    response4.status_code = 200
    response4.text = 'ABORTED'
    base_get_mock.side_effect = [response4, response4]
    client = CadcTapClient(net.Subject())
    with pytest.raises(RuntimeError):
        client.create_index('sometable', 'col1')

    response5 = Mock()
    response5.status_code = 500
    base_get_mock.side_effect = [response1, response4, response4]
    client = CadcTapClient(net.Subject())
    with pytest.raises(RuntimeError):
        client.create_index('sometable', 'col1')


@patch('cadcutils.net.ws.BaseWsClient.get')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
def test_schema(caps_get_mock, base_get_mock):
    caps_get_mock.return_value = BASE_URL
    client = CadcTapClient(net.Subject())
    # default schema
    client.schema()
    base_get_mock.assert_called_with((TABLES_CAPABILITY_ID, None),
                                     params={'detail': 'min'})
    # table schema
    client.schema('mytable')
    base_get_mock.assert_called_with((TABLES_CAPABILITY_ID, 'mytable'),
                                     params={'detail': 'min'})


@patch('cadcutils.net.ws.BaseWsClient.post')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
def test_query(caps_get_mock, base_post_mock):
    caps_get_mock.return_value = BASE_URL
    response = Mock()
    response.status_code = 200
    response.raw.read.return_value = b'<VOTable format>'
    response.text = 'Header 1\nVal1\nVal2\n'
    # NOTE: post mock returns a context manager with the responose, hence
    # the __enter__
    base_post_mock.return_value.__enter__.return_value = response
    client = CadcTapClient(net.Subject())
    # default format
    def_name = 'tmptable'
    def_table = os.path.join(TESTDATA_DIR, 'votable.xml')

    fields = {'LANG': 'ADQL',
              'QUERY': 'query',
              'FORMAT': 'VOTable'}
    tablefile = os.path.basename(def_table)
    fields['UPLOAD'] = '{},param:{}'.format(def_name, tablefile)
    fields[tablefile] = (def_table, open(def_table, 'rb'))
    with patch('cadctap.core.sys.stdout', new_callable=BytesIO):
        client.query('query', tmptable='tmptable:'+def_table)
    assert base_post_mock.call_args_list[0][0][0] == \
        '{}/{}'.format(BASE_URL, 'sync')

    base_post_mock.reset_mock()
    with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
        client.query('query', data_only=True, response_format='tsv')
    assert stdout_mock.getvalue() == 'Val1\nVal2\n'

    # save in a file
    tf = tempfile.NamedTemporaryFile()
    base_post_mock.reset_mock()
    base_post_mock.return_value.__enter__.return_value = response
    client.query('query', output_file=tf.name, response_format='tsv')
    actual = open(tf.name).read()
    assert actual == 'Header 1\n-----------------------\n' \
                     'Val1\nVal2\n\n(2 rows affected)\n'

    # different format => result from server not altered
    base_post_mock.reset_mock()
    base_post_mock.return_value.__enter__.return_value = response
    with patch('sys.stdout', new_callable=BytesIO) as stdout_mock:
        client.query('query')
    assert stdout_mock.getvalue() == response.raw.read()


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

        usage = ('usage: cadc-tap [-h] [-V] '
                 '{schema,query,create,delete,index,load} ...'
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

    @patch('sys.exit', Mock(side_effect=[MyExitError, MyExitError, MyExitError,
                                         MyExitError, MyExitError, MyExitError,
                                         MyExitError, MyExitError]))
    def test_exit_on_exception(self):
        """ Test the exit_on_exception function """

        # test handling of 'Bad Request' server errors
        with patch('sys.stderr', new_callable=StringIO) as stderr_mock:
            expected_message = "source error message"
            orig_ex = RuntimeError("Bad Request")
            ex = exceptions.HttpException(expected_message, orig_ex)
            try:
                raise ex
            except Exception as ex:
                with self.assertRaises(MyExitError):
                    exit_on_exception(ex)
                self.assertTrue(expected_message in stderr_mock.getvalue())

        # test handling of certificate expiration message from server
        with patch('sys.stderr', new_callable=StringIO) as stderr_mock:
            expected_message = "Certificate expired"
            orig_ex = RuntimeError(
                "this message indicates certificate expired")
            ex = exceptions.HttpException(None, orig_ex)
            try:
                raise ex
            except Exception as ex:
                with self.assertRaises(MyExitError):
                    exit_on_exception(ex)
                self.assertTrue(expected_message in stderr_mock.getvalue())

        # test handling of other server error messages
        with patch('sys.stderr', new_callable=StringIO) as stderr_mock:
            expected_message = "other error message"
            orig_ex = RuntimeError(expected_message)
            ex = exceptions.HttpException(None, orig_ex)
            try:
                raise ex
            except Exception as ex:
                with self.assertRaises(MyExitError):
                    exit_on_exception(ex)
                self.assertTrue(expected_message in stderr_mock.getvalue())

        # test handling of other non-server error messages
        with patch('sys.stderr', new_callable=StringIO) as stderr_mock:
            expected_message = "non-server error message"
            ex = RuntimeError(expected_message)
            try:
                raise ex
            except Exception as ex:
                with self.assertRaises(MyExitError):
                    exit_on_exception(ex)
                self.assertTrue(expected_message in stderr_mock.getvalue())

    @patch('cadctap.CadcTapClient.load')
    @patch('cadctap.CadcTapClient.create_index')
    @patch('cadctap.CadcTapClient.delete_table')
    @patch('cadctap.CadcTapClient.create_table')
    @patch('cadctap.CadcTapClient.query')
    @patch('cadctap.CadcTapClient.schema')
    def test_main(self, schema_mock, query_mock, create_mock, delete_mock,
                  index_mock, load_mock):
        sys.argv = ['cadc-tap', 'schema']
        main_app()
        calls = [call(None)]
        schema_mock.assert_has_calls(calls)

        sys.argv = ['cadc-tap', 'schema', 'mytable']
        main_app()
        calls = [call('mytable')]
        schema_mock.assert_has_calls(calls)

        sys.argv = ['cadc-tap', 'create', '-d', 'tablename', 'path/to/file']
        main_app()
        calls = [call('tablename', 'path/to/file', 'VOSITable')]
        create_mock.assert_has_calls(calls)

        sys.argv = ['cadc-tap', 'index', '-v', 'tablename', 'columnName']
        main_app()
        calls = [call('tablename', 'columnName', False)]
        index_mock.assert_has_calls(calls)

        sys.argv = ['cadc-tap', 'load', 'tablename', 'path/to/file']
        main_app()
        calls = [call('tablename', ['path/to/file'], 'tsv')]
        load_mock.assert_has_calls(calls)

        sys.argv = ['cadc-tap', 'query', '-s', 'http://someservice', 'QUERY']
        main_app()
        calls = [call('QUERY', None, 'tsv', None, data_only=False, timeout=2)]
        query_mock.assert_has_calls(calls)

        query_mock.reset_mock()
        sys.argv = ['cadc-tap', 'query', '-q', '-s', 'http://someservice',
                    'QUERY']
        main_app()
        calls = [call('QUERY', None, 'tsv', None, data_only=True, timeout=2)]
        query_mock.assert_has_calls(calls)

        query_mock.reset_mock()
        sys.argv = ['cadc-tap', 'query', '-s', 'http://someservice',
                    '--timeout', '7', 'QUERY']
        main_app()
        calls = [call('QUERY', None, 'tsv', None, data_only=False, timeout=7)]
        query_mock.assert_has_calls(calls)

        query_mock.reset_mock()
        query_file = os.path.join(TESTDATA_DIR, 'example_query.sql')
        sys.argv = ['cadc-tap', 'query', '-i', query_file, '-s', 'tap']
        main_app()
        calls = [call('SELECT TOP 10 target_name FROM caom2.Observation', None,
                      'tsv', None, data_only=False, timeout=2)]
        query_mock.assert_has_calls(calls)
