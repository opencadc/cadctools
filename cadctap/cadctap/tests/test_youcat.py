# # -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2018.                            (c) 2018.
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

from cadcutils import net
from cadctap import youcat
from mock import Mock, patch, call
import pytest

# The following is a temporary workaround for Python issue
# 25532 (https://bugs.python.org/issue25532)
call.__wrapped__ = None

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')


@patch('cadctap.youcat.net.BaseWsClient.put')
def test_create_table(base_put_mock):
    client = youcat.YoucatClient(net.Subject())
    # default format
    def_table = os.path.join(TESTDATA_DIR, 'createTable.vosi')
    def_table_content = open(def_table, 'rb').read()
    client.create_table('sometable', def_table)
    base_put_mock.assert_called_with(
        (youcat.TABLES_CAPABILITY_ID, 'sometable'), data=def_table_content,
        headers={'Content-Type': '{}'.format(
            youcat.ALLOWED_TB_DEF_TYPES['VOSITable'])})

    # FITSTable format
    base_put_mock.reset_mock()
    client.create_table('sometable', def_table, 'FITSTable')
    base_put_mock.assert_called_with(
        (youcat.TABLES_CAPABILITY_ID, 'sometable'), data=def_table_content,
        headers={'Content-Type': '{}'.format(
            youcat.ALLOWED_TB_DEF_TYPES['FITSTable'])})

    # VOTable format
    base_put_mock.reset_mock()
    client.create_table('sometable', def_table, 'VOTable')
    base_put_mock.assert_called_with(
        (youcat.TABLES_CAPABILITY_ID, 'sometable'), data=def_table_content,
        headers={'Content-Type': '{}'.format(
            youcat.ALLOWED_TB_DEF_TYPES['VOTable'])})

    # error cases
    with pytest.raises(AttributeError):
        client.create_table(None, def_table)
    with pytest.raises(AttributeError):
        client.create_table('sometable', None)


@patch('cadctap.youcat.net.BaseWsClient.delete')
def test_delete_table(base_delete_mock):
    client = youcat.YoucatClient(net.Subject())
    client.delete_table('sometable')
    base_delete_mock.assert_called_with((youcat.TABLES_CAPABILITY_ID,
                                        'sometable'))

    # error case
    with pytest.raises(AttributeError):
        client.delete(None)


@patch('cadctap.youcat.net.BaseWsClient.post')
def test_load_table(base_put_mock):
    client = youcat.YoucatClient(net.Subject())
    test_load_tb = os.path.join(TESTDATA_DIR, 'loadTable.txt')

    # default format (tsv)
    with open(test_load_tb, 'rb') as fh:
        with patch('cadctap.youcat.open') as open_mock:
            open_mock.return_value = fh
            client.load('schema.sometable', [test_load_tb])
    base_put_mock.assert_called_with(
        (youcat.TABLES_CAPABILITY_ID, 'schema.sometable'), data=fh,
        headers={'Content-Type': str(youcat.ALLOWED_CONTENT_TYPES['tsv'])})

    # tsv format
    with open(test_load_tb, 'rb') as fh:
        with patch('cadctap.youcat.open') as open_mock:
            open_mock.return_value = fh
            client.load('schema.sometable', [test_load_tb], fformat='tsv')
    base_put_mock.assert_called_with(
        (youcat.TABLES_CAPABILITY_ID, 'schema.sometable'), data=fh,
        headers={'Content-Type': str(youcat.ALLOWED_CONTENT_TYPES['tsv'])})

    # csv format
    with open(test_load_tb, 'rb') as fh:
        with patch('cadctap.youcat.open') as open_mock:
            open_mock.return_value = fh
            client.load('schema.sometable', [test_load_tb], fformat='csv')
    base_put_mock.assert_called_with(
        (youcat.TABLES_CAPABILITY_ID, 'schema.sometable'), data=fh,
        headers={'Content-Type': str(youcat.ALLOWED_CONTENT_TYPES['csv'])})

    # error cases
    with pytest.raises(AttributeError):
        client.load(None, [test_load_table])
    with pytest.raises(AttributeError):
        client.load('sometable', None)
    with pytest.raises(AttributeError):
        client.load('sometable', [])


@patch('cadctap.youcat.net.BaseWsClient.post')
@patch('cadctap.youcat.net.BaseWsClient.get')
def test_create_index(base_get_mock, base_post_mock):
    client = youcat.YoucatClient(net.Subject())
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
    post_calls = [call((youcat.TABLE_UPDATE_CAPABILITY_ID, None),
                  allow_redirects=False,
                  data={'table': 'schema.sometable', 'uniquer': True,
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
    base_get_mock.side_effect = [response4]
    client = youcat.YoucatClient(net.Subject())
    with pytest.raises(RuntimeError):
        client.create_index('sometable', 'col1')

    response5 = Mock()
    response5.status_code = 500
    base_get_mock.side_effect = [response1, response4]
    client = youcat.YoucatClient(net.Subject())
    with pytest.raises(RuntimeError):
        client.create_index('sometable', 'col1')


@patch('cadctap.youcat.net.BaseWsClient.get')
def test_schema(base_get_mock):
    client = youcat.YoucatClient(net.Subject())
    # default format
    client.schema()
    base_get_mock.assert_called_with(
        (youcat.TABLES_CAPABILITY_ID, None))


@patch('cadctap.youcat.net.BaseWsClient.post')
def test_query(base_post_mock):
    client = youcat.YoucatClient(net.Subject())
    # default format
    def_name = 'tmptable'
    def_table = os.path.join(TESTDATA_DIR, 'votable.xml')

    fields = {'LANG': 'ADQL',
              'QUERY': 'query',
              'FORMAT': 'VOTable'}
    tablefile = os.path.basename(def_table)
    fields['UPLOAD'] = '{},param:{}'.format(def_name, tablefile)
    fields[tablefile] = (def_table, open(def_table, 'rb'))
    client.query('query', tmptable='tmptable:'+def_table)
    print(base_post_mock.call_args_list[0][0][0])
    assert base_post_mock.call_args_list[0][0][0] == \
        (youcat.QUERY_CAPABILITY_ID, None)
