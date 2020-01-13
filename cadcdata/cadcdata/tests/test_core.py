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
import logging
import shutil

from six import StringIO
from six.moves import xrange
from cadcutils.net import auth
from cadcutils import exceptions
from cadcdata import transfer
from cadcdata import CadcDataClient
from cadcdata.core import main_app, TRANSFER_RESOURCE_ID
import cadcdata
from mock import Mock, patch, ANY, call
import pytest
import hashlib

# The following is a temporary workaround for Python issue
# 25532 (https://bugs.python.org/issue25532)
call.__wrapped__ = None

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')


class MyExitError(Exception):
    def __init__(self):
        self.message = "MyExitError"


mycontent = ''


@patch('cadcdata.core.net.BaseWsClient')
@patch('cadcdata.core.TransferReader')
def test_get_file(trans_reader_mock, basews_mock):
    # test a simple get - no decompress
    file_name = '/tmp/afile.txt'
    file_chunks = ['aaaa'.encode(), 'bbbb'.encode(), ''.encode()]
    response = Mock()
    hash_md5 = hashlib.md5()
    for i in file_chunks:
        hash_md5.update(i)
    response.headers.get.return_value = \
        'filename={}'.format('orig_file_name')
    response.raw.read.side_effect = file_chunks  # returns multiple blocks
    basews_mock.return_value.get.return_value = response
    client = CadcDataClient(auth.Subject())
    with pytest.raises(exceptions.HttpException):
        # no URLs returned in the transfer negotiations
        client.get_file('TEST', 'afile', destination=file_name)
    t = transfer.Transfer('ad:TEST/afile', 'pullFromVoSpace')
    p = transfer.Protocol
    p.endpoint = Mock()
    t.protocols = [p]
    trans_reader_mock.return_value.read.return_value = t
    client.get_file('TEST', 'afile', destination=file_name,
                    md5_check=False)
    expected_content = \
        (''.join([c.decode() for c in file_chunks])).encode()
    with open(file_name, 'rb') as f:
        assert expected_content == f.read()
    os.remove(file_name)
    # do it again with the file now open
    response = Mock()
    response.headers = {'filename': 'orig_file_name',
                        'content-MD5': hash_md5.hexdigest()}
    response.raw.read.side_effect = file_chunks
    basews_mock.return_value.get.return_value = response
    with open(file_name, 'wb') as f:
        client.get_file('TEST', 'afile', destination=f)
    with open(file_name, 'rb') as f:
        assert expected_content == f.read()
    os.remove(file_name)

    # test a get with decompress and md5 check enabled
    file_name = 'bfile.txt'
    file_content = 'aaaabbbb'
    hash_md5 = hashlib.md5()
    hash_md5.update(file_content.encode())
    file_chunks = [file_content.encode(), ''.encode()]
    decoded_file_content = 'MNOPRST6789'
    decoded_file_chunks = [decoded_file_content.encode(), ''.encode()]
    response = Mock()
    response.headers = \
        {'content-MD5': '{}'.format(hash_md5.hexdigest()),
         'filename': file_name}
    response.raw.read.side_effect = file_chunks
    response.raw._decode.side_effect = decoded_file_chunks
    basews_mock.return_value.get.return_value = response
    client = CadcDataClient(auth.Subject())
    client.get_file('TEST', file_name=file_name, decompress=True,
                    md5_check=True)
    with open(file_name, 'r') as f:
        # note the check against the decoded content
        assert decoded_file_content == f.read()
    os.remove(file_name)

    # repeat test with a bad md5
    file_name = 'bfile.txt'
    file_content = 'ABCDEFGH12345'
    file_chunks = [file_content.encode(), ''.encode()]
    decoded_file_content = 'MNOPRST6789'
    decoded_file_chunks = [decoded_file_content.encode(), ''.encode()]
    response = Mock()
    response.headers = {'content-MD5': 'abc', 'filename': file_name}
    response.raw.read.side_effect = file_chunks
    response.raw._decode.side_effect = decoded_file_chunks
    basews_mock.return_value.get.return_value = response
    client = CadcDataClient(auth.Subject())
    with pytest.raises(exceptions.HttpException):
        client.get_file('TEST', file_name=file_name, decompress=True,
                        md5_check=True)

    # test process_bytes and send the content to /dev/null after.
    # Use no decompress
    def concatenate_chunks(chunk):
        global mycontent
        mycontent = '{}{}'.format(mycontent, chunk.decode())

    file_name = 'bfile.txt'
    file_content = 'ABCDEFGH12345'
    file_chunks = [file_content[i:i + 5].encode()
                   for i in xrange(0, len(file_content), 5)]
    file_chunks.append('')  # last chunk is empty
    response = Mock()
    response.headers = {'filename': '{}.gz'.format(file_name)}
    response.raw.read.side_effect = file_chunks
    basews_mock.return_value.get.return_value = response
    client = CadcDataClient(auth.Subject())
    client.logger.setLevel(logging.INFO)
    # md5_check does not take place because no content-MD5 received
    # from server
    client.get_file('TEST', 'afile', destination='/dev/null',
                    process_bytes=concatenate_chunks)
    assert file_content == mycontent

    # failed md5 checksum
    response = Mock()
    response.headers = {'filename': '{}.gz'.format(file_name),
                        'content-MD5': '33'}
    response.raw.read.side_effect = file_chunks
    basews_mock.return_value.get.return_value = response
    client = CadcDataClient(auth.Subject())
    client.logger.setLevel(logging.INFO)
    # md5_check does not take place because no content-MD5 received
    # from server
    with pytest.raises(exceptions.HttpException):
        client.get_file('TEST', 'afile', destination='/dev/null',
                        process_bytes=concatenate_chunks)

    # test get fhead
    response = Mock()
    response.headers.get.return_value = 'filename={}.gz'.format(file_name)
    response.raw.read.side_effect = file_chunks
    response.history = []
    response.status_code = 200
    response.url = 'someurl'
    post_mock = Mock(return_value=response)
    basews_mock.return_value.post = post_mock
    file_name = 'getfile'
    archive = 'TEST'
    p.endpoint = 'http://someurl/transfer/{}/{}'.format(archive, file_name)
    client.get_file('TEST', 'getfile', decompress=True, wcs=True,
                    md5_check=False)
    trans_doc = \
        ('<vos:transfer xmlns:'
         'vos="http://www.ivoa.net/xml/VOSpace/v2.0">\n  '
         '<vos:target>ad:TEST/getfile</vos:target>\n  '
         '<vos:direction>pullFromVoSpace</vos:direction>\n  '
         '<vos:protocol uri="ivo://ivoa.net/vospace/core#httpget"/>\n'
         '  <vos:protocol uri="ivo://ivoa.net/vospace/core#httpsget"/>\n'
         '</vos:transfer>\n').encode()
    post_mock.assert_called_with(resource=(TRANSFER_RESOURCE_ID, None),
                                 params={'wcs': True}, data=trans_doc,
                                 headers={'Content-Type': 'text/xml'})
    response.raw.read.side_effect = file_chunks
    post_mock.reset_mock()
    client.get_file('TEST', 'getfile', decompress=True, fhead=True,
                    md5_check=False)
    post_mock.assert_called_with(resource=(TRANSFER_RESOURCE_ID, None),
                                 params={'fhead': True}, data=trans_doc,
                                 headers={'Content-Type': 'text/xml'})
    response.raw.read.side_effect = file_chunks
    post_mock.reset_mock()
    client.get_file('TEST', 'getfile', decompress=True, cutout='[1:1]',
                    md5_check=False)
    post_mock.assert_called_with(resource=(TRANSFER_RESOURCE_ID, None),
                                 params={'cutout': '[1:1]'},
                                 data=trans_doc,
                                 headers={'Content-Type': 'text/xml'})
    response.raw.read.side_effect = file_chunks
    post_mock.reset_mock()
    client.get_file('TEST', 'getfile',
                    decompress=True, cutout='[[1:1], 2]',
                    md5_check=False)
    post_mock.assert_called_with(resource=(TRANSFER_RESOURCE_ID, None),
                                 params={'cutout': '[[1:1], 2]'},
                                 data=trans_doc,
                                 headers={'Content-Type': 'text/xml'})


@pytest.mark.skipif(cadcdata.core.MAGIC_WARN is not None,
                    reason='libmagic not available')
@patch('cadcdata.core.net.BaseWsClient')
def test_put_file(basews_mock):
    client = CadcDataClient(auth.Subject())
    # test a put
    file_name = '/tmp/putfile.txt'
    file_content = 'ABCDEFGH12345'
    hash_md5 = hashlib.md5()
    hash_md5.update(file_content.encode())
    hash_md5 = hash_md5.hexdigest()
    # write the file
    with open(file_name, 'w') as f:
        f.write(file_content)
    put_mock = Mock()
    basews_mock.return_value.put = put_mock
    with pytest.raises(exceptions.UnauthorizedException):
        client.put_file('TEST', 'putfile', file_name)
    client._data_client.subject.anon = False  # authenticate the user
    transf_end_point = 'http://test.ca/endpoint'

    def mock_get_trans_protocols(archive, file_name, is_get, headers):
        protocol = Mock()
        protocol.endpoint = '{}/{}'.format(transf_end_point, file_name)
        return [protocol]

    client._get_transfer_protocols = mock_get_trans_protocols
    client.put_file('TEST', file_name)
    # Note Content* headers automatically created by cadc-data except when
    # MAGIC_WANT -- libmagic not present
    put_mock.assert_called_with(
        '{}/{}'.format(transf_end_point, os.path.basename(file_name)),
        data=ANY,
        headers={'Content-Type': 'text/plain',
                 'Content-Encoding': 'us-ascii',
                 'Content-MD5': '{}'.format(hash_md5)})

    # mimic libmagic missing
    cadcdata.core.MAGIC_WARN = 'Some warning'
    put_mock.reset_mock()
    client.put_file('TEST', file_name)
    put_mock.assert_called_with(
        '{}/{}'.format(transf_end_point, os.path.basename(file_name)),
        data=ANY,
        headers={'Content-MD5': '835e7e6cd54e18ae21d50af963b0c32b'})
    cadcdata.core.MAGIC_WARN = None

    # specify an archive stream and override the name of the file
    input_name = 'abc'
    client.put_file('TEST', file_name, archive_stream='default',
                    input_name=input_name)
    put_mock.assert_called_with(
        '{}/{}'.format(transf_end_point, input_name), data=ANY,
        headers={'Content-Encoding': 'us-ascii',
                 'X-CADC-Stream': 'default', 'Content-Type': 'text/plain',
                 'Content-MD5': '{}'.format(hash_md5)})
    # specify the mime types
    client.put_file('TEST', file_name, archive_stream='default',
                    mime_type='ASCII', mime_encoding='GZIP')
    put_mock.assert_called_with(
        '{}/{}'.format(transf_end_point, os.path.basename(file_name)),
        data=ANY,
        headers={'Content-Encoding': 'GZIP',
                 'X-CADC-Stream': 'default', 'Content-Type': 'ASCII',
                 'Content-MD5': '{}'.format(hash_md5)})
    os.remove(file_name)


@patch('cadcdata.core.net.BaseWsClient')
def test_info_file(basews_mock):
    client = CadcDataClient(auth.Subject())
    # test an info
    file_name = 'myfile'
    file_name = 'myfile.txt'
    archive = 'TEST'
    size = '123'
    md5sum = '0x123'
    type = 'txt'
    encoding = 'gzip'
    lastmod = '11/11/11T11:11:11.000'
    usize = '1234'
    umd5sum = '0x1234'

    h = {}
    h['Content-Disposition'] = 'inline; filename={}'.format(file_name)
    h['Content-Length'] = size
    h['Content-MD5'] = md5sum
    h['Content-Type'] = type
    h['Content-Encoding'] = encoding
    h['Last-Modified'] = lastmod
    h['X-Uncompressed-Length'] = usize
    h['X-Uncompressed-MD5'] = umd5sum
    response = Mock()
    response.headers = h
    basews_mock.return_value.head.return_value = response
    info = client.get_file_info('TEST', 'myfile')
    assert archive == info['archive']
    assert file_name == info['name']
    assert size == info['size']
    assert md5sum == info['md5sum']
    assert type == info['type']
    assert encoding == info['encoding']
    assert lastmod == info['lastmod']
    assert usize == info['usize']
    assert umd5sum == info['umd5sum']


@patch('sys.exit', Mock(side_effect=[MyExitError, MyExitError, MyExitError,
                                     MyExitError, MyExitError,
                                     MyExitError]))
def test_help():
    """ Tests the helper displays for commands and subcommands in main"""
    # help
    with open(os.path.join(TESTDATA_DIR, 'help.txt'), 'r') as myfile:
        usage = myfile.read()

    with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
        sys.argv = ['cadc-data', '--help']
        with pytest.raises(MyExitError):
            main_app()
        assert usage == stdout_mock.getvalue()

    usage = ('usage: cadc-data [-h] [-V] {get,put,info} ...\n'
             'cadc-data: error: too few arguments\n')

    with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
        with patch('sys.stderr', new_callable=StringIO) as stderr_mock:
            sys.argv = ['cadc-data']
            with pytest.raises(MyExitError):
                main_app()
            assert usage == stderr_mock.getvalue()

    # get -h
    with open(os.path.join(TESTDATA_DIR, 'help_get.txt'), 'r') as myfile:
        usage = myfile.read()

    with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
        sys.argv = ['cadc-data', 'get', '--help']
        with pytest.raises(MyExitError):
            main_app()
        assert usage == stdout_mock.getvalue()

    # put -h
    with open(os.path.join(TESTDATA_DIR, 'help_put.txt'), 'r') as myfile:
        usage = myfile.read()

    with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
        sys.argv = ['cadc-data', 'put', '-h']
        with pytest.raises(MyExitError):
            main_app()
        assert usage == stdout_mock.getvalue()

    # info -h
    with open(os.path.join(TESTDATA_DIR, 'help_info.txt'), 'r') as myfile:
        usage = myfile.read()

    with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
        sys.argv = ['cadc-data', 'info', '--help']
        with pytest.raises(MyExitError):
            main_app()
        assert usage == stdout_mock.getvalue()


@patch('sys.exit', Mock(side_effect=[MyExitError, MyExitError, MyExitError,
                                     MyExitError, MyExitError,
                                     MyExitError]))
@patch('cadcdata.core.CadcDataClient.put_file')
@patch('cadcdata.core.CadcDataClient.get_file_info')
@patch('cadcdata.core.CadcDataClient.get_file')
def test_main(get_mock, info_mock, put_mock):
    sys.argv = ['cadc-data', 'get', 'TEST', 'fileid1', 'fileid2',
                'fileid3']
    main_app()
    calls = [call('TEST', 'fileid1', None, cutout=None, decompress=False,
                  fhead=False, wcs=False, md5_check=True),
             call('TEST', 'fileid2', None, cutout=None, decompress=False,
                  fhead=False, wcs=False, md5_check=True),
             call('TEST', 'fileid3', None, cutout=None, decompress=False,
                  fhead=False, wcs=False, md5_check=True)]
    get_mock.assert_has_calls(calls)

    # test with file names
    get_mock.reset_mock()
    sys.argv = ['cadc-data', 'get', 'TEST', '-o', 'file1.txt file2.txt',
                '--nomd5', 'fileid1', 'fileid2']
    main_app()
    calls = [call('TEST', 'fileid1', 'file1.txt', cutout=None,
                  decompress=False, fhead=False, wcs=False,
                  md5_check=False),
             call('TEST', 'fileid2', 'file2.txt', cutout=None,
                  decompress=False, fhead=False, wcs=False,
                  md5_check=False)]
    get_mock.assert_has_calls(calls)

    # number of file names does not match the number of file ids.
    # logger displays an error
    get_mock.reset_mock()
    sys.argv = ['cadc-data', 'get', 'TEST', '-o', 'file1.txt', 'fileid1',
                'fileid2']
    b = StringIO()
    logger = logging.getLogger('cadc-data')
    # capture the log message in a StreamHandler
    logger.addHandler(logging.StreamHandler(b))
    with pytest.raises(MyExitError):
        main_app()
    assert 'Different size of destination files list' in b.getvalue()

    # test info
    info_mock.return_value = {'archive': 'TEST', 'name': 'file1.txt.gz',
                              'size': '5', 'md5sum': '0x33',
                              'type': 'text',
                              'encoding': 'gzip',
                              'lastmod': '10/10/10T10:10:10.000',
                              'usize': '50', 'umd5sum': '0x234'}
    sys.argv = ['cadc-data', 'info', 'TEST', 'file1']
    with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
        main_app()
    expected = ('File file1:\n'
                '\t    archive: TEST\n'
                '\t   encoding: gzip\n'
                '\t    lastmod: 10/10/10T10:10:10.000\n'
                '\t     md5sum: 0x33\n'
                '\t       name: file1.txt.gz\n'
                '\t       size: 5\n'
                '\t       type: text\n'
                '\t    umd5sum: 0x234\n'
                '\t      usize: 50\n')

    assert expected == stdout_mock.getvalue()

    # test put directory
    # create a file structure of files to put
    put_dir = '/tmp/put_dir'
    put_subdir = '{}/somedir'.format(put_dir)
    file1 = 'file1'
    file2 = 'file2'
    if os.path.exists(put_dir):
        shutil.rmtree(put_dir)
    os.makedirs(put_dir)
    os.makedirs(put_subdir)
    with open(os.path.join(put_dir, '{}.txt'.format(file1)), 'w') as f:
        f.write('TEST FILE1')
    with open(os.path.join(put_dir, '{}.txt'.format(file2)), 'w') as f:
        f.write('TEST FILE2')
    # extra file that is not going to be put because it resides in
    # a subdirectory
    with open(os.path.join(put_subdir, 'file3.txt'), 'w') as f:
        f.write('TEST FILE3')

    sys.argv = ['cadc-data', 'put', 'TEST', put_dir]
    with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
        main_app()
    calls = [call('TEST', '/tmp/put_dir/file2.txt', archive_stream=None,
                  mime_type=None, mime_encoding=None, md5_check=True),
             call('TEST', '/tmp/put_dir/file1.txt', archive_stream=None,
                  mime_type=None, mime_encoding=None, md5_check=True)]
    # put with rename files
    put_mock.assert_has_calls(calls, any_order=True)
    put_mock.reset_mock()
    sys.argv = ['cadc-data', 'put', '-i', 'a.txt b.txt', 'TEST', put_dir]
    with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
        main_app()
    calls = [call('TEST', '/tmp/put_dir/file1.txt', archive_stream=None,
                  mime_type=None, mime_encoding=None, md5_check=True,
                  input_name='a.txt'),
             call('TEST', '/tmp/put_dir/file2.txt', archive_stream=None,
                  mime_type=None, mime_encoding=None, md5_check=True,
                  input_name='b.txt')]
    put_mock.assert_has_calls(calls, any_order=True)

    # put with file rename

    # number of file names does not match the number of file names.
    # logger displays an error
    get_mock.reset_mock()
    sys.argv = ['cadc-data', 'get', 'TEST', '-o', 'file1.txt', 'file1',
                'file2']
    b = StringIO()
    logger = logging.getLogger('cadc-data')
    # capture the log message in a StreamHandler
    logger.addHandler(logging.StreamHandler(b))
    with pytest.raises(MyExitError):
        main_app()
    assert 'Different size of destination files list' in b.getvalue()

    # cleanup
    shutil.rmtree(put_dir)

    # mistmatched number of input names and source files
    b = StringIO()
    sys.argv = ['cadc-data', 'put', '-i', 'A', 'TEST', TESTDATA_DIR]
    logger.addHandler(logging.StreamHandler(b))
    with pytest.raises(MyExitError):
        main_app()
    assert \
        'The number of input names does not match the number of sources: ' \
        '1 vs ' in b.getvalue()
