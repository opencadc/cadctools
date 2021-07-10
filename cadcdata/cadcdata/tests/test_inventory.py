# # -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2021.                            (c) 2021.
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
import os
import sys
import shutil

from io import StringIO
from six.moves import xrange
from mock import Mock, patch, ANY, call
import pytest
import hashlib
import base64
import datetime
from requests.structures import CaseInsensitiveDict

from cadcutils.net import auth
from cadcutils import exceptions
from cadcutils.util import str2ivoa
from cadcdata import StorageInventoryClient, cadcget_cli, cadcput_cli,\
    cadcinfo_cli, cadcremove_cli
import cadcdata

# The following is a temporary workaround for Python issue
# 25532 (https://bugs.python.org/issue25532)
call.__wrapped__ = None

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')


class MyExitError(Exception):
    def __init__(self):
        self.message = "MyExitError"


mycontent = ''


@patch('cadcdata.storageinv.net.BaseWsClient')
@patch('cadcdata.storageinv.net.Transfer')
def test_get(trans_mock, basews_mock):
    # test a simple get
    file_name = '/tmp/afile.txt'
    file_chunks = ['aaaa'.encode(), 'bbbb'.encode(), ''.encode()]
    response = Mock()
    hash_md5 = hashlib.md5()
    for i in file_chunks:
        hash_md5.update(i)
    b64encoded = base64.b64encode(
        hash_md5.hexdigest().encode('ascii')).decode('ascii')
    response.headers = {'digest': 'md5={}'.format(b64encoded)}
    response.raw.read.side_effect = file_chunks  # returns multiple blocks
    get_mock = Mock(return_value=response)
    basews_mock.return_value.get = get_mock
    client = StorageInventoryClient(auth.Subject())
    trans_mock.return_value.transfer.return_value = []
    with pytest.raises(exceptions.HttpException):
        # no URLs returned in the transfer negotiations
        client.cadcget('cadc:TEST/afile', dest=file_name)
    trans_mock.return_value.transfer.return_value = ['url1', 'url2']
    client.cadcget('cadc:TEST/afile', dest=file_name)
    expected_content = \
        (''.join([c.decode() for c in file_chunks])).encode()
    with open(file_name, 'rb') as f:
        assert expected_content == f.read()
    os.remove(file_name)
    # do it again with the file now open
    response = Mock()
    response.headers = {
        'content-disposition': 'inline; filename=orig_file_name',
        'digest': 'md5={}'.format(b64encoded)}
    response.raw.read.side_effect = file_chunks
    basews_mock.return_value.get.return_value = response
    with open(file_name, 'wb') as f:
        client.cadcget('cadc:TEST/afile', dest=f)
    with open(file_name, 'rb') as f:
        assert expected_content == f.read()
    os.remove(file_name)

    # repeat test with a bad md5
    file_name = 'bfile.txt'
    file_content = 'ABCDEFGH12345'
    file_chunks = [file_content.encode(), ''.encode()]
    decoded_file_content = 'MNOPRST6789'
    decoded_file_chunks = [decoded_file_content.encode(), ''.encode()]
    response = Mock()
    wrong_b64encoded = base64.b64encode('abc'.encode('ascii')).decode('ascii')
    response.headers = {
        'digest': 'md5={}'.format(wrong_b64encoded),
        'content-disposition': 'inline; filename={}'.format(file_name)}
    response.raw.read.side_effect = file_chunks
    response.raw._decode.side_effect = decoded_file_chunks
    basews_mock.return_value.get.return_value = response
    client = StorageInventoryClient(auth.Subject())
    with pytest.raises(exceptions.HttpException):
        client.cadcget('cadc:TEST:bfile.txt', file_name)

    # transfer exceptions on all the URLs in the list
    get_mock.reset_mock()
    url_list = ['url1', 'url2']
    trans_mock.return_value.transfer.return_value = list(url_list)  # copy
    get_mock.side_effect = [exceptions.TransferException()] * len(url_list) * \
        cadcdata.storageinv.MAX_TRANSIENT_TRIES
    with pytest.raises(exceptions.HttpException):
        client.cadcget('cadc:TEST:bfile.txt', file_name)
    assert get_mock.call_count == \
           len(url_list) * cadcdata.storageinv.MAX_TRANSIENT_TRIES

    # transfer exception on one of the urls only (the second url is tried once
    # only
    get_mock.reset_mock()
    trans_mock.return_value.transfer.return_value = list(url_list)  # copy
    get_mock.side_effect = [exceptions.TransferException(),
                            exceptions.NotFoundException(),
                            exceptions.TransferException(),
                            exceptions.TransferException()]
    with pytest.raises(exceptions.HttpException):
        client.cadcget('cadc:TEST:bfile.txt', file_name)
    # 1 NotFoundError + 3 max number of retries on the remaining URL
    assert get_mock.call_count == 1 + cadcdata.storageinv.MAX_TRANSIENT_TRIES

    # 1 transfer + 2 non intermittent - transfer URL is retried, the other
    # two are not
    get_mock.reset_mock()
    trans_mock.return_value.transfer.return_value = list(url_list)  # copy
    get_mock.side_effect = [exceptions.TransferException(),
                            exceptions.NotFoundException(),
                            exceptions.NotFoundException()]
    with pytest.raises(exceptions.HttpException):
        client.cadcget('cadc:TEST:bfile.txt', file_name)
    # 1 Transfer + 2 NotFoundError
    assert get_mock.call_count == 3

    # test process_bytes and send the content to /dev/null after.
    def concatenate_chunks(chunk):
        global mycontent
        mycontent = '{}{}'.format(mycontent, chunk.decode())

    file_name = 'bfile.txt'
    file_content = 'ABCDEFGH12345'
    file_chunks = [file_content[i:i + 5].encode()
                   for i in xrange(0, len(file_content), 5)]
    hash_md5 = hashlib.md5()
    for i in file_chunks:
        hash_md5.update(i)
    b64encoded = base64.b64encode(
        hash_md5.hexdigest().encode('ascii')).decode('ascii')
    file_chunks.append('')  # last chunk is empty
    response = Mock()
    response.headers = {
        'content-disposition': 'inline; filename={}.gz'.format(file_name),
        'digest': 'md5={}'.format(b64encoded)}
    response.raw.read.side_effect = file_chunks
    get_mock.reset_mock()
    get_mock.side_effect = [response]
    trans_mock.return_value.transfer.return_value = list(url_list)
    client = StorageInventoryClient(auth.Subject())
    # md5_check does not take place because no content-MD5 received
    # from server
    client.cadcget('cadc:TEST/afile', dest='/dev/null',
                   process_bytes=concatenate_chunks)
    assert file_content == mycontent

    # failed md5 checksum
    response = Mock()
    response.headers = {
        'content-disposition': 'inline; filename={}.gz'.format(file_name),
        'digest': 'md5={}'.format(
            base64.b64encode('f00'.encode('ascii')).decode('ascii'))}
    response.raw.read.side_effect = file_chunks
    trans_mock.return_value.transfer.return_value = list(url_list)
    client = StorageInventoryClient(auth.Subject())
    # this is considered a TransferException so the client is going to
    # try MAX_TRANSIENT_TRIES for each url in the list
    get_mock.side_effect = [response] * len(url_list) * \
        cadcdata.storageinv.MAX_TRANSIENT_TRIES
    with pytest.raises(exceptions.HttpException):
        client.cadcget('cadc:TEST/afile', dest='/dev/null',
                       process_bytes=concatenate_chunks)

    # failed md5 checksum on the first URL, the good one on the second
    good_response = Mock()
    good_response.headers = {
        'content-disposition': 'inline; filename={}.gz'.format(file_name),
        'digest': 'md5={}'.format(b64encoded)}
    bad_response = Mock()
    bad_response.headers = {
        'content-disposition': 'inline; filename={}.gz'.format(file_name),
        'digest': 'md5={}'.format(
            base64.b64encode('f00'.encode('ascii')).decode('ascii'))}
    good_response.raw.read.side_effect = file_chunks
    trans_mock.return_value.transfer.return_value = list(url_list)
    client = StorageInventoryClient(auth.Subject())
    get_mock.side_effect = [bad_response, good_response]
    get_mock.reset_mock()
    client.cadcget('cadc:TEST/afile', dest='/dev/null',
                   process_bytes=concatenate_chunks)
    assert 2 == get_mock.call_count

    # file not found on any of the transfer URLs
    basews_mock.reset_mock()
    trans_mock.return_value.transfer.return_value = list(url_list)
    get_mock.side_effect = \
        [exceptions.NotFoundException(), exceptions.NotFoundException()]
    with pytest.raises(exceptions.HttpException):
        client.cadcget(id)

    # TODO test get fhead
    # response = Mock()
    # response.headers = {'digest': 'filename={}.gz'.format(file_name)}
    # response.raw.read.side_effect = file_chunks
    # response.history = []
    # response.status_code = 200
    # response.url = 'someurl'
    # post_mock = Mock(return_value=response)
    # basews_mock.return_value.post = post_mock
    # p.endpoint = 'http://someurl/transfer/{}/{}'.format(archive, file_name)
    # client.get_file('TEST', 'getfile', decompress=True, wcs=True,
    #                 md5_check=False)
    # trans_doc = \
    #     ('<vos:transfer xmlns:'
    #      'vos="http://www.ivoa.net/xml/VOSpace/v2.0">\n  '
    #      '<vos:target>ad:TEST/getfile</vos:target>\n  '
    #      '<vos:direction>pullFromVoSpace</vos:direction>\n  '
    #      '<vos:protocol uri="ivo://ivoa.net/vospace/core#httpget"/>\n'
    #      '  <vos:protocol uri="ivo://ivoa.net/vospace/core#httpsget"/>\n'
    #      '</vos:transfer>\n').encode()
    # post_mock.assert_called_with(resource=(TRANSFER_RESOURCE_ID, None),
    #                              params={'wcs': True}, data=trans_doc,
    #                              headers={'Content-Type': 'text/xml'})
    # response.raw.read.side_effect = file_chunks
    # post_mock.reset_mock()
    # client.get_file('TEST', 'getfile', decompress=True, fhead=True,
    #                 md5_check=False)
    # post_mock.assert_called_with(resource=(TRANSFER_RESOURCE_ID, None),
    #                              params={'fhead': True}, data=trans_doc,
    #                              headers={'Content-Type': 'text/xml'})
    # response.raw.read.side_effect = file_chunks
    # post_mock.reset_mock()
    # client.get_file('TEST', 'getfile', decompress=True, cutout='[1:1]',
    #                 md5_check=False)
    # post_mock.assert_called_with(resource=(TRANSFER_RESOURCE_ID, None),
    #                              params={'cutout': '[1:1]'},
    #                              data=trans_doc,
    #                              headers={'Content-Type': 'text/xml'})
    # response.raw.read.side_effect = file_chunks
    # post_mock.reset_mock()
    # client.get_file('TEST', 'getfile',
    #                 decompress=True, cutout='[[1:1], 2]',
    #                 md5_check=False)
    # post_mock.assert_called_with(resource=(TRANSFER_RESOURCE_ID, None),
    #                              params={'cutout': '[[1:1], 2]'},
    #                              data=trans_doc,
    #                              headers={'Content-Type': 'text/xml'})


@pytest.mark.skipif(cadcdata.storageinv.MAGIC_WARN is not None,
                    reason='libmagic not available')
@patch('cadcdata.core.net.BaseWsClient')
def test_put(basews_mock):
    client = StorageInventoryClient(auth.Subject())
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
        client.cadcput('cadc:TEST/putfile', file_name)
    client._cadc_client.subject.anon = False  # authenticate the user

    # new file
    client._get_transfer_urls = Mock(return_value=['url1'])
    client.cadcinfo = Mock(side_effect=exceptions.NotFoundException())
    client.cadcput('cadc:TEST/putfile', file_name)
    # Note Content* headers automatically created by cadcput except when
    # MAGIC_WANT -- libmagic not present
    put_mock.assert_called_with('url1', data=ANY,
                                headers={'Content-Type': 'text/plain',
                                         'Content-Encoding': 'us-ascii'})

    # mimic libmagic missing
    cadcdata.storageinv.MAGIC_WARN = 'Some warning'
    put_mock.reset_mock()
    client.cadcinfo = Mock(side_effect=exceptions.NotFoundException())
    client.cadcput('cadc:TEST/putfile', file_name)
    put_mock.assert_called_with(
        'url1',
        data=ANY,
        headers={})
    cadcdata.core.MAGIC_WARN = None

    # replace file
    client._get_transfer_urls = Mock(return_value=['url1'])
    client.cadcinfo = Mock(return_value={'id': 'cadc:TEST/putfile',
                                         'type': 'application/file',
                                         'encoding': 'none'})
    client.cadcput('cadc:TEST/putfile', file_name, replace=True,
                   file_type='text/plain', file_encoding='us-ascii')
    put_mock.assert_called_with('url1', data=ANY,
                                headers={'Content-Type': 'text/plain',
                                         'Content-Encoding': 'us-ascii'})

    # update metadata only
    put_mock.reset_mock()
    post_mock = Mock()
    basews_mock.return_value.post = post_mock
    client._get_transfer_urls = Mock(return_value=['url1'])
    client.cadcinfo = Mock(
        return_value=cadcdata.FileInfo('cadc:TEST/putfile',
                                       file_type='application/file',
                                       encoding='none', md5sum='0x123456789'))
    client.cadcput('cadc:TEST/putfile', file_name, replace=True,
                   file_type='text/plain', file_encoding='us-ascii',
                   md5_checksum='0x123456789')
    put_mock.assert_not_called()
    post_mock.assert_called_with('url1',
                                 headers={'Content-Type': 'text/plain',
                                          'Content-Encoding': 'us-ascii'})

    # no update required - data and metadata identical
    put_mock.reset_mock()
    post_mock.reset_mock()
    client._get_transfer_urls = Mock(return_value=['url1'])
    client.cadcinfo = Mock(
        return_value=cadcdata.FileInfo('cadc:TEST/putfile',
                                       file_type='text/plain',
                                       encoding='us-ascii',
                                       md5sum='0x123456789'))
    client.cadcput('cadc:TEST/putfile', file_name, replace=True,
                   file_type='text/plain', file_encoding='us-ascii',
                   md5_checksum='0x123456789')
    put_mock.assert_not_called()
    post_mock.assert_not_called()

    # replace non existing file
    client.cadcinfo.side_effect = [exceptions.NotFoundException()]
    with pytest.raises(AttributeError):
        client.cadcput('cadc:TEST/putfile', file_name, replace=True)

    # put a new file that already exists
    client.cadcinfo = Mock(
        return_value=cadcdata.FileInfo('cadc:TEST/putfile',
                                       file_type='text/plain',
                                       encoding='us-ascii',
                                       md5sum='0x123456789'))
    with pytest.raises(AttributeError):
        client.cadcput('cadc:TEST/putfile', file_name, replace=False)

    # Transfer error on all urls
    put_mock.reset_mock()
    basews_mock.return_value.post = post_mock
    url_list = ['url1', 'url2']
    client._get_transfer_urls = Mock(return_value=list(url_list))  # copy list
    put_mock.side_effect = [exceptions.TransferException()] * 2 * \
        cadcdata.storageinv.MAX_TRANSIENT_TRIES
    client.cadcinfo = Mock(side_effect=exceptions.NotFoundException())
    with pytest.raises(exceptions.HttpException):
        client.cadcput('cadc:TEST/putfile', file_name,
                       file_type='text/plain', file_encoding='us-ascii',
                       md5_checksum='0x1234567890')
    assert put_mock.call_count == \
           len(url_list) * cadcdata.storageinv.MAX_TRANSIENT_TRIES

    # Transfer error on one url, NotFound on the other
    put_mock.reset_mock()
    basews_mock.return_value.post = post_mock
    url_list = ['url1', 'url2']
    client._get_transfer_urls = Mock(return_value=list(url_list))  # copy list
    put_mock.side_effect = [exceptions.TransferException(),
                            exceptions.NotFoundException(),
                            exceptions.TransferException(),
                            exceptions.TransferException]
    client.cadcinfo = Mock(side_effect=exceptions.NotFoundException())
    with pytest.raises(exceptions.HttpException):
        client.cadcput('cadc:TEST/putfile', file_name,
                       file_type='text/plain', file_encoding='us-ascii',
                       md5_checksum='0x1234567890')
    assert put_mock.call_count == 1 + cadcdata.storageinv.MAX_TRANSIENT_TRIES

    # Transfer error on one url followed by 2 NotFound
    put_mock.reset_mock()
    basews_mock.return_value.post = post_mock
    url_list = ['url1', 'url2']
    client._get_transfer_urls = Mock(return_value=list(url_list))  # copy list
    put_mock.side_effect = [exceptions.TransferException(),
                            exceptions.NotFoundException(),
                            exceptions.NotFoundException()]
    client.cadcinfo = Mock(side_effect=exceptions.NotFoundException())
    with pytest.raises(exceptions.HttpException):
        client.cadcput('cadc:TEST/putfile', file_name,
                       file_type='text/plain', file_encoding='us-ascii',
                       md5_checksum='0x1234567890')
    assert put_mock.call_count == 3


@patch('cadcdata.core.net.BaseWsClient')
def test_remove(basews_mock):
    client = StorageInventoryClient(auth.Subject())
    # test a put
    remove_mock = Mock()
    basews_mock.return_value.delete = remove_mock
    with pytest.raises(exceptions.UnauthorizedException):
        client.cadcremove('cadc:TEST/removefile')
    client._cadc_client.subject.anon = False  # authenticate the user

    client._get_transfer_urls = Mock(return_value=['url1'])
    client.cadcremove('cadc:TEST/removefile')
    remove_mock.assert_called_with('url1')

    # file not found at the location
    basews_mock.return_value.delete.side_effect = \
        [exceptions.NotFoundException()]
    with pytest.raises(exceptions.HttpException):
        client.cadcremove(id)


@patch('cadcdata.storageinv.net.BaseWsClient')
def test_info(basews_mock):
    client = StorageInventoryClient(auth.Subject())
    # test an info
    file_name = 'myfile.txt'
    id = 'cadc:TEST/myfile.txt'
    size = '123'
    md5sum = '0x123'
    file_type = 'txt'
    file_encoding = 'gzip'
    lastmod = 'Mon, 17 May 2021 09:49:18 GMT'

    h = CaseInsensitiveDict()
    h['Content-Disposition'] = 'inline; filename={}'.format(file_name)
    h['Content-Length'] = size
    h['Digest'] = 'md5={}'.format(
        base64.b64encode(md5sum.encode('ascii')).decode('ascii'))
    h['Content-Type'] = file_type
    h['Content-Encoding'] = file_encoding
    h['Last-Modified'] = lastmod
    response = Mock()
    response.headers = h
    basews_mock.return_value.head.return_value = response
    info = client.cadcinfo(id)
    assert id == info.id
    assert file_name == info.name
    assert int(size) == info.size
    assert md5sum == info.md5sum
    assert file_type == info.file_type
    assert file_encoding == info.encoding
    assert datetime.datetime.strptime(
        lastmod, '%a, %d %b %Y %I:%M:%S %Z') == info.lastmod

    # file not found
    basews_mock.return_value.head.side_effect = \
        [exceptions.NotFoundException()]
    with pytest.raises(exceptions.NotFoundException):
        client.cadcinfo(id)


@patch('sys.exit', Mock(side_effect=[MyExitError, MyExitError, MyExitError,
                                     MyExitError, MyExitError,
                                     MyExitError]))
def test_help():
    """ Tests the helper displays for cadc* commands"""
    # help
    for cmd in ['cadcget', 'cadcput', 'cadcinfo', 'cadcremove']:
        print('Testing "{} --help"'.format(cmd))
        usage = open(
            os.path.join(TESTDATA_DIR, '{}_help.txt'.format(cmd)), 'r').read()

        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            sys.argv = [cmd, '--help']
            with pytest.raises(MyExitError):
                getattr(cadcdata, '{}_cli'.format(cmd))()
        assert usage == stdout_mock.getvalue()


@patch('sys.exit', Mock(side_effect=[MyExitError, MyExitError]))
@patch('cadcdata.StorageInventoryClient.cadcget')
def test_cadcget_cli(cadcget_mock):
    sys.argv = ['cadcget', 'cadc:TEST/file']
    cadcget_cli()
    calls = [call(id='cadc:TEST/file', dest=None)]
    cadcget_mock.assert_has_calls(calls)

    # test with file names
    cadcget_mock.reset_mock()
    sys.argv = ['cadcget', 'cadc:TEST/file', '-o', 'file.txt']
    cadcget_cli()
    calls = [call(id='cadc:TEST/file', dest='file.txt')]
    cadcget_mock.assert_has_calls(calls)


@patch('sys.exit', Mock(side_effect=[MyExitError]))
@patch('cadcdata.StorageInventoryClient.cadcinfo')
def test_cadcinfo_cli(cadcinfo_mock):
    cadcinfo_mock.return_value = \
        cadcdata.FileInfo('cadc:TEST/file1.txt.gz', name='file1.txt.gz',
                          size=5, md5sum='0x33', file_type='text',
                          lastmod=str2ivoa('2001-11-11T10:00:00.000'),
                          encoding='gzip')

    sys.argv = ['cadcinfo', 'cadc:TEST/file1.txt.gz']
    with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
        cadcinfo_cli()
    expected = ('CADC Storage Inventory identifier cadc:TEST/file1.txt.gz:\n'
                '\t            name: file1.txt.gz\n'
                '\t            size: 5\n'
                '\t            type: text\n'
                '\t        encoding: gzip\n'
                '\t   last modified: 2001-11-11T10:00:00.000\n'
                '\t          md5sum: 0x33\n')
    assert expected == stdout_mock.getvalue()

    # multiple files
    cadcinfo_mock.reset_mock()
    cadcinfo_mock.side_effect = [
        cadcdata.FileInfo('cadc:TEST/file1.txt.gz', name='file1.txt.gz',
                          size=5, md5sum='0x33', file_type='text',
                          lastmod=str2ivoa('2021-11-11T10:00:00.000'),
                          encoding='gzip'),
        cadcdata.FileInfo('cadc:TEST/file2.txt', name='file2.txt',
                          size='5000', md5sum='0x123456', file_type='text',
                          lastmod=str2ivoa('2021-12-22T10:00:00.000'))]

    sys.argv = ['cadcinfo', 'cadc:TEST/file1.txt.gz', 'cadc:TEST/file2.txt']
    with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
        cadcinfo_cli()
    expected = ('CADC Storage Inventory identifier cadc:TEST/file1.txt.gz:\n'
                '\t            name: file1.txt.gz\n'
                '\t            size: 5\n'
                '\t            type: text\n'
                '\t        encoding: gzip\n'
                '\t   last modified: 2021-11-11T10:00:00.000\n'
                '\t          md5sum: 0x33\n'
                'CADC Storage Inventory identifier cadc:TEST/file2.txt:\n'
                '\t            name: file2.txt\n'
                '\t            size: 5000\n'
                '\t            type: text\n'
                '\t        encoding: None\n'
                '\t   last modified: 2021-12-22T10:00:00.000\n'
                '\t          md5sum: 0x123456\n')
    assert expected == stdout_mock.getvalue()


@patch('sys.exit', Mock(side_effect=[MyExitError, MyExitError, MyExitError]))
@patch('cadcdata.storageinv._create_client')
def test_cadcput_cli(putclient_mock):
    # mock client to escape authentication
    mock_client = StorageInventoryClient()
    cadcput_mock = Mock()
    mock_client.cadcput = cadcput_mock
    putclient_mock.return_value = mock_client

    # create a file structure of files to put
    put_dir = '/tmp/put_dir'
    put_subdir = '{}/somedir'.format(put_dir)
    file1 = 'file1'
    file2 = 'file2'
    file3 = 'file3'
    netrc = '/tmp/.cadcput_netrc'
    if os.path.exists(put_dir):
        shutil.rmtree(put_dir)
    if not os.path.isfile(netrc):
        open(netrc, 'w').write('')
    os.makedirs(put_dir)
    os.makedirs(put_subdir)
    file1_path = os.path.join(put_dir, '{}.txt'.format(file1))
    file2_path = os.path.join(put_dir, '{}.txt'.format(file2))
    file3_path = os.path.join(put_subdir, '{}.txt'.format(file3))
    open(file1_path, 'w').write('TEST FILE1')
    open(file2_path, 'w').write('TEST FILE2')
    open(file3_path, 'w').write('TEST FILE3')

    # replace one file
    sys.argv = ['cadcput', '-r', '--netrc-file', netrc,
                'cadc:TEST/{}'.format(file1), file1_path]
    with patch('sys.stdout', new_callable=StringIO):
        cadcput_cli()
    calls = [call(id='cadc:TEST/file1', src=file1_path,
                  file_type=None, file_encoding=None, replace=True)]
    cadcput_mock.assert_has_calls(calls, any_order=True)

    # put multiple files in directory
    cadcput_mock.reset_mock()
    sys.argv = ['cadcput', '--netrc-file', netrc, '--type', 'application/text',
                '--encoding', 'encoded', 'cadc:TEST/', put_dir]
    with patch('sys.stdout', new_callable=StringIO):
        cadcput_cli()
    # file3 is in subdirectory and not part of the list
    calls = [call(id='cadc:TEST/file2.txt', src=file2_path, replace=False,
                  file_type='application/text', file_encoding='encoded'),
             call(id='cadc:TEST/file1.txt', src=file1_path, replace=False,
                  file_type='application/text', file_encoding='encoded')]
    cadcput_mock.assert_has_calls(calls, any_order=True)

    # multiple files in directory and explicitly
    cadcput_mock.reset_mock()
    sys.argv = ['cadcput', '--netrc-file', netrc, 'cadc:TEST/',
                put_dir, file3_path]
    with patch('sys.stdout', new_callable=StringIO):
        cadcput_cli()
    calls = [call(id='cadc:TEST/file2.txt', src=file2_path, replace=False,
                  file_type=None, file_encoding=None),
             call(id='cadc:TEST/file3.txt', src=file3_path,  replace=False,
                  file_type=None, file_encoding=None),
             call(id='cadc:TEST/file1.txt', src=file1_path,  replace=False,
                  file_type=None, file_encoding=None)]
    cadcput_mock.assert_has_calls(calls, any_order=True)
    # cleanup
    shutil.rmtree(put_dir)

    # credentials are always required with cadcput
    with patch('sys.stderr', new_callable=StringIO) as stderr_mock:
        sys.argv = ['cadcput', 'cadc:TEST/file']
        with pytest.raises(MyExitError):
            cadcremove_cli()
        assert 'cadcput: error: one of the arguments --cert -n ' \
               '--netrc-file -u/--user is' in stderr_mock.getvalue()


@patch('sys.exit', Mock(side_effect=[MyExitError, MyExitError]))
@patch('cadcdata.storageinv._create_client')
def test_cadcremove_cli(removeclient_mock):
    # mock client to escape authentication
    mock_client = StorageInventoryClient()
    cadcremove_mock = Mock()
    mock_client.cadcremove = cadcremove_mock
    removeclient_mock.return_value = mock_client
    netrc = '/tmp/.cadcput_netrc'
    if not os.path.isfile(netrc):
        open(netrc, 'w').write('')
    sys.argv = ['cadcremove', '--netrc-file', netrc, 'cadc:TEST/file']
    cadcremove_cli()
    calls = [call(id='cadc:TEST/file')]
    cadcremove_mock.assert_has_calls(calls)

    # credentials are always required with cadcremove
    with patch('sys.stderr', new_callable=StringIO) as stderr_mock:
        sys.argv = ['cadcremove', 'cadc:TEST/file']
        with pytest.raises(MyExitError):
            cadcremove_cli()
        assert 'cadcremove: error: one of the arguments --cert -n ' \
               '--netrc-file -u/--user is' in stderr_mock.getvalue()
