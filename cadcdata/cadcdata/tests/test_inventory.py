# # -*- coding: utf-8 -*-
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
#  $Revision: 4 $
#
# ***********************************************************************
#
import os
import sys
import shutil

from io import StringIO
from mock import Mock, patch, call
import pytest
import hashlib
import base64
import datetime
from requests.structures import CaseInsensitiveDict
import argparse

from cadcutils.net import auth
from cadcutils import exceptions
from cadcutils.util import str2ivoa
from cadcdata import StorageInventoryClient, cadcget_cli, cadcput_cli,\
    cadcinfo_cli, cadcremove_cli
from cadcdata import storageinv
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


def test_get():
    client = StorageInventoryClient(auth.Subject())
    client._get_transfer_urls = Mock(
        return_value=['https://url1', 'https://url2'])
    download_file_mock = Mock()
    client._cadc_client.download_file = download_file_mock
    client.cadcget('cadc:COLLECTION/file', dest='/tmp')
    download_file_mock.assert_called_once_with(url='https://url1', dest='/tmp')

    # raise error on the first url
    client._get_transfer_urls.reset_mock()
    download_file_mock.reset_mock()
    download_file_mock.side_effect = [exceptions.TransferException(), None]
    client.cadcget('cadc:COLLECTION/file', dest='/tmp')
    assert 2 == download_file_mock.call_count
    assert call(url='https://url1', dest='/tmp') in \
           download_file_mock.mock_calls
    assert call(url='https://url2', dest='/tmp') in \
           download_file_mock.mock_calls

    # fhead call
    client._get_transfer_urls.reset_mock()
    download_file_mock.reset_mock()
    download_file_mock.side_effect = None
    client.cadcget('cadc:COLLECTION/file', dest='/tmp', fhead=True)
    download_file_mock.assert_called_once_with(url='https://url1?META=true',
                                               dest='/tmp')

    # no urls after transfer negotiation
    client._get_transfer_urls.reset_mock()
    client._get_transfer_urls.return_value = []
    with pytest.raises(exceptions.HttpException):
        client.cadcget('cadc:COLLECTION/file', dest='/tmp')

    # non transient errors on both urls
    client._get_transfer_urls = Mock(return_value=['https://url1',
                                                   'https://url2'])
    # can be any error except exceptionsTransient error
    download_file_mock.side_effect = [AttributeError(), AttributeError()]
    with pytest.raises(AttributeError):
        client.cadcget('cadc:COLLECTION/file', dest='/tmp')

    # too many transient errors (max is storageinv.MAX_TRANSIENT_TRIES for each
    # of the 2 urls)
    num_transient_errors = storageinv.MAX_TRANSIENT_TRIES * 2 + 1
    download_file_mock.side_effect = \
        [exceptions.TransferException()] * num_transient_errors
    with pytest.raises(exceptions.TransferException):
        client.cadcget('cadc:COLLECTION/file', dest='/tmp')


@pytest.mark.skipif(cadcdata.storageinv.MAGIC_WARN is not None,
                    reason='libmagic not available')
@patch('cadcdata.core.net.BaseDataClient')
@patch('cadcdata.storageinv.net.extract_md5')
@patch('cadcdata.storageinv.util.Md5File')
def test_put(md5file_mock, extract_md5_mock, basews_mock):
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
    upload_mock = Mock()
    basews_mock.return_value.upload_file = upload_mock
    with pytest.raises(exceptions.UnauthorizedException):
        client.cadcput('cadc:TEST/putfile', file_name)
    client._cadc_client.subject.anon = False  # authenticate the user

    # new file
    location1 = 'https://url1/minoc/files'
    client._get_transfer_urls = Mock(return_value=[location1])
    client.cadcinfo = Mock(side_effect=exceptions.NotFoundException())
    extract_md5_mock.return_value = hash_md5
    md5file_mock.return_value.__enter__.return_value.md5_checksum = hash_md5
    client.cadcput('cadc:TEST/putfile', file_name)
    # Note Content* headers automatically created by cadcput except when
    # MAGIC_WANT -- libmagic not present
    upload_mock.assert_called_with(
        url=location1, src=file_name, md5_checksum=None,
        headers={'Content-Type': 'text/plain', 'Content-Encoding': 'us-ascii'})

    # mimic libmagic missing
    cadcdata.storageinv.MAGIC_WARN = 'Some warning'
    upload_mock.reset_mock()
    client.cadcinfo = Mock(side_effect=exceptions.NotFoundException())
    client.cadcput('cadc:TEST/putfile', file_name)
    upload_mock.assert_called_with(
        url='https://url1/minoc/files', src=file_name,
        md5_checksum=None, headers={})
    cadcdata.core.MAGIC_WARN = None

    # replace file
    client._get_transfer_urls = Mock(
        return_value=['https://url1/minoc/files'])
    client.cadcinfo = Mock(return_value={'id': 'cadc:TEST/putfile',
                                         'type': 'application/file',
                                         'encoding': 'none'})
    client.cadcput('cadc:TEST/putfile', file_name, replace=True,
                   file_type='text/plain', file_encoding='us-ascii')
    upload_mock.assert_called_with(
        url='https://url1/minoc/files', src=file_name, md5_checksum=None,
        headers={'Content-Type': 'text/plain', 'Content-Encoding': 'us-ascii'})

    # update metadata only
    upload_mock.reset_mock()
    post_mock = Mock()
    basews_mock.return_value.post = post_mock
    client._get_transfer_urls = Mock(
        return_value=['https://url1/minoc/files'])
    client.cadcinfo = Mock(
        return_value=cadcdata.FileInfo('cadc:TEST/putfile',
                                       file_type='application/file',
                                       encoding='none', md5sum='0x123456789'))
    client.cadcput('cadc:TEST/putfile', file_name, replace=True,
                   file_type='text/plain', file_encoding='us-ascii',
                   md5_checksum='0x123456789')
    upload_mock.assert_not_called()
    post_mock.assert_called_with('https://url1/minoc/files',
                                 headers={'Content-Type': 'text/plain',
                                          'Content-Encoding': 'us-ascii',
                                          'digest': 'md5=MHgxMjM0NTY3ODk='})

    # no update required - data and metadata identical
    upload_mock.reset_mock()
    post_mock.reset_mock()
    client._get_transfer_urls = Mock(
        return_value=['https://url1/minoc/files'])
    client.cadcinfo = Mock(
        return_value=cadcdata.FileInfo('cadc:TEST/putfile',
                                       file_type='text/plain',
                                       encoding='us-ascii',
                                       md5sum='0x123456789'))
    client.cadcput('cadc:TEST/putfile', file_name, replace=True,
                   file_type='text/plain', file_encoding='us-ascii',
                   md5_checksum='0x123456789')
    upload_mock.assert_not_called()
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
    upload_mock.reset_mock()
    basews_mock.return_value.post = post_mock
    url_list = ['https://url1/minoc/files/', 'https://url2/minoc/files/']
    client._get_transfer_urls = Mock(return_value=list(url_list))  # copy list
    upload_mock.side_effect = [exceptions.TransferException()] * 2 * \
        cadcdata.storageinv.MAX_TRANSIENT_TRIES
    client.cadcinfo = Mock(side_effect=exceptions.NotFoundException())
    with pytest.raises(exceptions.HttpException):
        client.cadcput('cadc:TEST/putfile', file_name,
                       file_type='text/plain', file_encoding='us-ascii',
                       md5_checksum='0x1234567890')
    assert upload_mock.call_count == \
           len(url_list) * cadcdata.storageinv.MAX_TRANSIENT_TRIES

    # Transfer error on one url, NotFound on the other
    upload_mock.reset_mock()
    basews_mock.return_value.post = post_mock
    url_list = ['https://url1/minoc/files/', 'https://url2/minoc/files/']
    client._get_transfer_urls = Mock(return_value=list(url_list))  # copy list
    upload_mock.side_effect = [exceptions.TransferException(),
                               exceptions.NotFoundException(),
                               exceptions.TransferException(),
                               exceptions.TransferException]
    client.cadcinfo = Mock(side_effect=exceptions.NotFoundException())
    with pytest.raises(exceptions.HttpException):
        client.cadcput('cadc:TEST/putfile', file_name,
                       file_type='text/plain', file_encoding='us-ascii',
                       md5_checksum='0x1234567890')
    assert \
        upload_mock.call_count == 1 + cadcdata.storageinv.MAX_TRANSIENT_TRIES

    # Transfer error on one url followed by 2 NotFound
    upload_mock.reset_mock()
    basews_mock.return_value.post = post_mock
    url_list = ['https://url1/minoc/files/', 'https://url2/minoc/files/']
    client._get_transfer_urls = Mock(return_value=list(url_list))  # copy list
    upload_mock.side_effect = [exceptions.TransferException(),
                               exceptions.NotFoundException(),
                               exceptions.NotFoundException()]
    client.cadcinfo = Mock(side_effect=exceptions.NotFoundException())
    with pytest.raises(exceptions.HttpException):
        client.cadcput('cadc:TEST/putfile', file_name,
                       file_type='text/plain', file_encoding='us-ascii',
                       md5_checksum='0x1234567890')
    assert upload_mock.call_count == 3


@patch('cadcdata.core.net.BaseDataClient')
def test_remove(basews_mock):
    client = StorageInventoryClient(auth.Subject())
    # test a put
    remove_mock = Mock()
    basews_mock.return_value.delete = remove_mock
    with pytest.raises(exceptions.UnauthorizedException):
        client.cadcremove('cadc:TEST/removefile')
    client._cadc_client.subject.anon = False  # authenticate the user

    client._get_transfer_urls = Mock(return_value=['url1'])
    client.cadcinfo = Mock()
    client.cadcremove('cadc:TEST/removefile')
    remove_mock.assert_called_with('url1')

    # file not found at the location
    basews_mock.return_value.delete.side_effect = \
        [exceptions.NotFoundException()]
    with pytest.raises(exceptions.HttpException):
        client.cadcremove('cadc:TEST/removefile')
    with pytest.raises(AttributeError):
        client.cadcremove(None)
    with pytest.raises(AttributeError):
        client.cadcremove('invalid-uri')

    # file not found in "global"
    basews_mock.return_value.delete = remove_mock
    client.cadcinfo.side_effect = \
        [exceptions.NotFoundException()]
    with pytest.raises(exceptions.HttpException):
        client.cadcremove('cadc:TEST/removefile')

    # file initially found in global, not found at locations, and later not
    # found in global either (eventual consistency removed the file from
    # global while cadcremove was trying the sites).
    basews_mock.return_value.delete.side_effect = \
        [exceptions.NotFoundException()]
    client.cadcinfo.side_effect = \
        [Mock(), exceptions.NotFoundException()]
    with pytest.raises(exceptions.HttpException):
        client.cadcremove('cadc:TEST/removefile')


@patch('cadcdata.storageinv.net.BaseDataClient')
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

        # Make it Python 3.10 compatible
        actual = stdout_mock.getvalue().\
            replace('options:', 'optional arguments:').strip('\n')
        assert usage.strip('\n') == actual


@patch('sys.exit', Mock(side_effect=[MyExitError, MyExitError]))
@patch('cadcdata.StorageInventoryClient.cadcget')
def test_cadcget_cli(cadcget_mock):
    sys.argv = ['cadcget', 'cadc:TEST/file']
    cadcget_cli()
    calls = [call(id='cadc:TEST/file', dest=None, fhead=False)]
    cadcget_mock.assert_has_calls(calls)

    # test with file names
    cadcget_mock.reset_mock()
    sys.argv = ['cadcget', 'cadc:TEST/file', '-o', 'file.txt', '--fhead']
    cadcget_cli()
    calls = [call(id='cadc:TEST/file', dest='file.txt', fhead=True)]
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


def test_validate_uri():
    valid_uri = 'cadc:TEST/somefile.txt'
    assert None is storageinv.validate_uri(valid_uri)
    assert valid_uri == storageinv.argparse_validate_uri(valid_uri)

    # missing id
    with pytest.raises(AttributeError):
        storageinv.validate_uri(None)

    with pytest.raises(argparse.ArgumentTypeError):
        storageinv.argparse_validate_uri(None)

    # no scheme
    with pytest.raises(AttributeError):
        storageinv.validate_uri('/tmp/somefile.txt')

    with pytest.raises(argparse.ArgumentTypeError):
        storageinv.argparse_validate_uri('/tmp/somefile.txt')
