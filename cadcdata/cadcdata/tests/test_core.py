# # -*- coding: utf-8 -*-
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
from cadcutils.net import auth
from cadcutils import exceptions
from cadcdata import transfer
from cadcdata import CadcDataClient
from cadcdata.core import main_app, TRANSFER_RESOURCE_ID
from mock import Mock, patch, MagicMock, ANY, call

# The following is a temporary workaround for Python issue 25532 (https://bugs.python.org/issue25532)
call.__wrapped__ = None


class MyExitError(Exception):

    def __init__(self):
        self.message = "MyExitError"

mycontent = ''

class TestCadcDataClient(unittest.TestCase):
    """Test the CadcDataClient class"""

    @patch('cadcdata.core.net.BaseWsClient')
    @patch('cadcdata.core.TransferReader')
    def test_get_file(self, trans_reader_mock, basews_mock):
        # test a simple get - no decompress
        file_name = '/tmp/afile.txt'
        file_chunks = ['aaaa'.encode(), 'bbbb'.encode(), ''.encode()]
        response = Mock()
        response.headers.get.return_value = 'filename={}'.format('orig_file_name')
        response.raw.read.side_effect = file_chunks #read returns multiple blocks
        basews_mock.return_value.get.return_value = response
        client = CadcDataClient(auth.Subject())
        with self.assertRaises(exceptions.HttpException):
            # no URLs returned in the transfer negotiations
            client.get_file('TEST', 'afile', destination=file_name)
        t = transfer.Transfer('ad:TEST/afile', 'pullFromVoSpace')
        p = transfer.Protocol
        p.endpoint = Mock()
        t.protocols = [p]
        trans_reader_mock.return_value.read.return_value = t
        client.get_file('TEST', 'afile', destination=file_name)
        expected_content = (''.join([c.decode() for c in file_chunks])).encode()
        with open(file_name, 'rb') as f:
            self.assertEquals(expected_content, f.read())
        os.remove(file_name)
        # do it again with the file now open
        response = Mock()
        response.headers.get.return_value = 'filename={}'.format('orig_file_name')
        response.raw.read.side_effect = file_chunks
        basews_mock.return_value.get.return_value = response
        with open(file_name, 'wb') as f:
            client.get_file('TEST', 'afile', destination=f)
        with open(file_name, 'rb') as f:
            self.assertEquals(expected_content, f.read())
        os.remove(file_name)

        # test a get with decompress
        file_name = 'bfile.txt'
        file_content = 'ABCDEFGH12345'
        file_chunks = [file_content[i:i+5].encode() for i in xrange(0, len(file_content), 5)]
        file_chunks.append('') # last chunk is empty
        response = Mock()
        response.headers.get.return_value = 'filename={}.gz'.format(file_name)
        response.raw.read.side_effect = file_chunks
        basews_mock.return_value.get.return_value = response
        client = CadcDataClient(auth.Subject())
        client.get_file('TEST', 'afile', decompress=True)
        with open(file_name, 'r') as f:
            self.assertEquals(file_content, f.read())
        os.remove(file_name)

        # test process_bytes and send the content to /dev/null after. Use no decompress
        def concatenate_chunks(chunk):
            global mycontent
            mycontent = '{}{}'.format(mycontent, chunk.decode())
        file_name = 'bfile.txt'
        file_content = 'ABCDEFGH12345'
        file_chunks = [file_content[i:i+5].encode() for i in xrange(0, len(file_content), 5)]
        file_chunks.append('') # last chunk is empty
        response = Mock()
        response.headers.get.return_value = 'filename={}.gz'.format(file_name)
        response.raw.read.side_effect = file_chunks
        basews_mock.return_value.get.return_value = response
        client = CadcDataClient(auth.Subject())
        client.logger.setLevel(logging.INFO)
        client.get_file('TEST', 'afile', destination='/dev/null',
                        process_bytes=concatenate_chunks)
        self.assertEquals(file_content, mycontent)

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
        client.get_file('TEST', 'getfile', decompress=True, wcs=True)
        trans_doc = ('<vos:transfer xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.0">\n  '
                     '<vos:target>ad:TEST/getfile</vos:target>\n  '
                     '<vos:direction>pullFromVoSpace</vos:direction>\n  '
                     '<vos:protocol uri="ivo://ivoa.net/vospace/core#httpget"/>\n</vos:transfer>\n').encode()
        post_mock.assert_called_with(resource=(TRANSFER_RESOURCE_ID, None), params={'wcs': True}, data=trans_doc,
                                     headers={'Content-Type': 'text/xml'})
        response.raw.read.side_effect = file_chunks
        post_mock.reset_mock()
        client.get_file('TEST', 'getfile', decompress=True, fhead=True)
        post_mock.assert_called_with(resource=(TRANSFER_RESOURCE_ID, None), params={'fhead': True}, data=trans_doc,
                                     headers={'Content-Type': 'text/xml'})
        response.raw.read.side_effect = file_chunks
        post_mock.reset_mock()
        client.get_file('TEST', 'getfile', decompress=True, cutout='[1:1]')
        post_mock.assert_called_with(resource=(TRANSFER_RESOURCE_ID, None), params={'cutout': '[1:1]'}, data=trans_doc,
                                     headers={'Content-Type': 'text/xml'})
        response.raw.read.side_effect = file_chunks
        post_mock.reset_mock()
        client.get_file('TEST', 'getfile', decompress=True, cutout='[[1:1], 2]')
        post_mock.assert_called_with(resource=(TRANSFER_RESOURCE_ID, None), params={'cutout': '[[1:1], 2]'}, data=trans_doc,
                                     headers={'Content-Type': 'text/xml'})


        # test a put
        file_name = '/tmp/putfile.txt'
        file_content = 'ABCDEFGH12345'
        # write the file
        with open(file_name, 'w') as f:
            f.write(file_content)
        put_mock = Mock()
        basews_mock.return_value.put = put_mock
        with self.assertRaises(exceptions.UnauthorizedException):
            client.put_file('TEST', 'putfile', file_name)
        client._data_client.subject.anon = False # authenticate the user
        transf_end_point = 'http://test.ca/endpoint'
        def mock_get_trans_protocols(archive, file_name, is_get, headers):
            protocol = Mock()
            protocol.endpoint = transf_end_point
            return [protocol]
        client._get_transfer_protocols = mock_get_trans_protocols
        client.put_file('TEST', file_name)
        put_mock.assert_called_with(transf_end_point, data=ANY, headers={})

        # specify an archive stream
        client.put_file('TEST', file_name, archive_stream='default')
        put_mock.assert_called_with(transf_end_point, data=ANY, headers={'X-CADC-Stream':'default'})
        os.remove(file_name)

        # test an info
        file_name ='myfile'
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
        self.assertEqual(archive, info['archive'])
        self.assertEqual(file_name, info['name'])
        self.assertEqual(size, info['size'])
        self.assertEqual(md5sum, info['md5sum'])
        self.assertEqual(type, info['type'])
        self.assertEqual(encoding, info['encoding'])
        self.assertEqual(lastmod, info['lastmod'])
        self.assertEqual(usize, info['usize'])
        self.assertEqual(umd5sum, info['umd5sum'])

    @patch('sys.exit', Mock(side_effect=[MyExitError, MyExitError, MyExitError,
                                         MyExitError, MyExitError, MyExitError]))
    def test_help(self):
        """ Tests the helper displays for commands and subcommands in main"""
        self.maxDiff = None
        #help
        usage = \
'''usage: cadc-data [-h] [-V] {get,put,info} ...

Client for accessing the data Web Service at the Canadian Astronomy Data Centre (www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/data)

positional arguments:
  {get,put,info}  supported commands. Use the -h|--help argument of a command
                  for more details
    get           Retrieve files from a CADC archive
    put           Upload files into a CADC archive
    info          Get information regarding files in a CADC archive

optional arguments:
  -h, --help      show this help message and exit
  -V, --version   show program's version number and exit
'''
        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            sys.argv = ['cadc-data', '--help']
            with self.assertRaises(MyExitError):
                main_app()
            self.assertEqual(usage, stdout_mock.getvalue())

        # get -h
        usage = \
'''usage: cadc-data get [-h]
                     [--cert CERT | -n | --netrc-file NETRC_FILE | -u USER]
                     [--host HOST] [--resource-id RESOURCE_ID] [-d | -q | -v]
                     [-o OUTPUT] [--cutout [CUTOUT [CUTOUT ...]]] [-z] [--wcs]
                     [--fhead]
                     archive filename [filename ...]

Retrieve files from a CADC archive

positional arguments:
  archive               CADC archive
  filename              the name of the file in the archive

optional arguments:
  --cert CERT           location of your X509 certificate to use for
                        authentication (unencrypted, in PEM format)
  --cutout [CUTOUT [CUTOUT ...]]
                        specify one or multiple extension and/or pixel range
                        cutout operations to be performed. Use cfitsio syntax
  -d, --debug           debug messages
  -z, --decompress      decompress the data (gzip only)
  --fhead               return the FITS header information
  -h, --help            show this help message and exit
  --host HOST           base hostname for services - used mainly for testing
                        (default: www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca)
  -n                    use .netrc in $HOME for authentication
  --netrc-file NETRC_FILE
                        netrc file to use for authentication
  -o, --output OUTPUT   space-separated list of destination files (quotes
                        required for multiple elements)
  -q, --quiet           run quietly
  --resource-id RESOURCE_ID
                        resource identifier (default ivo://cadc.nrc.ca/data)
  -u, --user USER       name of user to authenticate. Note: application
                        prompts for the corresponding password!
  -v, --verbose         verbose messages
  --wcs                 return the World Coordinate System (WCS) information

Examples:
- Anonymously getting a public file:
        cadc-data get -v GEMINI 00aug02_002.fits
- Use certificate to get a cutout and save it to a file:
        cadc-data get --cert ~/.ssl/cadcproxy.pem -o /tmp/700000o-cutout.fits --cutout [1] CFHT 700000o
- Use default netrc file ($HOME/.netrc) to get FITS header of a file:
        cadc-data get -v -n --fhead GEMINI 00aug02_002.fits
- Use a different netrc file to download wcs information:
        cadc-data get -d --netrc ~/mynetrc -o /tmp/700000o-wcs.fits --wcs CFHT 700000o
- Connect as user to download two files and uncompress them (prompt for password if user not in $HOME/.netrc):
        cadc-data get -v -u auser -z GEMINI 00aug02_002.fits 00aug02_003.fits
'''
        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            sys.argv = ['cadc-data', 'get', '--help']
            with self.assertRaises(MyExitError):
                main_app()
            self.assertEqual(usage, stdout_mock.getvalue())

        # put -h
        usage = \
'''usage: cadc-data put [-h]
                     [--cert CERT | -n | --netrc-file NETRC_FILE | -u USER]
                     [--host HOST] [--resource-id RESOURCE_ID] [-d | -q | -v]
                     [-s ARCHIVE_STREAM] [-c]
                     archive source [source ...]

Upload files into a CADC archive

positional arguments:
  archive               CADC archive
  source                file or directory containing the files to be put

optional arguments:
  -s, --archive-stream ARCHIVE_STREAM
                        specific archive stream to add the file to
  --cert CERT           location of your X509 certificate to use for
                        authentication (unencrypted, in PEM format)
  -c, --compress        gzip compress the data
  -d, --debug           debug messages
  -h, --help            show this help message and exit
  --host HOST           base hostname for services - used mainly for testing
                        (default: www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca)
  -n                    use .netrc in $HOME for authentication
  --netrc-file NETRC_FILE
                        netrc file to use for authentication
  -q, --quiet           run quietly
  --resource-id RESOURCE_ID
                        resource identifier (default ivo://cadc.nrc.ca/data)
  -u, --user USER       name of user to authenticate. Note: application
                        prompts for the corresponding password!
  -v, --verbose         verbose messages

Examples:
- Use certificate to put a file in an archive stream:
        cadc-data put --cert ~/.ssl/cadcproxy.pem -as default TEST myfile.fits.gz
- Use default netrc file ($HOME/.netrc) to put two files:
        cadc-data put -v -n TEST myfile1.fits.gz myfile2.fits.gz
- Use a different netrc file to put files from a directory:
        cadc-data put -d --netrc ~/mynetrc TEST dir
- Connect as user to put files from multiple sources (prompt for password if user not in $HOME/.netrc):
        cadc-data put -v -u auser TEST myfile.fits.gz dir1 dir2
'''
        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            sys.argv = ['cadc-data', 'put', '-h']
            with self.assertRaises(MyExitError):
                main_app()
            self.assertEqual(usage, stdout_mock.getvalue())

        # info -h
        usage = \
'''usage: cadc-data info [-h]
                      [--cert CERT | -n | --netrc-file NETRC_FILE | -u USER]
                      [--host HOST] [--resource-id RESOURCE_ID] [-d | -q | -v]
                      archive filename [filename ...]

Get information regarding files in a CADC archive on the form:
File:
	 -name
	 -size
	 -md5sum
	 -encoding
	 -type
	 -usize
	 -umd5sum
	 -lastmod

positional arguments:
  archive               CADC archive
  filename              the name of the file in the archive

optional arguments:
  --cert CERT           location of your X509 certificate to use for
                        authentication (unencrypted, in PEM format)
  -d, --debug           debug messages
  -h, --help            show this help message and exit
  --host HOST           base hostname for services - used mainly for testing
                        (default: www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca)
  -n                    use .netrc in $HOME for authentication
  --netrc-file NETRC_FILE
                        netrc file to use for authentication
  -q, --quiet           run quietly
  --resource-id RESOURCE_ID
                        resource identifier (default ivo://cadc.nrc.ca/data)
  -u, --user USER       name of user to authenticate. Note: application
                        prompts for the corresponding password!
  -v, --verbose         verbose messages

Examples:
- Anonymously getting information about a public file:
        cadc-data info GEMINI 00aug02_002.fits
- Use certificate to get information about a file:
        cadc-data info --cert ~/.ssl/cadcproxy.pem CFHT 700000o
- Use default netrc file ($HOME/.netrc) to get information about a file:
        cadc-data info -n GEMINI 00aug02_002.fits
- Use a different netrc file to get information about a file:
        cadc-data info --netrc ~/mynetrc CFHT 700000o
- Connect as user to get information about two files (prompt for password if user not in $HOME/.netrc):
        cadc-data info -u auser GEMINI 00aug02_002.fits 00aug02_003.fits
'''

        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            sys.argv = ['cadc-data', 'info', '--help']
            with self.assertRaises(MyExitError):
                main_app()
            self.assertEqual(usage, stdout_mock.getvalue())

    @patch('sys.exit', Mock(side_effect=[MyExitError, MyExitError, MyExitError,
                                         MyExitError, MyExitError, MyExitError]))
    @patch('cadcdata.core.CadcDataClient.put_file')
    @patch('cadcdata.core.CadcDataClient.get_file_info')
    @patch('cadcdata.core.CadcDataClient.get_file')
    def test_main(self, get_mock, info_mock, put_mock):
        sys.argv = ['cadc-data', 'get', 'TEST', 'fileid1', 'fileid2', 'fileid3']
        main_app()
        calls = [call('TEST', 'fileid1', None, cutout=None, decompress=False, fhead=False, wcs=False),
                 call('TEST', 'fileid2', None, cutout=None, decompress=False, fhead=False, wcs=False),
                 call('TEST', 'fileid3', None, cutout=None, decompress=False, fhead=False, wcs=False)]
        get_mock.assert_has_calls(calls)

        #test with file names
        get_mock.reset_mock()
        sys.argv = ['cadc-data', 'get', 'TEST', '-o', 'file1.txt file2.txt', 'fileid1', 'fileid2']
        main_app()
        calls = [call('TEST', 'fileid1', 'file1.txt', cutout=None, decompress=False, fhead=False, wcs=False),
                 call('TEST', 'fileid2', 'file2.txt', cutout=None, decompress=False, fhead=False, wcs=False)]
        get_mock.assert_has_calls(calls)

        # number of file names does not match the number of file ids. logger displays an error
        get_mock.reset_mock()
        sys.argv = ['cadc-data', 'get', 'TEST', '-o', 'file1.txt', 'fileid1', 'fileid2']
        b = StringIO()
        logger = logging.getLogger('cadc-data')
        # capture the log message in a StreamHandler
        logger.addHandler(logging.StreamHandler(b))
        with self.assertRaises(MyExitError):
            main_app()
        self.assertTrue('Different size of destination files list' in b.getvalue())

        # test info
        info_mock.return_value = {'archive':'TEST', 'name':'file1.txt.gz',
                                  'size':'5', 'md5sum':'0x33', 'type':'text', 'encoding':'gzip',
                                  'lastmod' : '10/10/10T10:10:10.000', 'usize':'50', 'umd5sum':'0x234'}
        sys.argv = ['cadc-data', 'info', 'TEST', 'file1']
        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            main_app()
        expected = \
'''File file1:
	    archive: TEST
	   encoding: gzip
	    lastmod: 10/10/10T10:10:10.000
	     md5sum: 0x33
	       name: file1.txt.gz
	       size: 5
	       type: text
	    umd5sum: 0x234
	      usize: 50
'''
        self.assertEqual(expected, stdout_mock.getvalue())

        #test put directory
        #create a file structure of files to put
        put_dir = '/tmp/put_dir'
        put_subdir = '{}/somedir'.format(put_dir)
        file1 = 'file1'
        file2 =  'file2'
        if os.path.exists(put_dir):
            shutil.rmtree(put_dir)
        os.makedirs(put_dir)
        os.makedirs(put_subdir)
        with open(os.path.join(put_dir, '{}.txt'.format(file1)), 'w') as f:
            f.write('TEST FILE1')
        with open(os.path.join(put_dir, '{}.txt'.format(file2)), 'w') as f:
            f.write('TEST FILE2')
        #extra file that is not going to be put because it resides in a subdirectory
        with open(os.path.join(put_subdir, 'file3.txt'), 'w') as f:
            f.write('TEST FILE3')

        sys.argv = ['cadc-data', 'put', 'TEST', put_dir]
        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            main_app()
        calls = [call('TEST', '/tmp/put_dir/file2.txt', archive_stream=None),
                 call('TEST', '/tmp/put_dir/file1.txt', archive_stream=None)]
        put_mock.assert_has_calls(calls, any_order=True)
        # number of file names does not match the number of file names. logger displays an error
        get_mock.reset_mock()
        sys.argv = ['cadc-data', 'get', 'TEST', '-o', 'file1.txt', 'file1', 'file2']
        b = StringIO()
        logger = logging.getLogger('cadc-data')
        # capture the log message in a StreamHandler
        logger.addHandler(logging.StreamHandler(b))
        with self.assertRaises(MyExitError):
            main_app()
        self.assertTrue('Different size of destination files list' in b.getvalue())

        #cleanup
        shutil.rmtree(put_dir)
