#!/usr/bin/env python2.7
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

import argparse
import imp
import logging
import os
import os.path
import sys
from StringIO import StringIO
from datetime import datetime

from cadcutils import net, util, exceptions
from caom2.obs_reader_writer import ObservationReader, ObservationWriter
from caom2.version import version as caom2_version
from six.moves.urllib.parse import urlparse
import zlib

# from . import version as caom2repo_version
from cadcdata import version

__all__ = ['CadcDataClient']

# TODO replace with SERVICE_URI when server supports it
SERVICE_URL = 'www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/'
# IVOA dateformat
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
DEFAULT_RESOURCE_ID = 'ivo://cadc.nrc.ca/data'
ARCHIVE_STREAM_HTTP_HEADER = 'X-CADC-Stream'
APP_NAME = 'cadc-data'

READ_BLOCK_SIZE=8*1024

logger = logging.getLogger(APP_NAME)

class CadcDataClient:

    """Class to access CADC archival data."""

    logger = logging.getLogger('CadcDataClient')

    def __init__(self, subject, resource_id=DEFAULT_RESOURCE_ID, host=None):
        """
        Instance of a CAOM2RepoClient
        :param subject: the subject(user) performing the action
        :type subject: Subject
        :param resource_id: The identifier of the service resource (e.g 'ivo://cadc.nrc.ca/data')
        :param host: Host server for the caom2repo service
        """

        self.resource_id = resource_id

        # TODO This is just a temporary hack to be replaced with proper registry lookup functionaliy
        resource_url = urlparse(resource_id)
        self.host = host

        agent = "cadc-data-client/{}".format(version.version)

        self._repo_client = net.BaseWsClient(resource_id, subject,
                                             agent, retry=True, host=self.host)
        self.logger.info('Service URL: {}'.format(self._repo_client.base_url))


    def get_file(self, archive, file_id, file=None, decompress=False,
                 cutout=None, fhead=False, wcs=False, process_bytes=None):
        """
        Get a file from an archive. The entire file is delivered unless the cutout argument
         is present specifying a cutout to extract from the file.
        :param archive: name of the archive containing the file
        :param file_id: the ID of the file to retrieve
        :param file: file to save data to (file or file_name). If None, it uses the response
        content disposition to determine the name of the destination file
        :param cutout: perform cutout operation on the file before the result is delivered
        :param wcs: True if the wcs is to be included with the file
        :param process_bytes: function to be applied to the received bytes
        :return: the data stream object
        """
        assert archive is not None
        assert file_id is not None
        resource = '/{}/{}'.format(archive, file_id)
        params = {}
        if fhead:
            params['fhead'] = fhead
        if wcs:
            params['wcs'] = wcs
        if cutout:
            params['cutout'] = cutout
        self.logger.debug('GET '.format(resource))
        response = self._repo_client.get(resource, stream=True, params=params)
        if file is not None:
            if not hasattr(file, 'read'):
                # got a file name?
                with open(file, 'w') as f:
                    self._save_bytes(response, f, decompress, process_bytes)
            else:
                self._save_bytes(response, file, decompress, process_bytes)
        else:
            # get the file name from the content disposition
            content_disp = response.headers.get('content-disposition', '')
            file = file_id
            for content in content_disp.split():
                if 'filename=' in content:
                    file = content[9:]
            if file.endswith('gz') and decompress:
                file = os.path.splitext(file)[0]
            self.logger.debug('Using content disposition file name: {}'.format(file))
            with open(file, 'w') as f:
                self._save_bytes(response, f, decompress, process_bytes)
        self.logger.debug('Successfully saved file\n')


    def _save_bytes(self, response, file, decompress=False, process_bytes=None):
        # requests automatically decompresses the data. Tell it to do it only if it had to
        if not decompress:
            chunk = response.raw.read(READ_BLOCK_SIZE)
            while len(chunk) > 0:
                if process_bytes is not None:
                    process_bytes(chunk)
                file.write(chunk)
                chunk = response.raw.read(READ_BLOCK_SIZE)
        else:
            chunk = response.iter_content(READ_BLOCK_SIZE)
            while len(chunk) > 0:
                if process_bytes is not None:
                    process_bytes(chunk)
                file.write(chunk)
                chunk = response.iter_content(READ_BLOCK_SIZE)


    def put_file(self, archive, file_id, file, archive_stream=None):
        """
        Puts a file into the archive storage
        :param archive: name of the archive
        :param file_id: ID of file in the archive storage
        :param file: location of the source file
        :param archive_stream: specific archive stream
        """
        assert archive is not None
        assert file_id is not None
        resource = '/{}/{}'.format(archive, file_id)
        self.logger.debug('PUT {}'.format(resource))
        headers = {}
        if archive_stream is not None:
            headers[ARCHIVE_STREAM_HTTP_HEADER] = str(archive_stream)
        with open(file, 'rb') as f:
            self._repo_client.put(resource, headers=headers, data=f)
        self.logger.debug('Successfully updated file\n')


    def get_file_info(self, archive, file_id):
        """
        Get information regarding a file in the archive
        :param archive: Name of the archive
        :param file_id: ID of the file
        :returns dictionary of attributes/values
        """
        assert archive is not None
        assert file_id is not None
        resource = '/{}/{}'.format(archive, file_id)
        self.logger.debug('HEAD {}'.format(resource))
        response = self._repo_client.head(resource)
        h = response.headers
        file_info = {}
        file_info['id'] = file_id
        file_info['archive'] = archive
        file_info['name'] = h.get('Content-Disposition', '').replace('inline; filename=', '')
        file_info['size'] = h.get('Content-Length', None)
        file_info['md5sum'] = h.get('Content-MD5', None)
        file_info['type'] = h.get('Content-Type', None)
        file_info['encoding'] = h.get('Content-Encoding', None)
        file_info['lastmod'] = h.get('Last-Modified', None)
        file_info['usize'] = h.get('X-Uncompressed-Length', None)
        file_info['umd5sum'] = h.get('X-Uncompressed-MD5', None)
        #TODOfile_info['ingest_date'] = h[?]
        self.logger.debug("File info: {}".format(file_info))
        return file_info

def handle_error(msg, exit=True):
    """
    Prints error message and exit (by default)
    :param msg: error message to print
    :return:
    """
    logger.error(msg)
    if exit:
        sys.exit(-1) #TODO use different error codes?

def main_app():

    parser = util.get_base_parser(version=version.version, default_resource_id=DEFAULT_RESOURCE_ID)

    parser.description = ('Client for accessing the data Web Service at the Canadian Astronomy Data Centre '+
                          '(www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/data)')

    subparsers = parser.add_subparsers(dest='cmd',
                                       help='Supported commands. Use the -h|--help argument of a command ' +\
                                        'for more details')
    get_parser = subparsers.add_parser('get',
                                          description='Retrieve files from a CADC archive',
                                          help='Retrieve files from a CADC archive')
    get_parser.add_argument('-a', '--archive', help='CADC archive', required=True)
    get_parser.add_argument('-o', '--output',
                            help='Space-separated list of destination files (quotes required for multiple elements)',
                            required=False)
    get_parser.add_argument('--cutout', help='Perform one or multiple cutout operations as specified by the argument',
                            required=False)
    get_parser.add_argument('-de','--decompress', help='Decompress the data (gzip only)',
                            action='store_true', required=False)
    get_parser.add_argument('--wcs', help='Return the World Coordinate System (WCS) information',
                            action='store_true', required=False)
    get_parser.add_argument('--fhead', help='Return the FITS header information',
                            action='store_true', required=False)
    get_parser.add_argument('fileID', help='The ID of the file in the archive', nargs='+')

    put_parser = subparsers.add_parser('put',
                                        description='Upload files into a CADC archive',
                                        help='Upload files into a CADC archive')
    put_parser.add_argument('-a','--archive', help='CADC archive', required=True)
    put_parser.add_argument('-as', '--archive-stream', help='Specific archive stream to add the file to',
                            required=False)
    put_parser.add_argument('-c', '--compress', help='gzip compress the data',
                            action='store_true', required=False)
    put_parser.add_argument('--fileID',
                            help='file ID to use for single source (not to be used with multiple sources)',
                            required=False)
    put_parser.add_argument('source',
                            help='File or directory containing the files to be put', nargs='+')

    info_parser = subparsers.add_parser('info',
                                          description=('Get information regarding files in a '
                                                       'CADC archive on the form:\n'
                                                       'File id:\n'
                                                       '\t -name\n'
                                                       '\t -size\n'
                                                       '\t -md5sum\n'
                                                       '\t -encoding\n'
                                                       '\t -type\n'
                                                       '\t -usize\n'
                                                       '\t -umd5sum\n'
                                                       #'\t -ingest_date\n'
                                                       '\t -lastmod'),
                                          help='Get information regarding files in a CADC archive')
    info_parser.add_argument('-a', '--archive', help='CADC archive', required=True)
    # info_parser.add_argument('--file-id', action='store_true', help='File ID')
    # info_parser.add_argument('--file-name', action='store_true', help='File name')
    # info_parser.add_argument('--file-size', action='store_true', help='File size')
    # info_parser.add_argument('--md5sum', action='store_true', help='md5sum')
    # info_parser.add_argument('--content-encoding', action='store_true', help='Content encoding')
    # info_parser.add_argument('--content-type', action='store_true', help='Content type')
    # info_parser.add_argument('--uncompressed-size', action='store_true', help='Uncompressed size')
    # info_parser.add_argument('--uncompressed-md5sum', action='s   tore_true', help='Uncompressed md5sum of the file')
    # info_parser.add_argument('--ingest-date', action='store_true', help='Last modified')
    # info_parser.add_argument('--last-modified', action='store_true', help='Ingest date')
    info_parser.add_argument('fileID',
                             help='The ID of the file in the archive', nargs='+')


    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    elif args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARN)

    subject = net.Subject.get_subject(args)

    client = CadcDataClient(subject, args.resourceID, host=args.host)
    try:
        if args.cmd == 'get':
            logger.info('get')
            archive = args.archive
            file_ids = args.fileID
            if args.output is not None:
                files = args.output.split()
                if len(files) != len(file_ids):
                    handle_error('Different size of destination files list ({}) and list of file IDs ({})'.
                                  format(files, file_ids))
                for f, fid in list(zip(files, file_ids)):
                    try:
                        client.get_file(archive, fid, f, decompress=args.decompress,
                                     fhead=args.fhead, wcs=args.wcs, cutout=args.cutout)
                    except exceptions.NotFoundException as e:
                        handle_error('File ID {} not found'.format(fid), exit=False)
            else:
                for fid in file_ids:
                    try:
                        client.get_file(archive, fid, None, decompress=args.decompress,
                                        fhead=args.fhead, wcs=args.wcs, cutout=args.cutout)
                    except exceptions.NotFoundException as e:
                        handle_error('File ID {} not found'.format(fid), exit=False)
        elif args.cmd == 'info':
            logger.info('info')
            archive = args.archive
            for file_id in args.fileID:
                try:
                    file_info = client.get_file_info(archive, file_id)
                except exceptions.NotFoundException as e:
                    handle_error('File ID {} not found'.format(file_id), exit=False)
                    continue
                print('File {}:'.format(file_id))
                for field in sorted(file_info):
                    print('\t {:>10}: {}'.format(field, file_info[field]))
        else:
            logger.info('put')
            archive = args.archive
            sources = args.source

            files = []
            for file in sources:
                if os.path.isfile(file):
                    files.append(file)
                elif os.path.isdir(file):
                    for f in os.listdir(file):
                        if os.path.isfile(os.path.join(file, f)):
                            files.append(os.path.join(file, f))
                        else:
                            logger.warn('{} not added to the list of files to put'.format(f))
            logger.debug('Files to put: {}'.format(files))
            file_ids = []
            if args.fileID is not None:
                if len(file_ids) > 1:
                    handle_error('Cannot use fileID argument with multiple source files')
                else:
                    file_ids.append(args.fileID)
            else:
                # create the list of file_ids
                for file in files:
                    file_ids.append(os.path.basename(file).split('.')[0])

            for file, file_id in list(zip(files, file_ids)):
                client.put_file(archive, file_id, file, archive_stream=args.archive_stream)
    except exceptions.UnauthorizedException as e:
        if subject.anon:
            handle_error('Operation cannot be performed anonymously. '
                         'Use one of the available methods to authenticate')
        else:
            handle_error('Unexpected authentication problem')
    except exceptions.ForbiddenException as e:
        handle_error('Unauthorized to perform operation')
    except exceptions.UnexpectedException as e:
        logger.debug(e.orig_exception)
        handle_error('Unexpected server error')

    logger.info("DONE")

#TODO remove
if __name__ == '__main__':
    main_app()