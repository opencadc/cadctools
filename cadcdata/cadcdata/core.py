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

from cadcutils import net
from cadcutils import util
from caom2.obs_reader_writer import ObservationReader, ObservationWriter
from caom2.version import version as caom2_version
from six.moves.urllib.parse import urlparse

# from . import version as caom2repo_version
from cadcdata import version

__all__ = ['CadcDataClient']

# TODO replace with SERVICE_URI when server supports it
SERVICE_URL = 'www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/'
# IVOA dateformat
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
DEFAULT_RESOURCE_ID = 'ivo://cadc.nrc.ca/data'
ARCHIVE_STREAM_HTTP_HEADER = 'X-CADC-Stream'


class CadcDataClient:

    """Class to access CADC archival data."""

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
        logging.info('Service URL: {}'.format(self._repo_client.base_url))


    def get_file(self, archive, file_id, file=file,
                 cutout=None, wcs=None, process_bytes=None):
        """
        Get a file from an archive. The entire file is delivered unless the cutout argument
         is present specifying a cutout to extract from the file.
        :param archive: name of the archive containing the file
        :param file_id: the ID of the file to retrieve
        :param file: file to save data to (file or file_name)
        :param cutout: perform cutout operation on the file before the result is delivered
        :param wcs: True if the wcs is to be included with the file
        :param process_bytes: function to be applied to the received bytes
        :return: the data stream object
        """
        assert archive is not None
        assert file_id is not None
        resource = '/{}/{}'.format(archive, file_id)
        logging.debug('GET '.format(resource))
        response = self._repo_client.get(resource, stream=True)
        if not hasattr(file, 'read'):
            with open(file, 'w') as f:
                for chunk in response.iter_content(1024):
                    if process_bytes is not None:
                        process_bytes(chunk)
                    f.write(chunk)
        else:
            for chunk in response.iter_content(1024):
                if process_bytes is not None:
                    process_bytes(chunk)
                file.write(chunk)
        logging.debug('Successfully saved file\n')


    def put_file(self, archive, file_id, file, archive_stream=None, replace=False):
        """
        Puts a file into the archive storage
        :param archive: name of the archive
        :param file_id: ID of file in the archive storage
        :param file: location of the source file
        :param archive_stream: specific archive stream
        :param replace: True if this a replacement of an existing file, False otherwise
        """
        assert archive is not None
        assert file_id is not None
        resource = '/{}/{}'.format(archive, file_id)
        logging.debug('PUT {}'.format(resource))
        headers = {}
        if archive_stream is not None:
            headers[ARCHIVE_STREAM_HTTP_HEADER] = str(archive_stream)
        with open(file, 'rb') as f:
            self._repo_client.put(resource, headers=headers, data=f)
        logging.debug('Successfully updated file\n')


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
        logging.debug('HEAD {}'.format(resource))
        response = self._repo_client.head(resource)
        logging.info('Successfully deleted Observation {}\n')
        return response.content #TODO might need to return the headers


def main():

    parser = util.get_base_parser(version=version.version, default_resource_id=DEFAULT_RESOURCE_ID)

    parser.description = ('Client for accessing the data Web Service at the Canadian Astronomy Data Centre '+
                          '(www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/data')

    subparsers = parser.add_subparsers(dest='cmd',
                                       help='Supported commands. Use the -h|--help argument of a command ' +\
                                            'for more details')
    get_parser = subparsers.add_parser('get',
                                          description='Retrieve files from a CADC archive',
                                          help='Retrieve files from a CADC archive')
    get_parser.add_argument('-a', '--archive', help='CADC archive', required=True)
    get_parser.add_argument('-o', '--output',
                            help='Sspace-separated list of destination files (quotes required for multiple elements)',
                            required=False)
    get_parser.add_argument('--coutout', help='Perform one or multiple cutout operations as specified by the argument',
                            required=False)
    get_parser.add_argument('-de','--decompress', help='Decompress the data',
                            action='store_true', required=False)
    get_parser.add_argument('--wcs', help='Return the World Coordinate System (WCS) information',
                            action='store_true', required=False)
    get_parser.add_argument('--fhead', help='Return the FITS header information',
                            action='store_true', required=False)
    get_parser.add_argument('FILEIDs',
                            help='File ID(s)')

    put_parser = subparsers.add_parser('put',
                                        description='Upload files into a CADC archive',
                                        help='Upload files into a CADC archive')
    put_parser.add_argument('-a','--archive', help='CADC archive', required=True)
    put_parser.add_argument('-as', '--archive-stream', help='Specific archive stream to add the file to',
                            required=False)
    get_parser.add_argument('-c', '--compress', help='Compress the data',
                            action='store_true', required=False)
    put_parser.add_argument('--fileIDs',
                            help='Space-separated list of file IDs to use (quotes required for multiple elements)',
                            required=False)
    put_parser.add_argument('files',
                            help='Space-separate list of files or directories containing the files')

    info_parser = subparsers.add_parser('info',
                                          description=('Get information regarding files in a '
                                                       'CADC archive on the form:\n'
                                                       'File FILEID:\n'
                                                       '\t -file name\n'
                                                       '\t -file size\n'
                                                       '\t -md5sum\n'
                                                       '\t -content encoding\n'
                                                       '\t -content type\n'
                                                       '\t -uncompressed size\n'
                                                       '\t -uncompressed md5sum\n'
                                                       '\t -ingest date\n'
                                                       '\t -last modified'),
                                          help='Get information regarding files in a CADC archive')
    info_parser.add_argument('-a', '--archive', help='CADC archive', required=True)
    # info_parser.add_argument('--file-id', action='store_true', help='File ID')
    # info_parser.add_argument('--file-name', action='store_true', help='File name')
    # info_parser.add_argument('--file-size', action='store_true', help='File size')
    # info_parser.add_argument('--md5sum', action='store_true', help='md5sum')
    # info_parser.add_argument('--content-encoding', action='store_true', help='Content encoding')
    # info_parser.add_argument('--content-type', action='store_true', help='Content type')
    # info_parser.add_argument('--uncompressed-size', action='store_true', help='Uncompressed size')
    # info_parser.add_argument('--uncompressed-md5sum', action='store_true', help='Uncompressed md5sum of the file')
    # info_parser.add_argument('--ingest-date', action='store_true', help='Last modified')
    # info_parser.add_argument('--last-modified', action='store_true', help='Ingest date')
    info_parser.add_argument('FILEIDs',
                             help='Space-separated list of file IDs')


    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)

    subject = net.Subject(username=args.user,
                          certificate=args.cert, use_netrc=args.n, netrc_file=args.netrc_file)

    client = CadcDataClient(subject, args.resourceID, host=args.host)
    if args.cmd == 'get':
        logging.info("get")
        archive = args.archive
        file_id = args.FILEIDs
        #logging.debug("Call visitor with plugin={}, start={}, end={}, dataset={}".
        #              format(plugin, start, end, collection, retries))
        with open(args.output, 'w') as dest:
            client.get_file(archive, file_id, dest)

    elif args.cmd == 'create':
        logging.info("Create")
        print(args.__dict__)
    elif args.cmd == 'read':
        logging.info("Read")
        observation = client.get_observation(args.collection, args.observation)
        observation_writer = ObservationWriter()
        if args.output:
            with open(args.output, 'w') as obsfile:
                observation_writer.write(observation, obsfile)
        else:
            observation_writer.write(observation, sys.stdout)
    elif args.cmd == 'update':
        logging.info("Update")
        obs_reader = ObservationReader()
        # TODO not sure if need to read in string first
        client.post_observation(obs_reader.read(args.observation))
    else:
        logging.info("Delete")
        client.delete_observation(collection=args.collection, observation_id=args.observationID)

    logging.info("DONE")

#TODO remove
if __name__ == '__main__':
    main()