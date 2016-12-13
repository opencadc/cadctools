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
from . import version

__all__ = ['CadcDataClient']

# TODO replace with SERVICE_URI when server supports it
SERVICE_URL = 'www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/'
# IVOA dateformat
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
DEFAULT_RESOURCE_ID = 'ivo://cadc.nrc.ca/data'


class CAOM2RepoClient:

    """Class to access CADC archival data."""

    def __init__(self, resource_id=DEFAULT_RESOURCE_ID, anon=True, cert_file=None, host=None):
        """
        Instance of a CAOM2RepoClient
        :param resource_id: The identifier of the service resource (e.g 'ivo://cadc.nrc.ca/data')
        :param anon: True if anonymous access, False otherwise
        :param cert_file: Location of X509 certificate used for authentication
        :param host: Host server for the caom2repo service
        """

        self.resource_id = resource_id

        # TODO This is just a temporary hack to be replaced with proper registry lookup functionaliy
        resource_url = urlparse(resource_id)
        self.host = host

        agent = "cadc-data-client/{}".format(version.version)

        self._repo_client = net.BaseWsClient(resource_id, anon=anon, cert_file=cert_file,
                                             agent=agent, retry=True, host=self.host)
        logging.info('Service URL: {}'.format(self._repo_client.base_url))


    def save_file(self, archive, file_id, archive_stream=None, file=file,
                 cutout=None, wcs=None, process_bytes=None):
        """
        Get a file from an archive. The entire file is delivered unless the cutout argument
         is present specifying a cutout to extract from the file.
        :param archive: name of the archive containing the file
        :param file_id: the ID of the file to retrieve
        :param archive_stream: the name of the stream in the archive
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


    def post_observation(self, observation):
        """
        Updates an observation in the CAOM2 repo
        :param observation: observation to update
        :return: updated observation
        """
        assert observation.collection is not None
        assert observation.observation_id is not None
        resource = '/{}/{}'.format(observation.collection, observation.observation_id)
        logging.debug('POST {}'.format(resource))

        ibuffer = StringIO()
        ObservationWriter().write(observation, ibuffer)
        obs_xml = ibuffer.getvalue()
        headers = {'Content-Type': 'application/xml'}
        response = self._repo_client.post(
            resource, headers=headers, data=obs_xml)
        logging.debug('Successfully updated Observation\n')

    def put_observation(self, observation):
        """
        Add an observation to the CAOM2 repo
        :param observation: observation to add to the CAOM2 repo
        :return: Added observation
        """
        assert observation.collection is not None
        assert observation.observation_id is not None
        resource = '/{}/{}'.format(observation.collection, observation.observation_id)
        logging.debug('PUT {}'.format(resource))

        ibuffer = StringIO()
        ObservationWriter().write(observation, ibuffer)
        obs_xml = ibuffer.getvalue()
        headers = {'Content-Type': 'application/xml'}
        response = self._repo_client.put(
            resource, headers=headers, data=obs_xml)
        logging.debug('Successfully put Observation\n')

    def delete_observation(self, collection, observation_id):
        """
        Delete an observation from the CAOM2 repo
        :param collection: Name of the collection
        :param observation_id: ID of the observation
        """
        assert observation_id is not None
        resource = '/{}/{}'.format(collection, observation_id)
        logging.debug('DELETE {}'.format(resource))
        response = self._repo_client.delete(resource)
        logging.info('Successfully deleted Observation {}\n')


def main():

    base_parser = util.get_base_parser(version=version.version, default_resource_id=DEFAULT_RESOURCE_ID)

    parser = argparse.ArgumentParser(parents=[base_parser])

    parser.description = ('Client for a CAOM2 repo. In addition to CRUD (Create, Read, Update and Delete) '
                          'operations it also implements a visitor operation that allows for updating '
                          'multiple observations in a collection')
    parser.formatter_class = argparse.RawTextHelpFormatter

    subparsers = parser.add_subparsers(dest='cmd', )

    create_parser = subparsers.add_parser('create', parents=[base_parser],
                                          description='Create a new observation',
                                          help='Create a new observation')
    create_parser.add_argument('observation', metavar='<new observation file>', type=file)

    read_parser = subparsers.add_parser('read', parents=[base_parser],
                                        description='Read an existing observation',
                                        help='Read an existing observation')
    read_parser.add_argument('--collection', metavar='<collection>', required=True)
    read_parser.add_argument('--output', '-o', metavar='<destination file>', required=False)
    read_parser.add_argument('observation', metavar='<observation>')

    update_parser = subparsers.add_parser('update', parents=[base_parser],
                                          description='Update an existing observation',
                                          help='Update an existing observation')
    update_parser.add_argument('observation', metavar='<observation file>', type=file)

    delete_parser = subparsers.add_parser('delete', parents=[base_parser],
                                          description='Delete an existing observation',
                                          help='Delete an existing observation')
    delete_parser.add_argument('--collection', metavar='<collection>', required=True)
    delete_parser.add_argument('observationID', metavar='<ID of observation>')

    # Note: RawTextHelpFormatter allows for the use of newline in epilog
    visit_parser = subparsers.add_parser('visit', parents=[base_parser],
                                         formatter_class=argparse.RawTextHelpFormatter,
                                         description='Visit observations in a collection',
                                         help='Visit observations in a collection')
    visit_parser.add_argument('--plugin', required=True, type=file,
                              metavar='<pluginClassFile>',
                              help='Plugin class to update each observation')
    visit_parser.add_argument('--start', metavar='<datetime start point>',
                              type=util.str2ivoa,
                              help='oldest dataset to visit (UTC %%Y-%%m-%%d format)')
    visit_parser.add_argument('--end', metavar='<datetime end point>',
                              type=util.str2ivoa,
                              help='earliest dataset to visit (UTC %%Y-%%m-%%d format)')
    visit_parser.add_argument('--retries', metavar='<number of retries>', type=int,
                              help='number of tries with transient server errors')
    visit_parser.add_argument("-s", "--server", metavar='<CAOM2 service URL>',
                              help="URL of the CAOM2 repo server")

    visit_parser.add_argument('collection', metavar='<datacollection>', type=str,
                              help='data collection in CAOM2 repo')
    visit_parser.epilog =\
"""
Minimum plugin file format:
----
   from caom2.caom2_observation import Observation

   class ObservationUpdater:

    def update(self, observation):
        assert isinstance(observation, Observation), (
            'observation {} is not an Observation'.format(observation))
        # custom code to update the observation
----
"""
    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)

    cert_file = None
    if os.path.isfile(args.certfile):
        cert_file = args.certfile

    client = CAOM2RepoClient(args.resourceID, anon=args.anonymous, cert_file=cert_file, host=args.host)
    if args.cmd == 'visit':
        logging.info("Visit")
        plugin = args.plugin
        start = args.start
        end = args.end
        retries = args.retries
        collection = args.collection
        logging.debug("Call visitor with plugin={}, start={}, end={}, dataset={}".
                      format(plugin, start, end, collection, retries))
        client.visit(plugin.name, collection, start=start, end=end)

    elif args.cmd == 'create':
        logging.info("Create")
        obs_reader = ObservationReader()
        client.put_observation(obs_reader.read(args.observation))
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
