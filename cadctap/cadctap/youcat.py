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

import logging
import os
import datetime
from cadctap import version
from cadcutils import net
import magic
from requests_toolbelt.multipart.encoder import MultipartEncoder

logger = logging.getLogger(__name__)

DEFAULT_RESOURCE_ID = 'ivo://cadc.nrc.ca/youcat'
# capabilities ids
TABLES_CAPABILITY_ID = 'ivo://ivoa.net/std/VOSI#tables-1.1'
TABLE_UPDATE_CAPABILITY_ID = 'ivo://ivoa.net/std/VOSI#table-update-1.x'
QUERY_CAPABILITY_ID = 'ivo://ivoa.net/std/TAP'

# allowed file formats for load
ALLOWED_CONTENT_TYPES = {'tsv': 'text/tab-separated-values', 'csv': 'text/csv', 'FITSTable': 'application/fits'}
ALLOWED_TB_DEF_TYPES = {'VOSITable': 'text/xml',
                        'VOTable': 'application/x-votable+xml'}


class YoucatClient(object):
    """Class to access CADC databases.
    Example of usage:
    import os
    from cadcutils import net
    from cadctap.youcat import YoucatClient

    # create possible types of subjects
    anonSubject = net.Subject()
    certSubject = net.Subject(
       certificate=os.path.join(os.environ['HOME'], ".ssl/cadcproxy.pem"))
    netrcSubject = net.Subject(netrc=True)
    authSubject = net.Subject(netrc=os.path.join(os.environ['HOME'], ".netrc"))
    client = YoucatClient(anonSubject) # connect to ivo://cadc.nrc.ca/data

    # create a table
    client.create_table('newTableName', 'Description of table')

    # get query results
    client.query('SELECT column1, column2 FROM schema.table')

    # get the tables available for queries
    client.schema()
    """

    def __init__(self, subject, resource_id=DEFAULT_RESOURCE_ID,
                 host=None, agent=None):
        """
        Instance of a YoucatClient
        :param subject: the subject performing the action
        :type cadcutils.auth.Subject
        :param resource_id: the resource ID of the service
        :param host: Host for the caom2repo service
        :param agent: The name of the agent (to be used in server logging)
        """
        self.logger = logging.getLogger('YoucatClient')
        self.resource_id = resource_id
        self.host = host
        self._subject = subject
        if agent is None:
            agent = "cadc-tap-client/{}".format(version.version)

        self.agent = agent

        self._tap_client = net.BaseWsClient(resource_id, subject,
                                            agent, retry=True, host=self.host)

    def create_table(self, table_name, table_defintion, type=None):
        """
        Creates a table in youcat.
        :param table_name: Name of the table in the TAP service
        :param table_defintion: Stream containing the table definition
        :param type: Type of the table definition file
        """
        if not table_name or not table_defintion:
            raise AttributeError(
                'table name and definition required in create: {}/{}'.
                format(table_name, table_defintion))

        if type:
            if type not in ALLOWED_TB_DEF_TYPES.keys():
                raise AttributeError(
                    'Table definition file type {} not supported ({})'.
                    format(type, ' '.join(ALLOWED_TB_DEF_TYPES.keys)))
            else:
                file_type = type
        else:
            m = magic.Magic()
            t = m.from_file(table_defintion)
            if 'XML' in t:
                file_type = 'VOTable'
            elif 'ASCII' in t:
                file_type = 'VOSITable'
            else:
                raise AttributeError('Cannot determine the type of the table '
                                     'definition file {}'.
                                     format(table_defintion))
            logger.info('Assuming the table defintion file type: {}'.
                        format(file_type))
        logger.debug('Creating {} from file {} of type {}'.
                     format(table_name, table_defintion, file_type))
        headers = {'Content-Type': ALLOWED_TB_DEF_TYPES[file_type]}
        self._tap_client.put((TABLES_CAPABILITY_ID, table_name),
                             headers=headers,
                             data=open(table_defintion, 'rb').read())
        logger.debug('Successfully created table {}'.format(table_name))

    def delete_table(self, table_name):
        """
        Deletes a table in youcat
        :param table_name: name of the table to delete
        """
        if not table_name:
            raise AttributeError('table name required in delete')

        logger.debug('Deleting {}'.format(table_name))
        self._tap_client.delete((TABLES_CAPABILITY_ID, table_name))
        logger.debug('Successfully deleted table {}'.format(table_name))

    def create_index(self, table_name, column_name, unique=False):
        """
        Creates a table index in youcat
        :param table_name: name of the table
        :param column_name: name of the column
        :param unique: True if index is unique, False otherwise
        """
        if not table_name or not column_name:
            raise AttributeError(
                'table name and column required in index: {}/{}'.
                format(table_name, column_name))

        logger.debug('Index for column{} in table {}'.format(column_name,
                                                             table_name))
        result = self._tap_client.post((TABLE_UPDATE_CAPABILITY_ID, None),
                                       data={'table': table_name,
                                             'index': column_name,
                                             'uniquer': unique},
                                       allow_redirects=False)
        if result.status_code == 303:
            job_url = result.headers['Location']
            if not job_url:
                raise RuntimeError(
                    'table update job location missing in response')
            # start the job
            self._tap_client.post('{}/phase'.format(job_url),
                                  data={'PHASE': 'RUN'})
            # start polling
            short_waits = 10
            wait = 1
            while True:
                if short_waits:
                    short_waits = short_waits - 1
                else:
                    wait = 30
                result = self._tap_client.get('{}/phase'.format(job_url),
                                              data={'WAIT': wait})
                if result.text in ['COMPLETED']:
                    logger.debug('Index creation completed')
                    return
                elif result.text in ['HELD', 'SUSPENDED', 'ABORTED']:
                    # re-queue the job and continue to monitor for completion.
                    raise RuntimeError('UWS status: {0}'.format(result.text))
                elif result.text == 'EXECUTING':
                    logger.debug(
                        'EXECUTING ({})'.format(datetime.datetime.now()))
        else:
            raise RuntimeError('table update expected status 303 received {}'.
                               format(result.status_code))

    def load(self, table_name, source, fformat='tsv'):
        """
        Loads conent to a table
        :param table_name: name of the table
        :param source: list of files to load content from
        :param fformat: format of the content files
        :return:
        """
        if not table_name or not source:
            raise AttributeError(
                'table name and source requiered in upload: {}/{}'.
                format(table_name, source))
        for f in source:
            logger.debug('Uploading file {}'.format(f))

            headers = {'Content-Type': ALLOWED_CONTENT_TYPES[fformat]}
            with open(f, 'rb') as fh:
                self._tap_client.post((TABLES_CAPABILITY_ID, table_name),
                                      headers=headers,
                                      data=fh)
            logger.debug('Done uploading file {}'.format(fh.name))

    def query(self, query, output_file=None, response_format='VOTable',
              tmptable=None, lang='ADQL'):
        """
        Send query to database and output or save results
        :param lang: the language to use for the query (should be ADQL)
        :param query: the query to send to the database
        :param response_format: (VOTable, csv, tsv) format of returned result
        :param tmptable: tablename:/path/to/table, tmp table to upload
        :param output_file: name of the file to save results to
        """
        pass
        if not query:
            raise AttributeError('missing query')

        fields = {'LANG': lang,
                  'QUERY': query,
                  'FORMAT': response_format}
        if tmptable is not None:
            tmp = tmptable.split(':')
            tablename = tmp[0]
            tablepath = tmp[1]
            tablefile = os.path.basename(tablepath)
            fields['UPLOAD'] = '{},param:{}'.format(tablename, tablefile)
            fields[tablefile] = (tablepath, open(tablepath, 'rb'))

        logger.debug('QUERY fileds: {}'.format(fields))
        m = MultipartEncoder(fields=fields)
        with self._tap_client.post((QUERY_CAPABILITY_ID, None),
                                   data=m, headers={
                                       'Content-Type': m.content_type},
                                   stream=True) as result:
            if not output_file:
                print(result.text)
            else:
                with open(output_file, "wb") as f:
                    f.write(result.raw.read())

    def schema(self):
        """
        Outputs the tables available for queries
        """
        results = self._tap_client.get((TABLES_CAPABILITY_ID, None))
        print(results.text)
