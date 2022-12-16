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

import logging
import traceback
import sys
from clint.textui import progress
import datetime
from cadcutils import net, util, exceptions
import netrc as netrclib
import os
from cadctap import version
from urllib.parse import urlparse, urlencode
import contextlib
import cadcutils
from xml.dom import minidom
import re
from argparse import ArgumentError

from requests_toolbelt.multipart.encoder import MultipartEncoder

logger = logging.getLogger(__name__)

# Prefix to be prepended to the short name of a service ID
SERVICE_ID_PREFIX = 'ivo://cadc.nrc.ca/'
# ID of the default catalog Web service
DEFAULT_SERVICE_ID = SERVICE_ID_PREFIX + 'youcat'
# capabilities ids
TABLES_CAPABILITY_ID = 'ivo://ivoa.net/std/VOSI#tables-1.1'
TABLE_UPDATE_CAPABILITY_ID = 'ivo://ivoa.net/std/VOSI#table-update-async-1.x'
TABLE_LOAD_CAPABILITY_ID = 'ivo://ivoa.net/std/VOSI#table-load-sync-1.x'
QUERY_CAPABILITY_ID = 'ivo://ivoa.net/std/TAP'
PERMISSIONS_CAPABILITY_ID = 'ivo://ivoa.net/std/VOSI#table-permissions-1.x'
CADC_AC_SERVICE = 'ivo://cadc.nrc.ca/gms'
CADC_LOGIN_CAPABILITY = 'ivo://ivoa.net/std/UMS#login-0.1'
CADC_SSO_COOKIE_NAME = 'CADC_SSO'
CADC_REALMS = ['.canfar.net', '.cadc-ccda.hia-iha.nrc-cnrc.gc.ca',
               '.cadc.dao.nrc.ca', '.canfar.phys.uvic.ca']

# allowed file formats for load
ALLOWED_CONTENT_TYPES = {'tsv': 'text/tab-separated-values',
                         'csv': 'text/csv',
                         'FITSTable': 'application/fits'}
ALLOWED_TB_DEF_TYPES = {'VOSITable': 'text/xml',
                        'VOTable': 'application/x-votable+xml'}
AUTH_OPTION_EXPLANATION = \
    '\nTo obtain the host associated with a service, execute a subcommand\n'\
    'with the service in verbose mode without specifying any authentication\n'\
    'option\n\n'\
    'If no authentication option is specified, cadc-tap will determine the\n'\
    'host associated with the service and look in the ~/.netrc file for the\n'\
    'host, and if found, will use the -n option. If not, cadc-tap will look\n'\
    'for ~/.ssl/cadcproxy.pem file, and if found, will use the --cert\n'\
    'option. If not, cadc-tap will use the --anon option.'

cadctap_agent = 'cadc-tap-client/{}'.format(version.version)

# make the stream bar show up on stdout
progress.STREAM = sys.stdout

__all__ = ['CadcTapClient']

TABLES_CAPABILITY = 'ivo://ivoa.net/std/VOSI#tables-1.1'
TAP_CAPABILITY = 'ivo://ivoa.net/std/TAP'

APP_NAME = 'cadc-tap'

# for default authentication
CADC_DOMAIN = 'cadc-ccda.hia-iha.nrc-cnrc.gc.ca'
CANFAR_DOMAIN = 'canfar.net'


class CadcTapClient(object):
    """Class to access CADC databases.
    Example of usage:
    import os
    from cadcutils import net
    from cadctap import CadcTapClient

    # create possible types of subjects
    anonSubject = net.Subject()
    certSubject = net.Subject(
       certificate=os.path.join(os.environ['HOME'], ".ssl/cadcproxy.pem"))
    netrcSubject = net.Subject(netrc=True)
    authSubject = net.Subject(netrc=os.path.join(os.environ['HOME'], ".netrc"))
    client = CadcTapClient(anonSubject) # connect to ivo://cadc.nrc.ca/data

    # create a table
    client.create_table('newTableName', 'Description of table')

    # get query results
    client.query('SELECT column1, column2 FROM schema.table')

    # get the tables available for queries
    client.schema()
    """

    def __init__(self, subject, resource_id=DEFAULT_SERVICE_ID,
                 host=None, agent=None, insecure=False):
        """
        Instance of a CadcTapClient
        :param subject: the subject performing the action
        :type cadcutils.auth.Subject
        :param resource_id: the resource ID of the service
        :param host: Host for the caom2repo service
        :param agent: The name of the agent (to be used in server logging)
        :param insecure Allow insecure server connections over SSL
        """
        self.resource_id = resource_id
        self.host = host
        util.check_version(version=version.version)
        # cache schema info for multiple calls
        self._db_schemas = {}

        self._subject = subject
        if agent is None:
            self.agent = cadctap_agent
        else:
            self.agent = agent

        # for the CADC TAP services, BasicAA is not supported anymore, so
        # we need to login and get a cookie when the subject uses
        # user/passwd
        if resource_id.startswith('ivo://cadc.nrc.ca') and\
           net.auth.SECURITY_METHODS_IDS['basic'] in \
           subject.get_security_methods():
            login = net.BaseWsClient(CADC_AC_SERVICE, net.Subject(),
                                     self.agent, insecure=insecure,
                                     retry=True, host=self.host)
            login_url = login._get_url((CADC_LOGIN_CAPABILITY, None))
            realm = urlparse(login_url).hostname
            auth = subject.get_auth(realm)
            if not auth:
                raise RuntimeError(
                    'No user/password for realm {} in .netrc'.format(realm))
            data = urlencode([('username', auth[0]), ('password', auth[1])])
            headers = {
                "Content-type": "application/x-www-form-urlencoded",
                "Accept": "text/plain"
            }
            cookie_response = \
                login.post((CADC_LOGIN_CAPABILITY, None), data=data,
                           headers=headers)
            cookie_response.raise_for_status()
            for cadc_realm in CADC_REALMS:
                subject.cookies.append(
                    net.auth.CookieInfo(cadc_realm, CADC_SSO_COOKIE_NAME,
                                        '"{}"'.format(cookie_response.text)))

        self._tap_client = net.BaseWsClient(resource_id, subject, self.agent,
                                            retry=True, host=self.host,
                                            insecure=insecure)
        # check for the presence of optional TAP features
        self.permissions_support = True
        try:
            self._tap_client.caps.get_access_url(PERMISSIONS_CAPABILITY_ID)
        except Exception as ex:
            if PERMISSIONS_CAPABILITY_ID in str(ex):
                self.permissions_support = False
                logger.debug('Service has no support for permissions')

    def create_table(self, table_name, table_definition, type='VOSITable'):
        """
        Creates a table in the catalog service.
        :param table_name: Name of the table in the TAP service
        :param table_definition: Stream containing the table definition
        :param type: Type of the table definition file
        """
        if not table_name or not table_definition:
            raise AttributeError(
                'table name and definition required in create: {}/{}'.
                format(table_name, table_definition))

        if type not in ALLOWED_TB_DEF_TYPES.keys():
            raise AttributeError(
                'Table definition file type {} not supported ({})'.
                format(type, ' '.join(ALLOWED_TB_DEF_TYPES.keys)))
        else:
            file_type = type
        logger.debug('Creating {} from file {} of type {}'.
                     format(table_name, table_definition, file_type))
        headers = {'Content-Type': ALLOWED_TB_DEF_TYPES[file_type]}
        self._tap_client.put((TABLES_CAPABILITY_ID, table_name),
                             headers=headers,
                             data=open(table_definition, 'rb').read())
        logger.debug('Successfully created table {}'.format(table_name))

    def delete_table(self, table_name):
        """
        Deletes a table from the catalog service
        :param table_name: name of the table to delete
        """
        if not table_name:
            raise AttributeError('table name required in delete')

        logger.debug('Deleting {}'.format(table_name))
        self._tap_client.delete((TABLES_CAPABILITY_ID, table_name))
        logger.debug('Successfully deleted table {}'.format(table_name))

    def create_index(self, table_name, column_name, unique=False):
        """
        Creates a table index in the catalog service
        :param table_name: name of the table
        :param column_name: name of the column
        :param unique: True if index is unique, False otherwise
        """
        if not table_name or not column_name:
            raise AttributeError(
                'table name and column required in index: {}/{}'.
                format(table_name, column_name))

        logger.debug('{} for column {} in table {}'.
                     format('Unique index' if unique else 'Index',
                            column_name, table_name,))
        result = self._tap_client.post((TABLE_UPDATE_CAPABILITY_ID, None),
                                       data={'table': table_name,
                                             'index': column_name,
                                             'unique': 'true' if unique
                                             else 'false'},
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
                logger.debug(
                    'Polling the job with wait time: {}s'.format(wait))
                result = self._tap_client.get('{}/phase'.format(job_url),
                                              data={'WAIT': wait})
                if result.text in ['COMPLETED']:
                    logger.debug('Index creation completed')
                    return
                elif result.text in ['HELD', 'SUSPENDED', 'ABORTED', 'ERROR']:
                    # re-queue the job and continue to monitor for completion.
                    # TODO parse xml
                    details = self._tap_client.get(job_url).text
                    msg = 'Problems with job: {}\n Details: {}'.\
                        format(result.text, details)
                    raise RuntimeError(msg)
                elif result.text == 'EXECUTING':
                    logger.debug(
                        'EXECUTING ({})'.format(datetime.datetime.now()))
        else:
            raise RuntimeError('table update expected status 303 received {}'.
                               format(result.status_code))

    def load(self, table_name, source, fformat='tsv'):
        """
        Loads content to a table
        :param table_name: name of the table
        :param source: list of files to load content from
        :param fformat: format of the content files
        :return:
        """
        if not table_name or not source:
            raise AttributeError(
                'table name and source requiered in upload: {}/{}'.
                format(table_name, source))

        if source == '-':
            source = ["/dev/stdin"]

        for f in source:
            if 'stdin' in f:
                logger.info('Uploading from stdin')
            else:
                logger.info('Uploading file {}'.format(f))

            headers = {'Content-Type': ALLOWED_CONTENT_TYPES[fformat]}
            with open(f, 'rb') as fh:
                self._tap_client.post((TABLE_LOAD_CAPABILITY_ID, table_name),
                                      headers=headers,
                                      data=fh)

            if 'stdin' in f:
                logger.info('Done uploading from stdin')
            else:
                logger.info('Done uploading file {}'.format(fh.name))

    def query(self, query, output_file=None, response_format='VOTable',
              tmptable=None, lang='ADQL', timeout=2, data_only=False,
              no_column_names=False, maxrec=None):
        """
        Send query to database and output or save results
        :param query: the query to send to the database
        :param response_format: (VOTable, csv, tsv) format of returned result
        :param tmptable: tablename:/path/to/table, tmp table to upload
        :param output_file: name of the file or a buffer (BytesIO) to save
        results to.
        :param lang: the language to use for the query (should be ADQL)
        :param timeout: time in minutes before the query should timeout when no
        response receive from server.
        :param data_only: print only data with name of columns
        :param no_column_name: print just data with no column names
        :param maxrec: maximum number of records to return (minimum 0)
        """
        if not query:
            raise AttributeError('missing query')

        if maxrec is not None and maxrec < 0:
            raise ValueError('maxrec cannot be negative: {}'.format(maxrec))

        fields = {'LANG': lang,
                  'QUERY': query,
                  'FORMAT': response_format}

        if maxrec:
            fields['MAXREC'] = str(maxrec)

        if tmptable is not None:
            tmp = tmptable.split(':')
            tablename = tmp[0]
            tablepath = tmp[1]
            tablefile = os.path.basename(tablepath)
            fields['UPLOAD'] = '{},param:{}'.format(tablename, tablefile)
            fields[tablefile] = (tablepath, open(tablepath, 'rb'))

        logger.debug('QUERY fileds: {}'.format(fields))
        m = MultipartEncoder(fields=fields)
        # TODO the following if/else is temporary to support both TAP1.0 and
        # TAP1.1 capabilities. For TAP1.1 the resource argument in the post
        # should be the (QUERY_CAPABILITY_ID, None) tuple
        url = self._tap_client._get_url((QUERY_CAPABILITY_ID, None))
        if url.endswith('async'):
            resource = url.replace('async', 'sync')
        else:
            resource = self._tap_client._get_url((QUERY_CAPABILITY_ID, 'sync'))

        rows = 0
        with self._tap_client.post(resource, params=fields,
                                   data=m, headers={
                                       'Content-Type': m.content_type},
                                   stream=True, timeout=timeout*60) as result:
            with smart_open(output_file, response_format) as f:
                header = True
                if data_only or no_column_names or \
                        response_format == 'VOTable':
                    for chunk in result.iter_content(chunk_size=8192):
                        if chunk:  # filter out keep-alive new chunks
                            if response_format != 'VOTable':
                                chunk = chunk.decode('utf-8')
                                if header and no_column_names and \
                                        '\n' in chunk:
                                    chunk = chunk[chunk.index('\n')+1:]
                                    header = False
                            f.write(chunk)
                    return
                header = True
                for chunk in result.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive new chunks
                        chunk = chunk.decode('utf-8')
                        if header and '\n' in chunk:
                            index = chunk.index('\n')
                            f.write(chunk[:index])
                            f.write('\n-----------------------')
                            f.write(chunk[index:])
                            header = False
                            rows = chunk.count('\n') - 1
                        else:
                            f.write(chunk)
                            rows += chunk.count('\n')
                if rows == 1:
                    footer = '\n(1 row affected)\n'
                else:
                    footer = '\n({} rows affected)\n'.format(rows)
                f.write(footer)

    def schema(self, name=None):
        """
        Outputs information about the tables available for queries

        :param name: name of the schema or table (schema.tablename) to
        get the schema for. Set to None to get the names of all the schemas
        and tables.
        """
        try:
            tab_info = self.get_schema(name)
            if not tab_info:
                tab_info = self.get_table_schema(name)
        except cadcutils.exceptions.NotFoundException:
            raise AttributeError('Resource {} not found.'.format(name))
        except cadcutils.exceptions.ForbiddenException:
            raise AttributeError('Resource {} access denied'.format(name))
        if not tab_info:
            raise AttributeError('{} not found'.format(name))
        for tab in tab_info:
            print('')
            self.display_tab(tab)

    def display_tab(self, info):
        """
        Displays tabular information
        :param info: Information to display. Expected TabularInfo type
        """
        # determine the maximum length of the table names:
        if isinstance(info, str):
            print(info)
            return
        print('\n{}: {}\n'.format(info.name, info.description))
        for i, f in enumerate(info.columns):
            sys.stdout.write('{field: <{fsize}}  '.
                             format(field=f,
                                    fsize=info.max_col_lengths[i]))
        print('')
        for col in info.max_col_lengths:
            sys.stdout.write('-' * col + '  ')
        print('')
        for r in info.rows:
            for i, f in enumerate(r):
                sys.stdout.write(
                    '{field: <{fsize}}  '.
                    format(field=f, fsize=info.max_col_lengths[i]))
            sys.stdout.write('\n')
        if len(info.rows) == 1:
            print('\n(1 row affected)')
        else:
            print('\n({} rows affected)'.format(len(info.rows)))

    def get_permissions(self, resource):
        """
        Returns the permissions associated with a resource
        :param resource:
        :return:
        """
        if not self.permissions_support:
            return ''
        try:
            response = self._tap_client.get(
                (PERMISSIONS_CAPABILITY_ID, resource))
        except cadcutils.exceptions.ForbiddenException:
            return 'Permissions - Available to owners only'
        perm = TabularInfo('Permissions',
                           'Permissions for ' + resource,
                           ['Owner', 'Others Read',
                            'Group Read', 'Group Write'])
        permissions = {}
        for row in (response.text.split('\n')):
            p = row.split('=')
            if len(p) == 2:
                permissions[p[0]] = p[1]
            elif len(p) > 2:
                permissions[p[0]] = row[row.index('=')+1:]
            else:
                permissions[p[0]] = '?'
        # simplify display of local groups by using just their name
        pro = permissions['r-group'] if permissions['r-group'] else '-'
        if pro.startswith(CADC_AC_SERVICE):
            pro = pro[pro.find('?') + 1:]  # use the short group name
        prw = permissions['rw-group'] if permissions['rw-group'] else '-'
        if prw.startswith(CADC_AC_SERVICE):
            prw = prw[prw.find('?') + 1:]  # use the short group name
        perm.add_row(
            (permissions['owner'], permissions['public'], pro, prw))
        return perm

    def get_schema(self, schema_name=None):
        """
        Return DB schema in tabular format.
        :param schema_name: name of the schema or all the schemas if name is
        None
        :return: List of TabularInfo objects describing schema of one database
        or all databases if schema_name not found.
        Columns are ['Table', 'Description']
        """
        if not self._db_schemas:
            results = self._tap_client.get((TABLES_CAPABILITY_ID, None),
                                           params={'detail': 'min'})
            doc = minidom.parseString(results.text)
            for s in doc.getElementsByTagName('schema'):
                schema_info = TabularInfo(
                    name=s.getElementsByTagName('name')[0].
                    firstChild.nodeValue,
                    description='DB schema',
                    columns=['Table', 'Description'])
                for t in s.getElementsByTagName('table'):
                    name = \
                        t.getElementsByTagName('name')[0].firstChild.nodeValue
                    try:
                        description = \
                            t.getElementsByTagName('description')[0]. \
                            firstChild.nodeValue
                    except Exception:
                        description = ''
                    schema_info.add_row((name, description))
                self._db_schemas[schema_info.name] = schema_info

        if not schema_name:
            return list(self._db_schemas.values())
        else:
            if schema_name not in self._db_schemas.keys():
                return None

        result = [self._db_schemas[schema_name]]
        result.append(self.get_permissions(schema_name))
        return result

    def get_table_schema(self, table):
        """
        Returns the schema information regarding a table in TabularInfo form
        NOTE: columns ctypes are currently ignored
        :param table: table name
        :return: Information about the table as a list of TabularInfo objects.
        First object represents information about columns, while the second
        entry describes table foreign keys if any.
        """
        response = self._tap_client.get((TABLES_CAPABILITY_ID, table),
                                        params={'detail': 'min'})
        doc = minidom.parseString(response.text)
        try:
            tab_descr = doc.getElementsByTagName(
                                'description')[0].firstChild.nodeValue
        except Exception:
            tab_descr = ''
        cols_info = TabularInfo(name=table,
                                description=tab_descr,
                                columns=['Name', 'Type', 'Index',
                                         'Description'])
        for s in doc.getElementsByTagName('column'):
            name = s.getElementsByTagName('name')[0].firstChild.nodeValue
            try:
                description = s.getElementsByTagName('description')[0]. \
                    firstChild.nodeValue
            except Exception:
                description = ''
            if s.getElementsByTagName('utype'):
                col_type = s.getElementsByTagName('utype')[0]. \
                    firstChild.nodeValue
            else:
                col_type = ''
            flag = s.getElementsByTagName('flag')
            if flag and flag[0].firstChild.nodeValue == 'indexed':
                indexed = 'Y'
            else:
                indexed = 'N'
            cols_info.add_row((name, col_type, indexed, description))
        result = [cols_info]

        fk = doc.getElementsByTagName('foreignKey')
        if fk:
            fk_info = TabularInfo('Foreign Keys', 'Foreign Keys for table',
                                  ['Target Table', 'Target Col',
                                   'From Column', 'Description'])
            for row in fk:
                target_table = row.getElementsByTagName('targetTable')[0].\
                    firstChild.nodeValue
                target_column = row.getElementsByTagName('targetColumn')[0]. \
                    firstChild.nodeValue
                from_column = row.getElementsByTagName('fromColumn')[0]. \
                    firstChild.nodeValue
                if row.getElementsByTagName('description'):
                    description = row.getElementsByTagName('description')[0]. \
                        firstChild.nodeValue
                else:
                    description = ''
                fk_info.add_row((target_table, target_column,
                                 from_column, description))
            result.append(fk_info)
        result.append(self.get_permissions(table))
        return result

    def set_permissions(self, resource, read_anon=None, read_only=None,
                        read_write=None):
        """
        Set permissions on a resource (schema or table). Only the permissions
        that are specified (not None) are being updated on the server.

        :param read_anon: True if anonymous reads are allowed, False otherwise
        :param read_only: Group URI or empty string (clear existing group)
        :param read_write: Group URI or empty string (clear existing group)
        :return:
        """
        if not self.permissions_support:
            raise AttributeError(
                'Service does not support permission-based access')
        logger.debug('set_permissions on resource {}: read_anon={}, '
                     'read_only={}, read_write={}'.format(
                        resource, read_anon, read_only, read_write))
        if not resource:
            raise AttributeError("No resource")
        if read_anon is None and read_only is None and read_write is None:
            logger.warning('No permissions values passed in set_permissions')
            return
        if read_only and not read_only.startswith('ivo://'):
            raise AttributeError('Expected URI for read group: {}'.
                                 format(read_only))
        if read_write and not read_write.startswith('ivo://'):
            raise AttributeError('Expected URI for write group: {}'.
                                 format(read_write))
        params = ''
        if read_anon is not None:
            params += 'public={}\n'.format(str(read_anon).lower())
        if read_only is not None:
            ro = read_only
            params += 'r-group={}\n'.format(ro)
        if read_write is not None:
            rw = read_write
            params += 'rw-group={}\n'.format(rw)
        self._tap_client.post((PERMISSIONS_CAPABILITY_ID, resource),
                              data=params,
                              headers={
                                  'Content-Type': 'text/plain'})


class TabularInfo:
    """
    Class to store a very simple tabular information to be displayed in tabular
    format. The table has a name, a description and an arbitrary number of
    columns (all the text format).
    """
    def __init__(self, name, description, columns):
        """
        :param name: name of the schema
        :param description: description of the schema
        :param columns: name of the columns. Must match the number of
        tuple fields in rows
        """
        self.name = name
        self.description = description
        self.columns = columns
        self._rows = []
        self._max_col_lengths = []
        for c in self.columns:
            self._max_col_lengths.append(len(c))

    @property
    def rows(self):
        """
        :return: Info regarding elements in the schena. Each row is a tuple
        """
        return self._rows

    @property
    def max_col_lengths(self):
        return self._max_col_lengths

    def add_row(self, values):
        """
        Adds info regarding and entry in the schema info
        :param values: tuple with the values. Must have the same length as the
        columns in this class
        """
        if len(values) != len(self.columns):
            raise AttributeError(
                'Number of tuple elements does not match the number of '
                'the columns: {} != {}'.format(values, self.columns))
        for i in range(len(values)):
            if self._max_col_lengths[i] < len(values[i]):
                self._max_col_lengths[i] = len(values[i])
        self.rows.append(values)


@contextlib.contextmanager
def smart_open(filename=None, content_format=None):
    # handles writing to files and stdout uniformly. If filename is None,
    # it returns stdout to write to.
    close_file = False
    if filename and filename != '-' and isinstance(filename, str):
        if content_format == 'VOTable':
            fh = open(filename, 'wb')
        else:
            fh = open(filename, 'w')
        close_file = True
    else:
        if filename and filename != '-':
            fh = filename
        else:
            if content_format == 'VOTable' and hasattr(sys.stdout, 'buffer'):
                fh = sys.stdout.buffer
            else:
                fh = sys.stdout

    try:
        yield fh
    finally:
        if close_file:
            fh.close()


def _add_anon_option(parser):
    # cadc-tap supports '-a | --anon' authentication option
    # This is a hack. It depends on the implementation of ArgumentParser.
    for m_group in parser.common_parser._mutually_exclusive_groups:
        for g_action in m_group._group_actions:
            for o_string in g_action.option_strings:
                if 'cert' in o_string or 'netrc' in o_string:
                    # add the --anon authentication option
                    m_group.add_argument(
                        '-a', '--anon', action='store_true',
                        help='use the service anonymously')

                    '''
                    Note: argparse does not display the mutually exclusive
                    options correctly when an option is added to the mutually
                    exclusive group after other arguments are added. The
                    following code is a work around to display the mutually
                    exclusive options correctly in help.
                    '''
                    # get the option strings for the auth options
                    g_action_o_strings = []
                    for group_action in m_group._group_actions:
                        g_action_o_strings.append(group_action.option_strings)

                    # get the actions for auth and non auth options
                    auth_actions = []
                    non_auth_actions = []
                    for action in parser.common_parser._optionals._actions:
                        is_auth_action = False
                        for o_str in g_action_o_strings:
                            if action.option_strings == o_str:
                                auth_actions.append(action)
                                is_auth_action = True
                                break
                        if not is_auth_action:
                            non_auth_actions.append(action)

                    # fix actions in each mutually exclusive group
                    for g in parser.common_parser._mutually_exclusive_groups:
                        g._actions = []
                        g._actions.extend(auth_actions)
                        g._actions.extend(non_auth_actions)

                    # fix the actions in the common parser
                    parser.common_parser._actions = m_group._actions
                    return

    raise RuntimeError("Missing authentication option")


def _get_subject_from_netrc(args):
    # Checks to see if user has the required user/passwd in the .netrc file
    # for the service host in order to use it.
    try:
        anon_subject = net.Subject()
        if args.service.startswith('ivo://cadc.nrc.ca'):
            # CADC services do not support BasicAA. Need to login first to
            # get cookie, so check if login url is in .netrc
            login = net.BaseWsClient(CADC_AC_SERVICE, anon_subject,
                                     agent=cadctap_agent)
            service_url = login._get_url((CADC_LOGIN_CAPABILITY, None))
            service_host = urlparse(service_url).hostname
        else:
            dummy_client = CadcTapClient(anon_subject,
                                         resource_id=args.service,
                                         host=args.host)
            service_host = dummy_client._tap_client._host
        logger.info('host for service {} is {}'.format(args.service,
                                                       service_host))
        hosts = netrclib.netrc(None).hosts
        for host in hosts.keys():
            if (service_host in host):
                return net.Subject(netrc=True)
        return None
    except Exception:
        return None


def _get_subject_from_certificate():
    # if ~/.ssl/cadcproxy.pem exists, use certificate and return a subject
    cert_path = os.path.join(os.environ['HOME'], ".ssl/cadcproxy.pem")
    if os.path.isfile(cert_path):
        return net.Subject(certificate=cert_path)
    else:
        return None


def _get_subject(args):
    # returns a subject either with the specified authentication option or
    # by default pick the -n option if cadc.ugly or canfar.net is present in
    # ~/.netrc or pick the -cert option if ~/ssl/cadcproxy.pem is available.
    subject = net.Subject.from_cmd_line_args(args)
    if (not subject.anon or args.anon):
        # authentication option specified
        logger.debug('authentication option is specified')
        return subject
    else:
        # default, i.e. no authentication option specified
        netrc_subject = _get_subject_from_netrc(args)
        if (netrc_subject is not None):
            # pick -n option
            logger.debug('use -n option')
            return netrc_subject
        else:
            cert_subject = _get_subject_from_certificate()
            if (cert_subject is not None):
                # pick -cert option
                logger.debug('use --cert option')
                return cert_subject
            else:
                # use anon subject
                logger.debug('use --anon option')
                return subject


def exit_on_exception(ex):
    """
    Exit program due to an exception,
    print the exception and exit with error code.
    :param ex:
    :param message: error message to display
    :return:
    """
    # Note: this could probably be updated to use an appropriate logging
    # handler instead of writing to stderr
    message = None
    if isinstance(ex, exceptions.HttpException):
        message = str(ex.orig_exception)
        if 'certificate expired' in str(ex.orig_exception):
            message = "Certificate expired."
        else:
            message = str(ex)
    if message:
        # could be VOTable format
        try:
            doc = minidom.parseString(message)
            # in the absence of a VOTable parser, try a simple w
            for el in doc.getElementsByTagName('INFO'):
                if el.attributes['name'].value == 'QUERY_STATUS' and \
                        el.attributes['value'].value == 'ERROR':
                    message = el.firstChild.nodeValue + '\n'
        except Exception:
            pass
    if message:
        sys.stderr.write('ERROR:: {}\n'.format(message))
    else:
        sys.stderr.write('ERROR:: {}\n'.format(str(ex)))
    tb = traceback.format_exc()
    logging.debug(tb)
    sys.exit(getattr(ex, 'errno', -1)) if getattr(ex, 'errno',
                                                  -1) else sys.exit(-1)


def _get_permission_modes(opt):
    """
    Extracts permissions modes from the mode argument. Duplicated from vchmod
    :param opt: argparse arguments
    :return: dictionary of permission modes
    """
    group_names = opt.GROUPS

    mode = opt.MODE

    props = {'read_anon': None, 'read_only': None, 'read_write': None}
    if 'o' in mode['who']:
        if mode['op'] == '-':
            props['read_anon'] = False
        else:
            props['read_anon'] = True
    if 'g' in mode['who']:
        if '-' == mode['op']:
            if not len(group_names) == 0:
                raise ArgumentError(
                    None,
                    "Names of groups not valid with remove permission")
            if 'r' in mode['what']:
                props['read_only'] = ''
            if "w" in mode['what']:
                props['read_write'] = ''
        else:
            if not len(group_names) == len(mode['what']):
                name = len(mode['what']) > 1 and "names" or "name"
                raise ArgumentError(None,
                                    "{} group {} required for {}".format(
                                        len(mode['what']), name,
                                        mode['what']))
            if mode['what'].find('r') > -1:
                # remove duplicate whitespaces
                rgroups = " ".join(
                    group_names[mode['what'].find('r')].split())
                props['read_only'] = \
                    '{}?{}'.format(CADC_AC_SERVICE,
                                   rgroups.replace(" ", " " + CADC_AC_SERVICE))
            if mode['what'].find('w') > -1:
                wgroups = " ".join(
                    group_names[mode['what'].find('w')].split())
                props['read_write'] = \
                    '{}?{}'.format(CADC_AC_SERVICE,
                                   wgroups.replace(" ", " " + CADC_AC_SERVICE))
    elif group_names:
        raise ArgumentError(None, 'Unexpected group name(s)')
    return props


def main_app(command='cadc-tap query'):
    parser = util.get_base_parser(version=version.version,
                                  service=DEFAULT_SERVICE_ID)

    _add_anon_option(parser)
    parser.description = (
        'Client for accessing databases using TAP protocol at the Canadian '
        'Astronomy Data Centre (www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca)')

    subparsers = parser.add_subparsers(
        dest='cmd',
        help='supported commands. Use the -h|--help argument of a command '
             'for more details')
    schema_parser = subparsers.add_parser(
        'schema',
        description=('Print the tables available for querying.\n') +
        AUTH_OPTION_EXPLANATION,
        help='print the tables available for querying.')
    schema_parser.add_argument(
        'tablename', metavar='SCHEMA.TABLENAME',
        help='table to get the schema for', nargs='?')
    query_parser = subparsers.add_parser(
        'query',
        description=('Run an adql query\n') + AUTH_OPTION_EXPLANATION,
        help='run an adql query')
    query_parser.add_argument(
        '-o', '--output-file',
        default=None,
        help='write query results to file (default is to STDOUT)',
        required=False)
    query_parser.add_argument(
        '-m', '--maxrec', type=int,
        help='limit the number of returned records to this maximum',
        required=False
    )
    options_parser = query_parser.add_mutually_exclusive_group(required=True)
    options_parser.add_argument(
        'QUERY',
        default=None,
        nargs='?',
        help='ADQL query to run, format is a string with quotes around it, '
             'for example "SELECT observationURI FROM caom2.Observation"')
    options_parser.add_argument(
        '-i', '--input-file',
        default=None,
        help='read query string from file (default is from STDIN),'
             ' location of file')
    # Maybe adding async option later
    """
    query_parser.add_argument(
        '-a', '--async-job',
        action='store_true',
        help='issue an asynchronous query (default is synchronous'
             ' which only outputs the top 2000 results)',
        required=False)
    """
    query_parser.add_argument(
        '--timeout', default=2, help='query timeout in minutes. Default 2min',
        required=False, type=int)
    query_parser.add_argument(
        '-f', '--format',
        default='tsv',
        choices=['VOTable', 'csv', 'tsv'],
        help='output format, either tsv(default), csv, fits (TBD), or VOTable',
        required=False)
    query_parser.add_argument(
        '-t', '--tmptable',
        default=None,
        help='temp table upload, the value is in format: '
             '"tablename:/path/to/table". In query to reference the table'
             ' use tap_upload.tablename',
        required=False)
    query_parser.epilog = (
        'Examples:\n'
        '- Anonymously run a query string:\n'
        '      {0} -a -s tap "SELECT TOP 10 type FROM caom2.Observation"\n'
        '- Use certificate to run a query:\n'
        '      {0} -s tap "SELECT TOP 10 type FROM caom2.Observation"'
        ' --cert ~/.ssl/cadcproxy.pem\n'
        '- Use username/password to run a query on the tap service:\n'
        '      {0} -s ivo://cadc.nrc.ca/tap '
        '"SELECT TOP 10 type FROM caom2.Observation"'
        ' -u <username>\n'
        '- Use netrc file to run a query on the ams/mast service'
        ' :\n'
        '      {0} -n -s ivo://cadc.nrc.ca/ams/mast'
        ' "SELECT TOP 10 target_name FROM caom2.Observation"\n'.
        format(command))

    create_parser = subparsers.add_parser(
        'create',
        description='Create a table\n' + AUTH_OPTION_EXPLANATION,
        help='create a table')
    create_parser.add_argument(
        '-f', '--format', choices=sorted(ALLOWED_TB_DEF_TYPES.keys()),
        required=False, default='VOSITable',
        help='format of the table definition file. Default VOSITable format')
    create_parser.add_argument(
        'TABLENAME',
        help='name of the table (<schema.table>) in the tap service')
    create_parser.add_argument(
        'TABLEDEFINITION',
        help='file containing the definition of the table or "-" if definition'
        ' in stdin')

    delete_parser = subparsers.add_parser(
        'delete',
        description='Delete a table\n' + AUTH_OPTION_EXPLANATION,
        help='delete a table')
    delete_parser.add_argument(
        'TABLENAME',
        help='name of the table (<schema.table)'
             'in the tap service to be deleted')

    index_parser = subparsers.add_parser(
        'index',
        description='create a table index\n' + AUTH_OPTION_EXPLANATION,
        help='create a table index')
    index_parser.add_argument(
        '-U', '--unique', action='store_true',
        help='index is unique')
    index_parser.add_argument(
        'TABLENAME',
        help='name of the table in the tap service to create the index for')
    index_parser.add_argument(
        'COLUMN',
        help='name of the column to create the index for')

    load_parser = subparsers.add_parser(
        'load',
        description='load data to a table\n' + AUTH_OPTION_EXPLANATION,
        help='load data to a table')
    load_parser.add_argument(
        '-f', '--format', choices=sorted(ALLOWED_CONTENT_TYPES.keys()),
        required=False, default='tsv',
        help='format of the data file')
    load_parser.add_argument(
        'TABLENAME',
        help='name of the table (<schema.table>) to load data to')
    load_parser.add_argument(
        'SOURCE', nargs='+',
        help='source of the data. It can be files or "-" for stdin.'
    )

    permission_parser = subparsers.add_parser(
        'permission',
        description='Update access permissions of a table or a schema. '
                    'Use schema command to display the existing permissions',
        help='control table access'
    )

    def check_mode(mode):
        """
        Checks the validity of a mode attribute
        :param mode:
        :return: mode dictionary
         :rtype: re.groupdict
        """
        _mode = re.match(
            r"(?P<who>og|go|o|g)(?P<op>[+\-=])(?P<what>rw|wr|r|w)",
            mode)
        if _mode is None:
            raise ArgumentError(_mode, 'Invalid mode: {}'.format(mode))
        return _mode.groupdict()

    permission_parser.add_argument(
        'MODE', type=check_mode,
        help='permission setting accepted modes: (og|go|o|g)[+-=](rw|wr|r|w)')
    permission_parser.add_argument('TARGET', help='table or schema name')
    permission_parser.add_argument(
        'GROUPS', nargs='*',
        help="name(s) of group(s) to assign read/write permission to. "
             "One group per r or w permission.")

    args = parser.parse_args()
    if len(sys.argv) < 2:
        parser.print_usage(file=sys.stderr)
        sys.stderr.write("{}: error: too few arguments\n".format(APP_NAME))
        sys.exit(-1)
    if args.verbose:
        logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    elif args.debug:
        logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
    else:
        logging.basicConfig(level=logging.WARN, stream=sys.stdout)

    try:
        if (not args.service.startswith('http') and
                'cadc.nrc.ca' not in args.service):
            args.service = SERVICE_ID_PREFIX + args.service

        subject = _get_subject(args)

        client = CadcTapClient(subject, resource_id=args.service,
                               host=args.host, insecure=args.insecure)

        if args.cmd == 'create':
            client.create_table(args.TABLENAME, args.TABLEDEFINITION,
                                args.format)
        elif args.cmd == 'delete':
            reply = input(
                'You are about to delete table {} and its content... '
                'Continue? [yes/no] '.format(args.TABLENAME))
            while True:
                if reply == 'yes':
                    client.delete_table(args.TABLENAME)
                    break
                elif reply == 'no':
                    logger.warn(
                        'Table {} not deleted.'.
                        format(args.TABLENAME))
                    sys.exit(-1)
                else:
                    reply = input('Please reply with yes or no: ')
        elif args.cmd == 'index':
            client.create_index(args.TABLENAME, args.COLUMN, args.unique)
        elif args.cmd == 'load':
            client.load(args.TABLENAME, args.SOURCE, args.format)
        elif args.cmd == 'query':
            if args.input_file is not None:
                with open(args.input_file) as f:
                    query = f.read().strip()
            else:
                query = args.QUERY
            client.query(query, args.output_file, args.format, args.tmptable,
                         timeout=args.timeout, no_column_names=args.quiet,
                         maxrec=args.maxrec)
        elif args.cmd == 'schema':
            client.schema(args.tablename)
        elif args.cmd == 'permission':
            try:
                perms = _get_permission_modes(args)
            except ArgumentError as e:
                permission_parser.print_usage(file=sys.stderr)
                raise e
            client.set_permissions(args.TARGET, read_anon=perms['read_anon'],
                                   read_only=perms['read_only'],
                                   read_write=perms['read_write'])
    except Exception as ex:
        exit_on_exception(ex)
    except KeyboardInterrupt:
        sys.stderr.write('KeyboardInterrupt\n')
        sys.exit(0)
