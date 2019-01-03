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
import sys
from clint.textui import progress
import datetime
from cadcutils import net, util
# from cadcutils.net import wscapabilities
# from cadcutils.net import ws
from six.moves import input
import os
from cadctap import version

import magic
from requests_toolbelt.multipart.encoder import MultipartEncoder

logger = logging.getLogger(__name__)

# ID of the default catalog Web service
DEFAULT_SERVICE_ID = 'ivo://cadc.nrc.ca/youcat'
# capabilities ids
TABLES_CAPABILITY_ID = 'ivo://ivoa.net/std/VOSI#tables-1.1'
TABLE_UPDATE_CAPABILITY_ID = 'ivo://ivoa.net/std/VOSI#table-update-async-1.x'
TABLE_LOAD_CAPABILITY_ID = 'ivo://ivoa.net/std/VOSI#table-load-sync-1.x'
QUERY_CAPABILITY_ID = 'ivo://ivoa.net/std/TAP'

# allowed file formats for load
ALLOWED_CONTENT_TYPES = {'tsv': 'text/tab-separated-values',
                         'csv': 'text/csv',
                         'FITSTable': 'application/fits'}
ALLOWED_TB_DEF_TYPES = {'VOSITable': 'text/xml',
                        'VOTable': 'application/x-votable+xml'}

# make the stream bar show up on stdout
progress.STREAM = sys.stdout

__all__ = ['CadcTapClient']

TABLES_CAPABILITY = 'ivo://ivoa.net/std/VOSI#tables-1.1'
TAP_CAPABILITY = 'ivo://ivoa.net/std/TAP'
DEFAULT_URI = 'ivo://cadc.nrc.ca/'

APP_NAME = 'cadc-tap'


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
                 host=None, agent=None):
        """
        Instance of a CadcTapClient
        :param subject: the subject performing the action
        :type cadcutils.auth.Subject
        :param resource_id: the resource ID of the service
        :param host: Host for the caom2repo service
        :param agent: The name of the agent (to be used in server logging)
        """
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
        Creates a table in the catalog service.
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
            elif 'FITS' in t:
                file_type = 'FITSTable'
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
                self._tap_client.post((TABLE_LOAD_CAPABILITY_ID, table_name),
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


def _customize_parser(parser):
    # cadc-tap customizes some of the options inherited from the CADC parser
    # TODO make it work or process list of subparsers
    for i, op in enumerate(parser._actions):
        if op.dest == 'resource_id':
            # Remove --resource-id option for now
            parser._remove_action(parser._actions[i])
            for action in parser._action_groups:
                vars_action = vars(action)
                var_group_actions = vars_action['_group_actions']
                for x in var_group_actions:
                    if x.dest == 'resource_id':
                        var_group_actions.remove(x)
    parser.add_argument(
        '-s', '--service',
        default=DEFAULT_SERVICE_ID,
        help='set the TAP service. Use ivo format, eg. default is {}'.format(
             DEFAULT_SERVICE_ID))


def main_app(command='cadc-tap query'):
    parser = util.get_base_parser(version=version.version,
                                  default_resource_id=DEFAULT_SERVICE_ID)

    _customize_parser(parser)
    parser.description = (
        'Client for accessing databases using TAP protocol at the Canadian '
        'Astronomy Data Centre (www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca)')

    subparsers = parser.add_subparsers(
        dest='cmd',
        help='supported commands. Use the -h|--help argument of a command '
             'for more details')
    schema_parser = subparsers.add_parser(
        'schema',
        description=('Print the tables available for querying.'),
        help='Print the tables available for querying.')
    query_parser = subparsers.add_parser(
        'query',
        description=('Run an adql query'),
        help='Run an adql query')
    query_parser.add_argument(
        '-o', '--output-file',
        default=None,
        help='write query results to file (default is to STDOUT)',
        required=False)
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
        '-f', '--format',
        default='VOTable',
        choices=['VOTable', 'csv', 'tsv'],
        help='output format, either tsv, csv, fits (TBD), or votable(default)',
        required=False)
    query_parser.add_argument(
        '-t', '--tmptable',
        default=None,
        help='Temp table upload, the value is in format: '
             '"tablename:/path/to/table". In query to reference the table'
             ' use tap_upload.tablename',
        required=False)
    query_parser.epilog = (
        'Examples:\n'
        '- Anonymously run a query string:\n'
        '      '+command+' "SELECT TOP 10 type FROM caom2.Observation"\n'
        '- Use certificate to run a query from a file:\n'
        '      '+command+' -i /data/query.sql --cert ~/.ssl/cadcproxy.pem\n'
        '- Use username/password to run a query:\n'
        '      '+command+' "SELECT TOP 10 type FROM caom2.Observation"'
        ' -u username\n'
        '- Use netrc file to run a query on the ams/mast service'
        ' :\n'
        '      '+command+' -i data/query.sql -n -s ams/mast\n')

    create_parser = subparsers.add_parser(
        'create',
        description='Create a table',
        help='Create a table')
    create_parser.add_argument(
        '-f', '--format', choices=sorted(ALLOWED_TB_DEF_TYPES.keys()),
        required=False,
        help='Format of the table definition file')
    create_parser.add_argument(
        'TABLENAME',
        help='name of the table (<schema.table>) in the tap service')
    create_parser.add_argument(
        'TABLEDEFINITION',
        help='file containing the definition of the table or "-" if definition'
        ' in stdin')

    delete_parser = subparsers.add_parser(
        'delete',
        description='Delete a table',
        help='delete a table')
    delete_parser.add_argument(
        'TABLENAME',
        help='name of the table (<schema.table)'
             'in the tap service to be deleted')

    index_parser = subparsers.add_parser(
        'index',
        description='Create a table index',
        help='Create a table index')
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
        description='Load data to a table',
        help='Load data to a table')
    load_parser.add_argument(
        '-f', '--format', choices=sorted(ALLOWED_CONTENT_TYPES.keys()),
        required=False, default='tsv',
        help='Format of the data file')
    load_parser.add_argument(
        'TABLENAME',
        help='name of the table (<schema.table>) to load data to')
    load_parser.add_argument(
        'SOURCE', nargs='+',
        help='source of the data. It can be files or "-" for stdout.'
    )

    # handle errors
    errors = [0]

    def handle_error(msg, exit_after=True):
        """
        Prints error message and exit (by default)
        :param msg: error message to print
        :param exit_after: True if log error message and exit,
        False if log error message and return
        :return:
        """

        errors[0] += 1
        logger.error(msg)
        if exit_after:
            sys.exit(-1)  # TODO use different error codes?

    _customize_parser(schema_parser)
    _customize_parser(query_parser)
    _customize_parser(create_parser)
    _customize_parser(delete_parser)
    _customize_parser(index_parser)
    _customize_parser(load_parser)
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

    subject = net.Subject.from_cmd_line_args(args)

    client = CadcTapClient(subject, resource_id=args.service)
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
        client.query(query, args.output_file, args.format, args.tmptable)
    elif args.cmd == 'schema':
        client.schema()
    print('DONE')


#############################################################################

# Following is code design to work with the astroquery.cadc package.
# Whether we are going to use it or not is still debatable, hence
# it is kept here.

# import warnings
# warnings.filterwarnings("ignore", module='astropy.io.votable.*')
# BASICAA_ID = 'ivo://ivoa.net/sso#BasicAA'
# CERTIFICATE_ID = 'ivo://ivoa.net/sso#tls-with-certificate'
# class CadcTapClient(object):
    # """Class to access CADC TAP services.

    # Example of usage:
    # from cadcutils import net
    # from cadctap import CadcTapClient

    # from astroquery.cadc import Cadc
    # from astroquery.cadc import auth

    # # create possible types of subjects for CadcTapClient
    # anonSubject = net.Subject()
    # netrcSubject = net.Subject(netrc=True)
    # certSubject = net.Subject(
    #     certificate='/home/dunnj/Downloads/cadcproxy.pem')

    # # create possible types of authentication for astroquery
    # anon=auth.AnonAuthMethod()
    # netrc=auth.NetrcAuthMethod()
    # cert=auth.CertAuthMethod(
    #     certificate='/home/dunnj/Downloads/cadcproxy.pem')

    # client = CadcTapClient(anonSubject) # connect to ivo://cadc.nrc.ca/data
    # # get list of tables
    # client.get_tables(authentication=anon)

    # client = CadcTapClient(certSubject)
    # # get list of columns of a table
    # client.get_table('caom2.caom2.Observation', authentication=cert)

    # client = CadcTapClient(netrcSubject)
    # # get the results of a query
    # client.run_query(
    #     query='SELECT TOP 10 type FROM caom2.Observation',
    #     authentication=netrc)
    # """

    # def __init__(self, subject, resource_id=DEFAULT_SERVICE_ID,
    #             host=None):
    #     """
    #     Instance of a CadcDataClient
    #     :param subject: the subject(user) performing the action
    #     :type subject: cadcutils.net.Subject
    #     :param resource_id: The identifier of the service resource
    #                         (e.g 'ivo://cadc.nrc.ca/data')
    #     :param host: Host server for the caom2repo service
    #     """
    #     self.logger = logging.getLogger(APP_NAME+'.CadcTapClient')

    #     self.resource_id = resource_id

    #     self.host = host

    #     agent = "{}/{}".format(APP_NAME, version.version)

    #     self._data_client = net.BaseWsClient(resource_id, subject,
    #                                        agent, retry=True, host=self.host)
    #     reader = wscapabilities.CapabilitiesReader()
    #     web = ws.WsCapabilities(self._data_client, host)
    #     if 'http' in resource_id:
    #         service = resource_id.strip('/')
    #         if not service.endswith('capabilities'):
    #             service = service + '/capabilities'
    #     elif 'ivo://' in resource_id:
    #         service = web._get_capability_url()
    #     else:
    #         source = resource_id.strip('/')
    #         uri = DEFAULT_URI + source
    #         web.ws.resource_id = uri
    #         service = web._get_capability_url()
    #     content = web._get_content(web.caps_file,
    #                                service,
    #                                web.last_capstime)
    #     self._capabilities = reader.parsexml(content.encode('utf-8'))

    # def run_query(self, query=None, input_file=None, isasync=False,
    #               format='votable', verbose=False, output_file=False,
    #               tmptable=None, authentication=None, url=None):
    #     if input_file is not None:
    #         with open(input_file) as f:
    #             adql_query = f.read().strip()
    #     else:
    #         adql_query = query
    #     Cadc = cadc.CadcTAP(url=url, verbose=verbose)
    #     if isasync is True:
    #         operation = 'async'
    #     else:
    #         operation = 'sync'
    #     if tmptable is not None:
    #         tmp = tmptable.split(':')
    #         tablename = tmp[0]
    #         tblpath = tmp[1]
    #     else:
    #         tablename = None
    #         tblpath = None
    #     if output_file is True:
    #        filename = None
    #         output = True
    #     elif output_file is False:
    #         filename = None
    #         output = False
    #     else:
    #         filename = output_file
    #         output = True
    #     try:
    #         job = Cadc.run_query(adql_query, operation, filename, format,
    #                              verbose, output, False,
    #                              tblpath, tablename,
    #                               authentication)
    #     except Exception as e:
    #          if len(e.args) == 1:
    #             raise exceptions.HttpException(e.args[0])
    #         elif e.args[1] == 'No such file or directory':
    #             raise RuntimeError(
    #               "[Errno "+str(e.args[0])+"] "+e.args[1]+": '"+tblpath+"'")
    #
    #     if output is False:
    #         print('----------------')
    #         print('Query Results ')
    #         print('----------------')
    #         print(job.get_results(verbose=verbose,
    #                               authentication=authentication))

    # This goes into the main_app function

    # if args.user is not None:
    #     authentication = auth.NetrcAuthMethod(username=args.user)
    #     security_id = [BASICAA_ID]
    # elif args.n is not False:
    #     authentication = auth.NetrcAuthMethod()
    #     security_id = [BASICAA_ID]
    # elif args.netrc_file is not None:
    #     authentication = auth.NetrcAuthMethod(filename=args.netrc_file)
    #     security_id = [BASICAA_ID]
    # elif args.cert is not None:
    #     authentication = auth.CertAuthMethod(certificate=args.cert)
    #     security_id = [CERTIFICATE_ID]
    # else:
    #     authentication = auth.AnonAuthMethod()
    #     security_id = []
    #
    #
    # client = CadcTapClient(subject, args.service, host=args.host)
    # check_cap = client._capabilities._caps.get(feature)
    # if not security_id:
    #     method = None
    # else:
    #     method = security_id[0]
    # if check_cap.get_interface(method) is None:
    #     logger.info("Downgrading authentication type to anonymous,\n"
    #                 "type {0} not accepted by resource {1}"
    #                 .format(method, args.service))
    #     subject = net.Subject()
    #     authentication = auth.AnonAuthMethod()
    #     security_id = []
    # url = client._capabilities.get_access_url(feature, security_id)
    # if url.endswith('sync') or url.endswith('tables'):
    #     index = url.rfind('/')
    #     length = len(url)
    #     sub_url = url[:-(length-index)]
    # else:
    #     sub_url = url
    # try:
    #     if args.cmd == 'query':
    #         logger.info('query')
    #         client.run_query(args.query, args.input_file, args.async_job,
    #                          args.format, show_prints,
    #                          args.output_file, args.tmptable,
    #                          authentication, sub_url)
    # except exceptions.UnauthorizedException:
    #     if subject.anon:
    #         handle_error('Operation cannot be performed anonymously. '
    #                      'Use one of the available methods to authenticate')
    #     else:
    #         handle_error('Unexpected authentication problem')
    # except exceptions.ForbiddenException:
    #     handle_error('Unauthorized to perform operation')
    # except exceptions.UnexpectedException as e:
    #     handle_error('Unexpected server error: {}'.format(str(e)))
    # except Exception as e:
    #     handle_error(str(e))
    #
    # if errors[0] > 0:
    #     logger.error('Finished with {} error(s)'.format(errors[0]))
    #     sys.exit(-1)
    # else:
    #     logger.info("DONE")
