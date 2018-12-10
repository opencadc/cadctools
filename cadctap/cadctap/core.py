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
import re
import sys
from datetime import datetime
from clint.textui import progress

from cadcutils import net, util, exceptions
from cadcutils.net import wscapabilities
from cadcutils.net import ws
from six.moves import input

from cadctap import version

#import astroquery.cadc as cadc
#from astroquery.cadc import auth

from .youcat import YoucatClient, ALLOWED_CONTENT_TYPES, ALLOWED_TB_DEF_TYPES

import warnings
warnings.filterwarnings("ignore", module='astropy.io.votable.*')

# make the stream bar show up on stdout
progress.STREAM = sys.stdout

__all__ = ['CadcTapClient']

# IVOA dateformat
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
# resource ID for info
DEFAULT_RESOURCE_ID = 'ivo://cadc.nrc.ca/youcat'
APP_NAME = 'cadc-tap'
BASICAA_ID = 'ivo://ivoa.net/sso#BasicAA'
CERTIFICATE_ID = 'ivo://ivoa.net/sso#tls-with-certificate'
TABLES_CAPABILITY = 'ivo://ivoa.net/std/VOSI#tables-1.1'
TAP_CAPABILITY = 'ivo://ivoa.net/std/TAP'
DEFAULT_URI = 'ivo://cadc.nrc.ca/'

logger = logging.getLogger(APP_NAME)


class CadcTapClient(object):
    """Class to access CADC TAP services.

    Example of usage:
    from cadcutils import net
    from cadctap import CadcTapClient

    from astroquery.cadc import Cadc
    from astroquery.cadc import auth

    # create possible types of subjects for CadcTapClient
    anonSubject = net.Subject()
    netrcSubject = net.Subject(netrc=True)
    certSubject = net.Subject(
        certificate='/home/dunnj/Downloads/cadcproxy.pem')

    # create possible types of authentication for astroquery
    anon=auth.AnonAuthMethod()
    netrc=auth.NetrcAuthMethod()
    cert=auth.CertAuthMethod(
        certificate='/home/dunnj/Downloads/cadcproxy.pem')

    client = CadcTapClient(anonSubject) # connect to ivo://cadc.nrc.ca/data
    # get list of tables
    client.get_tables(authentication=anon)

    client = CadcTapClient(certSubject)
    # get list of columns of a table
    client.get_table('caom2.caom2.Observation', authentication=cert)

    client = CadcTapClient(netrcSubject)
    # get the results of a query
    client.run_query(
        query='SELECT TOP 10 type FROM caom2.Observation',
        authentication=netrc)
    """

    def __init__(self, subject, resource_id=DEFAULT_RESOURCE_ID, host=None):
        """
        Instance of a CadcDataClient
        :param subject: the subject(user) performing the action
        :type subject: cadcutils.net.Subject
        :param resource_id: The identifier of the service resource
                            (e.g 'ivo://cadc.nrc.ca/data')
        :param host: Host server for the caom2repo service
        """
        self.logger = logging.getLogger(APP_NAME+'.CadcTapClient')

        self.resource_id = resource_id

        self.host = host

        agent = "{}/{}".format(APP_NAME, version.version)

        self._data_client = net.BaseWsClient(resource_id, subject,
                                             agent, retry=True, host=self.host)
        reader = wscapabilities.CapabilitiesReader()
        web = ws.WsCapabilities(self._data_client, host)
        if 'http' in resource_id:
            service = resource_id.strip('/')
            if not service.endswith('capabilities'):
                service = service + '/capabilities'
        elif 'ivo://' in resource_id:
            service = web._get_capability_url()
        else:
            source = resource_id.strip('/')
            uri = DEFAULT_URI + source
            web.ws.resource_id = uri
            service = web._get_capability_url()
        #print(service)
        content = web._get_content(web.caps_file,
                                   service,
                                   web.last_capstime)
        self._capabilities = reader.parsexml(content.encode('utf-8'))

    def run_query(self, query=None, input_file=None, isasync=False,
                  format='votable', verbose=False, output_file=False,
                  tmptable=None, authentication=None, url=None):
        if input_file is not None:
            with open(input_file) as f:
                adql_query = f.read().strip()
        else:
            adql_query = query
        Cadc = cadc.CadcTAP(url=url, verbose=verbose)
        if isasync is True:
            operation = 'async'
        else:
            operation = 'sync'
        if tmptable is not None:
            tmp = tmptable.split(':')
            tablename = tmp[0]
            tablepath = tmp[1]
        else:
            tablename = None
            tablepath = None
        if output_file is True:
            filename = None
            output = True
        elif output_file is False:
            filename = None
            output = False
        else:
            filename = output_file
            output = True
        try:
            job = Cadc.run_query(adql_query, operation, filename, format,
                                 verbose, output, False,
                                 tablepath, tablename,
                                 authentication)
        except Exception as e:
            if len(e.args) == 1:
                raise exceptions.HttpException(e.args[0])
            elif e.args[1] == 'No such file or directory':
                raise RuntimeError("[Errno "+str(e.args[0])+"] "+e.args[1]+": '"+tablepath+"'")
                
        if output is False:
            print('----------------')
            print('Query Results ')
            print('----------------')
            print(job.get_results(verbose=verbose,
                                  authentication=authentication))


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
        default=DEFAULT_RESOURCE_ID,
        help='set the TAP service. Use ivo format, eg. default is {}'.format(
            DEFAULT_RESOURCE_ID) )


def main_app(command='cadc-tap query'):
    parser = util.get_base_parser(version=version.version,
                                  default_resource_id=DEFAULT_RESOURCE_ID)

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
    schema_parser.add_argument(
        '-c', '--columns',
        default=None,
        help='Name of the table to print the columns.',
        required=False)
    query_parser = subparsers.add_parser(
        'query',
        description=('Run an adql query'),
        help='Run an adql query')
    query_parser.add_argument(
        '-o', '--output-file',
        default=False,
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
    query_parser.add_argument(
        '-a', '--async-job',
        action='store_true',
        help='issue an asynchronous query (default is synchronous'
             ' which only outputs the top 2000 results)',
        required=False)
    query_parser.add_argument(
        '-f', '--format',
        default='votable',
        choices=['votable', 'csv', 'tsv'],
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
        '- Use username/password to run an asynchronous query:\n'
        '      '+command+' "SELECT TOP 10 type FROM caom2.Observation"'
        ' -a -u username\n'
        '- Use netrc file to run a query on the ams/mast service'
        ' :\n'
        '      '+command+' -i data/query.sql -n -s ams/mast\n')

    create_parser = subparsers.add_parser(
        'create',
        description='Create a table',
        help='Create a table')
    create_parser.add_argument(
        '-f', '--format', choices=ALLOWED_TB_DEF_TYPES.keys(),
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
        '-f', '--format', choices=ALLOWED_CONTENT_TYPES.keys(),
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

    #_customize_parser(parser)
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
    show_prints = False
    if args.verbose:
        logging.basicConfig(level=logging.INFO, stream=sys.stdout)
        show_prints = True
    elif args.debug:
        logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
        show_prints = True
    else:
        logging.basicConfig(level=logging.WARN, stream=sys.stdout)

    subject = net.Subject.from_cmd_line_args(args)


    #if args.cmd == 'schema':
    #    feature = TABLES_CAPABILITY
    #elif args.cmd == 'query':
    #    feature = TAP_CAPABILITY
    # create, delete, index, load
    # create a a CadcTap client
    subject = net.Subject.from_cmd_line_args(args)
    client = YoucatClient(subject, resource_id=args.service)
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
        client.query(args.QUERY, args.output_file, args.format, args.tmptable)
    elif args.cmd == 'schema':
        client.schema(args.columns)
    print('DONE')
    sys.exit(0)

    # Following is code design to work with the astroquery.cadc package.
    # Whether we are going to use it or not is still debatable, hence
    # it is kept here.

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
