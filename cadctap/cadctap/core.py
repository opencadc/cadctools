# # -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2017.                            (c) 2017.
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
import re
import os.path
import sys
import time
from datetime import datetime
from clint.textui import progress

from cadcutils import net, util, exceptions
from cadcutils.net import wscapabilities
from cadcutils.net import ws

from cadctap import version

import astroquery.cadc as cadc 
from astroquery.cadc import auth

# make the stream bar show up on stdout
progress.STREAM = sys.stdout

__all__ = ['CadcTapClient']

# IVOA dateformat
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
# resource ID for info
DEFAULT_RESOURCE_ID = 'ivo://cadc.nrc.ca/tap'
APP_NAME = 'cadc-tap'
BASICAA_ID='ivo://ivoa.net/sso#BasicAA'
CERTIFICATE_ID='ivo://ivoa.net/sso#tls-with-certificate'
TABLES_CAPABILITY='ivo://ivoa.net/std/VOSI#tables-1.1'
TAP_CAPABILITY='ivo://ivoa.net/std/TAP'

logger = logging.getLogger(APP_NAME)

class CadcTapClient(object):
    """Class to access CADC archival data.

    Example of usage:
    import os
    from cadcutils import net
    from cadcdata import CadcDataClient

    # create possible types of subjects
    anonSubject = net.Subject()
    certSubject = net.Subject(
       certificate=os.path.join(os.environ['HOME'], ".ssl/cadcproxy.pem"))
    netrcSubject = net.Subject(netrc=True)
    authSubject = net.Subject(netrc=os.path.join(os.environ['HOME'], ".netrc"))

    client = CadcDataClient(anonSubject) # connect to ivo://cadc.nrc.ca/data
    # save fhead of file
    client.get_file('CFHT', '700000o', '/tmp/700000o_fhead', fhead=True)

    client = CadcDataClient(certSubject)
    client.put('TEST', '/tmp/myfile.txt', archive_stream='default')

    client = CadcDataClient(netrcSubject)
    print(client.get_file_info('CFHT', '700000o'))

    client = CadcDataClient(authSubject)
    # get the file in an internal buffer.
    # The file is too large to load into memory. Therefore, for this example,
    # we are retrieving just the wcs info in a buffer.
    import io
    f = io.BytesIO()
    client.get_file('CFHT', '700000o', f, wcs=True)
    print(f.getvalue())

    # process the bytes as they are received - count_bytes proc.
    # Note - the bytes are then thrown to /dev/null
    byte_count = 0
    def count_bytes(bytes):
        global byte_count
        byte_count += len(bytes)

    client.get_file('CFHT', '700000o', f, wcs=True,
                    process_bytes = count_bytes)
    print('Processed {} bytes'.format(byte_count))
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
        reader=wscapabilities.CapabilitiesReader()
        web=ws.WsCapabilities(self._data_client, host)
        content = web._get_content(web.caps_file,
                                   web._get_capability_url(),
                                   web.last_capstime)
        self._capabilities=reader.parsexml(content.encode('utf-8'))

    def get_tables(self, verbose, authentication, url):
        Cadc=cadc.CadcTAP(url=url, verbose=verbose)
        tables=Cadc.get_tables(verbose=verbose, authentication=authentication)
        print('-----------')
        print('Tables')
        print('-----------')
        for table in tables:
            print(table.get_qualified_name())

    def get_table(self, table, verbose, authentication, url):
        Cadc=cadc.CadcTAP(url=url)
        columns=Cadc.get_table(table, verbose=verbose, authentication=authentication)
        if columns is None: 
            raise ValueError("No table exists with the name '%s'" % table)
        print('--------------------')
        print('Columns of ', table)
        print('--------------------')
        for col in columns.get_columns():
            print(col.get_name())

    def run_query(self, query, async, file_name, file_format, verbose, save_to_file,
                  background, upload_file, upload_table_name, authentication, url):
        with open(query) as f:
            file_data=f.read().strip()
        Cadc=cadc.CadcTAP(url=url)
        if async == True:
            operation='async'
        else:
            operation='sync'
        if upload_table_name is not None:
            validname=re.match(r'^[aA-zZ][_|aA-zZ|0-9]*$', upload_table_name)
            if validname is None:
                raise ValueError("Upload table name must start with a letter then have only letters, numbers or an underscore")
            else:
                upload_table_name=validname.group()
        try:
            job=Cadc.run_query(file_data, operation, file_name, file_format, verbose, save_to_file,
                               background, upload_file, upload_table_name, authentication)
        except Exception as e:
            if e.args[0] == 'Internal Server Error':
                raise exceptions.HttpException('500 Internal Server Error, probably from an invaild table form')
            if e.args[0] == 'Bad Request':
                raise exceptions.HttpException('400 Bad Request, probably from an invaild query')
            raise exceptions.HttpException(e.args[0])
        if save_to_file == False and background == False:
            print('----------------')
            print('Query Results ')
            print('----------------')
            print(job.get_results(verbose=verbose, authentication=authentication))
        if background == True:
            print('----------------')
            if operation == 'async':
                jobid=job.get_jobid()
            else:
                jobid='Synchronous queries do not have a jobid'
            print('Jobid: ',jobid)
            print('----------------')
            if save_to_file == True:
                print('Results not saved becuase -b, --background is set')
    
    def load_async_job(self, jobid, file_name, save_to_file, verbose, authentication, url):
        Cadc=cadc.CadcTAP(url=url)
        job=Cadc.load_async_job(jobid, output_file=file_name, verbose=verbose, authentication=authentication)
        if not save_to_file:
            print('-----------')
            print('Query Results')
            print('-----------')
            print(job.get_results(verbose=verbose))
        else:
            Cadc.save_results(job, authentication=authentication, verbose=verbose)

    def list_async_jobs(self, verbose, file_name, save_to_file, authentication, url):
        Cadc=cadc.CadcTAP(url=url)
        job_list=Cadc.list_async_jobs(verbose=verbose, authentication=authentication)
        if not save_to_file:
            print('-----------')
            print('Jobids')
            print('-----------')
            for job in job_list:
                print(job.get_jobid())
        else:
            if file_name is None:
                dateTime = datetime.now().strftime("%Y%m%d%H%M%S")
                file_name='joblist_'+dateTime+'.txt'
            else:
                if not file_name.endswith('.txt'):
                    file_name=file_name+'.txt'
            jlist='Jobids\n'
            for job in job_list:
                jlist=jlist+job.get_jobid()+'\n'
            with open(file_name, "w") as f:
                f.write(jlist)
                f.close()

def main_app():
    parser = util.get_base_parser(version=version.version,
                                  default_resource_id=DEFAULT_RESOURCE_ID)

    parser.description = (
        'Client for accessing databases using TAP service at the Canadian Astronomy '
        'Data Centre (www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca)')

    subparsers = parser.add_subparsers(
        dest='cmd',
        help='supported commands. Use the -h|--help argument of a command '
             'for more details')
    get_tables_parser = subparsers.add_parser(
        'get-tables',
        description='Get a list of the tables in the CADC database',
        help='Get a list of the tables in the CADC database')
    get_tables_parser.epilog = (
        'Examples:\n'
        '- Anonymously list the tables in the database:\n'
        '        cadc-tap get-tables -v\n'
        '- Use certificate to get a list of the tables in the database:\n'
        '        cadc-tap get-tables --cert ~/.ssl/cadcproxy.pem\n'
        '- Use default netrc file ($HOME/.netrc) to the list of tables:\n'
        '        cadc-tap get-tables -v -n\n'
        '- Use a different netrc file to get the list of tables:\n'
        '        cadc-tap get-tables --netrc-file ~/mynetrc\n')

    get_table_parser = subparsers.add_parser(
        'get-table',
        description='Get the columns of a table',
        help='Get the columns of a table')
    get_table_parser.add_argument(
        '-t', '--table',
        help='The table name, must be the qualified name, for example caom2.coam2.Observations.'
             'The qualified table names will be outputed by the get-tables command',
        required=True)
    run_query_parser = subparsers.add_parser(
        'run-query',
        description=('Run an adql query'),
        help='Run an adql query')
    run_query_parser.add_argument(
        '-Q', '--query',
        help='Location of file with an ADQL query to run',
        required=True)
    run_query_parser.add_argument(
        '-a', '--async',
        action='store_true',
        help='Run the query asynchronously, default is to run synchronously which'
             'only outputs the top 2000 results',
        required=False)
    run_query_parser.add_argument(
        '-f', '--file-name',
        default=None,
        help='Name of the file to output the results, default is "operation_datetime"',
        required=False)
    run_query_parser.add_argument(
        '-ff', '--file-format',
        default='votable',
        choices=['votable', 'csv', 'tsv'],
        help='Format of the output file, votable(default), csv or tsv',
        required=False)
    run_query_parser.add_argument(
        '-s', '--save-to-file',
        action='store_true',
        help='Save the output to a file instead of outputing to stdout')
    run_query_parser.add_argument(
        '-b', '--background',
        action='store_true',
        help='Do not return the results, only the jobid. Only affects asychronous queries.')
    run_query_parser.add_argument(
        '-uf', '--upload-file',
        default=None,
        help='File name of the table to upload for the query',
        required=False)
    run_query_parser.add_argument(
        '-un', '--upload-table-name',
        default=None,
        help='Required if --upload_resource is used. Name of the table to upload, to'
             ' reference the table use tap_upload.<tablename>',
        required=False)
    load_async_job_parser = subparsers.add_parser(
        'load-async-job',
        description=('Get the results of a job using the jobid, the query will be'
                     ' run again. If a job was created using authentication, authentication'
                     ' will be needed to load the job, any type'),
        help='Get the results of a job using the jobid, the query will be run again')
    load_async_job_parser.add_argument(
        '-j', '--jobid',
        help='A jobid',
        required=True)
    load_async_job_parser.add_argument(
        '-f', '--file-name',
        default=None,
        help='Name of the file to output the results, default is "operation_datetime"',
        required=False)
    load_async_job_parser.add_argument(
        '-s', '--save-to-file',
        action='store_true',
        help='Flag to save results to a file')
    list_async_jobs_parser = subparsers.add_parser(
        'list-async-jobs',
        description=('List all asynchronous jobs that you have created'),
        help='List all asynchronous jobs that you have created')
    list_async_jobs_parser.add_argument(
        '-s', '--save-to-file',
        action='store_true',
        help='Flag to have list of jobs saved to a text file')
    list_async_jobs_parser.add_argument(
        '-f', '--file-name',
        default=None,
        help='Name of the file to output the results, default is "joblist_datetime.txt"',
        required=False)

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

    args = parser.parse_args()
    if len(sys.argv) < 2:
        parser.print_usage(file=sys.stderr)
        sys.stderr.write("{}: error: too few arguments\n".format(APP_NAME))
        sys.exit(-1)
    show_prints=False
    if args.verbose:
         logging.basicConfig(level=logging.INFO, stream=sys.stdout)
         show_prints=True
    elif args.debug:
         logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
         show_prints=True
    else:
         logging.basicConfig(level=logging.WARN, stream=sys.stdout)
   
    subject = net.Subject.from_cmd_line_args(args)
    if args.user is not None:
       authentication=auth.NetrcAuthMethod(username=args.user)
       security_id=[BASICAA_ID]
    elif args.n is not False:
        authentication=auth.NetrcAuthMethod()
        security_id=[BASICAA_ID]
    elif args.netrc_file is not None:
        authentication=auth.NetrcAuthMethod(filename=args.netrc_file)
        security_id=[BASICAA_ID]
    elif args.cert is not None:
        authentication=auth.CertAuthMethod(certificate=args.cert)
        security_id=[CERTIFICATE_ID]
    else:
        authentication=auth.AnonAuthMethod()
        security_id=[]
    client = CadcTapClient(subject, args.resource_id, host=args.host)
    if args.cmd == 'get-tables' or args.cmd == 'get-table':
        feature=TABLES_CAPABILITY
    else:
        feature=TAP_CAPABILITY  
    check_cap=client._capabilities._caps.get(feature)
    if not security_id:
        method=None
    else:
        method=security_id[0]
    if check_cap.get_interface(method) is None:
        print("Downgrading authentication type to anonymous,\n"
              "type {0} not accepted by resource {1}".format(method, args.resource_id))
        subject = net.Subject()
        authentication=auth.AnonAuthMethod()
        security_id=[]
    url=client._capabilities.get_access_url(feature, security_id)
    if url.endswith('sync') or url.endswith('tables'):
        index=url.rfind('/')
        length=len(url)
        sub_url=url[:-(length-index)]
    else:
        sub_url=url
    try:
        if args.cmd == 'get-tables':
            logger.info('get-tables')
            client.get_tables(show_prints, authentication, sub_url)
        elif args.cmd == 'get-table':
            logger.info('get-table')
            client.get_table(args.table, show_prints, authentication, sub_url)
        elif args.cmd == 'run-query':
            logger.info('run-query')
            client.run_query(args.query, args.async, args.file_name, args.file_format,
                             show_prints, args.save_to_file, args.background,
                             args.upload_file, args.upload_table_name, authentication, 
                             sub_url)
        elif args.cmd == 'load-async-job':
            logger.info('load-async-job')
            client.load_async_job(args.jobid, args.file_name, args.save_to_file, show_prints, authentication, sub_url)
        elif args.cmd == 'list-async-jobs':
            logger.info('list-async-jobs')
            client.list_async_jobs(show_prints, args.file_name, args.save_to_file, 
                                   authentication, sub_url)
    except exceptions.UnauthorizedException:
        if subject.anon:
            handle_error('Operation cannot be performed anonymously. '
                         'Use one of the available methods to authenticate')
        else:
            handle_error('Unexpected authentication problem')
    except exceptions.ForbiddenException:
        handle_error('Unauthorized to perform operation')
    except exceptions.UnexpectedException as e:
        handle_error('Unexpected server error: {}'.format(str(e)))
    except Exception as e:
        handle_error(str(e))

    if errors[0] > 0:
        logger.error('Finished with {} error(s)'.format(errors[0]))
        sys.exit(-1)
    else:
        logger.info("DONE")
