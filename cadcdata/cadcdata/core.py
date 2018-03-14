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
import os.path
import sys
import time
import socket
from clint.textui import progress

from cadcutils import net, util, exceptions
from cadcdata.transfer import Transfer, TransferReader, TransferWriter

from cadcdata import version

# make the stream bar show up on stdout
progress.STREAM = sys.stdout

__all__ = ['CadcDataClient']

CADC_AD_CAPABILITY_ID = "vos://cadc.nrc.ca~vospace/CADC/std/archive#file-1.0"
# IVOA dateformat
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
# resource ID for info
DEFAULT_RESOURCE_ID = 'ivo://cadc.nrc.ca/data'
# resource ID for negotiating the transfer
TRANSFER_RESOURCE_ID = 'ivo://ivoa.net/std/VOSpace#sync-2.1'
ARCHIVE_STREAM_HTTP_HEADER = 'X-CADC-Stream'
APP_NAME = 'cadc-data'

READ_BLOCK_SIZE = 8 * 1024

logger = logging.getLogger(APP_NAME)


class CadcDataClient(object):
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

    logger = logging.getLogger('CadcDataClient')

    def __init__(self, subject, resource_id=DEFAULT_RESOURCE_ID, host=None):
        """
        Instance of a CadcDataClient
        :param subject: the subject(user) performing the action
        :type subject: cadcutils.net.Subject
        :param resource_id: The identifier of the service resource
                            (e.g 'ivo://cadc.nrc.ca/data')
        :param host: Host server for the caom2repo service
        """

        self.resource_id = resource_id
        self._transfer_writer = TransferWriter()
        self._transfer_reader = TransferReader()

        self.host = host

        agent = "{}/{}".format(APP_NAME, version.version)

        self._data_client = net.BaseWsClient(resource_id, subject,
                                             agent, retry=True, host=self.host)

    def get_file(self, archive, file_name, destination=None, decompress=False,
                 cutout=None, fhead=False, wcs=False, process_bytes=None):
        """
        Get a file from an archive. The entire file is delivered unless the
         cutout argument is present specifying a cutout to extract from file.
        :param archive: name of the archive containing the file
        :param file_name: the name of the file to retrieve
        :param destination: file to save data to (file, file_name, stream or
        anything that supports open/close and write). If None, the file is
        saved locally with the name provided by the content disposion received
        from the service.
        :param decompress: True to decompress the file (if applicable),
        False otherwise
        :param cutout: the arguments of cutout operation to be performed by
        the service
        :param fhead: download just the head of a fits file
        :param wcs: True if the wcs is to be included with the file
        :param process_bytes: function to be applied to the received bytes
        :return: the data stream object
        """
        assert archive is not None
        assert file_name is not None
        params = {}
        if fhead:
            params['fhead'] = fhead
        if wcs:
            params['wcs'] = wcs
        if cutout:
            params['cutout'] = cutout
        file_info = '{}/{}'.format(archive, file_name)
        self.logger.debug('GET {}'.format(file_info))
        # TODO negotiate transfer even for fhead or wcs?
        protocols = self._get_transfer_protocols(archive, file_name,
                                                 params=params)
        if len(protocols) == 0:
            raise exceptions.HttpException('No URLs available to access data')

        # get the list of transfer points
        for protocol in protocols:
            url = protocol.endpoint
            if url is None:
                self.logger.debug(
                    'No endpoint for URI, skipping.')
                continue
            self.logger.debug('GET from URL {}'.format(url))
            try:
                response = self._data_client.get(url, stream=True)

                if destination is not None:
                    if not hasattr(destination, 'read'):
                        # got a destination name?
                        with open(destination, 'wb') as f:
                            self._save_bytes(response, f, file_info,
                                             decompress=decompress,
                                             process_bytes=process_bytes)
                    else:
                        self._save_bytes(response, destination, file_info,
                                         decompress=decompress,
                                         process_bytes=process_bytes)
                else:
                    # get the destination name from the content disposition
                    content_disp = response.headers.get('content-disposition',
                                                        '')
                    destination = file_name
                    for content in content_disp.split():
                        if 'filename=' in content:
                            destination = content[9:]
                    if destination.endswith('gz') and decompress:
                        destination = os.path.splitext(destination)[0]
                    self.logger.debug(
                        'Using content disposition destination name: {}'.
                        format(destination))
                    with open(destination, 'wb') as f:
                        self._save_bytes(response, f, file_info,
                                         decompress=decompress,
                                         process_bytes=process_bytes)
                return
            except (exceptions.HttpException, socket.timeout) as e:
                # try a different URL
                self.logger.info(
                    'WARN: Cannot retrieve data from {}. Exception: {}'.
                    format(url, e))
                self.logger.warn('Try the next URL')
                continue
        raise exceptions.HttpException(
            'Unable to download data from any of the available URLs')

    def _save_bytes(self, response, dest_file, resource, decompress=False,
                    process_bytes=None):
        # requests automatically decompresses the data.
        # Tell it to do it only if it had to
        total_length = 0

        class RawRange(object):
            """
            Wrapper class to make response.raw.read work as iterator and behave
            the same way as the corresponding response.iter_content
            """

            def __init__(self, rsp, decode_content):
                """
                :param rsp: HTTP response object
                """
                self.decode_content = decode_content
                self._read = rsp.raw.read
                self.block_size = 0

            def __iter__(self):
                return self

            def __next__(self):
                return self.next()

            def next(self):
                # reads the next raw block
                data = self._read(self.block_size,
                                  decode_content=self.decode_content)
                if len(data) > 0:
                    return data
                else:
                    raise StopIteration()

            def get_instance(self, block_size):
                self.block_size = block_size
                return self

        try:
            total_length = int(response.headers.get('content-length', 0))
        except ValueError:
            pass

        rr = RawRange(response, decompress)
        reader = rr.get_instance
        if self.logger.isEnabledFor(logging.INFO):
            if total_length != 0:
                chunks = progress.bar(reader(
                    READ_BLOCK_SIZE),
                    expected_size=((total_length / READ_BLOCK_SIZE) + 1))
            else:
                chunks = progress.mill(reader(READ_BLOCK_SIZE),
                                       expected_size=0)
        else:
            chunks = reader(READ_BLOCK_SIZE)
        start = time.time()
        for chunk in chunks:
            if process_bytes is not None:
                process_bytes(chunk)
            dest_file.write(chunk)
        duration = time.time() - start
        self.logger.info(
            'Successfully downloaded file {} as {} in {}s (avg. speed: {}MB/s)'
            ''.format(resource, dest_file.name, round(duration, 2),
                      round(total_length / 1024 / 1024 / duration, 2)))

    def put_file(self, archive, src_file, archive_stream=None):
        """
        Puts a file into the archive storage
        :param archive: name of the archive
        :param src_file: location of the source file
        :param archive_stream: specific archive stream
        """
        assert archive is not None

        # We actually raise an exception here since the web
        # service will normally respond with a 200 for an
        # anonymous put, though not provide any endpoints.
        if self._data_client.subject.anon:
            raise exceptions.UnauthorizedException(
                'Must be authenticated to put data')

        self.logger.debug('PUT {}/{}'.format(archive, src_file))
        headers = {}
        if archive_stream is not None:
            headers[ARCHIVE_STREAM_HTTP_HEADER] = str(archive_stream)

        protocols = self._get_transfer_protocols(
            archive, src_file, is_get=False, headers=headers)
        if len(protocols) == 0:
            raise exceptions.HttpException('No URLs available to put data to')

        # get the list of transfer points
        for protocol in protocols:
            url = protocol.endpoint
            if url is None:
                self.logger.debug(
                    'No endpoint for URI, skipping.')
                continue
            self.logger.debug('PUT to URL {}'.format(url))

            try:
                start = time.time()
                with open(src_file, 'rb') as f:
                    self._data_client.put(url, headers=headers, data=f)
                duration = time.time() - start
                stat_info = os.stat(src_file)
                self.logger.info(
                    ('Successfully uploaded archive/file {}/{} in {}s '
                     '(avg. speed: {}MB/s)').format(
                        archive, src_file, round(duration, 2),
                        round(stat_info.st_size / 1024 / 1024 / duration, 2)))
                return
            except (exceptions.HttpException, socket.timeout) as e:
                # try a different URL
                self.logger.info('WARN: Cannot put data to {}. Exception: {}'.
                                 format(url, e))
                self.logger.warn('Try the next URL')
                continue
        raise exceptions.HttpException(
            'Unable to put data from any of the available URLs')

    def get_file_info(self, archive, file_name):
        """
        Get information regarding a file in the archive
        :param archive: Name of the archive
        :param file_name: name of the file
        :returns dictionary of attributes/values
        """
        assert archive is not None
        assert file_name is not None
        resource = (CADC_AD_CAPABILITY_ID, '{}/{}'.format(archive, file_name))
        self.logger.debug('HEAD {}'.format(resource))
        response = self._data_client.head(resource)
        h = response.headers
        hmap = {'name': 'Content-Disposition',
                'size': 'Content-Length',
                'md5sum': 'Content-MD5',
                'type': 'Content-Type',
                'encoding': 'Content-Encoding',
                'lastmod': 'Last-Modified',
                'usize': 'X-Uncompressed-Length',
                'umd5sum': 'X-Uncompressed-MD5'}
        file_info = {'archive': archive}
        for key in hmap:
            file_info[key] = h.get(hmap[key], None)
        if file_info['name'] is not None:
            file_info['name'] = file_info['name'].replace(
                'inline; filename=', '')
        # TODO file_info['ingest_date'] = h[?]
        self.logger.debug("File info: {}".format(file_info))
        return file_info

    def _get_transfer_protocols(self, archive, file_name, is_get=True,
                                headers=None, params=None):
        if headers is None:
            headers = {}
        if 'MAST' == archive:
            uri_transfer = 'mast:{}'.format(file_name)
        else:
            uri_transfer = 'ad:{}/{}'.format(archive, file_name)
        # Direction-dependent setup
        if is_get:
            tran = Transfer(uri_transfer, 'pullFromVoSpace')
        else:
            tran = Transfer(uri_transfer, 'pushToVoSpace')

        # obtain list of endpoints by sending a transfer document and
        # looking at the URLs in the returned document
        request_xml = self._transfer_writer.write(tran)
        h = headers.copy()
        h['Content-Type'] = 'text/xml'
        logger.debug(request_xml)
        response = self._data_client.post(
            resource=(TRANSFER_RESOURCE_ID, None),
            data=request_xml, headers=h, params=params)
        response_str = response.text.encode('utf-8')

        self.logger.debug('POST had {} redirects'.format(
            len(response.history)))
        self.logger.debug('Response code: {}, URL: {}'.format(
            response.status_code, response.url))
        self.logger.debug('Full XML response:\n{}'.format(response_str))

        tran = self._transfer_reader.read(response_str)
        return tran.protocols


def main_app():
    parser = util.get_base_parser(version=version.version,
                                  default_resource_id=DEFAULT_RESOURCE_ID)

    parser.description = (
        'Client for accessing the data Web Service at the Canadian Astronomy '
        'Data Centre (www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/data)')

    subparsers = parser.add_subparsers(
        dest='cmd',
        help='supported commands. Use the -h|--help argument of a command '
             'for more details')
    get_parser = subparsers.add_parser(
        'get',
        description='Retrieve files from a CADC archive',
        help='Retrieve files from a CADC archive')
    get_parser.add_argument(
        '-o', '--output',
        help='space-separated list of destination files '
             '(quotes required for multiple elements)',
        required=False)
    get_parser.add_argument(
        '--cutout', nargs='*',
        help=('specify one or multiple extension and/or pixel range cutout '
              'operations to be performed. Use cfitsio syntax'),
        required=False)
    get_parser.add_argument(
        '-z', '--decompress', help='decompress the data (gzip only)',
        action='store_true', required=False)
    get_parser.add_argument(
        '--wcs', help='return the World Coordinate System (WCS) information',
        action='store_true', required=False)
    get_parser.add_argument(
        '--fhead', help='return the FITS header information',
        action='store_true', required=False)
    get_parser.add_argument('archive', help='CADC archive')
    get_parser.add_argument(
        'filename', help='the name of the file in the archive', nargs='+')
    get_parser.epilog = (
        'Examples:\n'
        '- Anonymously getting a public file:\n'
        '        cadc-data get -v GEMINI 00aug02_002.fits\n'
        '- Use certificate to get a cutout and save it to a file:\n'
        '        cadc-data get --cert ~/.ssl/cadcproxy.pem -o '
        '/tmp/700000o-cutout.fits --cutout [1] CFHT 700000o\n'
        '- Use default netrc file ($HOME/.netrc) to get FITS header of a '
        'file:\n'
        '        cadc-data get -v -n --fhead GEMINI 00aug02_002.fits\n'
        '- Use a different netrc file to download wcs information:\n'
        '        cadc-data get -d --netrc ~/mynetrc -o /tmp/700000o-wcs.fits '
        '--wcs CFHT 700000o\n'
        '- Connect as user to download two files and uncompress them '
        '(prompt for password if user not in $HOME/.netrc):\n'
        '        cadc-data get -v -u auser -z GEMINI 00aug02_002.fits '
        '00aug02_003.fits')

    put_parser = subparsers.add_parser(
        'put',
        description='Upload files into a CADC archive',
        help='Upload files into a CADC archive')
    put_parser.add_argument(
        '-s', '--archive-stream',
        help='specific archive stream to add the file to',
        required=False)
    put_parser.add_argument('-c', '--compress', help='gzip compress the data',
                            action='store_true', required=False)
    put_parser.add_argument('archive', help='CADC archive')
    put_parser.add_argument(
        'source',
        help='file or directory containing the files to be put', nargs='+')
    put_parser.epilog = (
        'Examples:\n'
        '- Use certificate to put a file in an archive stream:\n'
        '        cadc-data put --cert ~/.ssl/cadcproxy.pem -as default TEST '
        'myfile.fits.gz\n'
        '- Use default netrc file ($HOME/.netrc) to put two files:\n'
        '        cadc-data put -v -n TEST myfile1.fits.gz myfile2.fits.gz\n'
        '- Use a different netrc file to put files from a directory:\n'
        '        cadc-data put -d --netrc ~/mynetrc TEST dir\n'
        '- Connect as user to put files from multiple sources (prompt for '
        'password if user not in $HOME/.netrc):\n'
        '        cadc-data put -v -u auser TEST myfile.fits.gz dir1 dir2')

    info_parser = subparsers.add_parser(
        'info',
        description=('Get information regarding files in a '
                     'CADC archive on the form:\n'
                     'File:\n'
                     '\t -name\n'
                     '\t -size\n'
                     '\t -md5sum\n'
                     '\t -encoding\n'
                     '\t -type\n'
                     '\t -usize\n'
                     '\t -umd5sum\n'
                     # '\t -ingest_date\n'
                     '\t -lastmod'),
        help='Get information regarding files in a CADC archive')
    info_parser.add_argument('archive', help='CADC archive')
    info_parser.add_argument('filename',
                             help='the name of the file in the archive',
                             nargs='+')
    info_parser.epilog = (
        'Examples:\n'
        '- Anonymously getting information about a public file:\n'
        '        cadc-data info GEMINI 00aug02_002.fits\n'
        '- Use certificate to get information about a file:\n'
        '        cadc-data info --cert ~/.ssl/cadcproxy.pem CFHT 700000o\n'
        '- Use default netrc file ($HOME/.netrc) to get information about '
        'a file:\n'
        '        cadc-data info -n GEMINI 00aug02_002.fits\n'
        '- Use a different netrc file to get information about a file:\n'
        '        cadc-data info --netrc ~/mynetrc CFHT 700000o\n'
        '- Connect as user to get information about two files '
        '(prompt for password if user not in $HOME/.netrc):\n'
        '        cadc-data info -u auser GEMINI 00aug02_002.fits '
        '00aug02_003.fits')

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
    if args.verbose:
        logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    elif args.debug:
        logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
    else:
        logging.basicConfig(level=logging.WARN, stream=sys.stdout)

    subject = net.Subject.from_cmd_line_args(args)

    client = CadcDataClient(subject, args.resource_id, host=args.host)
    try:
        if args.cmd == 'get':
            logger.info('get')
            archive = args.archive
            file_names = args.filename
            if args.output is not None:
                files = args.output.split()
                if len(files) != len(file_names):
                    handle_error(
                        'Different size of destination files list ({}) '
                        'and list of file names ({})'.format(files,
                                                             file_names))
                for f, fname in list(zip(files, file_names)):
                    try:
                        client.get_file(
                            archive, fname, f, decompress=args.decompress,
                            fhead=args.fhead, wcs=args.wcs, cutout=args.cutout)
                    except exceptions.NotFoundException:
                        handle_error('File name {} not found'.format(fname),
                                     exit_after=False)
            else:
                for fname in file_names:
                    try:
                        client.get_file(
                            archive, fname, None, decompress=args.decompress,
                            fhead=args.fhead, wcs=args.wcs, cutout=args.cutout)
                    except exceptions.NotFoundException:
                        handle_error('File name not found {}'.format(fname),
                                     exit_after=False)
        elif args.cmd == 'info':
            logger.info('info')
            archive = args.archive
            for file_name in args.filename:
                try:
                    file_info = client.get_file_info(archive, file_name)
                except exceptions.NotFoundException:
                    handle_error(
                        'File name {} not found in archive {}'.format(
                            file_name, archive),
                        exit_after=False)
                    continue
                print('File {}:'.format(file_name))
                for field in sorted(file_info):
                    print('\t {:>10}: {}'.format(field, file_info[field]))
        else:
            logger.info('put')
            archive = args.archive
            sources = args.source

            files = []
            for file1 in sources:
                if os.path.isfile(file1):
                    files.append(file1)
                elif os.path.isdir(file1):
                    for f in os.listdir(file1):
                        if os.path.isfile(os.path.join(file1, f)):
                            files.append(os.path.join(file1, f))
                        else:
                            logger.warn(
                                '{} not added to the list of files to '
                                'put'.format(f))
            logger.debug('Files to put: {}'.format(files))
            if len(files) == 0:
                handle_error('No files found to put')
            for f in files:
                client.put_file(archive, f, archive_stream=args.archive_stream)
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
