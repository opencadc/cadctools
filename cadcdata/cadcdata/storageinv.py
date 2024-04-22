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
import os
import os.path
import sys
import time
from clint.textui import progress
import hashlib
import datetime
import traceback
from urllib.parse import urlparse, urlencode
import argparse

from cadcutils import net, util, exceptions
from cadcutils.util import date2ivoa

from cadcdata import version

CADC_AC_SERVICE = 'ivo://cadc.nrc.ca/gms'
CADC_LOGIN_CAPABILITY = 'ivo://ivoa.net/std/UMS#login-0.1'
CADC_SSO_COOKIE_NAME = 'CADC_SSO'
CADC_REALMS = ['.canfar.net', '.cadc-ccda.hia-iha.nrc-cnrc.gc.ca',
               '.cadc.dao.nrc.ca']
SUPPORTED_SERVER_VERSIONS = {'storage-inventory/raven': '1.0',
                             'storage-inventory/minoc': '1.0'}

MAGIC_WARN = None
try:
    import magic
except ImportError as e:
    if 'libmagic' in str(e):
        MAGIC_WARN = ('Can not determine the MIME info. Please install '
                      'libmagic system library or explicitly specify MIME '
                      'type and encoding for each file.')
    else:
        raise e


# make the stream bar show up on stdout
progress.STREAM = sys.stdout

__all__ = ['StorageInventoryClient', 'FileInfo', 'cadcput_cli', 'cadcget_cli',
           'cadcinfo_cli', 'cadcremove_cli']

# IVOA dateformat
DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'
# default inventory storage resource ID
DEFAULT_RESOURCE_ID = 'ivo://cadc.nrc.ca/global/raven'
# data resource ID - shell service backwards compatible with the initial CADC
# data web service
DATA_RESOURCE_ID = 'ivo://cadc.nrc.ca/data'

# resource IDs for file transfers (LOCATE is for transfer negotiation
# and FILES is for direct access)
FILES_STANDARD_ID = 'http://www.opencadc.org/std/storage#files-1.0'
LOCATE_STANDARD_ID = 'http://www.opencadc.org/std/storage#locate-1.0'

# size of the read blocks in data transfers
READ_BLOCK_SIZE = 8 * 1024
logger = logging.getLogger(__name__)

# maximum number of times to try an URL with transient error
MAX_TRANSIENT_TRIES = 3


# TODO This is a dataclass for when Py3.7 becomes the minimum supported version
class FileInfo:
    """
    Container for the metadata of a file:
        - ID
        - size
        - name
        - md5sum
        - file_type
        - encoding
    """
    def __init__(self, id, size=None, name=None, md5sum=None, lastmod=None,
                 file_type=None, encoding=None):
        if not id:
            raise AttributeError(
                'ID of the file in Storage Inventory is required')
        self.id = id
        self.size = size
        self.name = name
        self.md5sum = md5sum
        self.lastmod = lastmod
        self.file_type = file_type
        self.encoding = encoding

    def __str__(self):
        return (
            'id={}, name={}, size={}, type={}, encoding={}, last modified={}, '
            'md5sum={}'.format(self.id, self.name, self.size, self.file_type,
                               self.encoding, date2ivoa(self.lastmod),
                               self.md5sum))


def handle_error(exception, exit_after=True):
    """
    Prints error message and exit (by default)
    :param msg: error message to print
    :param exit_after: True if log error message and exit,
    False if log error message and return
    :return:
    """

    if isinstance(exception, exceptions.UnauthorizedException):
        # TODO - magic authentication
        # if subject.anon:
        #     handle_error('Operation cannot be performed anonymously. '
        #                  'Use one of the available methods to authenticate')
        # else:
        print('ERROR: Unexpected authentication problem')
    elif isinstance(exception, exceptions.NotFoundException):
        print('ERROR: Not found: {}'.format(str(exception)))
    elif isinstance(exception, exceptions.ForbiddenException):
        print('ERROR: Unauthorized to perform operation')
    elif isinstance(exception, exceptions.UnexpectedException):
        print('ERROR: Unexpected server error: {}'.format(str(exception)))
    else:
        print('ERROR: {}'.format(exception))

    if logger.isEnabledFor(logging.DEBUG):
        traceback.print_stack()

    if exit_after:
        sys.exit(-1)  # TODO use different error codes?


def validate_uri(uri, strict=True):
    """
    Validate URI format for ID
    :param uri:
    :param strict: need to include a scheme
    :return: None if uri valid or raises AttributeError otherwise
    """
    if not uri:
        raise AttributeError('URI required')
    res = urlparse(uri)
    if strict and not res.scheme:
        raise ValueError(
            '{} not a valid id (missing URI scheme)'.format(uri))


def validate_get_uri(uri):
    """
    Validate ID URI format for a get command that accepts parameters such as
    CUTOUT
    :param uri:
    :param strict: need to include a scheme
    :return: None if uri valid or raises AttributeError otherwise
    """
    validate_uri(uri, False)
    square_error_msg = \
        'Typo? Square brackets ([]) only allowed with the "CUTOUT=" parameters: ' + uri
    if ('[' in uri) or (']' in uri):
        res = urlparse(uri)
        if res.query:
            for param in res.query.lower().split('&'):
                if (('[' in param) or (']' in param)) and 'cutout=[' not in param:
                    raise ValueError(square_error_msg)
        else:
            raise ValueError(square_error_msg)


def argparse_validate_uri(uri):
    """
    Same as `validate_uri` but customized to be used with argparse
    :param uri:
    :return:
    """
    try:
        validate_uri(uri, False)
    except AttributeError as e:
        raise argparse.ArgumentTypeError(str(e))
    return uri


def argparse_validate_uri_strict(uri):
    """
    Same as `validate_uri` strict but customized to be used with argparse
    :param uri:
    :return:
    """
    try:
        validate_uri(uri, True)
    except (AttributeError, ValueError) as e:
        raise argparse.ArgumentTypeError(str(e))
    return uri


def argparse_validate_get_uri(uri):
    """
    Same as `validate__get_uri` but customized to be used with argparse
    :param uri:
    :return:
    """
    try:
        validate_get_uri(uri)
    except (AttributeError, ValueError) as e:
        raise argparse.ArgumentTypeError(str(e))
    return uri


def _fix_uri(func):
    def wrapper(*args, **kwargs):
        if 'id' in kwargs:
            id = kwargs['id']
        else:
            id = args[1]
        fixed = args[0]._get_uris(id)
        for uri in fixed:
            if 'id' in kwargs:
                kwargs['id'] = uri
            else:
                tmp = list(args)
                tmp[1] = uri
                args = tuple(tmp)
            try:
                return func(*args, **kwargs)
            except exceptions.NotFoundException:
                if id != uri:
                    logger.debug(uri + ' not found.')
        if len(fixed) > 1:
            logger.debug('Not found any of the possible URIs: {}'.format(' '.join(fixed)))
        raise exceptions.NotFoundException(id)
    return wrapper


class StorageInventoryClient(object):
    """Class to access CADC storage inventory.

    Example of usage:
    import os
    from cadcutils import net
    from cadcdata import StorageInventoryClient

    # create possible types of subjects
    anonSubject = net.Subject()
    certSubject = net.Subject(
       certificate=os.path.join(os.environ['HOME'], ".ssl/cadcproxy.pem"))
    netrcSubject = net.Subject(netrc=True)
    authSubject = net.Subject(netrc=os.path.join(os.environ['HOME'], ".netrc"))

    client = StorageInventoryClient(anonSubject)
    # save file
    client.cadcget('cadc:CFHT/700000o.fits.fz', '/tmp/700000o.fits.fz')

    client = StorageInventoryClient(certSubject)
    client.cadcput('cadc:/TEST/myfile.txt', '/tmp/myfile.txt')

    client = StorageInventoryClient(netrcSubject)
    print(client.cadcinfo('cadc:CFHT/700000o.fits.fz'))

    client = StorageInventoryClient(authSubject)
    # get the file in an internal buffer.
    buffer = os.BytesIO()
    client.cadcget('cadc:TEST/myfile.txt', dest=buffer)
    print(client.getvalue())

    # process the bytes as they are received - count_bytes proc.
    # Note - the bytes are then thrown to /dev/null
    byte_count = 0
    def count_bytes(bytes):
        global byte_count
        byte_count += len(bytes)

    client.cadcget('cadc:CFHT/700000o.fits.fz', f,
    process_bytes = count_bytes)
    print('Processed {} bytes'.format(byte_count))
    """

    def __init__(self, subject=net.Subject(), resource_id=DEFAULT_RESOURCE_ID,
                 host=None, insecure=False):
        """
        Instance of a StorageInventoryClient
        :param subject: the subject(user) performing the action
        :type subject: cadcutils.net.Subject
        :param resource_id: The identifier of the service resource
                            (e.g 'ivo://cadc.nrc.ca/data')
        :param host: Host server for the caom2repo service
        :param insecure Allow insecure server connections over SSL
        """

        self.resource_id = resource_id
        self.host = host
        agent = '{}/{}'.format('SIClient', version.version)
        util.check_version(version=version.version)
        # TODO
        # Storage Inventory does not support Basic Auth. The following block
        # retrieves a cookie instead. It is temporary until the token spec
        # is finalized
        if resource_id.startswith('ivo://cadc.nrc.ca') and\
           net.auth.SECURITY_METHODS_IDS['basic'] in \
           subject.get_security_methods():
            login = net.BaseWsClient(CADC_AC_SERVICE, net.Subject(),
                                     agent,
                                     retry=True, host=self.host,
                                     server_versions=SUPPORTED_SERVER_VERSIONS)
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

        self._cadc_client = net.BaseDataClient(
            resource_id, subject,
            agent, retry=True, host=self.host,
            insecure=insecure,
            server_versions=SUPPORTED_SERVER_VERSIONS)

        # for now, this is only used to get the pub schema-archive mapping info
        self._data_client = net.BaseWsClient(DATA_RESOURCE_ID, net.Subject(),
                                             agent, retry=True, host=self.host,
                                             insecure=insecure,
                                             server_versions=SUPPORTED_SERVER_VERSIONS)

    @property
    def transfer(self):
        """
        Denotes a global service capable of transfer negotiation
        :return:
        """
        try:
            return \
                self._cadc_client._get_url((LOCATE_STANDARD_ID, None))
        except KeyError:
            return None

    @property
    def files(self):
        """
        Denotes a local service where files are directly accessible
        :return:
        """
        try:
            return \
                self._cadc_client._get_url((FILES_STANDARD_ID, None))
        except KeyError:
            return None

    @_fix_uri
    def cadcget(self, id, dest=None, fhead=False, process_bytes=None):
        """
        Get a file from an archive. The entire file is delivered unless the
        cutout argument is present in the id in which case only the
        specified sections of a FITS file are downloaded.
        :param id: the CADC Storage Inventory identifier (URI) of the file to
        retrieve. If the scheme in the URI is missing, the system will try to
        guess it and return the first match. The id could also contain cutout
        parameters if only parts of a FITS file are required ex:
        CFHT/806045o.fits.fz?cutout=[1][10:120,20:30]&cutout=[2][10:120,20:30]
        :param dest: file to save data to (file, file_name, stream or
        anything that supports open/close and write).
        :param fhead: return the FITS header information (for all extensions)
        :param process_bytes: function to be applied to the received bytes
        """

        validate_get_uri(id)
        logger.debug('cadcget GET {} -> {}'.format(id, dest))
        params = {}
        uri = urlparse(id)
        if 'cutout=[' in uri.query.lower():
            lquery = uri.query.lower()
            params['SUB'] = [x.strip('&') for x in lquery.split('cutout=')[1:]]
            id = uri.scheme + ":" + uri.path
        urls = self._get_transfer_urls(id, params=params)
        if len(urls) == 0:
            raise exceptions.HttpException('No URLs available to access data')
        last_exception = None
        if fhead:
            if params and ('SUB' in params):
                raise AttributeError(
                    'Cannot perform fhead and cutout at the same time')
            else:
                params['META'] = 'true'
        for url in urls:
            logger.debug('GET from URL {}'.format(url))
            try:
                self._cadc_client.download_file(url=url, dest=dest, params=params)
                return
            except Exception as e:
                # try a different URL
                logger.debug(
                    'WARN: Cannot retrieve data from {}. Exception: {}'.
                    format(url, e))
                last_exception = e
                if isinstance(e, exceptions.TransferException) and \
                        urls.count(url) < MAX_TRANSIENT_TRIES:
                    # this is a transient exception - append url to try later
                    logger.debug('Transient error, retry later: {} - {}'.
                                 format(url, str(e)))
                    urls.append(url)
                if urls:
                    logger.debug('Try the next URL')
        if last_exception:
            raise last_exception
        else:
            raise exceptions.HttpException(
                'BUG: Unable to download data to any of the available URLs')

    def cadcput(self, id, src, replace=False, file_type=None,
                file_encoding=None, md5_checksum=None):
        """
        Puts a file into the inventory system
        :param id: unique identifier (URI) for the file in the CADC inventory
        system. The URI must include the scheme.
        :param src: location of the source file
        :param replace: boolean indicated whether this is expected to be
        a replacement of an existing file in the inventory system or not (file
        is new). Wrong assumption results in an error. This is a safeguard
        for accidental file replacements.
        :param file_type: file MIME type
        :param file_encoding: file MIME encoding
        :param md5_checksum: md5 sum of the content. For replacements,
        the content will not be sent over if the md5_checksum of a replaced
        file matches the source one. Bytes are always transferred when this
        argument is not provided.
        """
        validate_uri(id)
        # We actually raise an exception here since the web
        # service will normally respond with a 200 for an
        # anonymous put, though not provide any endpoints.
        if self._cadc_client.subject.anon:
            raise exceptions.UnauthorizedException(
                'Must be authenticated to put data')

        headers = {}

        try:
            file_info = self.cadcinfo(id)
        except exceptions.NotFoundException:
            file_info = None

        if file_info and not replace:
            raise AttributeError('Attempting to override identifier {} '
                                 'without using the replace flag'.format(id))
        if not file_info and replace:
            raise AttributeError('Attempting to put a new identifier {} '
                                 'using the replace flag'.format(id))

        if file_type is not None:
            mtype = file_type
        elif MAGIC_WARN:
            mtype = None
            logger.warning(MAGIC_WARN)
        else:
            m = magic.Magic(mime=True)
            mtype = m.from_file(os.path.realpath(src))
        if mtype is not None:
            headers['Content-Type'] = mtype
            logger.debug('Set MIME type: {}'.format(mtype))

        if file_encoding:
            mencoding = file_encoding
        elif MAGIC_WARN:
            mencoding = None
            if mtype:
                logger.warning(MAGIC_WARN)
        else:
            m = magic.Magic(mime_encoding=True)
            mencoding = m.from_file(os.path.realpath(src))
        if mencoding:
            headers['Content-Encoding'] = mencoding
            logger.debug('Set MIME encoding: {}'.format(mencoding))

        if md5_checksum:
            net.add_md5_header(headers, md5_checksum=md5_checksum)

        operation = 'put'
        if replace and md5_checksum and (file_info.md5sum == md5_checksum):
            if (file_info.file_type != headers['Content-Type']) or \
               (file_info.encoding != headers['Content-Encoding']):
                operation = 'post'
            else:
                logger.info(
                    'Source {} already in the storage inventory'.format(src))
                return

        urls = self._get_transfer_urls(id, is_get=False)
        if len(urls) == 0:
            raise exceptions.HttpException('No URLs available to put data to')

        last_exception = None
        # get the list of transfer points
        for url in urls:
            if operation == 'post':
                logger.debug('POST to URL {}'.format(url))
                start = time.time()
                result = self._cadc_client.post(url, headers=headers)
                result.raise_for_status()
                duration = time.time() - start
                logger.info('Updated metadata for identifier {} in {} ms'.
                            format(id, duration))
                return
            logger.debug('PUT to URL {}'.format(url))
            try:
                file_info = os.stat(src)
                start = time.time()
                self._cadc_client.upload_file(
                    url=url,
                    src=src,
                    md5_checksum=md5_checksum,
                    headers=headers)
                duration = time.time() - start
                logger.info(
                    ('Successfully uploaded file {} in {}s '
                     '(avg. speed: {}MB/s)').format(
                        id, round(duration, 2),
                        round(file_info.st_size / 1024 / 1024 / duration, 2)))
                return
            except Exception as e:
                last_exception = e
                if isinstance(e, exceptions.TransferException) and \
                        urls.count(url) < MAX_TRANSIENT_TRIES:
                    # this is a transient exception - append url to try later
                    urls.append(url)
                # try a different URL
                logger.debug('WARN: Cannot {} data to {}. Exception: {}'.
                             format(operation, url, e))
                if urls:
                    logger.debug('Try the next URL')
                continue
        if last_exception:
            raise last_exception
        else:
            raise exceptions.HttpException(
                'Unable to {} data from any of the available '
                'URLs'.format(operation))

    def cadcremove(self, id):
        """
        Removes a file into the inventory system. `NotFoundException` is raised
        if the `id` is not found.
        :param id: unique identifier (URI) for the file in the CADC inventory
        system. The URI must include the scheme.
        """

        # We actually raise an exception here since the web
        # service will normally respond with a 200 for an
        # anonymous put, though not provide any endpoints.
        validate_uri(id)
        if self._cadc_client.subject.anon:
            raise exceptions.UnauthorizedException(
                'Must be authenticated to remove data')

        # check file is there
        self.cadcinfo(id)
        urls = self._get_transfer_urls(id, is_get=False)
        if len(urls) == 0:
            raise exceptions.NotFoundException(
                'File not found: {}'.format(id))

        error_msg = ''
        for url in urls:
            logger.debug(
                'REMOVE file with identifier {} from URL {}'.format(id, url))
            try:
                start = time.time()
                self._cadc_client.delete(url)
                duration = time.time() - start
                logger.info('{} removed in {} ms'.format(id, duration))
                return
            except Exception as e:
                logger.debug('WARN: Cannot remove data from {}. Exception: {}'.
                             format(url, e))
                error_msg += str(e) + '\n'
                if urls:
                    # try a different URL
                    logger.debug('Try the next URL')
                continue
        # no successful DELETE so far. Double check the existence of file
        # in global
        try:
            self.cadcinfo(id=id)
        except exceptions.NotFoundException:
            logger.debug('{} not in global anymore. File removed')
            raise exceptions.NotFoundException(id)
        raise exceptions.HttpException(error_msg)

    @_fix_uri
    def cadcinfo(self, id):
        """
        Get information regarding a file in SI.
        :param id: unique identifier (URI) for the file in the CADC inventory
        system. If the scheme in the URI is missing, the system will try to
        guess it and return the first match.
        :returns FileInfo object with the file metadata
        """
        validate_uri(id)
        resource = (FILES_STANDARD_ID, id)
        logger.debug('HEAD {}'.format(resource))
        try:
            response = self._cadc_client.head(resource, allow_redirects=True)
        except exceptions.NotFoundException as e:
            raise exceptions.NotFoundException(id, e)
        h = response.headers
        file_info = FileInfo(id)
        size = h.get('Content-Length', None)
        if size is not None:
            file_info.size = int(size)
        file_info.md5sum = net.extract_md5(response.headers)
        file_info.name = net.netutils.get_header_filename(response.headers)
        if h.get('Last-Modified', None):
            file_info.lastmod = \
                datetime.datetime.strptime(h.get('Last-Modified'),
                                           '%a, %d %b %Y %H:%M:%S %Z')
        file_info.file_type = h.get('Content-Type', None)
        file_info.encoding = h.get('Content-Encoding', None)
        logger.debug('File info: {}'.format(file_info))
        return file_info

    def _get_transfer_urls(self, id, params=None, is_get=True):
        if not self.transfer:
            # this is site location
            return ['{}/{}'.format(self.files, id)]
        trans = net.Transfer(self._cadc_client._get_session(), )
        return trans.transfer(
            endpoint_url=self.transfer, uri=id,
            direction='pullFromVoSpace' if is_get else 'pushToVoSpace',
            with_uws_job=False, cutout=params)

    def _get_md5sum(self, filename):
        # return the md5sum of a file
        hash_md5 = hashlib.md5()
        with open(filename, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _get_uris(self, target):
        # takes a target URI and if the URI is not fully qualified (schema
        # is missing, returns a list of possible fully qualified
        # corresponding URIs
        parts = urlparse(target)
        if parts.scheme:
            return [target]

        scheme_file = os.path.join(
            os.path.dirname(self._data_client.caps.caps_file),
            '.data_uri_scheme_map')
        scheme_url = self._data_client.caps.caps_urls[DATA_RESOURCE_ID].replace('/capabilities', '/uri-scheme-map')
        content = util.get_url_content(url=scheme_url,
                                       cache_file=scheme_file,
                                       refresh_interval=24 * 60 * 60)
        schemes = self._parse_scheme_config(content)
        archive = target.split('/')[0]
        if archive in schemes:
            return ['{}:{}'.format(x.strip(':'), target) for x in schemes[archive]]
        else:
            return ['{}:{}'.format(schemes['default'][0], target)]

    def _parse_scheme_config(self, content):
        # parses the uri schemes map. Returns dictionary of archives and
        # corresponding list of schemes. It alwas contains the 'default` key
        result = {}
        for row in content.split('\n'):
            row = row.strip()
            if not row or row.startswith('#'):
                continue
            tokens = row.split(':')
            if not tokens:
                raise ValueError('Cannot parse archive/scheme content in row ' + row)
            archive = tokens[0].strip()
            if not archive:
                raise ValueError('Cannot find archive name in row ' + row)
            ns = ''.join(tokens[1:])
            name_spaces = [i.strip() for i in ns.split()]
            if not name_spaces:
                raise ValueError('No scheme found for archive {} (row {})'.format(archive, row))
            result[archive] = name_spaces
        if 'default' not in result:
            result['default'] = ['cadc']
        return result


def cadcput_cli():
    parser = util.get_base_parser(subparsers=False,
                                  version=version.version,
                                  service=DEFAULT_RESOURCE_ID,
                                  auth_required=True)

    parser.description = (
        'Upload files into the CADC Storage Inventory')

    parser.add_argument('-t', '--type',
                        help='MIME type to set in archive. If missing, the'
                             ' application will try to deduce it',
                        required=False)
    parser.add_argument('-e', '--encoding',
                        help='MIME encoding to set in archive. If missing,'
                             ' the application will try to deduce it',
                        required=False)
    parser.add_argument('-r', '--replace', action='store_true',
                        help='replace existing files or fail if identifier '
                             'is new. This is a safeguard for accidental '
                             'file replacements')
    parser.add_argument(
        'identifier', type=argparse_validate_uri_strict,
        help='unique identifier (URI) given to the file in the CADC '
             'Storage Inventory or a root identifier when multiple files'
             'are uploaded at the same time')
    parser.add_argument(
        'src',
        help='files or directories containing the files to be put. Multiple '
             'sources require a root identifier (terminated in "/"). URIs '
             'corresponding to each of the source file will be created as '
             'root URI + filename. A specified "type" or "encoding" applies '
             'to all the files.', nargs='+')
    parser.epilog = (
        'Examples:\n'
        '- Use user certificate to replace a file specify the type\n'
        '      cadcput --cert ~/.ssl/cadcproxy.pem -t "application/fits" \n'
        '              -r cadc:TEST/myfile.fits myfile.fits\n'
        '- Use default netrc file ($HOME/.netrc) to put two files files:\n'
        '      cadcput -v -n cadc:TEST/ myfile1.fits.gz myfile2.fits.gz\n'
        '- Use a different netrc file to put files from a directory to '
        'directory dir:\n'
        '      cadcput -d --netrc ~/mynetrc -s ivo:cadc.nrc.ca/cadc/minoc '
        'cadc:TEST/ dir\n'
        '- Connect as user to put files from multiple sources (prompt for\n'
        '  password if user not in $HOME/.netrc):\n'
        '      cadcput -v -u auser cadc:TEST/ myfile.fits.gz dir1 dir2')

    args = parser.parse_args()
    client = _create_client(args)

    files = []
    for file in args.src:
        if os.path.isdir(file):
            # append all files in the directory
            for i in os.listdir(file):
                if not os.path.isdir(os.path.join(file, i)):
                    files.append(os.path.join(file, i))
        else:
            files.append(file)
    if (len(files) > 1) and not args.identifier.endswith('/'):
        raise RuntimeError(
            'A root identifier (ending in "/") is required to put multiple '
            'files: {}'.format(args.identifier))

    for file in files:
        if len(files) > 1:
            file_id = '{}/{}'.format(args.identifier.strip('/'),
                                     os.path.basename(file))
        else:
            file_id = args.identifier
        logger.info('PUT {} -> {}'.format(file, file_id))
        execute_cmd(client.cadcput, {'id': file_id,
                                     'src': file,
                                     'file_type': args.type,
                                     'file_encoding': args.encoding,
                                     'replace': args.replace})


def cadcget_cli():
    parser = util.get_base_parser(subparsers=False,
                                  version=version.version,
                                  service=DEFAULT_RESOURCE_ID)

    parser.description = (
        'Download files from the CADC Storage Inventory into the current\n'
        'directory unless overriden with the -o option.')

    parser.add_argument(
        '-o', '--output',
        help='write to file or other directory instead of the current one.',
        required=False)
    parser.add_argument(
        'identifier', type=argparse_validate_get_uri,
        help='unique identifier (URI) given to the file in the CADC, typically'
             ' of the form <scheme>:<archive>/<filename> where <scheme> is a'
             ' concept internal to SI and is optional with this command. It is'
             ' possible to attach cutout arguments to the identifier to'
             ' download specific sections of a FITS file as in:'
             ' CFHT/806045o.fits.fz?cutout=[1][10:120,20:30]'
             'Storage Inventory')
    parser.add_argument(
        '--fhead', action='store_true',
        help='return the FITS header information (for all extensions')
    parser.epilog = (
        'Examples:\n'
        '- Anonymously download a file to current directory:\n'
        '      cadcget GEMINI/N20220825S0383.fits\n'
        '- Use certificate and a full specified id to get a cutout and save '
        'it to a file in the current directory (service provided file name):\n'
        '      cadcget --cert ~/.ssl/cadcproxy.pem '
        '"CFHT/806045o.fits.fz?cutout=[1][10:120,20:30]&cutout=[2][10:120,20:30]"\n')

    args = parser.parse_args()
    client = _create_client(args)
    logger.info('GET id {} -> {}'.format(
        args.identifier, args.output if args.output else 'stdout'))
    execute_cmd(client.cadcget, {'id': args.identifier, 'dest': args.output,
                                 'fhead': args.fhead})


def cadcinfo_cli():
    parser = util.get_base_parser(subparsers=False,
                                  version=version.version,
                                  service=DEFAULT_RESOURCE_ID)

    parser.description = (
        'Displays information about a file from the CADC Storage Inventory')

    parser.add_argument(
        'identifier', type=argparse_validate_uri,
        help='unique identifier (URI) given to the file in the CADC, typically'
             ' of the form <scheme>:<archive>/<filename> where <scheme> is a '
             ' concept internal to the storage system and is optional with this command.',
             nargs='+')
    parser.epilog = (
        'Examples:\n'
        '- Anonymously getting information about a public file:\n'
        '        cadcinfo CFHT/1000003f.fits.fz\n'
        '- Anonymously getting the information for the same public file '
        '  using a full URI:\n'
        '        cadcinfo cadc:CFHT/1000003f.fits.fz\n')

    args = parser.parse_args()
    client = _create_client(args)
    for id in args.identifier:
        logger.info('INFO for id {}'.format(id))
        try:
            file_info = execute_cmd(client.cadcinfo, {'id': id})
            print('CADC Storage Inventory artifact {}:'.format(id))
            print('\t {:>15}: {}'.format('id', file_info.id))
            print('\t {:>15}: {}'.format('name', file_info.name))
            print('\t {:>15}: {}'.format('size', file_info.size))
            print('\t {:>15}: {}'.format('type', file_info.file_type))
            print('\t {:>15}: {}'.format('encoding', file_info.encoding))
            print('\t {:>15}: {}'.format('last modified',
                                         date2ivoa(file_info.lastmod)))
            print('\t {:>15}: {}'.format('md5sum', file_info.md5sum))
        except exceptions.NotFoundException:
            logger.error('Identifier {} not found in the CADC Storage '
                         'Inventory'.format(id), exit_after=False)
            continue
    logger.info('DONE')


def cadcremove_cli():
    parser = util.get_base_parser(subparsers=False,
                                  version=version.version,
                                  service=DEFAULT_RESOURCE_ID,
                                  auth_required=True)
    parser.description = (
        'Remove files from the CADC Storage Inventory')

    parser.add_argument(
        'identifier', type=argparse_validate_uri_strict,
        help='unique identifier (URI) given to the file in the CADC '
             'Storage Inventory', nargs='+')
    parser.epilog = (
        'Examples:\n'
        '- Use certificate to remove a file from the storage inventory:\n'
        '       cadcremove --cert ~/.ssl/cadcproxy.pem cadc:CFHT/700000o.fz\n')

    args = parser.parse_args()
    client = _create_client(args)
    for id in args.identifier:
        logger.info('REMOVE id {}'.format(id))
        execute_cmd(client.cadcremove, {'id': id})


def _set_logging_level(args):
    if args.verbose:
        logger.setLevel(logging.INFO)
        logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                            format='%(message)s')
    elif args.debug:
        logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
    else:
        logging.basicConfig(level=logging.WARN, stream=sys.stdout)


def _create_client(args):
    # creates a StorageInventory client based on the cmd line args
    try:
        _set_logging_level(args)
        subject = net.Subject.from_cmd_line_args(args)
        return StorageInventoryClient(subject, args.service, host=args.host,
                                      insecure=args.insecure)
    except Exception as ex:
        handle_error(str(ex))


def execute_cmd(cmd, cmd_args):
    try:
        return cmd(**cmd_args)
    except Exception as e:
        handle_error(e)
