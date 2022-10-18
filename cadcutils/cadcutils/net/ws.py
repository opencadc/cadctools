# -*- coding: utf-8 -*-

# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2021.                            (c) 2021.
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
#
# ***********************************************************************

"""
Contains functionality related to interacting with Web Services. Users
of this class can instantiate the BaseWsClient in order to access
the webservices via one of the get, put, post, delete and head
functions that the service supports.

"""
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import logging
import sys
import time
import platform
import os
import hashlib

import requests
from requests import Session
from six.moves.urllib.parse import urlparse
import distro

from cadcutils import exceptions, util, net
from cadcutils import version as cadctools_version
from . import wscapabilities

# an issue related to the requests library
# (https://github.com/shazow/urllib3/issues/1060)
# prevents the latest versions of the requests library work when pyOpenSSL
# is installed in the system. The temporary workaround below makes
# requests ignore the install pyOpenSSL
# TODO remove after the above issue is closed
try:
    from requests.packages.urllib3.contrib import pyopenssl

    pyopenssl.extract_from_urllib3()
except ImportError:
    # it's an earlier version of requests that doesn't use pyOpenSSL
    pass

__all__ = ['BaseWsClient', 'BaseDataClient', 'get_resources', 'list_resources',
           'DEFAULT_REGISTRY']

BUFSIZE = 8388608  # Size of read/write buffer
MAX_RETRY_DELAY = 128  # maximum delay between retries
# start delay between retries when Try_After not sent by server.
DEFAULT_RETRY_DELAY = 30
MAX_NUM_RETRIES = 6

SERVICE_RETRY = 'Retry-After'
SERVICE_AVAILABILITY_ID = 'ivo://ivoa.net/std/VOSI#availability'

# Files up to this size might have their md5 checksum pre-computed before
# transferring. For larger files, the added overhead does not justify it.
# Can be overriden
MAX_MD5_COMPUTE_SIZE = 5 * 1024 * 1024
# can be overriden by environment
if os.getenv('CADC_MAX_MD5_COMPUTE_SIZE', None):
    MAX_MD5_COMPUTE_SIZE = int(os.getenv('CADC_MAX_MD5_COMPUTE_SIZE'))

# Files smaller that this size can be send with one request. Larger ones
# required to be split into segments that are sent and acknowledged
# individually
GIB = 1024 * 1024 * 1024
FILE_SEGMENT_THRESHOLD = 5 * GIB  # large files require segments
PREFERRED_SEGMENT_SIZE = 2 * GIB

MD5_MISMATCH_RETRY = 3  # number of times to retry on md5 mismatch errors

# HTTP attribute names
HTTP_LENGTH = 'Content-Length'

# PUT transactions headers/values
PUT_TXN_OP = 'x-put-txn-op'
PUT_TXN_ID = 'x-put-txn-id'
PUT_TXN_TOTAL_LENGTH = 'x-total-length'
PUT_TXN_MIN_SEGMENT = 'x-put-segment-minbytes'
PUT_TXN_MAX_SEGMENT = 'x-put-segment-maxbytes'
# transaction operations
PUT_TXN_START = 'start'
PUT_TXN_COMMIT = 'commit'
PUT_TXN_ABORT = 'abort'
PUT_TXN_REVERT = 'revert'

HEADERS = 'headers'  # name of the kwargs headers arg

# size of the read blocks in data transfers
READ_BLOCK_SIZE = 8 * 1024

# try to disable the unverified HTTPS call warnings
try:
    requests.packages.urllib3.disable_warnings()
except Exception:
    pass


def get_resources(with_caps=True):
    """
    Fetches the registry information regarding the available resources
    and their capabilities
    :param with_caps: True for including capabilities information,
    false otherwise
    :return: List of the form (resource_id, capability url,
    wscapabilities.Capabilities)}
    """
    resources = []
    cr = wscapabilities.CapabilitiesReader()
    response = requests.get(DEFAULT_REGISTRY)
    response.raise_for_status()
    for line in response.text.split('\n'):
        caps = None
        if (not line.startswith('#')) and (len(line.strip()) > 0):
            resource_id, url = line.split('=')
            if with_caps:
                caps_resp = requests.get(url.strip())
                caps_resp.raise_for_status()
                caps = cr.parsexml(caps_resp.text)
            resources.append((resource_id.strip(), url.strip(), caps))
    return resources


def list_resources():
    """
    Displays information about the services and their capabilities.
    """
    resources = get_resources()
    for r in resources:
        if r[2] is None:
            caps_str = 'NA'
        else:
            caps_str = ', '.join(sorted(r[2]._caps.keys()))
        print('{} ({}) - Capabilities: {}\n'.format(r[0], r[1], caps_str))


class BaseWsClient(object):
    """
    Web Service client primarily for CADC services. It is a wrapper class
       around the requests module to facilitate the interaction with the
       server via the get, put, post, delete and head functions (when the
       service supports it).

    Additional functionality that this class transparently offer:
            - discovery the capabilities of the service and their access
            methods
            - handling of transient errors with retrials
            - use of the appropriate credentials when interacting with the
            service
            - proper agent identification for logging on the server

    Three arguments are required in order to instantiate this class:
        1. A valid resource ID corresponding to the Web service that is
        being accessed. cadcutils.net.get_resources lists all the available
        resources at BOOTSTRAP_LOCATION location
        2. A cadcutils.net.Subject instance that contains the user credentials
        (no arguments for anonymous subject, certificate file or basic
        authentication).
        3. Agent is the name and version of the application as it will be
        presented in the HTTP
          request e.g. foo/1.0.2
    Once the BaseWsClient has been instantiated, the get/put/post/delete/head
    functions can be called on it.

    TODO: this implementation is very tightly coupled with the requests
    package, especially the arguments of get/post/put/delete/head that are
    blindly passed and the response object that is returned and the clients
    work with.
       """

    def __init__(self, resource_id, subject, agent, retry=True, host=None,
                 session_headers=None, insecure=False, idempotent_posts=False):
        """
        Client constructor
        :param resource_id -- ID of the resource being accessed (URI format)
        as it appears in the registry.
        :param subject -- The subject that is using the service
        :type subject: cadcutil.auth.Subject
        :param agent -- Name of the agent (application) that accesses the
        service and its version, e.g. foo/1.0.2
        :type agent: Subject
        :param retry -- True if the client retries on transient errors False
        otherwise
        :param host -- override the name of the host the service is running on
        (for testing purposes)
        :param session_headers -- Headers used throughout the session -
        dictionary format expected.
        :param insecure -- Allow insecure connections over SSL
        :param idempotent_post -- True if all HTTP POSTs can be considered
        idempotent, either because the server can deal with duplicate POSTs or
        because there's a higher level mechanism to deal with this. Idempotent
        POSTs can be automatically re-tried making them more fault tolerant.
        """

        self.logger = logging.getLogger('BaseWsClient')
        logging.getLogger('BaseWsClient').addHandler(logging.NullHandler())

        if resource_id is None:
            raise ValueError('No resource ID provided')
        if agent is None or not agent:
            raise ValueError('agent is None or empty string')

        self._session = None
        self.subject = subject
        self.resource_id = resource_id
        self.retry = retry
        self.session_headers = session_headers
        self.verify = not insecure
        self.idempotent_posts = idempotent_posts

        # agent is / delimited key value pairs, separated by a space,
        # containing the application name and version,
        # plus the name and version of application libraries.
        # eg: foo/1.0.2 foo-lib/1.2.3
        self.agent = agent

        # Get the package name and version, plus any imported libraries.
        self.package_info = "cadcutils/{} requests/{}".format(
            cadctools_version.version,
            requests.__version__)
        self.python_info = "{}/{}".format(platform.python_implementation(),
                                          platform.python_version())
        self.system_info = "{}/{}".format(platform.system(),
                                          platform.version())
        o_s = sys.platform
        if o_s.lower().startswith('linux'):
            distname, version, osid = distro.linux_distribution()
            self.os_info = "{} {}".format(distname, version)
        elif o_s == "darwin":
            release, version, machine = platform.mac_ver()
            self.os_info = "Mac OS X {}".format(release)
        elif o_s.lower().startswith("win32"):
            release, version, csd, ptype = platform.win32_ver()
            self.os_info = "{} {}".format(release, version)

        # build the corresponding capabilities instance
        self.caps = WsCapabilities(self, host)
        self._host = host
        if resource_id.startswith('http'):
            self._host = resource_id
        if self._host is None:
            base_url = self.caps.get_access_url(SERVICE_AVAILABILITY_ID)
            self._host = urlparse(base_url).hostname

        # Clients should add entries to this dict for specialized
        # conversion of HTTP error codes into particular exceptions.
        #
        # Use this form to include a search string in the response to
        # handle multiple possibilities for a single HTTP code.
        #     XXX : {'SEARCHSTRING1' : exceptionInstance1,
        #            'SEARCHSTRING2' : exceptionInstance2}
        #
        # Otherwise provide a simple HTTP code -> exception mapping
        #     XXX : exceptionInstance
        #
        # The actual conversion is performed by get_exception()
        self._HTTP_STATUS_CODE_EXCEPTIONS = {
            401: exceptions.UnauthorizedException()}

    @property
    def host(self):
        return self._host

    def post(self, resource=None, **kwargs):
        """Wrapper for POST so that we use this client's session
           :param resource represents the resource to access. It can take two
           forms: 1 - a URL or 2 - a tuple representing a Web Service resource
           in which the first member of the tuple is the URI of the resource
           (capability) and the second argument is the path.
           :param kwargs additional arguments to pass to the requests.post
           :returns response as received from the request library
        """
        return self._get_session().post(self._get_url(resource),
                                        verify=self.verify, **kwargs)

    def put(self, resource=None, **kwargs):
        """Wrapper for PUT so that we use this client's session
           :param resource represents the resource to access. It can take two
           forms: 1 - a URL or 2 - a tuple representing a Web Service resource
           in which the first member of the tuple is the URI of the resource
           (capability) and
           the second argument is the path.
           :param kwargs additional arguments to pass to the requests.post
           :returns response as received from the request library
        """
        return self._get_session().put(self._get_url(resource),
                                       verify=self.verify, **kwargs)

    def get(self, resource, params=None, **kwargs):
        """Wrapper for GET so that we use this client's session
           :param resource represents the resource to access. It can take two
           forms: 1 - a URL or 2 - a tuple representing a Web Service resource
           in which the first member of the tuple is the URI of the resource
           (capability) and the second argument is the path.
           :param params parameters to use with get
           :param kwargs additional arguments to pass to the requests.post
           :returns response as received from the request library
        """
        return self._get_session().get(self._get_url(resource), params=params,
                                       verify=self.verify, **kwargs)

    def delete(self, resource=None, **kwargs):
        """Wrapper for DELETE so that we use this client's session
           :param resource represents the resource to access. It can take two
           forms: 1 - a URL or 2 - a tuple representing a Web Service resource
           in which the first member of the tuple is the URI of the resource
           (capability) and the second argument is the path.
           :param kwargs additional arguments to pass to the requests.post
           :returns response as received from the request library
        """
        return self._get_session().delete(self._get_url(resource),
                                          verify=self.verify, **kwargs)

    def head(self, resource=None, **kwargs):
        """Wrapper for HEAD so that we use this client's session
           :param resource represents the resource to access. It can take two
           forms: 1 - a URL or 2 - a tuple representing a Web Service resource
           in which the first member of the tuple is the URI of the resource
           (capability) and the second argument is the path.
           :param kwargs additional arguments to pass to the requests.post
           :returns response as received from the request library
        """
        return self._get_session().head(self._get_url(resource),
                                        verify=self.verify, **kwargs)

    def is_available(self):
        """
        Checks whether the service is currently available or not
        :return: True if service is available, False otherwise
        """
        try:
            self.get((SERVICE_AVAILABILITY_ID, None))
        except exceptions.HttpException:
            return False
        return True

    def _get_url(self, resource):
        if type(resource) is tuple:
            # this is WS feature / path request
            path = ''
            if (resource[1] is not None) and (len(resource[1]) > 0):
                path = '/{}'.format(resource[1].strip('/'))
            if (len(resource) > 2):
                interface_type = resource[2]
                base_url = self.caps.get_access_url(resource[0],
                                                    interface_type)
            else:
                base_url = self.caps.get_access_url(resource[0])
            access_url = '{}{}'.format(base_url, path)
            return access_url
        else:
            # assume this is url.
            resource_url = urlparse(resource)
            if resource_url.scheme not in ['http', 'https']:
                raise ValueError('Incorrect resource URL: {}'.format(resource))
            return resource

    def _get_session(self):
        # Note that the cert goes into the adapter, but we can also
        # use name/password for the auth. We may want to enforce the
        # usage of only the cert in case both name/password and cert
        # are provided.
        if self._session is None:
            self.logger.debug('Creating session.')
            self._session = RetrySession(
                self.retry, idempotent_posts=self.idempotent_posts)
            # prevent requests from using .netrc
            self._session.trust_env = False
            if self.subject.certificate is not None:
                self._session.cert = (
                    self.subject.certificate, self.subject.certificate)
            elif self.subject.cookies:
                for cookie in self.subject.cookies:
                    cookie_obj = requests.cookies.create_cookie(
                        domain=cookie.domain, name=cookie.name,
                        value=cookie.value)
                    self._session.cookies.set_cookie(cookie_obj)
            else:
                if (not self.subject.anon) and (self.host is not None) and \
                        (self.subject.get_auth(self.host) is not None):
                    self._session.auth = self.subject.get_auth(self.host)

        user_agent = "{} {} {} {} ({})".format(self.agent, self.package_info,
                                               self.python_info,
                                               self.system_info, self.os_info)
        self._session.headers.update({"User-Agent": user_agent})
        if self.session_headers is not None:
            for header in self.session_headers:
                self._session.headers.update(self.session_headers)
        self._session.verify = self.verify
        return self._session


class BaseDataClient(BaseWsClient):
    """
    Base class for clients that interact with the CADC storage system (cadcdata
    and vos). Provides utilities for uploading and downloading files
    """

    def upload_file(self, url, src, md5_checksum=None, **kwargs):
        """Method to upload a file to CADC storage (archive or vospace). This
           method takes advantage of features in CADC services that uses
           PUTs with transactions in order to optimize and make the transfer
           more robust. A detailed description of the mechanism can be found
           at https://github.com/opencadc/storage-inventory/blob/master/minoc/PutTransaction.md
           :param url: URL to upload the file to
           :param src: name of the file to upload
           :param md5_checksum: optional md5 checksum of the file content. If
           available, the caller should set the attribute, otherwise the method
           might compute it (for small files) and introduce overhead.
           :param kwargs: other http attributes
           :throws: HttpExceptions
        """
        stat_info = os.stat(src)
        if stat_info.st_size == 0:
            raise ValueError('Cannot upload empty files')
        if HEADERS not in kwargs:
            kwargs[HEADERS] = {}
        orig_headers = kwargs.get(HEADERS)
        orig_headers[HTTP_LENGTH] = str(stat_info.st_size)

        def combine_headers(new_headers):
            # return a new dictionary that combines the original headers
            # and the new headers
            result = dict(orig_headers)
            result.update(new_headers)
            return result

        src_md5 = md5_checksum
        if not src_md5 and stat_info.st_size <= MAX_MD5_COMPUTE_SIZE:
            hash_md5 = hashlib.md5()
            with open(src, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            src_md5 = hash_md5.hexdigest()

        if src_md5:
            net.add_md5_header(headers=orig_headers, md5_checksum=src_md5)
            if stat_info.st_size < FILE_SEGMENT_THRESHOLD:
                # no transactions needed.
                retries = MD5_MISMATCH_RETRY
                while retries:
                    try:
                        with open(src, 'rb') as reader:
                            response = self._get_session().put(
                                url,
                                data=reader,
                                verify=self.verify, **kwargs)
                        self.logger.debug('{} uploaded (HTTP {})'.format(
                            src, response.status_code))
                        return
                    except exceptions.PreconditionFailedException as e:
                        # retry as this is likely caused by md5 mismatch
                        if not retries:
                            raise e
                        retries -= 1

        if stat_info.st_size < FILE_SEGMENT_THRESHOLD:
            # one go upload with transaction
            retries = MD5_MISMATCH_RETRY
            while retries:
                with util.Md5File(src, 'rb') as reader:
                    kwargs[HEADERS] = combine_headers(
                        {PUT_TXN_OP: PUT_TXN_START})
                    response = self._get_session().put(
                        url,
                        data=reader,
                        verify=self.verify, **kwargs)
                trans_id = response.headers[PUT_TXN_ID]
                # check the file made it OK
                dest_md5 = net.extract_md5(response.headers)
                if dest_md5 != reader.md5_checksum:
                    msg = 'File {} not properly uploaded. ' \
                          'Mismatched md5 src vs dest: {} vs {}'.format(
                            src, reader.md5_checksum, dest_md5)
                    self.logger.warning(msg)
                    # abort transaction
                    kwargs[HEADERS] = combine_headers(
                        {PUT_TXN_ID: trans_id,
                         PUT_TXN_OP: PUT_TXN_ABORT,
                         HTTP_LENGTH: '0'})
                    self._get_session().post(url, verify=self.verify, **kwargs)
                    retries -= 1
                    if retries:
                        self.logger.warning('Retrying')
                        continue
                    else:
                        raise exceptions.TransferException(msg)
                    # commit tran
                kwargs[HEADERS] = combine_headers({
                    PUT_TXN_ID: trans_id,
                    PUT_TXN_OP: PUT_TXN_COMMIT,
                    HTTP_LENGTH: '0'})
                self._get_session().put(url, verify=self.verify, **kwargs)
                return

        # large file that requires multiple segments
        kwargs[HEADERS] = combine_headers({
            HTTP_LENGTH: '0',
            PUT_TXN_TOTAL_LENGTH: str(stat_info.st_size),
            PUT_TXN_OP: PUT_TXN_START})
        response = self._get_session().put(url,
                                           verify=self.verify,
                                           **kwargs)
        trans_id = response.headers[PUT_TXN_ID]
        self.logger.debug('Starting transaction {} on url {}'.format(
            trans_id, url))
        min_segment = response.headers.get(PUT_TXN_MIN_SEGMENT, None)
        max_segment = response.headers.get(PUT_TXN_MAX_SEGMENT, None)
        seg_size = self._get_segment_size(stat_info.st_size,
                                          min_segment,
                                          max_segment)
        current_size = 0
        last_digest = hashlib.md5()
        try:
            # Obs -(-stat_info.st_size//seg_size) - ceiling division in PYTHON
            for segment in range(0, -(-stat_info.st_size//seg_size)):
                cur_seg_size = min(seg_size,
                                   stat_info.st_size-segment*seg_size)
                self.logger.debug('Sending segment {} of size {}'.format(
                    segment, cur_seg_size))
                # Note: setting the content length here is irrelevant as
                # requests is going to override it according to the size
                # of the data (as returned by Md5File file handler)
                current_size += cur_seg_size
                retries = MD5_MISMATCH_RETRY
                while retries:
                    try:
                        with util.Md5File(src, 'rb', segment*seg_size,
                                          cur_seg_size) as reader:
                            reader._md5_checksum = last_digest.copy()
                            kwargs[HEADERS] = combine_headers({
                                PUT_TXN_ID: trans_id,
                                HTTP_LENGTH: str(cur_seg_size)})
                            response = self._get_session().put(
                                url,
                                data=reader,
                                verify=self.verify,
                                **kwargs)
                    except exceptions.TransferException as e:
                        self.logger.warning('Errors transfering {} to {}: {}'.
                                            format(src, url, e.msg))
                        try:
                            kwargs[HEADERS] = combine_headers(
                                {PUT_TXN_ID: trans_id,
                                 HTTP_LENGTH: '0'})
                            response = self._get_session().head(
                                url,
                                **kwargs)
                        except Exception as e:
                            self.logger.error(
                                'Could not retrieve transaction {} '
                                'status from {}: {}'.format(trans_id, url,
                                                            str(e)))
                            raise e
                    # check the file made it OK
                    src_md5 = reader.md5_checksum
                    dest_md5 = net.extract_md5(response.headers)
                    if src_md5 != dest_md5:
                        msg = 'File {} not properly uploaded. ' \
                              'Mismatched md5 src vs dest: {} vs {}'.format(
                                src, src_md5, dest_md5)
                        self.logger.warning(msg)
                        retries -= 1
                        if retries:
                            # dest_md5 == None is the start state
                            if dest_md5 and \
                                    (dest_md5 != last_digest.hexdigest()):
                                self.logger.debug('Reverting transaction')
                                kwargs[HEADERS] = combine_headers(
                                    {PUT_TXN_ID: trans_id,
                                     PUT_TXN_OP: PUT_TXN_REVERT,
                                     HTTP_LENGTH: '0'})
                                response = self._get_session().post(
                                    url,
                                    verify=self.verify,
                                    **kwargs)
                                dest_md5 = net.extract_md5(response.headers)
                            if dest_md5 is None or \
                                    dest_md5 == last_digest.hexdigest():
                                self.logger.warning('Retrying')
                                continue
                            else:
                                self.logger.error(
                                    'BUG: reverted transaction does not match '
                                    'last md5: {} != {}'.format(
                                        dest_md5, last_digest.hexdigest()))
                        raise exceptions.TransferException(msg)
                    last_digest = reader._md5_checksum
                    break
        except BaseException as e:
            if trans_id:
                # abort transaction
                self.logger.debug('Aborting transaction {}'.format(trans_id))
                kwargs[HEADERS] = combine_headers({PUT_TXN_ID: trans_id,
                                                   PUT_TXN_OP: PUT_TXN_ABORT,
                                                   HTTP_LENGTH: '0'})
                self._get_session().post(url, verify=self.verify, **kwargs)
                self.logger.warning('Transaction {} aborted'.format(trans_id))
                raise e

        # commit tran
        self.logger.debug('Commit transaction')
        kwargs[HEADERS] = combine_headers({
            PUT_TXN_ID: trans_id,
            PUT_TXN_OP: PUT_TXN_COMMIT,
            HTTP_LENGTH: '0'})
        self._get_session().put(url, verify=self.verify, **kwargs)

    @staticmethod
    def _get_segment_size(file_size, min_segment, max_segment):
        segment_size = min(file_size, PREFERRED_SEGMENT_SIZE)
        if min_segment:
            segment_size = max(segment_size, int(min_segment))
        if max_segment:
            segment_size = min(segment_size, int(max_segment))
        return segment_size

    @staticmethod
    def _resolve_destination_file(dest, src_md5, default_file_name):
        # returns destination absolute file name as well as a temporary
        # destination to be used during the transfer
        if (dest is None) and (default_file_name is None):
            raise ValueError('BUG: Cannot resolve file name')
        if dest is not None:
            # got a dest name?
            if os.path.isdir(dest):
                final_dest = os.path.join(dest, default_file_name)
            else:
                final_dest = dest
        else:
            final_dest = os.path.basename(default_file_name)
        if src_md5:
            dir_name = os.path.dirname(final_dest)
            file_name = os.path.basename(final_dest)
            temp_dest = os.path.join(
                dir_name, '{}-{}.part'.format(file_name, src_md5))
            return final_dest, temp_dest
        else:
            return final_dest, final_dest

    def download_file(self, url, dest=None, **kwargs):
        """Method to download a file from CADC storage (archive or vospace).
           This method takes advantage of the HTTP Range feature available
           with the CADC services to optimize and make the transfer more
           robust.
           :param url: URL to get the file from
           :param dest: name of the file to store it to. If it's the name of
           the directory to save it to, it will use the Content-Disposition for
           the file name. By default, it saves the file in the current
           directory.
           :param kwargs: other http attributes
           :return: requests.Response object
           :throws: HttpExceptions

        """
        response = self.get(url, stream=True, **kwargs)
        src_md5 = net.extract_md5(response.headers)
        src_size = int(response.headers.get(HTTP_LENGTH, 0))
        content_disp = net.get_header_filename(response.headers)
        if hasattr(dest, 'read'):
            dest.write(response.raw.read())
            return
        else:
            final_dest, temp_dest = self._resolve_destination_file(
                dest=dest, src_md5=src_md5, default_file_name=content_disp)
            if os.path.isfile(final_dest) and \
               src_size == os.stat(final_dest).st_size and \
               src_md5 == \
                    hashlib.md5(open(final_dest, 'rb').read()).hexdigest():
                # nothing to be done
                return
            if src_md5 and src_size and os.path.isfile(temp_dest):
                stat_info = os.stat(temp_dest)
                if not stat_info.st_size or stat_info.st_size >= src_size:
                    # Note: the existence of a complete temporary file should
                    # be a bug. It's more likely that it's corrupted hence the
                    # removal below
                    os.remove(temp_dest)
                else:
                    if response.headers.get('Accept-Ranges', None) and \
                       response.headers.get('Accept-Ranges').strip() == \
                            'bytes':
                        # do a range request
                        response.raw.close()  # close existing stream
                        headers = {
                            'Range': 'bytes={}-'.format(stat_info.st_size)}
                        response = self.get(url, stream=True, headers=headers)
                        # at some point the warnings below should become errors
                        # even if the code can deal with a 200 response as well
                        # right now, these can be useful in debugging
                        if response.status_code != \
                                requests.codes.partial_content:
                            self.logger.warning(
                                'Expected partial content for range request')
                        exp_cr = 'bytes {}-{}/{}'.format(stat_info.st_size,
                                                         src_size - 1,
                                                         src_size)
                        actual_cr = response.headers.get('Content-Range', '')
                        if actual_cr != exp_cr:
                            self.logger.warning(
                                'Content-Range expected {} vs '
                                'received {}'.format(exp_cr, actual_cr))
            # need to send the original file content-length. The Range response
            # contains the content-length of the range.
            self._save_bytes(response, src_size, temp_dest, None)
            os.rename(temp_dest, final_dest)

    def _save_bytes(self, response, src_length, dest_file, process_bytes=None):
        # requests automatically decompresses the data.
        # Tell it to do it only if it had to
        hash_md5 = hashlib.md5()
        src_md5 = net.extract_md5(response.headers)

        class RawRange(object):
            """
            Wrapper class to make response.raw.read work as iterator and behave
            the same way as the corresponding response.iter_content. Useful
            with a progress bar.
            """

            def __init__(self, rsp):
                """
                :param rsp: HTTP response object
                """
                self._read = rsp.raw.read
                self.block_size = 0

            def __iter__(self):
                return self

            def __next__(self):
                return self.next()

            def next(self):
                # reads the next raw block
                data = self._read(self.block_size)
                if len(data) > 0:
                    if src_md5:
                        hash_md5.update(data)
                    return data
                else:
                    raise StopIteration()

            def get_instance(self, block_size):
                self.block_size = block_size
                return self

        update_mode = 'wb'
        dest_length = 0
        if os.path.isfile(dest_file) and os.stat(dest_file).st_size > 0:
            if response.status_code == requests.codes.partial_content:
                # Can resume download. Digest existing content on disk first
                update_mode = 'ab'
                dest_length = os.stat(dest_file).st_size
                with open(dest_file, 'rb') as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        hash_md5.update(chunk)
            else:
                os.remove(dest_file)

        rr = RawRange(response)
        reader = rr.get_instance
        # TODO - progress bar
        # if logger.isEnabledFor(logging.INFO) and total_length:
        #     chunks = progress.bar(reader(
        #         READ_BLOCK_SIZE),
        #         expected_size=((total_length / READ_BLOCK_SIZE) + 1))
        # else:
        dest_downloaded = 0
        chunks = reader(READ_BLOCK_SIZE)
        start = time.time()
        with open(dest_file, update_mode) as dest:
            for chunk in chunks:
                if process_bytes is not None:
                    process_bytes(chunk)
                dest.write(chunk)
                dest_downloaded += len(chunk)
        dest_md5 = hash_md5.hexdigest()
        dest_length += dest_downloaded
        if src_length and src_length != dest_length:
            error_msg = 'Sizes of source and downloaded file do not match: ' \
                        '{} vs {}'.format(src_length, dest_length)
        elif (src_md5 is not None) and (src_md5 != dest_md5):
            error_msg = 'Downloaded file is corrupted: expected md5({}) != ' \
                        'actual md5({})'.format(src_md5, dest_md5)
        else:
            duration = time.time() - start
            self.logger.info(
                'Successfully downloaded file {} in {}s '
                '(avg. speed: {}MB/s)'.format(
                    dest_file, round(duration, 2),
                    round(dest_downloaded / 1024 / 1024 / duration, 2)))
            return
        if not src_length or not src_md5:
            # clean up the temporary file
            os.remove(dest_file)
        raise exceptions.TransferException(error_msg)


class RetrySession(Session):
    """ Session that automatically does a number of retries for failed
        transient errors. The time between retries double every time until a
        maximum of 30sec is reached

        This does not work with POST method

        The following network errors are considered transient:
            requests.codes.unavailable,
            requests.codes.service_unavailable,
            requests.codes.gateway_timeout,
            requests.codes.request_timeout,
            requests.codes.timeout,
            requests.codes.precondition_failed,
            requests.codes.precondition,
            requests.codes.payment_required,
            requests.codes.payment

        In addition, the Connection error 'Connection reset by remote user'
        also triggers a retry
        """

    retry_errors = [requests.codes.unavailable,
                    requests.codes.service_unavailable,
                    requests.codes.gateway_timeout,
                    requests.codes.request_timeout,
                    requests.codes.timeout,
                    requests.codes.payment_required,
                    requests.codes.payment]

    def __init__(self, retry=True, start_delay=1, idempotent_posts=False,
                 *args, **kwargs):
        """
        ::param retry: set to False if retries not required
        ::param start_delay: start delay interval between retries (default=1s).
                Note that for HTTP 503, this code follows the retry timeout
                set by the server in Retry-After
        ::param idempotent_posts: POST requests in general are not idempotent
        and they are not automatically re-tried on failures. Setting this flag
        to true can override that, in case when a specific client-server
        implementation can handle duplicate POST requests at a higher level.
        """
        self.logger = logging.getLogger('RetrySession')
        self.retry = retry
        self.start_delay = start_delay
        self.idempotent_posts = idempotent_posts
        super(RetrySession, self).__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        """
        Send a given PreparedRequest, wrapping the connection to service in
        try/except that retries on
        Connection reset by peer.
        :param request: The prepared request to send._session
        :param kwargs: Any keywords the adaptor for the request accepts.
        :return: the response
        :rtype: requests.Response
        """
        # merge kwargs with env
        proxies = kwargs.get('proxies') or {}
        settings = self.merge_environment_settings(
            request.url, proxies, kwargs.get('stream'), kwargs.get('verify'),
            kwargs.get('cert'))
        kwargs.update(settings)

        # requests does not provide a default timeout, hence we might need
        # to add it
        if 'timeout' not in kwargs or kwargs['timeout'] is None:
            kwargs['timeout'] = 120

        if (request.method.upper() == 'POST') and self.idempotent_posts:
            self.logger.debug(
                'POST requests considered idempotent. re-tries enabled')

        if (request.method.upper() != 'POST' or self.idempotent_posts) \
           and self.retry:
            current_delay = max(self.start_delay, DEFAULT_RETRY_DELAY)
            current_delay = min(current_delay, MAX_RETRY_DELAY)
            num_retries = 0
            self.logger.debug(
                "Sending request {0}  to server.".format(request))
            current_error = None
            while True:
                try:
                    response = super(RetrySession, self).send(request,
                                                              **kwargs)
                    self.check_status(response)
                    return response
                except requests.exceptions.ConnectTimeout as ct:
                    # retry on timeouts
                    current_error = ct
                    self.logger.debug(ct)
                except requests.exceptions.ReadTimeout as rt:
                    # this could happen after the request has made it to
                    # the server so it should be re-done
                    raise exceptions.TransferException(
                        'Read timeout on {}'.format(request.url), rt)
                except requests.HTTPError as e:
                    if e.response.status_code not in self.retry_errors:
                        raise exceptions.HttpException(e)
                    current_error = e
                    if e.response.status_code == requests.codes.unavailable:
                        # is there a delay from the server (Retry-After)?
                        try:
                            current_delay = int(
                                e.response.headers.get(SERVICE_RETRY,
                                                       current_delay))
                            current_delay = min(current_delay, MAX_RETRY_DELAY)
                        except Exception:
                            pass
                except requests.ConnectionError as ce:
                    if 'Connection reset by peer' in str(ce):
                        # Likely a network error that the caller can re-try
                        raise exceptions.TransferException(
                            'Transfer error on URL: {}'.format(request.url))
                    else:
                        # Can't recover (bad url, etc)
                        raise exceptions.HttpException(orig_exception=ce)
                if num_retries == MAX_NUM_RETRIES:
                    break
                self.logger.debug(
                    "Error {}. Resending request in {}s ...".format(
                        str(current_error), current_delay))
                time.sleep(current_delay)
                num_retries += 1
                current_delay = min(current_delay * 2, MAX_RETRY_DELAY)
            raise exceptions.HttpException(current_error)
        else:
            response = super(RetrySession, self).send(request, **kwargs)
            self.check_status(response, False)
            return response

    def check_status(self, response, retry=True):
        """
        Check the response status. Maps the application related requests
        error status into Exceptions and raises the others
        :param response: response
        :param retry: request can be re-tried. Let the re-tried errors through
        :return:
        """
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                raise exceptions.NotFoundException(orig_exception=e)
            elif e.response.status_code == requests.codes.unauthorized:
                raise exceptions.UnauthorizedException(orig_exception=e)
            elif e.response.status_code == requests.codes.forbidden:
                raise exceptions.ForbiddenException(orig_exception=e)
            elif e.response.status_code == requests.codes.bad_request:
                raise exceptions.BadRequestException(orig_exception=e)
            elif e.response.status_code == requests.codes.precondition_failed:
                raise exceptions.PreconditionFailedException(orig_exception=e)
            elif e.response.status_code == requests.codes.conflict:
                raise exceptions.AlreadyExistsException(orig_exception=e)
            elif e.response.status_code == \
                    requests.codes.internal_server_error:
                raise exceptions.InternalServerException(orig_exception=e)
            elif e.response.status_code == \
                    requests.codes.request_entity_too_large:
                raise exceptions.ByteLimitException(orig_exception=e)
            elif retry and e.response.status_code in self.retry_errors:
                raise e
            else:
                raise exceptions.UnexpectedException(orig_exception=e)


DEFAULT_REGISTRY = \
    'https://ws.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/reg/resource-caps'
CACHE_REFRESH_INTERVAL = 10 * 60
CACHE_LOCATION = os.path.join(os.path.expanduser("~"), '.config',
                              'cadc-registry')
REGISTRY_FILE = 'resource-caps'


class WsCapabilities(object):
    """
    Contains the capabilities of Web Services. The most useful function is
    get_access_url that returns the url corresponding to a feature of a
    Web Service
    """

    def __init__(self, ws_client, host=None):
        """
        :param ws_client: WebService client that the capabilities are required
        for
        :param host: use this host rather than the default CADC host for
        reg lookup
        """
        self.logger = logging.getLogger('WsCapabilities')
        self.ws = ws_client
        self._host = host
        cache_location = CACHE_LOCATION
        if host is not None:
            cache_location = os.path.join(cache_location, 'alt-domains', host)

        if not os.path.isdir(cache_location):
            os.makedirs(cache_location)

        # check the registry file in cache if it requires a refresh
        self.reg_file = os.path.join(cache_location, REGISTRY_FILE)
        resource_id = urlparse(ws_client.resource_id)
        self.caps_file = os.path.join(cache_location, resource_id.netloc,
                                      resource_id.path.strip('/'))
        # the name of the cached capabilities files is preceeded by a ".". Old
        # files don't have this rule so delete them if they exist. The
        # following block should be eventually removed
        if os.path.isfile(self.caps_file):
            os.remove(self.caps_file)
        # prefix the name of the file with '.' to avoid collisions with
        # subdirectory names
        self.caps_file = '/.'.join(self.caps_file.rsplit('/', 1))
        self.last_regtime = 0
        self.last_capstime = 0
        self._caps_reader = wscapabilities.CapabilitiesReader()
        self.caps_urls = {}
        self.features = {}
        self.capabilities = {}

    def get_access_url(self, feature, interface_type='vs:ParamHTTP'):
        """
        Returns the access URL corresponding to a feature and the
        authentication information associated with the subject that created
        the Web Service client
        :param feature: Web Service feature
        :param interface_type: the type of the interface
        :return: corresponding access URL
        """

        if (time.time() - self.last_capstime) > CACHE_REFRESH_INTERVAL:
            if self.last_capstime == 0:
                # startup
                try:
                    self.last_capstime = os.path.getmtime(self.caps_file)
                except OSError:
                    # cannot read the cache file for whatever reason
                    pass
            caps = self._get_content(self.caps_file,
                                     self._get_capability_url(),
                                     self.last_capstime)
            # caps is a string but it's xml content claims it's utf-8 encode,
            # hence need to encode it before
            # parsing it.
            self.capabilities = self._caps_reader.parsexml(
                caps.encode('utf-8'))
            if (time.time() - self.last_capstime) > CACHE_REFRESH_INTERVAL:
                self.last_capstime = time.time()
        sms = self.ws.subject.get_security_methods()

        return self.capabilities.get_access_url(feature, sms, interface_type)

    @property
    def host(self):
        return self._host

    def _get_content(self, resource_file, url, last_accessed=None):
        """
         Return content from a local cache file if information is recent
         (it was accessed less than CACHE_REFRESH_INTERVAL seconds ago).
         If not or if last_accessed is not specified and the cache is stale,
         it updates the cache from the provided url before
         returning the content.
        """
        content = None
        if not last_accessed and os.path.exists(resource_file):
            last_accessed = os.path.getmtime(resource_file)
        if (last_accessed and (time.time() - last_accessed) < CACHE_REFRESH_INTERVAL):
            # get reg information from the cached file
            self.logger.debug(
                'Read cached content of {}'.format(resource_file))
            try:
                with open(resource_file, 'r') as f:
                    content = f.read()
            except Exception:
                # will download it
                pass
        # config dirs if they don't exist yet
        if not os.path.exists(os.path.dirname(resource_file)):
            os.makedirs(os.path.dirname(resource_file))

        if content is None:
            # get information from the bootstrap registry
            try:
                session = requests.Session()
                # do not allow requests to use .netrc file
                session.trust_env = False
                rsp = session.get(url, verify=self.ws.verify)
                rsp.raise_for_status()
                content = rsp.text
                if content is None or len(content.strip(' ')) == 0:
                    # workaround for a problem with CADC servers
                    raise exceptions.HttpException('Received empty content')
                with open(resource_file, 'w') as f:
                    f.write(content)
            except exceptions.HttpException as e:
                # problems with the bootstrap registry. Try to use the old
                # local one regardless of how old it is
                self.logger.error("ERROR: cannot read registry info from " +
                                  url + ": " + str(e))
                if os.path.exists(resource_file):
                    with open(resource_file, 'r') as f:
                        content = f.read()
        if content is None:
            raise RuntimeError(
                "Cannot get the registry info for resource " + url)
        return content

    def _get_capability_url(self):
        """
        Parses the registry information and returns the url of the
        capabilities feature of the Web Service
        :return: URL to the capabilities feature
        """
        if self.ws.resource_id.startswith('http'):
            return '{}/capabilities'.format(self.ws.resource_id)
        if (time.time() - self.last_regtime) > CACHE_REFRESH_INTERVAL:
            if self.last_regtime == 0:
                # startup
                try:
                    self.last_regtime = os.path.getmtime(self.reg_file)
                except OSError:
                    # cannot read the cache file for whatever reason
                    pass

            # replace registry host name if necessary
            registry_url = DEFAULT_REGISTRY
            url = urlparse(registry_url)
            if self._host is not None and url.netloc != self._host:
                registry_url = '{}://{}{}'.format(url.scheme, self._host,
                                                  url.path)
            self.logger.debug('Resolved URL: {}'.format(registry_url))

            reg = self._get_content(self.reg_file, registry_url,
                                    self.last_regtime)
            self.caps_urls = {}
            if (time.time() - self.last_regtime) > CACHE_REFRESH_INTERVAL:
                self.last_regtime = time.time()
            # parse it
            for line in reg.split('\n'):
                if not line.startswith('#') and (len(line) > 0):
                    feature, url = line.split('=')
                    self.caps_urls[feature.strip()] = url.strip()
        if self.ws.resource_id not in self.caps_urls:
            raise AttributeError(
                'Resource ID {} not found. Available resource IDs: {}'.
                format(self.ws.resource_id, self.caps_urls.keys()))
        return self.caps_urls[self.ws.resource_id]
