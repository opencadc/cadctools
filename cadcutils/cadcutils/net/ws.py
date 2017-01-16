# -*- coding: utf-8 -*-
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
#
# ***********************************************************************
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import logging
import sys
import time
import platform

import requests
from requests import Session
from six.moves.urllib.parse import urlparse

from cadcutils import exceptions
from .. import version as cadctools_version

__all__ = ['BaseWsClient']

BUFSIZE = 8388608  # Size of read/write buffer
MAX_RETRY_DELAY = 128  # maximum delay between retries
DEFAULT_RETRY_DELAY = 30  # start delay between retries when Try_After not sent by server.
MAX_NUM_RETRIES = 6

SERVICE_RETRY = 'Retry-After'

# try to disable the unverified HTTPS call warnings
try:
    requests.packages.urllib3.disable_warnings()
except:
    pass


class BaseWsClient(object):
    """Web Service client primarily for CADC services"""

    def __init__(self, resource_id, subject, agent, retry=True, host=None):
        """
        Client constructor
        :param resource_id -- ID of the resource being accessed (URI format)
        :param subject -- The subject that is using the service
        :type subject: cadcutil.auth.Subject
        :param agent -- Name of the agent (application) that accesses the service
        :type agent: Subject
        :param retry -- True if the client retries on transient errors False otherwise
        :param host -- override the name of the host the service is running on (for testing purposes)

        """

        self.logger = logging.getLogger('BaseWsClient')

        if resource_id is None:
            raise ValueError("No resource ID provided")
        if agent is None or not agent:
            raise ValueError('agent is None or empty string')

        self._session = None
        self.subject = subject
        self.retry = retry
        self.host = host

        # agent is / delimited key value pairs, separated by a space,
        # containing the application name and version,
        # plus the name and version of application libraries.
        # eg: foo/1.0.2 foo-lib/1.2.3
        self.agent = agent

        # Get the package name and version, plus any imported libraries.
        self.package_info = "cadcutils/{} requests/{}".format(cadctools_version.version,
                                                              requests.__version__)
        self.python_info = "{}/{}".format(platform.python_implementation(),
                                          platform.python_version())
        self.system_info = "{}/{}".format(platform.system(), platform.version())
        o_s = sys.platform
        if o_s.lower().startswith('linux'):
            distname, version, osid = platform.linux_distribution()
            self.os_info = "{} {}".format(distname, version)
        elif o_s == "darwin":
            release, version, machine = platform.mac_ver()
            self.os_info = "Mac OS X {}".format(release)
        elif o_s.lower().startswith("win32"):
            release, version, csd, ptype = platform.win32_ver()
            self.os_info = "{} {}".format(release, version)

        # Determine the name of the caom2repo service and host
        # TODO this should be discoverable
        url = urlparse(resource_id)
        if len(url.path) < 2 or len(url.scheme) < 2 or len(url.netloc) < 2:
            raise ValueError("Invalid resource ID {}".format(resource_id))
        self.service = url.path.strip('/')
        if self.host is None:
            self.host = url.netloc
        else:
            self.host = self.host.strip('/')

        # Unless the caller specifically requests an anonymous client,
        # check first for a certificate, then an externally created
        # HTTPBasicAuth object, and finally a name+password in .netrc.
        # TODO The service URL needs to be discoverable based on the URI/URL of the service with the
        # following steps:
        # 1. Download the configuration of services at the service provider and get the URL for the capabilities
        #    resource of the service (This is not yet implemented at the CADC)
        # 2. Check the capabilities of the service to determine the protocol and the URL of the end points of the
        #    service
        # This will eventually replace the hardcoded code below.

        # Base URL for web services.
        # Clients will probably append a specific service
        if self.service == u'sc2repo':
            self.base_url = '{}://{}/{}/{}'.format(
                'https' if self.subject.certificate is not None else 'http',
                self.host, self.service,
                'auth-observations' if self.subject.get_auth(host) is not None else 'observations')
        else:
            if self.subject.anon:
                self.protocol = 'http'
                self.base_url = '{}://{}/{}/pub'.format(self.protocol, self.host, self.service)
            else:
                if self.subject.certificate:
                    self.protocol = 'https'
                    self.base_url = '{}://{}/{}/pub'.format(self.protocol, self.host, self.service)
                else:
                    # For both anonymous and name/password authentication
                    self.protocol = 'http'
                    self.base_url = '{}://{}/{}/auth'.format(self.protocol, self.host, self.service)

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

    def post(self, resource=None, **kwargs):
        """Wrapper for POST so that we use this client's session
        TODO: change argument resource to feature and add path argument to all the functions
        """
        return self._get_session().post(self._get_url(resource), **kwargs)

    def put(self, resource=None, **kwargs):
        """Wrapper for PUT so that we use this client's session"""
        return self._get_session().put(self._get_url(resource), **kwargs)

    def get(self, resource, params=None, **kwargs):
        """Wrapper for GET so that we use this client's session"""
        return self._get_session().get(self._get_url(resource), params=params, **kwargs)

    def delete(self, resource=None, **kwargs):
        """Wrapper for DELETE so that we use this client's session"""
        return self._get_session().delete(self._get_url(resource), **kwargs)

    def head(self, resource=None, **kwargs):
        """Wrapper for HEAD so that we use this client's session"""
        return self._get_session().head(self._get_url(resource), **kwargs)

    def _get_url(self, resource):
        if resource is None:
            return self.base_url
        # try to determine if it is a full url, in which case just return it
        url = urlparse(resource)
        if len(url.scheme) > 0:
            return resource
        # TODO remove this in reg story
        if resource == 'transfer':
            return '{}/{}'.format(self.base_url.replace('pub', ''), resource)

        if str(resource).startswith('/'):
            return self.base_url + str(resource)
        else:
            return self.base_url + '/' + str(resource)

    def _get_session(self):
        # Note that the cert goes into the adapter, but we can also
        # use name/password for the auth. We may want to enforce the
        # usage of only the cert in case both name/password and cert
        # are provided.
        if self._session is None:
            self.logger.debug('Creating session.')
            self._session = RetrySession(self.retry)
            if self.subject.certificate is not None:
                self._session.cert = (self.subject.certificate, self.subject.certificate)
            else:
                if self.subject.get_auth(self.host) is not None:
                    self._session.auth = self.subject.get_auth(self.host)

        user_agent = "{} {} {} {} ({})".format(self.agent, self.package_info, self.python_info,
                                               self.system_info, self.os_info)
        self._session.headers.update({"User-Agent": user_agent})
        assert isinstance(self._session, requests.Session)
        return self._session


class RetrySession(Session):
    """ Session that automatically does a number of retries for failed transient errors. The time between retries
        double every time until a maximum of 30sec is reached

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

        In addition, the Connection error 'Connection reset be remote user' also triggers a retry
        """

    retry_errors = [requests.codes.unavailable,
                    requests.codes.service_unavailable,
                    requests.codes.gateway_timeout,
                    requests.codes.request_timeout,
                    requests.codes.timeout,
                    requests.codes.precondition_failed,
                    requests.codes.precondition,
                    requests.codes.payment_required,
                    requests.codes.payment]

    def __init__(self, retry=True, start_delay=1, *args, **kwargs):
        """
        ::param retry: set to False if retries no required
        ::param start_delay: start delay interval between retries (default=1s). Note that for HTTP 503,
                             this code follows the retry timeout set by the server in Retry-After
        """
        self.logger = logging.getLogger('RetrySession')
        self.retry = retry
        self.start_delay = start_delay
        super(RetrySession, self).__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        """
        Send a given PreparedRequest, wrapping the connection to service in try/except that retries on
        Connection reset by peer.
        :param request: The prepared request to send._session
        :param kwargs: Any keywords the adaptor for the request accepts.
        :return: the response
        :rtype: requests.Response
        """

        if self.retry:
            current_delay = max(self.start_delay, DEFAULT_RETRY_DELAY)
            current_delay = min(current_delay, MAX_RETRY_DELAY)
            num_retries = 0
            self.logger.debug("Sending request {0}  to server.".format(request))
            current_error = None
            while num_retries < MAX_NUM_RETRIES:
                try:
                    response = super(RetrySession, self).send(request, **kwargs)
                    self.check_status(response)
                    return response
                except requests.HTTPError as e:
                    if e.response.status_code not in self.retry_errors:
                        raise exceptions.HttpException(e)
                    current_error = e
                    if e.response.status_code == requests.codes.unavailable:
                        # is there a delay from the server (Retry-After)?
                        try:
                            current_delay = int(e.response.headers.get(SERVICE_RETRY, current_delay))
                            current_delay = min(current_delay, MAX_RETRY_DELAY)
                        except Exception:
                            pass

                except requests.ConnectionError as ce:
                    current_error = ce
                    # TODO not sure this appropriate for all the 'Connection reset by peer' errors.
                    # A post/put to vospace returns a document. If operation succeeded but the error
                    # occurs during the response the code below will send the request again. Since the
                    # resource has been created/updated, a new error (bad request maybe) might be issued
                    # by the server and that can confuse the caller.
                    # This code should probably deal with HTTP errors only as the 503s above.
                    self.logger.debug("Caught exception: {0}".format(ce))
                    if ce.errno != 104:
                        # Only continue trying on a reset by peer error.
                        raise exceptions.HttpException(ce)
                self.logger.warn("Resending request in {}s ...".format(current_delay))
                time.sleep(current_delay)
                num_retries += 1
                current_delay = min(current_delay*2, MAX_RETRY_DELAY)
            raise exceptions.HttpException(current_error)
        else:
            response = super(RetrySession, self).send(request, **kwargs)
            self.check_status(response)
            return response

    def check_status(self, response):
        """
        Check the response status. Maps the application related requests error status into Exceptions
        and raises the others
        :param response: response
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
            elif e.response.status_code == requests.codes.conflict:
                raise exceptions.AlreadyExistsException(orig_exception=e)
            elif e.response.status_code == requests.codes.internal_server_error:
                raise exceptions.InternalServerException(orig_exception=e)
            elif e.response.status_code in self.retry_errors:
                raise e
            else:
                raise exceptions.UnexpectedException(orig_exception=e)