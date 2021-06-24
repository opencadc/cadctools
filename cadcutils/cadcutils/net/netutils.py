#!/private/tmp/venv/bin/python
# -*- coding: utf-8 -*-

# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2020.                            (c) 2020.
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

import re
from six.moves.urllib.parse import unquote
import logging
from xml.etree import ElementTree
import sys
import errno
import time
import base64

logger = logging.getLogger(__name__)

__all__ = ['get_header_filename', 'extract_md5', 'Transfer']


VO_VIEW_DEFAULT = 'ivo://ivoa.net/vospace/core#defaultview'
# CADC specific views
VO_CADC_VIEW_URI = 'ivo://cadc.nrc.ca/vospace/view'
CADC_VO_VIEWS = {'data': '{}#data'.format(VO_CADC_VIEW_URI),
                 'manifest': '{}#manifest'.format(VO_CADC_VIEW_URI),
                 'rss': '{}#rss'.format(VO_CADC_VIEW_URI),
                 'cutout': '{}#cutout'.format(VO_CADC_VIEW_URI)}
SSO_SECURITY_METHODS = {
    'tls-with-certificate': 'ivo://ivoa.net/sso#tls-with-certificate',
    'cookie': 'ivo://ivoa.net/sso#cookie',
    'token': 'vos://cadc.nrc.ca~vospace/CADC/std/Auth#token-1.0'
}


def get_header_filename(headers):
    """
    Extracts the file name from Content-Disposition in the GET response

    Partial implementation of https://tools.ietf.org/html/rfc6266)
    :param headers: HTTP headers of the response
    :return: Name of the file or None if not found
    """
    cd = headers.get('content-disposition', None)
    if not cd:
        return None
    fname = re.findall(r'filename\*=([^;]+)', cd, flags=re.IGNORECASE)
    if not fname:
        fname = re.findall("filename=([^;]+)", cd, flags=re.IGNORECASE)
    if "utf-8''" in fname[0].lower():
        fname = re.sub("utf-8''", '', fname[0], flags=re.IGNORECASE)
        if not isinstance(fname, str):
            # TODO remove - this is just for Python2 support
            fname = unquote(fname.encode('utf-8')).decode('utf-8')
        else:
            fname = unquote(fname)
    else:
        fname = fname[0]
    # clean space and double quotes
    return fname.strip().strip('"')


def extract_md5(headers):
    # extracts the md5checksum from the digest header
    if ('digest' in headers) and (headers['digest'].startswith('md5')):
        return base64.b64decode(headers['digest'][4:]).decode('ascii')
    return None


class Transfer(object):
    IVOAURL = 'ivo://ivoa.net/vospace/core'
    VOSNS = 'http://www.ivoa.net/xml/VOSpace/v2.0'
    VOSVERSION = '2.1'
    XSINS = 'http://www.w3.org/2001/XMLSchema-instance'
    TYPE = '{{{}}}type'.format(XSINS)
    NODES = '{{{}}}nodes'.format(VOSNS)
    NODE = '{{{}}}node'.format(VOSNS)
    PROTOCOL = '{{{}}}protocol'.format(VOSNS)
    PROPERTIES = '{{{}}}properties'.format(VOSNS)
    PROPERTY = '{{{}}}property'.format(VOSNS)
    ACCEPTS = '{{{}}}accepts'.format(VOSNS)
    PROVIDES = '{{{}}}provides'.format(VOSNS)
    ENDPOINT = '{{{}}}endpoint'.format(VOSNS)
    TARGET = '{{{}}}target'.format(VOSNS)
    DATA_NODE = 'vos:DataNode'
    LINK_NODE = 'vos:LinkNode'
    CONTAINER_NODE = 'vos:ContainerNode'

    DIRECTION_PULL_FROM = "pullFromVoSpace"
    DIRECTION_PUSH_TO = "pushToVoSpace"

    def __init__(self, session):
        """
        Handle transfer related business.  This is here to be reused as needed.
        :param session : requests session to use to communicate with the server
        """
        self.session = session

    def transfer(self, endpoint_url, uri, direction, view=None, cutout=None,
                 security_methods=None, with_uws_job=True):
        """Build the transfer XML document
        :param endpoint_url: URL of the endpoint to POST to
        :param uri: the uri to transfer from or to VOSpace.
        :param direction: is this a pushToVoSpace or a pullFromVoSpace ?
        :param view: which view of the node (data/default/cutout/etc.) is
        being transferred
        :param cutout: a special parameter added to the 'cutout' view
        request. e.g. '[0][1:10,1:10]'
        :param security_methods: the IVOA SSO security methods (see
        vos.SSO_SECURITY_METHODS) that the client
        :param with_uws_job: False if simplified transfer negotiation with no
        UWS jobs (such as in storage inventory)
        intends to use. The service is supposed to return appropriate URLs
        for the security_method. When the security method is not present,
        the service returns pre-authorized URLs that only work for the
        intended purpose, e.g. a pre-authorized PUT URL cannot be used for
        a read (GET or HEAD)

        :raises When a network problem occurs, it raises one of the
        HttpException exceptions declared in the
        cadcutils.exceptions module
        """
        protocol = {
            Transfer.DIRECTION_PULL_FROM: "httpsget",
            Transfer.DIRECTION_PUSH_TO: "httpsput"}

        transfer_xml = ElementTree.Element("vos:transfer")
        transfer_xml.attrib['xmlns:vos'] = Transfer.VOSNS
        transfer_xml.attrib['version'] = Transfer.VOSVERSION
        ElementTree.SubElement(transfer_xml, "vos:target").text = uri
        ElementTree.SubElement(transfer_xml, "vos:direction").text = \
            direction

        if view == 'move':
            ElementTree.SubElement(transfer_xml,
                                   "vos:keepBytes").text = "false"
        else:
            if view == 'defaultview':
                ElementTree.SubElement(transfer_xml, "vos:view").attrib[
                    'uri'] = VO_VIEW_DEFAULT
            elif view is not None:
                vos_view = ElementTree.SubElement(transfer_xml, "vos:view")
                vos_view.attrib['uri'] = CADC_VO_VIEWS[view]
                if cutout is not None and view == 'cutout':
                    param = ElementTree.SubElement(vos_view, "vos:param")
                    param.attrib['uri'] = CADC_VO_VIEWS[view]
                    param.text = cutout
            protocol_element = ElementTree.SubElement(transfer_xml,
                                                      "vos:protocol")
            protocol_element.attrib['uri'] = "{0}#{1}".format(
                Transfer.IVOAURL, protocol[direction])
            if security_methods:
                for sm in security_methods:
                    if sm not in SSO_SECURITY_METHODS.values():
                        raise AttributeError(
                            'Invalid security method {}. Supported values: {}'.
                            format(sm, SSO_SECURITY_METHODS.values))
                    security_element = ElementTree.SubElement(
                        protocol_element, "vos:securityMethod")
                    security_element.attrib['uri'] = sm

        logging.debug(ElementTree.tostring(transfer_xml))
        logging.debug("Sending to : {}".format(endpoint_url))

        data = ElementTree.tostring(transfer_xml)
        resp = self.session.post(
            endpoint_url, data=data, allow_redirects=False,
            headers={'Content-Type': 'text/xml'})

        logging.debug("{0}".format(resp))
        logging.debug("{0}".format(resp.text))
        while resp.status_code == 303:
            goto_url = resp.headers.get('Location', None)

            if self.session.auth is not None and \
                    "auth" not in goto_url:
                goto_url = goto_url.replace('/vospace/', '/vospace/auth/')

            logging.debug(
                'Got back from transfer URL: {}'.format(goto_url))
            # for get or put we need the protocol value
            resp = self.session.get(goto_url, allow_redirects=False)
        if resp.status_code == 200:
            transfer_url = str(resp.url)
            if view == 'move':
                return transfer_url
            if with_uws_job:
                # check the status of the job first
                self.check_job_error(
                    str.replace(transfer_url, 'xfer', 'transfers'),
                    str(uri), True)
            xml_string = resp.text
            logging.debug('Transfer Document:{}'.format(xml_string))
            transfer_document = ElementTree.fromstring(xml_string)
            logging.debug(
                "XML version: {0}".format(
                    ElementTree.tostring(transfer_document)))
            all_protocols = transfer_document.findall(Transfer.PROTOCOL)
            if all_protocols is None or not len(all_protocols) > 0:
                raise RuntimeError(
                    "BUG: No protocol/endpoint returned for transfer URL {}".
                    format(transfer_url))
        elif resp.status_code == 404:
            raise OSError(resp.status_code,
                          "File not found: {0}".format(uri))
        else:
            raise OSError(resp.status_code,
                          "Failed to get transfer service response.")

        result = []
        for protocol in all_protocols:
            for node in protocol.findall(Transfer.ENDPOINT):
                result.append(node.text)
        return result

    def _get_phase(self, phase_url):
        response = self.session.get(phase_url, allow_redirects=True)
        response.raise_for_status()
        return response.text

    def get_transfer_error(self, url, uri):
        """Follow a transfer URL to completion and returns the error if it
        fails
        :param url: The URL of the transfer request that had the error.
        :param uri: The uri that we were trying to transfer (get or put).

        :raises When a network problem occurs, it raises one of the
        HttpException exceptions declared in the
        cadcutils.exceptions module. Returns None if job completes successfully
        """
        job_url = str.replace(url, "/results/transferDetails", "")

        try:
            phase_url = job_url + "/phase"
            sleep_time = 1
            roller = ('\\', '-', '/', '|', '\\', '-', '/', '|')
            phase = self._get_phase(phase_url)
            # do not remove the line below. It is used for testing
            logging.debug("Job URL: " + job_url + "/phase")
            while phase in ['PENDING', 'QUEUED', 'EXECUTING', 'UNKNOWN']:
                # poll the job. Sleeping time in between polls is doubling
                # each time until it gets to 32sec
                total_time_slept = 0
                if sleep_time <= 32:
                    sleep_time *= 2
                slept = 0
                if logger.getEffectiveLevel() == logging.INFO:
                    while slept < sleep_time:
                        sys.stdout.write(
                            '\r{} {}'.format(phase, roller[total_time_slept %
                                                           len(roller)]))
                        sys.stdout.flush()
                        slept += 1
                        total_time_slept += 1
                        time.sleep(1)
                    sys.stdout.write("\r                    \n")
                else:
                    time.sleep(sleep_time)
                phase = self._get_phase(phase_url)
                logging.debug(
                    'Async transfer Phase for url {}: {}'.format(url, phase))
        except KeyboardInterrupt:
            # abort the job when receiving a Ctrl-C/Interrupt from the client
            logging.error("Received keyboard interrupt")
            self.session.post(
                job_url + "/phase", allow_redirects=False, data="PHASE=ABORT",
                headers={'Content-type': 'application/x-www-form-urlencoded'})
            raise KeyboardInterrupt
        phase = self._get_phase(phase_url)
        logger.debug("Phase:  {0}".format(phase))
        if phase in ['COMPLETED']:
            return False
        if phase in ['HELD', 'SUSPENDED', 'ABORTED']:
            # re-queue the job and continue to monitor for completion.
            raise OSError("UWS status: {0}".format(phase), errno.EFAULT)
        return self.check_job_error(url, uri)

    def check_job_error(self, url, uri, check_phase=False):
        """
        Checks whether a job is an error state and raises the appropriate
        exception. Job does not have to be completed.
        :param url: job url
        :param uri: vospace artifac
        :param check_phase: True if job phase needs to be read from server,
        False otherwise
        :return: Raises an exception if job failed.
        """
        error_codes = {'NodeNotFound': errno.ENOENT,
                       'RequestEntityTooLarge': errno.E2BIG,
                       'PermissionDenied': errno.EACCES,
                       'OperationNotSupported': errno.EOPNOTSUPP,
                       'InternalFault': errno.EFAULT,
                       'ProtocolNotSupported': errno.EPFNOSUPPORT,
                       'ViewNotSupported': errno.ENOSYS,
                       'InvalidArgument': errno.EINVAL,
                       'InvalidURI': errno.EFAULT,
                       'TransferFailed': errno.EIO,
                       'DuplicateNode.': errno.EEXIST,
                       'NodeLocked': errno.EPERM}
        job_url = str.replace(str(url), "/results/transferDetails", "")
        if check_phase:
            phase = self._get_phase(job_url + "/phase")
            if phase not in ['ERROR']:
                return False
        error_url = job_url + "/error"
        error_message = self.session.get(error_url).text
        logger.debug(
            "Got transfer error {0} on URI {1}".format(error_message, uri))
        # Check if the error was that the link type is unsupported and try and
        # follow that link.
        target = re.search('Unsupported link target:(?P<target> .*)$',
                           error_message)
        if target is not None:
            return target.group('target').strip()
        raise OSError(error_codes.get(error_message, errno.EFAULT),
                      '{0}: {1}'.format(uri, error_message))
