# -*- coding: utf-8 -*-

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
#
# ***********************************************************************

"""
Web Service capabilities information
"""

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import logging
from lxml import etree

from six.moves.urllib.parse import urlparse

# nothing for public consumption
__all__ = []


class Capabilities(object):
    """
    Holds information regarding the capabilities of a Web Service.
    """

    def __init__(self):
        self.logger = logging.getLogger('Capabilities')
        self._caps = {}


    def add_capability(self, capability):
        self._caps[capability.standard_id] = capability

    def get_access_url(self, capability_id, security_methods):
        """
        Returns the access URL corresponding to a capability ID and a list of security methods. Raises
        a ValueError if no entry found.
        :param capability_id: ID of the capability
        :param security_methods: IDs of the security methods in the preferred order
        :return: URL to use for accessing the feature/capability
        """
        capability = self._caps[capability_id]
        if capability is None:
            raise ValueError('Capability {} not found'.format(capability_id))
        # if the capability supports anonymous access and this is the only capability interface or
        # the client comes with no security_methods, then return the url corresponding
        # to the anonymous access
        if (capability.get_interface(None) is not None) and \
                ((capability.num_interfaces == 1) or (len(security_methods) == 0)):
            return capability.get_interface(None)

        for sm in security_methods:
            if capability.get_interface(sm) is not None:
                return capability.get_interface(sm)
        raise ValueError('Capability {} does not support security methods {}'.
                             format(capability_id, security_methods))

def check_valid_url(url_str):
    """
    Checks whether the url argument as at least 3 components: scheme, netloc and path. Anything missing?
    :param url: url to check
    :raises ValueError if not valid
    """
    if url_str is None:
        raise ValueError('Invalid URL: {}'.format(url_str))
    url = urlparse(url_str)
    if (len(url.scheme) == 0) or\
       (len(url.netloc) == 0) or\
       (len(url.path) == 0):
        raise ValueError('Invalid URL: {}'.format(url_str))


class Capability(object):
    """
    Represents a capability of a Web Service. It has one or multiple _interfaces (access URLs)
    corresponding to different supported security methods
    """
    def __init__(self, standard_id):
        """
        :param standard_id: ID of the capability
        """
        self.logger = logging.getLogger('Capability')
        # validate standard id is valid uri with 3 components: scheme, netloc and path. Anything missing?
        check_valid_url(standard_id)
        self.standard_id = standard_id
        self._interfaces = {}

    def get_interface(self, security_method):
        # validate security_method is uri
        if security_method is not None:
            check_valid_url(security_method)
        return self._interfaces.get(security_method)

    def add_interface(self, access_url, security_method):
        # validate arguments
        check_valid_url(access_url)
        if security_method is not None:
            check_valid_url(security_method)
        self._interfaces[security_method] = access_url

    @property
    def num_interfaces(self):
        """
        Number of supported interfaces
        :return: number of supported interfaces
        """
        return len(self._interfaces)


class CapabilitiesReader(object):
    """
    Class to parse the capabilities xml file and return the corresponding capabilities object
    """
    def __init__(self):
        """
        Arguments:
        :param validate : If True enable schema validation, False otherwise
        """
        self.logger = logging.getLogger('CapabilitiesReader')

    def parsexml(self, content):
        """
        Parses the content and returns the corresponding Capabilities object
        :param content: string, containing the capabilities xml document
        :return: corresponding Capabilities
        """
        caps = Capabilities()
        try:
            doc = etree.fromstring(content)
        except Exception as e:
            raise ValueError('Error parsing capabilities document.', e)
        for cap in doc.iterfind('capability'):
            try:
                capability = Capability(cap.get('standardID'))
            except Exception as e:
                raise ValueError('Error parsing capabilities document. '
                                 'Capability standard ID is invalid URL: {}'.
                                 format(cap.get('standardID')))

            for child in cap.iterchildren(tag='interface'):
                if (child.find('accessURL') is not None) \
                    and (child.find('accessURL').text is not None):
                    access_url = child.find('accessURL').text.strip()
                else:
                    raise ValueError('Error parsing capabilities document. No accessURL for interface for {}'.
                                     format(capability.standard_id))
                security_method = child.find('securityMethod')
                if security_method is not None:
                    if security_method.get('standardID') is not None:
                        security_method = security_method.get('standardID').strip()
                    else:
                        raise ValueError('Error parsing capabilities document. '
                                         'Invalid security method {} for URL {} of capability {}'.
                                     format(security_method.get('standardID'),
                                            access_url, capability.standard_id))
                try:
                    capability.add_interface(access_url, security_method)
                except ValueError as e:
                    raise ValueError('Error parsing capabilities document. Invalid URL in access URL ({}) '
                                     'or security method ({})'.
                                     format(access_url, security_method))
            if len(capability._interfaces) == 0:
                raise ValueError('Error parsing capabilities document. No interfaces found for capability {}'.
                                     format(capability.standard_id))
            caps.add_capability(capability)
        if len(caps._caps) == 0:
            raise ValueError('Error parsing capabilities document: No capabilities found')
        return caps
