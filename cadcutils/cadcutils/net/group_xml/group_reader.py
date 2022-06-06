# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
# (c) 2021.                            (c) 2021.
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
# ***********************************************************************

from lxml import etree
from urllib.parse import urlparse

from cadcutils.util import str2ivoa
from .. import Group
from .group_property_reader import GroupPropertyReader
from .user_reader import UserReader

GROUP_URI = 'ivo://cadc.nrc.ca/gms?'


class GroupReader(object):
    """GroupReader """

    def read(self, xml_string, deep_copy=True):
        """Build an Group object from an XML document string.

        Arguments:
        xml_string -- string of XML containing the Group element
        return     -- a Group object
        """
        root = etree.fromstring(xml_string, etree.XMLParser())
        return self.get_group(root, deep_copy)

    def get_group(self, group_element, deep_copy):
        uri = group_element.get("uri")
        if uri is None:
            raise GroupParsingException("group missing required uri attribute")
        if GROUP_URI not in uri:
            raise GroupParsingException(
                "group uri attribute malformed: {0}".format(uri))

        url_parts = urlparse(uri)
        authority = '{}://{}{}'.format(
            url_parts.scheme, url_parts.netloc, url_parts.path)
        group_id = url_parts.query
        group = Group(group_id=group_id, authority=authority)
        group.owner = self._get_owner(group_element)

        if deep_copy:
            group.description = self._get_child_text(group_element,
                                                     'description')
            last_modified = self._get_child_text(group_element, 'lastModified')
            if last_modified is not None:
                group.last_modified = str2ivoa(last_modified)
            group.properties = self._get_group_properties(group_element)
            group.group_members = self._get_groups(group_element,
                                                   'groupMembers')
            group.user_members = self._get_users(group_element, 'userMembers')
            group.group_admins = self._get_groups(group_element, 'groupAdmins')
            group.user_admins = self._get_users(group_element, 'userAdmins')

        return group

    def _get_child_text(self, parent, tag):
        child_element = self._get_child_element(parent, tag)
        if child_element is None:
            return None
        else:
            return child_element.text

    def _get_child_element(self, parent, tag):
        for element in list(parent):
            if element.tag == tag:
                if element.keys() or element.text:
                    # element has attributes or content, return it
                    return element
                break
        return None

    def _get_owner(self, group_element):
        user_element = group_element.find('./owner/user')
        if user_element is None:
            return None

        user_reader = UserReader()
        user = user_reader.get_user(user_element)
        return user

    def _get_group_properties(self, group_element):
        properties = set()
        properties_element = self._get_child_element(group_element,
                                                     'properties')
        if properties_element is not None:
            property_reader = GroupPropertyReader()
            for property_element in properties_element.findall('property'):
                properties.add(
                    property_reader.get_group_property(property_element))
        return properties

    def _get_groups(self, parent, tag):
        groups = set()
        group_elements = self._get_child_element(parent, tag)
        if group_elements is not None:
            for group in group_elements.findall('group'):
                groups.add(self.get_group(group, False))
        return groups

    def _get_users(self, parent, tag):
        users = set()
        user_elements = self._get_child_element(parent, tag)
        if user_elements is not None:
            for user in user_elements.findall('user'):
                user_reader = UserReader()
                user = user_reader.get_user(user)
                users.add(user)
        return users


class GroupParsingException(Exception):
    """A group exception class for catching XML parsing exception"""

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)
