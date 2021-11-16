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

from cadcutils.util import date2ivoa
from .. import Group
from .group_property_writer import GroupPropertyWriter
from .user_writer import UserWriter


class GroupWriter(object):

    def write(self, group, deep_copy=True, declaration=True):

        if not isinstance(group, Group):
            raise AttributeError('group is not a Group instance')

        return etree.tostring(self.get_group_element(group, deep_copy),
                              xml_declaration=declaration,
                              encoding='UTF-8',
                              pretty_print=True)

    def get_group_element(self, group, deep_copy):
        group_element = etree.Element('group')
        group_element.set('uri', group.uri)
        self._add_owner_element(group_element, group.owner)

        if deep_copy:
            if group.description is not None:
                self._add_element(group_element, group.description,
                                  'description')
            if group.last_modified is not None:
                self._add_datetime_element(group_element, group.last_modified,
                                           'lastModified')
            self._add_properties(group_element, group.properties)
            self._add_groups(group_element, group.group_members,
                             'groupMembers')
            self._add_users(group_element, group.user_members, 'userMembers')
            self._add_groups(group_element, group.group_admins, 'groupAdmins')
            self._add_users(group_element, group.user_admins, 'userAdmins')

        return group_element

    def _add_owner_element(self, group_element, owner):
        if owner is None:
            return

        user_writer = UserWriter()
        owner_element = etree.SubElement(group_element, 'owner')
        owner_element.append(user_writer.get_user_element(owner))

    def _add_properties(self, group_element, properties):
        if properties is None or not properties:
            return
        properties_element = etree.SubElement(group_element, 'properties')
        property_writer = GroupPropertyWriter()
        for property in properties:
            if property is not None:
                properties_element.append(
                    property_writer.get_property_element(property))

    def _add_groups(self, group_element, groups, tag):
        if groups is None or not groups:
            return
        members_element = etree.SubElement(group_element, tag)
        for group in groups:
            members_element.append(self.get_group_element(group, False))

    def _add_users(self, parent, users, tag):
        if users is None or not users:
            return
        user_writer = UserWriter()
        members_element = etree.SubElement(parent, tag)
        for user in users:
            members_element.append(user_writer.get_user_element(user))

    def _add_element(self, parent, text, tag):
        if text is None:
            return
        element = etree.SubElement(parent, tag)
        if isinstance(text, str):
            element.text = text
        else:
            element.text = str(text)

    def _add_datetime_element(self, parent, value, tag):
        if value is None:
            return
        element = etree.SubElement(parent, tag)
        element.text = date2ivoa(value)
