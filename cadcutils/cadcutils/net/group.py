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
Module that contains functionality related to CADC groups.
"""
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import re

from cadcutils import version  # TODO should it be application version instead?

from cadcutils import util

CADC_LOGIN_CAPABILITY = 'ivo://ivoa.net/std/UMS#login-0.1'
CADC_SSO_COOKIE_NAME = 'CADC_SSO'
CADC_REALMS = ['.canfar.net', '.cadc-ccda.hia-iha.nrc-cnrc.gc.ca',
               '.cadc.dao.nrc.ca']

# ID of the default catalog Web service
CADC_AC_SERVICE_ID = 'ivo://cadc.nrc.ca/gms'
DEFAULT_SERVICE_ID = 'ivo://cadc.nrc.ca/gms'

SEARCH_CAPABILITY_ID = 'ivo://ivoa.net/std/gms#search-1.0'

# spec extensions relative to root of GMS service URL
GROUPS_PATH = 'groups'
GROUP_MEMBER_PATH = 'groupMembers'
USER_MEMBER_PATH = 'userMembers'


__all__ = ['User', 'Group', 'Role', 'GroupProperty', 'Identity']

# these are the security methods currently supported
SECURITY_METHODS_IDS = {
    'certificate': 'ivo://ivoa.net/sso#tls-with-certificate',
    'cookie': 'ivo://ivoa.net/sso#cookie'}

logger = util.get_logger(__name__)

cadcgms_agent = 'cadc-gms-client/{}'.format(version.version)


class User:
    """
    Class representing a CADC groups user
    """

    def __init__(self, internal_id=None):
        """
        internal_id is a uri which uniquely identifies the user.
        The uri scheme and scheme-specific-part uniquely identify the service,
        while the uri fragment uniquely identifies the user of that service.
        """
        self.internal_id = internal_id
        self.identities = {}

    def __eq__(self, other):
        return self.internal_id == other.internal_id

    def __hash__(self):
        return hash(self.internal_id)


class Role:
    """Helper class for constraining allowable roles"""

    allowable_roles = set(['owner', 'member', 'admin'])

    def __init__(self, role_name):
        name_lower = role_name.lower()
        if name_lower in self.allowable_roles:
            self._role = name_lower
        else:
            raise ValueError("Role %s is not in the allowable list: %s" % (
                name_lower, self.allowable_roles))

    def get_name(self):
        return self._role

    def __eq__(self, other):
        return self._role == other._role

    def __hash__(self):
        return hash(self._role)


class Identity():

    identity_types = ['X500', 'OpenID', 'HTTP', 'CADC', 'POSIX']

    def __init__(self, name, identity_type):
        if name is None or not name:
            raise ValueError('Identity name is None or empty')
        if identity_type not in self.identity_types:
            raise ValueError('Unknown Identity type ' + identity_type)
        self.name = name
        self.type = identity_type

    def __eq__(self, other):
        return self.name, self.type == other.name, other.type


class GroupProperty:

    STRING_TYPE = 'String'
    INTEGER_TYPE = 'Integer'

    def __init__(self, key, value, read_only=False):
        """
        A property representing metadata for a group.
        """
        if key is None:
            raise ValueError('GroupProperty key cannot be None')
        if value is None:
            raise ValueError('GroupProperty value cannot be None')

        self.key = key
        self.value = value
        self.read_only = read_only

    def __eq__(self, other):
        return self.key, self.value, self.read_only == other.key, other.value,\
               other.read_only

    def __hash__(self):
        return hash((self.key, self.value))


class Group(object):
    """
    Class representing a CADC group
    """

    def __init__(self, group_id, authority='ivo://cadc.nrc.ca/gms'):
        """
        Ctor
        :param group_id: Group name.
        :param authority: The authority under which this group name is
        registered
        """
        group_id_re = re.compile('^[a-zA-Z0-9\\-\\.~_]*$')
        if not group_id_re.match(group_id):
            raise Exception(
                'Invalid group ID {} may not contain space ( ), slash (/), '
                'escape (\\), or percent (%)'.format(group_id_re))

        self.group_id = group_id
        self.authority = authority
        self.uri = '{}?{}'.format(authority, group_id)

        self.owner = None
        self.description = None
        self.last_modified = None
        self.properties = set()
        self.user_members = set()
        self.group_members = set()
        self.user_admins = set()
        self.group_admins = set()

    def add_user_member(self, user):
        """

        :param user: member to add (User type)
        """
        if not isinstance(user, User):
            raise AttributeError('user must be of `User` type')
        self.user_members.add(user)

    def add_user_admin(self, user):
        """
        Adds an admin for the group
        :param user: admin to add (User type)
        """
        if not isinstance(user, User):
            raise AttributeError('user must be of `User` type')
        self.user_admins.add(user)

    def __eq__(self, other):
        return self.group_id == other.group_id

    def __hash__(self):
        return hash(self.group_id)
