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

import sys
import traceback
import logging
from six.moves.urllib.parse import urlparse, urlencode


from cadcutils import version  # TODO should it be application version instead?
from cadcutils import util, exceptions
from . import BaseWsClient, Subject
from .auth import SECURITY_METHODS_IDS, CookieInfo
from . import Role, Group, Identity
from .group_xml import GroupWriter, GroupReader, GroupsReader


CADC_LOGIN_CAPABILITY = 'ivo://ivoa.net/std/UMS#login-0.1'
CADC_SSO_COOKIE_NAME = 'CADC_SSO'
CADC_REALMS = ['.canfar.net', '.cadc-ccda.hia-iha.nrc-cnrc.gc.ca',
               '.cadc.dao.nrc.ca']

# ID of the default catalog Web service
CADC_AC_SERVICE_ID = 'ivo://cadc.nrc.ca/gms'
DEFAULT_SERVICE_ID = 'ivo://cadc.nrc.ca/gms'

SEARCH_CAPABILITY_ID = 'ivo://ivoa.net/std/GMS#search-1.0'

# spec extensions relative to root of GMS service URL
GROUPS_PATH = 'groups'
GROUP_MEMBER_PATH = 'groupMembers'
USER_MEMBER_PATH = 'userMembers'

APP_NAME = 'cadc-group'

__all__ = ['GroupsClient']

logger = util.get_logger(__name__)

cadcgms_agent = 'cadc-gms-client/{}'.format(version.version)


class GroupsClient():
    """Class for interacting with the access control web service"""

    def __init__(self, subject, resource_id=DEFAULT_SERVICE_ID,
                 host=None, agent=None, insecure=False):
        """
        Instance of a GroupsClient
        :param subject: the subject performing the action
        :type cadcutils.auth.Subject
        :param resource_id: the resource ID of the service (service ID)
        :param host: Host for the GMS service (use for testing mainly)
        :param agent: The name of the agent (to be used in server logging)
        :param insecure Allow insecure server connections over SSL (testing)
        """
        self.resource_id = resource_id
        self.host = host

        self._subject = subject
        if agent is None:
            self.agent = cadcgms_agent
        else:
            self.agent = agent

        # for the CADC TAP services, BasicAA is not supported anymore, so
        # we need to login and get a cookie when the subject uses
        # user/passwd
        if resource_id.startswith('ivo://cadc.nrc.ca') and\
           SECURITY_METHODS_IDS['basic'] in subject.get_security_methods():
            login = BaseWsClient(CADC_AC_SERVICE_ID, Subject(),
                                 self.agent, insecure=insecure,
                                 retry=True, host=self.host)
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
                    CookieInfo(cadc_realm, CADC_SSO_COOKIE_NAME,
                               '"{}"'.format(cookie_response.text)))

        self._gms_client = BaseWsClient(resource_id, subject, self.agent,
                                        retry=True, host=self.host,
                                        insecure=insecure)
        # double check it is a GMS server
        try:
            self._search_ep = \
                self._gms_client.caps.get_access_url(SEARCH_CAPABILITY_ID)
        except Exception as ex:
            logger.error('Not a GMS service', ex)
            raise ex

        # non-standard end points
        self._groups_ep = \
            self._search_ep[:self._search_ep.rfind('/')] + '/groups'

    def create_group(self, group):
        """ Persist the given Group """
        if group is None:
            raise ValueError("Group cannot be None.")

        writer = GroupWriter()
        xml_string = writer.write(group)
        self._gms_client.put(self._groups_ep, data=xml_string)

    def get_group(self, group_id):

        if group_id is None or group_id.strip() == '':
            raise ValueError("Group ID cannot be None or empty.")

        xml_string = self._gms_client.get('{}/{}'.format(
            self._groups_ep, group_id)).content
        reader = GroupReader()
        group = reader.read(xml_string)
        return group

    def update_group(self, group):
        """
            Persist the given existing group Group
        """
        if group is None:
            raise ValueError("Group cannot be None.")

        writer = GroupWriter()
        xml_string = writer.write(group)
        self._gms_client.post('{}/{}'.format(self._groups_ep, group.group_id),
                              data=xml_string)

    def add_user_member(self, group_id, identity):
        """Add the user to the group's user members.
        group_id - the group to add the user member
        identity - the user to add as a user member
        """

        if identity is None:
            raise ValueError("User identity cannot be None.")
        if group_id is None or group_id.strip() == '':
            raise ValueError("Group ID cannot be None or empty.")

        url = '{}/{}/{}/{}'.format(
            self._groups_ep, group_id, USER_MEMBER_PATH, identity.name)
        parameters = {'idType': identity.type,
                      'userID': identity.name,
                      'groupID': group_id}
        self._gms_client.put(url, params=parameters)

    def remove_user_member(self, group_id, identity):
        """Remove the user from the group's user members.
        group_id - the group to remove the user member
        identity - the user to remove as a user member
        """

        if identity is None:
            raise ValueError("User identity cannot be None.")
        if group_id is None or group_id.strip() == '':
            raise ValueError("Group ID cannot be None or empty.")

        url = '{}/{}/{}/{}'.format(
            self._groups_ep, group_id, USER_MEMBER_PATH, identity.name)
        parameters = {'idType': identity.type,
                      'userID': identity.name,
                      'groupID': group_id}

        self._gms_client.delete(url, params=parameters)

    def add_group_member(self, group_id, member_group_id):
        """Add the group to the group's group members.
        group_id - ID of the group to add the group member to
        member_group_id - ID of the group to add to the group members
        """
        if group_id is None or not group_id.strip():
            raise ValueError("Group ID cannot be None or empty.")
        if member_group_id is None or not member_group_id.strip():
            raise ValueError("Member Group ID cannot be None or empty.")
        url = '{}/{}/{}/{}'.format(
            self._groups_ep, group_id, GROUP_MEMBER_PATH, member_group_id)
        params = {'groupID': group_id, 'groupID2': member_group_id}
        self._gms_client.put(url, params=params)

    def remove_group_member(self, group_id, member_group_id):
        """Remove the group from the group's group members.
        group_id - ID of the group to remove the member group from
        member_group_id - ID of the group to remove from group members
        """
        if group_id is None or not group_id.strip():
            raise ValueError("Group ID cannot be None or empty.")
        if member_group_id is None or not member_group_id.strip():
            raise ValueError("Member Group ID cannot be None or empty.")
        url = '{}/{}/{}/{}'.format(
            self._groups_ep, member_group_id, GROUP_MEMBER_PATH, group_id)
        params = {'groupID': group_id,
                  'groupID2': member_group_id}
        self._gms_client.delete(url, params=params)

    def get_membership(self, role=Role('member'), group_id=None):
        """Search for user group membership, of a certain role.
        role -- Role
        group_id -- if specified search only for this group
        """

        params = {}
        if group_id:
            params['groupID'] = group_id
        if role:
            params['role'] = role.get_name()
        xml_string = self._gms_client.get(
            self._search_ep, params=params).content
        reader = GroupsReader()
        groups = reader.read(xml_string, deep_copy=False)
        return groups

    def is_member(self, group_ids):
        """ Return True if user_id is a member (type of role) of at
        least one group in group_ids. False otherwise.
        group_ids -- list of group ID strings
        """

        # Allow the caller to supply a single string
        if not group_ids:
            raise ValueError('group_ids is required')
        if isinstance(group_ids, str):
            group_ids = [group_ids]

        # get_membership returns a single-element set if member of a
        # particular group. Stop as soon as we find one.
        groups = self._gms_client.get(self._search_ep,
                                      params={'group': group_ids}).text
        return len(groups.strip()) > 0

    def remove_group(self, group_id):
        """
        Remove a group from the CADC GMS
        :param group_id: The ID of the group to remove
        """
        if not group_id:
            raise ValueError('group_id is required')
        self._gms_client.delete('{}/{}'.format(self._groups_ep, group_id))

# TODO - not needed for now
#  class UsersClient(BaseClient):
#     """Class for interacting with the access control web service"""
#
#     def __init__(self, *args, **kwargs):
#         """GMS client constructor. The dn will be extracted from the
#         x509 cert and available as a default for user_id in other
#         method calls.
#         certfile -- Path to CADC proxy certificate
#         """
#
#         # This client does not support name/password authentication
#         super(UsersClient, self).__init__(usenetrc=False, *args, **kwargs)
#
#         # Specific base_url for AC webservice
#         host = os.getenv('AC_WEBSERVICE_HOST', self.host)
#         path = os.getenv('AC_WEBSERVICE_PATH', '/ac')
#         self.base_url = '%s://%s%s' % ('https', host, path)
#         self.logger.info('Base URL ' + self.base_url)
#
#         # This client will need the user DN
#         self.current_user_dn = self.get_current_user_dn()
#
#         # Specialized exceptions handled by this client
#         self._HTTP_STATUS_CODE_EXCEPTIONS[404] = {
#             "User": exceptions.UserNotFoundException(),
#             "Group": exceptions.GroupNotFoundException()
#         }
#         self._HTTP_STATUS_CODE_EXCEPTIONS[409] = \
#             exceptions.GroupExistsException()
#
#     def get_user(self, identity):
#
#         if identity is None:
#             raise ValueError("User Identity cannot be None.")
#         if not isinstance(identity, Identity):
#             raise ValueError("identity must be of type Identity.")
#
#         url = "{0}/users/{1}?idType={2}".format(self.base_url,
#         identity.name, identity.type)
#         xml_string = self._download_xml(url)
#         reader = UserReader()
#         user = reader.read(xml_string)
#         self.logger.info('Retrieved user {0}'.format(user.internal_id))
#         return user


def print_group(group):
    print('Group:')
    print('             ID: {}'.format(group.group_id))
    print('    Description: {}'.format(group.description))
    print('          Owner: {}'.format(group.owner.identities['X500'].name))
    print('  Last Modified: {}'.format(group.last_modified))
    print('    User Admins: {}'.format((',\n'+' '*17).join(
        ['{} ({})'.format(
            a.identities['HTTP'].name,
            a.identities['X500'].name) for a in group.user_admins])))
    print('   Group Admins: {}'.format(
        (',\n'+' '*17).join([gr.group_id for gr in group.group_admins])))
    print_group_members(group)


def print_group_members(group):
    print('   User Members: {}'.format((',\n'+' '*17).join(
        ['{} ({})'.format(
            m.identities['HTTP'].name,
            m.identities['X500x'].name) for m in group.user_members])))
    print('  Group Members: {}'.format(
        (',\n'+' '*17).join([gr.group_id for gr in group.group_members])))


def main_app():
    parser = util.get_base_parser(version=version.version, auth_required=True,
                                  default_resource_id=CADC_AC_SERVICE_ID)

    parser.description = (
        'Client for accessing the User Group Management System (GMS) at the '
        'Canadian Astronomy Data Centre')

    subparsers = parser.add_subparsers(
        dest='cmd',
        help='supported commands. Use the -h|--help argument of a command '
             'for more details')

    list_parser = subparsers.add_parser(
        'list',
        description='Retrieve name of all the groups the user has access to',
        help='Retrieve names of all the groups the user has access to')
    list_parser.add_argument(
        '-r', '--role', choices=['admin', 'member', 'owner'],
        help='Filter by role in the group',
        required=False)
    list_parser.epilog = (
        'Example:\n'
        '        cadc-groups list --cert ~/.ssl/cadcproxy.pem\n')

    get_parser = subparsers.add_parser(
        'get',
        description='Retrieve information regarding CADC groups',
        help='Retrieve information regarding CADC groups')
    get_parser.add_argument(
        'groupid', help='the id of the CADC group', nargs='+')
    get_parser.epilog = (
        'Example:\n'
        '        cadc-groups get --cert $HOME/.ssl/cadcproxy.pem FOO\n')

    create_parser = subparsers.add_parser(
        'create',
        description='Creates a new CADC Group. Caller is owner and member of'
                    'the new group',
        help='Creates a new CADC Group. Caller is owner and member of the new '
             'group')
    create_parser.add_argument(
        '--description',
        default=None,
        help='group description',
        required=False)
    create_parser.add_argument(
        'groupid',
        help='ID of the CADC group. ID cannot contain space ( ), slash (/), '
             'escape (\\), or percent (%) characters')
    create_parser.epilog = (
        'Examples:\n'
        '        cadc-groups create --cert ~/.ssl/cadcproxy.pem -d "My first '
        'group" -m "cn=abc" foo\n')

    members_parser = subparsers.add_parser(
        'members',
        description='Display, sets or deletes group membership information',
        help='Display, sets or deletes group membership information')
    cmd_group = members_parser.add_mutually_exclusive_group()
    cmd_group.add_argument('-clear', '--clear', action='store_true',
                           help='Clear group and user membership')
    cmd_group.add_argument('--remove-group', action='append',
                           help='Remove member group with provided ID')
    cmd_group.add_argument('--add-group', action='append',
                           help='Add member group with provided ID')
    cmd_group.add_argument('--remove-user', action='append',
                           help='Remove member user with CADC username or '
                                'distinguished name (DN)')
    cmd_group.add_argument('--add-user', action='append',
                           help='Add member user with with given CADC username'
                                ' or distinguished name (DN)')
    members_parser.add_argument('groupid', help='ID of the group')
    members_parser.epilog = (
        'Examples:\n'
        '        cadc-groups members --erase GEMINI foo\n')

    remove_parser = subparsers.add_parser(
        'remove',
        description='Remove a CADC group.',
        help='Remove a CADC group')
    remove_parser.add_argument('groupid', help='ID of the group',
                               nargs='+')

    def handle_error(exception, exit_after=True):
        """
        Prints error message and exit (by default)
        :param exception: error
        :param exit_after: True if log error message and exit,
        False if log error message and return
        :return:
        """

        if isinstance(exception, exceptions.UnauthorizedException):
            # TODO - magic authentication
            # if subject.anon:
            #     handle_error('Operation cannot be performed anonymously. '
            #      'Use one of the available methods to authenticate')
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

        # if logger.isEnabledFor(logging.DEBUG):
        #     traceback.print_stack()

        if exit_after:
            sys.exit(-1)  # TODO use different error codes?

    args = parser.parse_args()
    if len(sys.argv) < 2:
        parser.print_usage(file=sys.stderr)
        sys.stderr.write("{}: error: too few arguments\n".format(APP_NAME))
        sys.exit(-1)
    if args.verbose:
        logging.basicConfig(level=logging.INFO, stream=sys.stdout)
        logger.setLevel(logging.INFO)
    elif args.debug:
        logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
        logger.setLevel(logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING, stream=sys.stdout)

    subject = Subject.from_cmd_line_args(args)

    client = GroupsClient(subject, args.resource_id, host=args.host)
    try:
        if args.cmd == 'list':
            logger.info('list')
            if args.role:
                print(client.get_membership(Role(args.role)))
            else:
                print(client.get_membership())
        elif args.cmd == 'get':
            logger.info('get')
            for gi in args.groupid:
                print_group(client.get_group(group_id=gi))
        elif args.cmd == 'create':
            group = Group(args.groupid)
            group.description = args.description
            client.create_group(group=group)
        elif args.cmd == 'members':
            logger.info('members')
            group = client.get_group(args.groupid)
            if args.clear:
                group.group_members.clear()
                group.user_members.clear()
                client.update_group(group)
            elif args.add_user:
                for au in args.add_user:
                    if au.strip().lower().startswith('cn='):
                        id_type = 'X500'
                    else:
                        id_type = 'HTTP'
                    client.add_user_member(
                        group_id=group.group_id,
                        identity=Identity(name=au, identity_type=id_type))
            elif args.add_group:
                for ag in args.add_group:
                    client.add_group_member(group_id=group.group_id,
                                            member_group_id=ag)
            elif args.remove_user:
                for ru in args.remove_user:
                    if ru.strip().lower().startswith('cn='):
                        id_type = 'X500'
                    else:
                        id_type = 'HTTP'
                    client.remove_user_member(
                        group_id=group.group_id,
                        identity=Identity(name=ru, identity_type=id_type))
            elif args.remove_group:
                for rg in args.remove_group:
                    client.remove_group_member(group_id=group.group_id,
                                               member_group_id=rg)
            else:
                print_group_members(group)
        elif args.cmd == 'remove':
            logger.info('remove')
            for gi in args.groupid:
                client.remove_group(group_id=gi)
        else:
            raise RuntimeError('Unknown command option: ' + args.cmd)
    except Exception as ex:
        if logger.isEnabledFor(logging.DEBUG):
            traceback.print_stack()
        handle_error(ex, exit_after=True)
    else:
        logger.info("DONE")
