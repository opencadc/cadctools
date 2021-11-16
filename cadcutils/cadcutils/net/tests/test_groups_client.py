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

import sys
import pytest
import os
from mock import Mock, patch, call

from cadcutils.net import Subject, GroupsClient, Group, Identity, Role
from cadcutils.net.groups_client import main_app
from cadcutils.net.group_xml import GroupWriter, GroupsWriter


THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')


class MyExitError(Exception):
    def __init__(self):
        self.message = "MyExitError"


@patch('cadcutils.net.groups_client.BaseWsClient')
def test_group_create(basews_mock):
    put_mock = Mock()
    gms_service_url = 'https://service.org/groups'
    basews_mock.return_value.put = put_mock
    basews_mock.return_value.caps.get_access_url.return_value = gms_service_url
    group_id = 'grID'
    gw = GroupWriter()
    group = Group(group_id=group_id)
    gr_xml = gw.write(group)
    test_client = GroupsClient(Subject())
    test_client.create_group(group)
    put_mock.assert_called_with(gms_service_url, data=gr_xml)

    with pytest.raises(ValueError):
        test_client.create_group(None)


@patch('cadcutils.net.groups_client.BaseWsClient')
def test_group_get(basews_mock):
    group_id = 'grID'
    gw = GroupWriter()
    group = Group(group_id=group_id)
    gr_xml = gw.write(group)
    get_response = Mock(content=gr_xml)
    get_mock = Mock(return_value=get_response)
    gms_service_url = 'https://service.org/groups'
    basews_mock.return_value.get = get_mock
    basews_mock.return_value.caps.get_access_url.return_value = gms_service_url
    test_client = GroupsClient(Subject())
    actual_gr = test_client.get_group(group.group_id)
    get_mock.assert_called_with('{}/{}'.format(gms_service_url, group_id))
    assert group == actual_gr

    with pytest.raises(ValueError):
        test_client.get_group('')


@patch('cadcutils.net.groups_client.BaseWsClient')
def test_group_update(basews_mock):
    post_mock = Mock()
    gms_service_url = 'https://service.org/groups'
    basews_mock.return_value.post = post_mock
    basews_mock.return_value.caps.get_access_url.return_value = gms_service_url
    group_id = 'grID'
    gw = GroupWriter()
    group = Group(group_id=group_id)
    gr_xml = gw.write(group)
    test_client = GroupsClient(Subject())
    test_client.update_group(group)
    post_mock.assert_called_with(
        '{}/{}'.format(gms_service_url, group.group_id), data=gr_xml)

    with pytest.raises(ValueError):
        test_client.update_group(group=None)


@patch('cadcutils.net.groups_client.BaseWsClient')
def test_group_remove(basews_mock):
    delete_mock = Mock()
    gms_service_url = 'https://service.org/groups'
    basews_mock.return_value.delete = delete_mock
    basews_mock.return_value.caps.get_access_url.return_value = gms_service_url
    group_id = 'grID'
    test_client = GroupsClient(Subject())
    test_client.remove_group(group_id)
    delete_mock.assert_called_with(
        '{}/{}'.format(gms_service_url, group_id))

    with pytest.raises(ValueError):
        test_client.remove_group(None)


@patch('cadcutils.net.groups_client.BaseWsClient')
def test_add_member(basews_mock):
    put_mock = Mock()
    gms_service_url = 'https://service.org/groups'
    basews_mock.return_value.put = put_mock
    basews_mock.return_value.caps.get_access_url.return_value = gms_service_url
    group_id = 'grID'
    user_id = 'userID'
    test_client = GroupsClient(Subject())
    test_client.add_user_member(
        identity=Identity(name=user_id, identity_type='CADC'),
        group_id=group_id)
    params = {'idType': 'CADC', 'userID': user_id, 'groupID': group_id}
    put_mock.assert_called_with(
        '{}/{}/userMembers/{}'.format(gms_service_url, group_id, user_id),
        params=params)

    with pytest.raises(ValueError):
        test_client.add_user_member(identity=Identity(user_id, 'CADC'),
                                    group_id='   ')

    with pytest.raises(ValueError):
        test_client.add_user_member(identity=None, group_id='abc')


@patch('cadcutils.net.groups_client.BaseWsClient')
def test_remove_member(basews_mock):
    delete_mock = Mock()
    gms_service_url = 'https://service.org/groups'
    basews_mock.return_value.delete = delete_mock
    basews_mock.return_value.caps.get_access_url.return_value = gms_service_url
    group_id = 'grID'
    user_id = 'userID'
    test_client = GroupsClient(Subject())
    test_client.remove_user_member(
        identity=Identity(name=user_id, identity_type='CADC'),
        group_id=group_id)
    params = {'idType': 'CADC', 'userID': user_id, 'groupID': group_id}
    delete_mock.assert_called_with(
        '{}/{}/userMembers/{}'.format(gms_service_url, group_id, user_id),
        params=params)
    with pytest.raises(ValueError):
        test_client.remove_user_member(identity=None, group_id='abc')
    with pytest.raises(ValueError):
        test_client.remove_user_member(identity='abc', group_id='')


@patch('cadcutils.net.groups_client.BaseWsClient')
def test_add_group_member(basews_mock):
    put_mock = Mock()
    gms_service_url = 'https://service.org/groups'
    basews_mock.return_value.put = put_mock
    basews_mock.return_value.caps.get_access_url.return_value = gms_service_url
    group_id = 'grID'
    member_group_id = 'memberGrID'
    test_client = GroupsClient(Subject())
    params = {'groupID': group_id, 'groupID2': member_group_id}
    test_client.add_group_member(group_id=group_id,
                                 member_group_id=member_group_id)
    put_mock.assert_called_with(
        '{}/{}/groupMembers/{}'.format(gms_service_url,
                                       group_id, member_group_id),
        params=params)

    with pytest.raises(ValueError):
        test_client.add_group_member(None, member_group_id='abc')
    with pytest.raises(ValueError):
        test_client.add_group_member(group_id='abc', member_group_id=None)


@patch('cadcutils.net.groups_client.BaseWsClient')
def test_remove_group_member(basews_mock):
    delete_mock = Mock()
    gms_service_url = 'https://service.org/groups'
    basews_mock.return_value.delete = delete_mock
    basews_mock.return_value.caps.get_access_url.return_value = gms_service_url
    group_id = 'grID'
    member_group_id = 'memberGrID'
    test_client = GroupsClient(Subject())
    test_client.remove_group_member(group_id=group_id,
                                    member_group_id=member_group_id)
    params = {'groupID': group_id, 'groupID2': member_group_id}
    delete_mock.assert_called_with(
        '{}/{}/groupMembers/{}'.format(gms_service_url,
                                       member_group_id, group_id),
        params=params)

    with pytest.raises(ValueError):
        test_client.remove_group_member(group_id=None, member_group_id='abc')
    with pytest.raises(ValueError):
        test_client.remove_group_member(group_id='abc', member_group_id=None)


@patch('cadcutils.net.groups_client.BaseWsClient')
def test_get_membership(basews_mock):
    group_id1 = 'grID1'
    group1 = Group(group_id=group_id1)
    group_id2 = 'grID2'
    group2 = Group(group_id=group_id2)
    gw = GroupsWriter()
    grs_xml = gw.write([group1, group2])
    get_response = Mock(content=grs_xml)
    get_mock = Mock(return_value=get_response)
    gms_service_url = 'https://service.org/groups'
    basews_mock.return_value.get = get_mock
    basews_mock.return_value.caps.get_access_url.return_value = gms_service_url
    test_client = GroupsClient(Subject())
    actual_grs = test_client.get_membership()
    get_mock.assert_called_with(gms_service_url,
                                params={'role': 'member'})
    assert 2 == len(actual_grs)
    assert group1 in actual_grs
    assert group2 in actual_grs

    get_mock.reset_mock()
    test_client.get_membership(role=Role('admin'), group_id=group_id2)
    get_mock.assert_called_with(
        gms_service_url,
        params={'role': 'admin', 'groupID': group_id2})
    assert 2 == len(actual_grs)

    get_mock.reset_mock()
    test_client.get_membership(role=Role('owner'))
    get_mock.assert_called_with(gms_service_url, params={'role': 'owner'})


@patch('cadcutils.net.groups_client.BaseWsClient')
def test_is_member(basews_mock):
    group_id = 'grID'
    get_response = Mock(text=group_id + '\n')
    get_mock = Mock(return_value=get_response)
    gms_service_url = 'https://service.org/groups'
    basews_mock.return_value.get = get_mock
    basews_mock.return_value.caps.get_access_url.return_value = gms_service_url
    test_client = GroupsClient(Subject())
    assert test_client.is_member(group_ids=group_id)
    get_mock.assert_called_with(gms_service_url,
                                params={'group': [group_id]})
    # repeat for no groups
    get_mock.reset_mock()
    get_response = Mock(text='')  # no groups response
    get_mock = Mock(return_value=get_response)
    basews_mock.return_value.get = get_mock
    assert not test_client.is_member(group_ids=['gr1', 'gr2'])
    get_mock.assert_called_with(gms_service_url,
                                params={'group': ['gr1', 'gr2']})

    with pytest.raises(ValueError):
        test_client.is_member(None)


@patch('cadcutils.net.groups_client.Subject.from_cmd_line_args',
       Mock(return_value=Subject()))
@patch('cadcutils.net.groups_client.BaseWsClient')
@patch('cadcutils.net.GroupsClient.get_group')
def test_mainapp_get(get_group_mock, basews_mock):
    basews_mock.return_value.caps.get_access_url.return_value = \
        'https://serv.com/gms'
    sys.argv = 'cadc-groups get --cert cert.pem grID'.split()
    main_app()
    get_group_mock.assert_called_with(group_id='grID')

    # multiple groups
    get_group_mock.reset_mock()
    sys.argv = 'cadc-groups get --cert cert.pem grID1 grID2'.split()
    main_app()
    get_group_mock.has_calls([call(group_id='grID1'), call(group_id='grID2')])


@patch('cadcutils.net.groups_client.Subject.from_cmd_line_args',
       Mock(return_value=Subject()))
@patch('cadcutils.net.groups_client.BaseWsClient')
@patch('cadcutils.net.GroupsClient.create_group')
def test_mainapp_create(create_group_mock, basews_mock):
    basews_mock.return_value.caps.get_access_url.return_value = \
        'https://serv.com/gms'
    sys.argv = 'cadc-groups create --cert cert.pem --description Test_gr ' \
               'grID'.split()
    main_app()
    created_group = Group(group_id='grID')
    created_group.description = 'Test_gr'
    create_group_mock.assert_called_with(group=created_group)


@patch('cadcutils.net.groups_client.Subject.from_cmd_line_args',
       Mock(return_value=Subject()))
@patch('cadcutils.net.groups_client.BaseWsClient')
@patch('cadcutils.net.GroupsClient.remove_group')
def test_mainapp_remove(remove_group_mock, basews_mock):
    basews_mock.return_value.caps.get_access_url.return_value = \
        'https://serv.com/gms'
    sys.argv = 'cadc-groups remove --cert cert.pem grID'.split()
    main_app()
    remove_group_mock.assert_called_with(group_id='grID')


@patch('cadcutils.net.groups_client.Subject.from_cmd_line_args',
       Mock(return_value=Subject()))
@patch('cadcutils.net.groups_client.BaseWsClient')
@patch('cadcutils.net.GroupsClient.get_membership')
def test_mainapp_list(get_membership_mock, basews_mock):
    basews_mock.return_value.caps.get_access_url.return_value = \
        'https://serv.com/gms'
    sys.argv = 'cadc-groups list --cert cert.pem'.split()
    main_app()
    get_membership_mock.assert_called_with()

    get_membership_mock.reset_mock()
    sys.argv = 'cadc-groups list --cert cert.pem --role admin'.split()
    main_app()
    get_membership_mock.assert_called_with(Role('admin'))


@patch('cadcutils.net.groups_client.Subject.from_cmd_line_args',
       Mock(return_value=Subject()))
@patch('cadcutils.net.groups_client.BaseWsClient')
@patch('cadcutils.net.GroupsClient.get_group')
@patch('cadcutils.net.GroupsClient.add_group_member')
@patch('cadcutils.net.GroupsClient.add_user_member')
@patch('cadcutils.net.GroupsClient.remove_group_member')
@patch('cadcutils.net.GroupsClient.remove_user_member')
def test_mainapp_members(
        remove_user_member_mock,
        remove_group_member_mock,
        add_user_member_mock,
        add_group_member_mock,
        get_group_mock,
        basews_mock):
    basews_mock.return_value.caps.get_access_url.return_value = \
        'https://serv.com/gms'
    target_grid = 'GrID'
    get_group_mock.return_value = Group(group_id=target_grid)

    # add group member
    sys.argv = \
        'cadc-groups members --cert cert.pem --add-group ABC GrID'.split()
    main_app()
    add_group_member_mock.assert_called_with(group_id=target_grid,
                                             member_group_id='ABC')

    # add user member with CADC username
    sys.argv = \
        'cadc-groups members --cert cert.pem --add-user abc GrID'.split()
    main_app()
    add_user_member_mock.assert_called_with(
        group_id=target_grid,
        identity=Identity(name='abc', identity_type='HTTP'))

    # add user member with DN
    add_user_member_mock.reset_mock()
    sys.argv = 'cadc-groups members --cert cert.pem ' \
               '--add-user CN=abc,OU=org GrID'.split()
    main_app()
    add_user_member_mock.assert_called_with(
        group_id=target_grid,
        identity=Identity(name='CN=abc,OU=org', identity_type='X500'))

    # add group member
    sys.argv = \
        'cadc-groups members --cert cert.pem --remove-group ABC GrID'.split()
    main_app()
    remove_group_member_mock.assert_called_with(group_id=target_grid,
                                                member_group_id='ABC')

    # remove user member with CADC username
    sys.argv = \
        'cadc-groups members --cert cert.pem --remove-user abc GrID'.split()
    main_app()
    remove_user_member_mock.assert_called_with(
        group_id=target_grid,
        identity=Identity(name='abc', identity_type='HTTP'))

    # add user member with DN
    add_user_member_mock.reset_mock()
    sys.argv = 'cadc-groups members --cert cert.pem ' \
               '--remove-user CN=abc,OU=org GrID'.split()
    main_app()
    remove_user_member_mock.assert_called_with(
        group_id=target_grid,
        identity=Identity(name='CN=abc,OU=org', identity_type='X500'))
