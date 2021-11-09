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

from datetime import datetime

from cadcutils.net import Group, GroupProperty, User, Identity
from cadcutils.net.group_xml.group_reader import GroupReader
from cadcutils.net.group_xml.group_writer import GroupWriter


def test_minimal_group():
    expected = Group('groupID')
    writer = GroupWriter()
    xml_string = writer.write(expected, False)

    assert xml_string

    reader = GroupReader()
    actual = reader.read(xml_string)
    assert expected.group_id
    assert actual.group_id
    assert(actual.group_id == expected.group_id)

    assert expected.owner == actual.owner
    assert expected.description == actual.description
    assert expected.last_modified == actual.last_modified

    assert(actual.group_members == expected.group_members)
    assert(actual.user_members == expected.user_members)
    assert(actual.group_admins == expected.group_admins)
    assert(actual.user_admins == expected.user_admins)


def test_maximal_group():
    owner = User('ivo://cadc.nrc.ca/user?00000000-0000-0000-0000-00000000000a')
    owner.identities['X500'] = Identity('cn=foo,c=ca', 'X500')
    owner.identities['OpenID'] = Identity('foo@bar.com', 'OpenID')
    owner.identities['HTTP'] = Identity('foo', 'HTTP')
    owner.identities['CADC'] = Identity('00000000-0000-0000-0000-000000000001',
                                        'CADC')

    expected = Group('groupID')
    expected.owner = owner
    expected.description = 'description'
    expected.last_modified = datetime(2014, 1, 20, 19, 45, 37, 0)
    expected.properties.add(GroupProperty('key1', 'value1', True))
    expected.properties.add(GroupProperty('key2', 'value2', False))

    user1 = User('ivo://cadc.nrc.ca/user?00000000-0000-0000-0000-00000000000b')
    user2 = User('ivo://cadc.nrc.ca/user?00000000-0000-0000-0000-00000000000c')
    group_member1 = Group('groupMember1')
    group_member1.owner = user1
    group_member2 = Group('groupMember2')
    group_member2.owner = user2
    expected.group_members.add(group_member1)
    expected.group_members.add(group_member2)

    user_member1 = User(
        'ivo://cadc.nrc.ca/user?00000000-0000-0000-0000-00000000000d')
    user_member2 = User(
        'ivo://cadc.nrc.ca/user?00000000-0000-0000-0000-00000000000e')
    expected.user_members.add(user_member1)
    expected.user_members.add(user_member2)

    owner1 = User(
        'ivo://cadc.nrc.ca/user?00000000-0000-0000-0000-00000000000f')
    owner2 = User(
        'ivo://cadc.nrc.ca/user?00000000-0000-0000-0000-0000000000aa')
    group_admin1 = Group('adminMember1')
    group_admin1.owner = owner1
    group_admin2 = Group('adminMember2')
    group_admin2.owner = owner2
    expected.group_admins.add(group_admin1)
    expected.group_admins.add(group_admin2)

    user_admin1 = User(
        'ivo://cadc.nrc.ca/user?00000000-0000-0000-0000-0000000000ab')
    user_admin2 = User(
        'ivo://cadc.nrc.ca/user?00000000-0000-0000-0000-0000000000ac')
    expected.user_admins.add(user_admin1)
    expected.user_admins.add(user_admin2)

    writer = GroupWriter()
    xml_string = writer.write(expected, True)

    assert(xml_string)
    assert(len(xml_string) > 0)

    reader = GroupReader()
    actual = reader.read(xml_string)

    assert expected.group_id
    assert actual.group_id
    assert(actual.group_id == expected.group_id)

    assert(actual.owner.internal_id == expected.owner.internal_id)
    assert(actual.owner.identities == expected.owner.identities)
    assert(actual.description == expected.description)
    assert(actual.last_modified == expected.last_modified)

    assert(actual.properties == expected.properties)
    assert(actual.group_members == expected.group_members)
    assert(actual.user_members == expected.user_members)
    assert(actual.group_admins == expected.group_admins)
    assert(actual.user_admins == expected.user_admins)
