# -*- coding: utf-8 -*-
# ************************************************************************
# *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
# *
# *  (c) 2021.                            (c) 2021.
# *  Government of Canada                 Gouvernement du Canada
# *  National Research Council            Conseil national de recherches
# *  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
# *  All rights reserved                  Tous droits réservés
# *
# *  NRC disclaims any warranties,        Le CNRC dénie toute garantie
# *  expressed, implied, or               énoncée, implicite ou légale,
# *  statutory, of any kind with          de quelque nature que ce
# *  respect to the software,             soit, concernant le logiciel,
# *  including without limitation         y compris sans restriction
# *  any warranty of merchantability      toute garantie de valeur
# *  or fitness for a particular          marchande ou de pertinence
# *  purpose. NRC shall not be            pour un usage particulier.
# *  liable in any event for any          Le CNRC ne pourra en aucun cas
# *  damages, whether direct or           être tenu responsable de tout
# *  indirect, special or general,        dommage, direct ou indirect,
# *  consequential or incidental,         particulier ou général,
# *  arising from the use of the          accessoire ou fortuit, résultant
# *  software.  Neither the name          de l'utilisation du logiciel. Ni
# *  of the National Research             le nom du Conseil National de
# *  Council of Canada nor the            Recherches du Canada ni les noms
# *  names of its contributors may        de ses  participants ne peuvent
# *  be used to endorse or promote        être utilisés pour approuver ou
# *  products derived from this           promouvoir les produits dérivés
# *  software without specific prior      de ce logiciel sans autorisation
# *  written permission.                  préalable et particulière
# *                                       par écrit.
# *
# *  This file is part of the             Ce fichier fait partie du projet
# *  OpenCADC project.                    OpenCADC.
# *
# *  OpenCADC is free software:           OpenCADC est un logiciel libre ;
# *  you can redistribute it and/or       vous pouvez le redistribuer ou le
# *  modify it under the terms of         modifier suivant les termes de
# *  the GNU Affero General Public        la “GNU Affero General Public
# *  License as published by the          License” telle que publiée
# *  Free Software Foundation,            par la Free Software Foundation
# *  either version 3 of the              : soit la version 3 de cette
# *  License, or (at your option)         licence, soit (à votre gré)
# *  any later version.                   toute version ultérieure.
# *
# *  OpenCADC is distributed in the       OpenCADC est distribué
# *  hope that it will be useful,         dans l’espoir qu’il vous
# *  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
# *  without even the implied             GARANTIE : sans même la garantie
# *  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
# *  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
# *  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
# *  General Public License for           Générale Publique GNU Affero
# *  more details.                        pour plus de détails.
# *
# *  You should have received             Vous devriez avoir reçu une
# *  a copy of the GNU Affero             copie de la Licence Générale
# *  General Public License along         Publique GNU Affero avec
# *  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
# *  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
# *                                       <http://www.gnu.org/licenses/>.
# *
# ************************************************************************

import pytest
import os
import sys

from cadcutils.net import Subject, GroupsClient, Group
from cadcutils.net.groups_client import main_app

REG_HOST = 'ws.cadc-ccda.hia-iha.nrc-cnrc.gc.ca'

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')

# this must be CadcAuthtest1 cert
CADC_TESTCERT = os.environ.get('CADC_TESTCERT', '')

# ID of the test group
TEST_GROUP_ID = 'cadc-groups-int-test'
TEST_MEMBER_USER_ID = 'cadcauthtest2'
TEST_MEMBER_GROUP_ID = TEST_GROUP_ID + '-member'


@pytest.mark.intTests
@pytest.mark.skipif(not os.path.isfile(CADC_TESTCERT),
                    reason='CADC test cert required (CADC_TESTCERT environ')
def test_group_client():
    subject = Subject(certificate=CADC_TESTCERT)

    # This can only be executed once
    # sys.argv = 'cadc-groups create --cert {} {}'.format(
    #     CADC_TESTCERT, TEST_GROUP_ID).split()
    # main_app()
    # sys.argv = 'cadc-groups create --cert {} {}'.format(
    #      CADC_TESTCERT, TEST_MEMBER_GROUP_ID).split()
    # main_app()

    client = GroupsClient(subject=subject, host=REG_HOST)
    test_group = client.get_group(TEST_GROUP_ID)

    if test_group.group_members or test_group.user_members:
        test_group.user_members.clear()
        test_group.group_members.clear()
    if test_group.group_admins or test_group.user_admins:
        test_group.user_admins.clear()
        test_group.group_admins.clear()
    test_group.description = 'TEST GROUP'

    client.update_group(test_group)

    test_group = client.get_group(TEST_GROUP_ID)

    assert not test_group.group_members
    assert not test_group.user_members
    assert not test_group.group_admins
    assert not test_group.user_admins
    assert test_group.description == 'TEST GROUP'

    # test description
    test_group.description = 'NEW DESCRIPTION'
    client.update_group(test_group)
    test_group = client.get_group(TEST_GROUP_ID)
    assert test_group.description == 'NEW DESCRIPTION'

    # test user and group members
    sys.argv = 'cadc-groups members --cert {} --add-user {} {}'.format(
        CADC_TESTCERT, TEST_MEMBER_USER_ID, TEST_GROUP_ID).split()
    main_app()

    sys.argv = 'cadc-groups members --cert {} --add-group {} {}'.format(
        CADC_TESTCERT, TEST_MEMBER_GROUP_ID, TEST_GROUP_ID).split()
    main_app()

    test_group = client.get_group(TEST_GROUP_ID)

    assert len(test_group.group_members) == 1
    assert len(test_group.user_members) == 1
    assert not test_group.group_admins
    assert not test_group.user_admins

    sys.argv = 'cadc-groups members --cert {} --clear {}'.format(
        CADC_TESTCERT, TEST_GROUP_ID).split()
    main_app()
    test_group = client.get_group(TEST_GROUP_ID)

    assert not test_group.group_members
    assert not test_group.user_members
    assert not test_group.group_admins
    assert not test_group.user_admins

    test_group.group_admins.add(Group(group_id=TEST_MEMBER_GROUP_ID))
    # TODO currently not supported. Need to retrieve user info first
    # test_group.user_admins.add(
    #     User(internal_id=
    #          'ivo://cadc.nrc.ca/gms?00000000-0000-0000-0000-000000000005'))

    client.update_group(group=test_group)
    test_group = client.get_group(TEST_GROUP_ID)
    assert len(test_group.group_admins) == 1
    # assert len(test_group.user_admins) == 1

    test_group.user_admins.clear()
    test_group.group_admins.clear()
    client.update_group(group=test_group)
    test_group = client.get_group(TEST_GROUP_ID)

    assert not test_group.group_members
    assert not test_group.user_members
    assert not test_group.group_admins
