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

import os
import sys

from mock import Mock, patch, mock_open, call
from six import StringIO
import tempfile
import pytest

from cadcutils import old_get_cert
from cadcutils.net import Subject

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')


@patch('cadcutils.old_get_cert.get_cert', Mock(return_value='CERTVALUE'))
@patch('cadcutils.old_get_cert.BaseWsClient._get_url',
       Mock(return_value='http://the.cadc.domain/service'))
@patch('cadcutils.old_get_cert.os.access', Mock())
@patch('cadcutils.old_get_cert.os.getenv', Mock(return_value='/tmp'))
def test_get_cert_main():
    """ Test the getCert function """

    value = "CERTVALUE"

    # get certificate default location
    m = mock_open()  # use a mock to avoid overriding the original file
    with patch('cadcutils.old_get_cert.netrc.netrc') as \
            netrc_mock:
        with patch('cadcutils.old_get_cert.open', m, create=True):
            netrc_mock.return_value.authenticators.return_value = \
                ('someuser', None, 'somepass')
            sys.argv = ["getCert"]
            old_get_cert._main()
    m.assert_called_once_with('/tmp/.ssl/cadcproxy.pem', 'w')
    m().write.assert_called_once_with(value)

    # save certificate in a file
    m.reset_mock()
    cert_file = tempfile.NamedTemporaryFile()
    with patch('cadcutils.old_get_cert.netrc.netrc') as \
            netrc_mock:
        with patch('cadcutils.old_get_cert.open', m, create=True):
            netrc_mock.return_value.authenticators.return_value = \
                ('user', None, 'somepass')
            sys.argv = ['getCert', '--cert-filename', cert_file.name]
            old_get_cert._main()
    m.assert_called_once_with(cert_file.name, 'w')
    m().write.assert_called_once_with(value)

    # test when realm not in the .netrc
    cert_file = tempfile.NamedTemporaryFile()
    m.reset_mock()
    with patch('cadcutils.old_get_cert.netrc.netrc') as netrc_mock:
        with patch('cadcutils.old_get_cert.sys.stdin.readline') as stdin_mock:
            with patch('cadcutils.old_get_cert.Subject') as subject_mock:
                authenticators_mock = \
                    Mock(side_effect=[None, None, None, None, None])
                netrc_mock.return_value.authenticators = authenticators_mock
                stdin_mock.return_value = 'auser'
                subject_mock.return_value = Subject()
                sys.argv = ['getCert', '--cert-filename', cert_file.name]
                old_get_cert._main()
    calls = [call(), call(username='auser')]
    subject_mock.assert_has_calls(calls)
    netrc_calls = [call('the.cadc.domain')]
    for realm in old_get_cert.CADC_REALMS:
        netrc_calls.append(call(realm))
    authenticators_mock.assert_has_calls(netrc_calls)
    with open(cert_file.name, 'r') as f:
        assert value == f.read()

    # test when realm not in the .netrc but other CADC realm (the first one) is
    cert_file = tempfile.NamedTemporaryFile()
    m.reset_mock()
    with patch('cadcutils.old_get_cert.netrc.netrc') as netrc_mock:
        with patch('cadcutils.old_get_cert.Subject') as subject_mock:
            authenticators_mock = Mock(
                side_effect=[None, ('user1', None, 'pwd1')])
            netrc_mock.return_value.authenticators = authenticators_mock
            user_subject = Subject()
            subject_mock.return_value = user_subject
            sys.argv = ['getCert', '--cert-filename', cert_file.name]
            old_get_cert._main()
    netrc_calls = [call('the.cadc.domain'), call(old_get_cert.CADC_REALMS[0])]
    authenticators_mock.assert_has_calls(netrc_calls)
    assert user_subject._hosts_auth['the.cadc.domain'] == ('user1', 'pwd1')
    with open(cert_file.name, 'r') as f:
        assert value == f.read()


class NoExit(Exception):
    pass


@patch('sys.exit', Mock(side_effect=[NoExit, NoExit]))
@patch('cadcutils.old_get_cert.os.getenv', Mock(return_value='/tmp'))
def test_get_cert_main_help():
    """ Test the help option of the getCert app """
    # update the default cert location line

    usage = open(os.path.join(TESTDATA_DIR, 'getCert_help.txt'), 'r').read()

    with patch('cadcutils.old_get_cert.BaseWsClient._get_url', Mock(
            return_value='https://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca')):
        with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
            with pytest.raises(NoExit):
                sys.argv = ['getCert', '-h']
                old_get_cert._main()
    assert usage == stdout_mock.getvalue()
