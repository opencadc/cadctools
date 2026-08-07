# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2026.                            (c) 2026.
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

import unittest

import requests
from unittest.mock import Mock

from cadcutils import exceptions
from cadcutils.net import ssl_errors


class TestSslErrors(unittest.TestCase):
    """Tests for SSL error detection and messaging."""

    def test_unwrap_direct_ssl_error(self):
        ssl_err = requests.exceptions.SSLError('certificate verify failed')
        self.assertIs(ssl_errors.unwrap_ssl_error(ssl_err), ssl_err)

    def test_unwrap_nested_ssl_error(self):
        ssl_err = requests.exceptions.SSLError('certificate verify failed')
        conn_err = requests.exceptions.ConnectionError('connection failed')
        conn_err.__cause__ = ssl_err
        self.assertIs(ssl_errors.unwrap_ssl_error(conn_err), ssl_err)

    def test_unwrap_non_ssl_error(self):
        conn_err = requests.exceptions.ConnectionError('Some error')
        self.assertIsNone(ssl_errors.unwrap_ssl_error(conn_err))

    def test_cert_verify_failed_message(self):
        ssl_err = requests.exceptions.SSLError(
            '[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: '
            'unable to get local issuer certificate')
        exc = ssl_errors.ssl_exception_from_error(
            ssl_err, url='https://example.com/path')
        self.assertIsInstance(exc, exceptions.SslException)
        self.assertIn('unable to verify the server certificate', str(exc))
        self.assertIn('example.com', str(exc))
        self.assertNotIn('connecting to for', str(exc))

    def test_unknown_ca_message(self):
        ssl_err = requests.exceptions.SSLError(
            '[SSL: SSLV3_ALERT_UNKNOWN_CA] tlsv1 alert unknown ca')
        exc = ssl_errors.ssl_exception_from_error(
            ssl_err, url='https://www.example.com/tap')
        self.assertIsInstance(exc, exceptions.SslException)
        self.assertIn('unable to verify the server certificate', str(exc))
        self.assertIn('www.example.com', str(exc))
        self.assertNotIn('connecting to for', str(exc))

    def test_generic_ssl_fallback_message(self):
        ssl_err = requests.exceptions.SSLError('unexpected ssl failure')
        exc = ssl_errors.ssl_exception_from_error(
            ssl_err, url='https://www.example.com/')
        self.assertIsInstance(exc, exceptions.SslException)
        self.assertIn('SSL/TLS error connecting to www.example.com', str(exc))
        self.assertNotIn('connecting to for', str(exc))

    def test_expired_client_cert_message(self):
        ssl_err = requests.exceptions.SSLError(
            '[SSL: SSLV3_ALERT_CERTIFICATE_EXPIRED] certificate expired')
        exc = ssl_errors.ssl_exception_from_error(
            ssl_err, cert='/home/user/.ssl/cadcproxy.pem')
        self.assertIsInstance(exc, exceptions.SslException)
        self.assertIn('cadc-get-cert', str(exc))
        self.assertIn('cadcproxy.pem', str(exc))

    def test_self_signed_message(self):
        ssl_err = requests.exceptions.SSLError(
            'certificate verify failed: self signed certificate')
        exc = ssl_errors.ssl_exception_from_error(ssl_err)
        self.assertIn('--insecure', str(exc))

    def test_pem_load_error_message(self):
        ssl_err = requests.exceptions.SSLError('[SSL] PEM lib')
        exc = ssl_errors.ssl_exception_from_error(
            ssl_err, cert='bad.pem')
        self.assertIn('Could not load client certificate', str(exc))

    def test_connection_reset_by_peer(self):
        ce = requests.exceptions.ConnectionError('Connection reset by peer')
        exc = ssl_errors.connection_error_to_exception(
            ce, url='https://example.com/data')
        self.assertIsInstance(exc, exceptions.TransferException)

    def test_connection_error_with_ssl_cause(self):
        ssl_err = requests.exceptions.SSLError('certificate verify failed')
        ce = requests.exceptions.ConnectionError('HTTPSConnectionPool failed')
        ce.__cause__ = ssl_err
        exc = ssl_errors.connection_error_to_exception(ce)
        self.assertIsInstance(exc, exceptions.SslException)

    def test_generic_connection_error(self):
        ce = requests.exceptions.ConnectionError('Some error')
        exc = ssl_errors.connection_error_to_exception(ce)
        self.assertIsInstance(exc, exceptions.HttpException)
        self.assertNotIsInstance(exc, exceptions.SslException)

    def test_ssl_error_with_non_string_url(self):
        ssl_err = requests.exceptions.SSLError('certificate verify failed')
        ce = requests.exceptions.ConnectionError('failed')
        ce.__cause__ = ssl_err
        exc = ssl_errors.connection_error_to_exception(ce, url=Mock())
        self.assertIsInstance(exc, exceptions.SslException)
        self.assertIn('certificate verification failed', str(exc))
