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

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from OpenSSL import crypto

from cadcutils.net import cert_validation


def _make_pem_cert(not_after=None):
    """Create a self-signed PEM certificate for testing."""
    key = crypto.PKey()
    key.generate_key(crypto.TYPE_RSA, 2048)
    cert = crypto.X509()
    cert.set_pubkey(key)
    cert.get_subject().CN = 'test'
    if not_after is None:
        not_after = datetime.now(timezone.utc) + timedelta(days=30)
    cert.set_notAfter(not_after.strftime('%Y%m%d%H%M%SZ').encode('ascii'))
    cert.set_notBefore(
        (datetime.now(timezone.utc) - timedelta(days=1)).strftime(
            '%Y%m%d%H%M%SZ').encode('ascii'))
    cert.set_serial_number(1)
    cert.set_issuer(cert.get_subject())
    cert.sign(key, 'sha256')
    return crypto.dump_certificate(crypto.FILETYPE_PEM, cert)


class TestCertValidation(unittest.TestCase):
    """Tests for client certificate validation."""

    def test_valid_certificate(self):
        with tempfile.NamedTemporaryFile(suffix='.pem', delete=False) as f:
            f.write(_make_pem_cert())
            cert_path = f.name
        cert_validation.validate_client_certificate(cert_path)

    def test_expired_certificate(self):
        expired = datetime.now(timezone.utc) - timedelta(days=1)
        with tempfile.NamedTemporaryFile(suffix='.pem', delete=False) as f:
            f.write(_make_pem_cert(not_after=expired))
            cert_path = f.name
        with self.assertRaises(ValueError) as ctx:
            cert_validation.validate_client_certificate(cert_path)
        self.assertIn('expired', str(ctx.exception))
        self.assertIn('cadc-get-cert', str(ctx.exception))

    def test_invalid_pem(self):
        with tempfile.NamedTemporaryFile(suffix='.pem', delete=False) as f:
            f.write(b'not a certificate')
            cert_path = f.name
        with self.assertRaises(ValueError) as ctx:
            cert_validation.validate_client_certificate(cert_path)
        self.assertIn('invalid PEM format', str(ctx.exception))

    def test_missing_file(self):
        with self.assertRaises(ValueError) as ctx:
            cert_validation.validate_client_certificate('/no/such/cert.pem')
        self.assertIn('Cannot read certificate file', str(ctx.exception))

    @patch('cadcutils.net.cert_validation.logger')
    def test_expiring_soon_warning(self, logger_mock):
        soon = datetime.now(timezone.utc) + timedelta(days=3)
        with tempfile.NamedTemporaryFile(suffix='.pem', delete=False) as f:
            f.write(_make_pem_cert(not_after=soon))
            cert_path = f.name
        cert_validation.validate_client_certificate(cert_path, warn_days=3)
        logger_mock.warning.assert_called_once()
        self.assertIn('expires in', logger_mock.warning.call_args[0][0])

    @patch('cadcutils.net.cert_validation.crypto.load_certificate')
    def test_no_expiry_date(self, load_mock):
        cert = MagicMock()
        cert.get_notAfter.return_value = None
        load_mock.return_value = cert
        with tempfile.NamedTemporaryFile(suffix='.pem', delete=False) as f:
            f.write(b'placeholder')
            cert_path = f.name
        cert_validation.validate_client_certificate(cert_path)
