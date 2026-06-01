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

"""
Validation of client X509 certificates before use.
"""

from datetime import datetime, timezone

from OpenSSL import crypto

import logging

__all__ = ['validate_client_certificate']

logger = logging.getLogger(__name__)

DEFAULT_WARN_DAYS = 3


def validate_client_certificate(cert_path, warn_days=DEFAULT_WARN_DAYS):
    """
    Validate a PEM client certificate file.

    :param cert_path: path to the certificate (may also contain the private key)
    :param warn_days: log a warning when expiry is within this many days
    :raises ValueError: if the file cannot be read, is not valid PEM, or is
        expired
    """
    try:
        with open(cert_path, 'rb') as cert_file:
            pem_data = cert_file.read()
        cert = crypto.load_certificate(crypto.FILETYPE_PEM, pem_data)
    except OSError as oex:
        raise ValueError(
            'Cannot read certificate file {}: {}'.format(cert_path, oex))
    except crypto.Error:
        raise ValueError(
            'Could not load client certificate ({}): invalid PEM format. '
            'Check that the file contains a valid PEM-encoded certificate.'.
            format(cert_path))

    not_after_bytes = cert.get_notAfter()
    if not_after_bytes is None:
        return

    not_after = datetime.strptime(
        not_after_bytes.decode('ascii'), '%Y%m%d%H%M%SZ').replace(
            tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    if not_after < now:
        raise ValueError(
            'Client certificate ({}) expired on {}. Run: cadc-get-cert'.
            format(cert_path, not_after.strftime('%Y-%m-%d')))

    days_left = (not_after - now).days
    if days_left <= warn_days:
        logger.warning(
            'Client certificate (%s) expires in %d day(s) on %s. '
            'Run cadc-get-cert to renew.',
            cert_path, days_left, not_after.strftime('%Y-%m-%d'))
