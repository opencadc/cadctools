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
Utilities for detecting SSL/TLS errors and producing user-friendly messages.
"""

from urllib.parse import urlparse

import requests

from cadcutils import exceptions

__all__ = ['unwrap_ssl_error', 'connection_error_to_exception']


def unwrap_ssl_error(exc):
    """
    Return the underlying SSLError if *exc* is or wraps one, else None.
    """
    if exc is None:
        return None
    if isinstance(exc, requests.exceptions.SSLError):
        return exc
    ssl_error = unwrap_ssl_error(getattr(exc, '__cause__', None))
    if ssl_error is not None:
        return ssl_error
    for arg in getattr(exc, 'args', ()):
        if isinstance(arg, requests.exceptions.SSLError):
            return arg
        ssl_error = unwrap_ssl_error(arg if isinstance(arg, BaseException) else None)
        if ssl_error is not None:
            return ssl_error
    return None


def _host_from_url(url):
    if not url or not isinstance(url, str):
        return None
    return urlparse(url).hostname


def _ca_trust_message(host_part):
    return ('SSL certificate verification failed{}: unable to verify '
            'the server certificate. Your system may be missing CA '
            'certificates or a proxy may be intercepting HTTPS. '
            'Check network/proxy settings or run with --debug for '
            'details.'.format(host_part))


def _ssl_message(ssl_text, host, cert=None):
    """
    Map common SSL error text to a short, actionable message.
    """
    text = ssl_text.lower()
    host_part = ' for {}'.format(host) if host else ''

    if ('certificate has expired' in text or
            'certificate_expired' in text or
            'sslv3_alert_certificate_expired' in text):
        if cert:
            return ('Your client certificate ({}) has expired. '
                    'Run: cadc-get-cert'.format(cert))
        return ('SSL certificate has expired{}. '
                'If using a client certificate, run: cadc-get-cert'.
                format(host_part))

    if ('unknown ca' in text or 'sslv3_alert_unknown_ca' in text or
            'unable to get local issuer certificate' in text):
        return _ca_trust_message(host_part)

    if ('certificate_verify_failed' in text or
            'certificate verify failed' in text):
        if 'self signed certificate' in text:
            return ('SSL certificate verification failed{}: the server '
                    'certificate is not trusted (self-signed). '
                    'For testing only, use --insecure to skip verification.'.
                    format(host_part))
        return ('SSL certificate verification failed{}. Check network/proxy '
                'settings or run with --debug for details.'.
                format(host_part))

    if "doesn't match" in text or 'hostname' in text:
        return ('SSL hostname verification failed{}: the server certificate '
                'does not match the requested host. Check the service URL or '
                '--host option.'.format(host_part))

    if ('pem lib' in text or 'bad decrypt' in text or
            'no start line' in text or 'could not load pem' in text):
        cert_part = ' ({})'.format(cert) if cert else ''
        return ('Could not load client certificate{}. Check that the file '
                'exists, is readable, and is a valid PEM-encoded certificate.'.
                format(cert_part))

    return ('SSL/TLS error connecting to {}. Run with --debug for details.'.
            format(host or 'the server'))


def ssl_exception_from_error(ssl_error, url=None, cert=None,
                             orig_exception=None):
    """
    Build an SslException from an SSLError with a user-friendly message.
    """
    host = _host_from_url(url)
    if cert and isinstance(cert, tuple):
        cert = cert[0]
    msg = _ssl_message(str(ssl_error), host, cert=cert)
    return exceptions.SslException(msg, orig_exception=orig_exception or ssl_error)


def connection_error_to_exception(connection_error, url=None, cert=None):
    """
    Convert a requests ConnectionError into the most specific exception.
    """
    ssl_error = unwrap_ssl_error(connection_error)
    if ssl_error is not None:
        return ssl_exception_from_error(
            ssl_error, url=url, cert=cert,
            orig_exception=connection_error)
    if 'Connection reset by peer' in str(connection_error):
        return exceptions.TransferException(
            'Transfer error on URL: {}'.format(url or ''))
    return exceptions.HttpException(orig_exception=connection_error)
