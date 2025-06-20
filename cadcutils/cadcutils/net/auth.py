# -*- coding: utf-8 -*-

# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2022.                            (c) 2022.
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
Module that contains functionality related to authentication. The
main tools in this module are get_cert that interacts with the
CADC Credential Delegation Protocol Web Service to return a proxy
X509 certificate and Subject that incapsulates the credentials of
a user.
"""

import getpass
import netrc as netrclib
import os
import signal
import sys
from typing import Optional, Union

import html2text

from cadcutils.net import ws
from cadcutils import util, exceptions, version

CRED_RESOURCE_ID = 'ivo://cadc.nrc.ca/cred'
CRED_PROXY_FEATURE_ID = 'ivo://ivoa.net/std/CDP#proxy-1.0'

__all__ = ['get_cert', 'Subject']

# these are the security methods currently supported
SECURITY_METHODS_IDS = {
    'certificate': 'ivo://ivoa.net/sso#tls-with-certificate',
    'basic': 'ivo://ivoa.net/sso#BasicAA',
    'cookie': 'ivo://ivoa.net/sso#cookie',
    'token': 'ivo://ivoa.net/sso#token'}

SUPPORTED_SERVER_VERSIONS = {'cred': '2.0'}

logger = util.get_logger(__name__)


class CookieInfo(object):
    """
    Class to store information regarding an HTTP Cookie
    """
    def __init__(self, domain, name, value):
        self.domain = domain
        self.name = name
        self.value = value


class Subject(object):
    """
    Class that stores user authentication information to be used for accessing
        distributed resources over HTTP. For now, the supported credentials
        are:
        X509 certificate (via a proxy certificate file) or basic HTTP
        user/password (via netrc file that stores user/password or simply user
        name. Note that in the later case the library might prompt for the
        password before connecting.
    """

    def __init__(self, username: Optional[str] = None,
                 certificate: Optional[str] = None,
                 netrc: Union[bool, str] = False,
                 token: Optional[str] = None):
        """
            The subject is anonymous if neither of this arguments is set
        :param username: user name
        :param certificate: name of the X509 certificate file
        :param netrc: use information from .netrc. Value can be True (use
        default $HOME/.netrc) or the name of the netrc file to use.
        :param token: use the provided token
        """
        self.username = username
        self._hosts_auth = {}
        self._certificate = None
        self.certificate = certificate
        self._netrc = False
        self.netrc = netrc
        self._cookies = []
        self._token = token

    @property
    def token(self):
        """
        Token used for authentication
        :return: token string
        """
        return self._token

    @token.setter
    def token(self, value):
        """
        Token used for authentication
        :param value: token string
        """
        self._token = value

    @property
    def certificate(self):
        """
        X509 certificate associated with the subject
        :return: name of the X509 certificate file
        """
        return self._certificate

    @certificate.setter
    def certificate(self, value):
        if value is not None:
            if not os.path.isfile(value):
                raise ValueError('Certificate file {} not found'.format(value))
            self._certificate = value

    @property
    def netrc(self):
        """
        netrc file containing user/passwds credentials (if used by subject)
        :return: Name of netrc file
        """
        return self._netrc

    @netrc.setter
    def netrc(self, value):
        if value is not False:
            hosts = netrclib.netrc(value if value is not True else None).hosts
            if value is True:
                self._netrc = os.path.join(os.environ['HOME'], ".netrc")
            else:
                self._netrc = value
            for host_name in hosts:
                self._hosts_auth[host_name] = (hosts[host_name][0],
                                               hosts[host_name][2])

    @property
    def anon(self):
        """
        Is this an anonymous subject (has no authentication means)?
        :return:
        """
        return (self.certificate is None) and (self.netrc is False) and\
               (self.username is None) and (self.token is None)

    @property
    def cookies(self):
        return self._cookies

    @staticmethod
    def from_cmd_line_args(args):
        """
        Instantiates a subject based on attributes of a command line. It
        works with the base parser in cadcutils and uses the following command
        line arguments:
            args.user: username
            args.cert: x509 certificate location
            args.n: use netrc files for authentication info
            args.netrc_file: use this netrc file for authentication info
            args.token: use this token for authentication
        :param args: argparse command line arguments
        :return: corresponding subject
        """
        return Subject(username=args.user, certificate=args.cert,
                       netrc=(args.netrc_file if args.netrc_file
                              is not None else args.n), token=args.token)

    def get_auth(self, realm):
        """
        Returns a user/password touple for the given realm. Note that this
        function prompts for the password on stdout when the username of the
        subject is known but no corresponding password can be found

        :param realm: realm for the authentication
        :return: (username, password) touple or None if subject is anonymous
        or password not found.
        """
        if self.anon:
            return None
        if self.username is None:
            if realm in self._hosts_auth:
                return self._hosts_auth[realm]
            else:
                msg = 'No user/password for {}'.format(realm)
                if self.netrc is not False:
                    msg = '{} in {}'.format(msg,
                                            self.netrc if self.netrc
                                            is not True else '$HOME/.netrc')
                logger.debug(msg)
                return None
        else:
            if realm in self._hosts_auth \
                    and self.username == self._hosts_auth[realm][0]:
                return self._hosts_auth[realm]
            sys.stdout.write("{}@{}\n".format(self.username, realm))
            sys.stdout.flush()
            pswd = getpass.getpass().strip().strip('\n')
            if not pswd:
                raise ValueError('Password cannot be empty')
            self._hosts_auth[realm] = (self.username, pswd)
            sys.stdout.write("\n")
            sys.stdout.flush()
            return self._hosts_auth[realm]

    def get_security_methods(self):
        """
        returns the security method IDs that this subject is authentication
        for. The order of the returned methods is one that it is preferred:
        certificate, basic and anon.

        Note: a realm would be required here as some methods (cookie, basic)
        are bound to a realm
        :return: list of security method IDs
        """
        sms = []
        if self.certificate is not None:
            sms.append(SECURITY_METHODS_IDS['certificate'])
        if self.cookies:
            sms.append(SECURITY_METHODS_IDS['cookie'])
        if (self.netrc is not False) or (self.username is not None):
            sms.append(SECURITY_METHODS_IDS['basic'])
        if self.token:
            sms.append(SECURITY_METHODS_IDS['token'])
        return sms

    def __repr__(self):
        return (f"<Subject(username={self.username}, "
                f"token={'******' if self._token else '<not set>'}, "
                f"netrc={self.netrc})>, "
                f"certificate={self.certificate}, "
                f"cookies={self.cookies})>")


def get_cert(subject, days_valid=None, host=None, insecure=False):
    """Access the CADC Certificate Delegation Protocol (CDP) server and
       retrieve a X509 proxy certificate.

    :param: subject: subject performing the action
    :ptype: cadcutils.subject
    :param: host: name of the host (overrides the host returned by the service
    registry)
    :param: days_valid: number of days the proxy certificate is valid for
    :ptype daysValid: int
    :param insecure: Allow insecure server connections over SSL (testing)
    :ptype insecure: boolean

    :return content of the certificate

    """
    params = {}
    if days_valid is not None:
        params['daysValid'] = int(days_valid)
    util.check_version(version=version.version)
    client = ws.BaseWsClient(CRED_RESOURCE_ID, subject,
                             agent="cadc-get-cert/" + version.version, retry=True,
                             host=host, server_versions=SUPPORTED_SERVER_VERSIONS,
                             insecure=insecure)
    response = client.get((CRED_PROXY_FEATURE_ID, None), params=params)
    return response.text


def get_cert_main():
    """ Client to download an X509 certificate and save it in users home
    directory"""

    def _signal_handler(signal, frame):
        sys.stderr.write("\n")
        sys.exit(-1)

    signal.signal(signal.SIGINT, _signal_handler)

    parser = util.get_base_parser(subparsers=False, version=version.version,
                                  default_resource_id=CRED_RESOURCE_ID,
                                  auth_required=True)
    parser.description = ('Retrieve a security certificate for interaction '
                          'with a Web service such as VOSpace. Certificate '
                          'will be valid for days-valid and stored as local '
                          'file cert_filename.')
    parser.add_argument('--cert-filename',
                        default=os.path.join(os.getenv('HOME', '/tmp'),
                                             '.ssl/cadcproxy.pem'),
                        help=('filesystem location to store the proxy '
                              'certificate. (default: {})'.
                              format(os.path.join(os.getenv('HOME', '/tmp'),
                                                  '.ssl/cadcproxy.pem'))))
    parser.add_argument('--days-valid', type=int, default=10,
                        help='number of days the certificate should be valid.')

    args = parser.parse_args()

    dirname = os.path.dirname(args.cert_filename)
    if dirname:
        try:
            os.makedirs(dirname)
        except OSError as oex:
            if os.path.isdir(dirname):
                pass
            elif oex.errno == 20 or oex.errno == 17:
                sys.stderr.write("%s : %s\n" % (str(oex), dirname))
                sys.stderr.write("Expected %s to be a directory.\n" % dirname)
                sys.exit(oex.errno)
            else:
                raise oex

    try:
        subject = Subject.from_cmd_line_args(args)
        cert = get_cert(subject, days_valid=args.days_valid, host=args.host,
                        insecure=args.insecure)
        with open(args.cert_filename, 'w') as w:
            w.write(cert)
        if not args.quiet:
            print('DONE. {} day certificate saved in {}'.format(
                args.days_valid, args.cert_filename))
    except exceptions.UnauthorizedException:
        # unauthorized
        sys.stderr.write('FAILED: invalid username/password combination')
    except Exception as ex:
        sys.stderr.write("FAILED to retrieve {} day certificate\n".format(
            args.days_valid))
        sys.stderr.write('{}'.format(html2text.html2text(str(ex))))
        return getattr(ex, 'errno', 1)
