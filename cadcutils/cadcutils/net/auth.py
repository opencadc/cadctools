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

import getpass
import netrc
import argparse
import os
import signal
import sys
import requests
import logging

CERT_ENDPOINT = "/cred/proxyCert"
CERT_SERVER = "www.canfar.phys.uvic.ca"

__all__ = ['get_cert', 'get_user_password', 'Subject']

class Subject:
    def __init__(self, username=None, certificate=None, use_netrc=False, netrc_file=None):
        self.logger = logging.getLogger('Subject')
        self.username = username
        self.certificate = certificate
        self.use_netrc = use_netrc
        self.netrc_file = netrc_file
        self.netrc = None
        self.passwds = {}
        if certificate is not None:
            assert certificate is not '' and os.path.isfile(certificate)
            assert use_netrc is False
            assert username is None
            assert netrc_file is None
        else:
            if use_netrc:
                self.netrc = netrc.netrc(netrc_file)
            self.username = username
        self.anon = (self.certificate is None) and (self.use_netrc is False) \
                    and (username is None)

    def get_auth(self, realm):
        if self.anon:
            return None
        #TODO add the password typed by the user to the list of known hosts/username/passwords
        if realm in self.passwds:
            return (self.username, self.passwds[realm])
        if self.username is None:
            if realm in self.netrc.hosts:
                return (self.netrc.hosts[realm][0], self.netrc.hosts[realm][2])
            else:
                self.logger.error('No user/password for {} in {}'.format(
                    realm, self.netrc_file if self.netrc_file is not None else '$HOME/.netrc'))
                return None
        else:
            if (self.netrc is not None) and \
               (realm in self.netrc.hosts) and \
               (self.username == self.netrc.hosts[realm][0]):
                return (self.netrc.hosts[realm][0], self.netrc.hosts[realm][2])
            else:
                sys.stdout.write("Password for {}@{}: ".format(self.username, realm))
                self.passwds[realm] = getpass.getpass().strip('\n')
                return (self.username, self.passwds[realm])


def get_cert(cert_server=None, user=None,
             cert_endpoint=None, **kwargs):
    """Access the CADC Certificate Delecation Protocol (CDP) server and retrieve
       a X509 proxy certificate.

    :param cert_server: the http server that will provide the certificate
    :ptype cert_server: str
    :param user: CADC ID of the user
    :ptype user: str
    :param cert_endpoint: the endpoint on the server where the certificate service is
    :ptype cert_endpoint: str
    :param kwargs: not really any, but maybe daysValid.
    :ptype daysValid: int

    :return content of the certificate

    """

    cert_server = cert_server is None and CERT_SERVER or cert_server
    cert_endpoint = cert_endpoint is None and CERT_ENDPOINT or cert_endpoint

    username = user
    if username is None:
        sys.stdout.write("CADC user ID: ")
        username = sys.stdin.readline().strip('\n')

    username, passwd = get_user_password(cert_server, username=username)

    url = "http://{0}/{1}".format(cert_server, cert_endpoint)
    resp = requests.get(url, params=kwargs, auth=(username, passwd))
    resp.raise_for_status()
    logging.getLogger('get_Cert').debug('Saved user {} certificate'.format(username))
    return resp.content


def get_user_password(realm, username=None):
    """"Gett the username/password for realm from .netrc file or prompt the user

    :param realm: the server realm this user/password combination is for
    :ptype realm: str
    :param username: user name to get the password for. Prompt for password.
    :return (username, password)
    """
    if username is None:
        if os.access(os.path.join(os.environ.get('HOME', '/'), ".netrc"), os.R_OK):
            auth = netrc.netrc().authenticators(realm)
            return (auth[0], auth[2])
        else:
            return False
    else:
        sys.stdout.write("Password for {}: ".format(username))
        return(username, getpass.getpass().strip('\n'))


def get_cert_main():
    """ Client to download an X509 certificate and save it in users home directory"""

    def _signal_handler(signal, frame):
        sys.stderr.write("\n")
        sys.exit(-1)

    signal.signal(signal.SIGINT, _signal_handler)

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description=("Retrieve a security certificate for interaction with a Web service "
                                                  "such as VOSpace. Certificate will be valid for daysValid and stored "
                                                  "as local file cert_filename. First looks for an entry in the users "
                                                  ".netrc  matching the realm {0}, the user is prompted for a username "
                                                  "and password if no entry is found.".format(CERT_SERVER)))

    parser.add_argument('--cert-filename',
                        default=os.path.join(os.getenv('HOME', '/tmp'), '.ssl/cadcproxy.pem'),
                        help="Filesystem location to store the proxy certificate.")
    parser.add_argument('--cert-server',
                        default=CERT_SERVER,
                        help="Certificate server network address.")
    parser.add_argument('--daysValid', type=int, default=10, help='Number of days the certificate should be valid.')
    parser.add_argument('-u', '--user', type=str, help='CADC user ID associated with the certificate',
                        required=False)

    args = parser.parse_args()

    dirname = os.path.dirname(args.cert_filename)
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

    retry = True
    while retry:
        try:
            # if args.cert_filename is None:
            #    cert_filename = os.path.join(os.getenv("HOME", "/tmp"), ".ssl/cadcproxy.pem")

            cert = get_cert(cert_server=args.cert_server, user=args.user,
                            daysValid=args.daysValid)
            with open(args.cert_filename, 'w') as w:
                w.write(cert)
            retry = False
        except OSError as ose:
            if ose.errno != 401:
                sys.stderr.write(str(ose))
                return getattr(ose, 'errno', 1)
            else:
                sys.stderr.write("Access denied\n")
        except Exception as ex:
            sys.stderr.write(str(ex))
            return getattr(ex, 'errno', 1)
