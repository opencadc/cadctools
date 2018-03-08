#!/private/tmp/venv/bin/python
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
import html2text

CERT_ENDPOINT = "/cred/proxyCert"
CERT_SERVER = "www.canfar.phys.uvic.ca"

"""

The code in this file is deprecated being replaced by certificate related code
in the net.auth module.
It is kept here only to support users that still rely on getCert application

"""


def get_cert(cert_server=None, cert_filename=None,
             cert_endpoint=None, **kwargs):
    """Access the cadc certificate server.

    :param cert_filename: the name of the file to write the certificate to
    :param cert_server: the http server that will provide the certificate
    :ptype cert_server: str
    :param cert_endpoint: the endpoint on the server where the certificate
    service is
    :ptype cert_endpoint: str
    :param kwargs: not really any, but maybe daysValid.
    :ptype daysValid: int

    """

    cert_server = cert_server is None and CERT_SERVER or cert_server
    cert_endpoint = cert_endpoint is None and CERT_ENDPOINT or cert_endpoint

    if cert_filename is None:
        cert_filename = os.path.join(os.getenv("HOME", "/tmp"),
                                     ".ssl/cadcproxy.pem")

    dirname = os.path.dirname(cert_filename)
    try:
        os.makedirs(dirname)
    except OSError as oex:
        if os.path.isdir(dirname):
            pass
        elif oex.errno == 20 or oex.errno == 17:
            sys.stderr.write("%s : %s \n" % (str(oex), dirname))
            sys.stderr.write("Expected %s to be a directory.\n" % dirname)
            sys.exit(oex.errno)
        else:
            raise oex
    username, passwd = get_user_password(cert_server)

    url = "http://{0}/{1}".format(cert_server, cert_endpoint)
    resp = requests.get(url, params=kwargs, auth=(username, passwd))
    if resp.status_code != 200:
        raise OSError(resp.status_code, html2text.html2text(resp.text))
    with open(cert_filename, 'w') as w:
        w.write(resp.text)
    return resp


def get_user_password(realm):
    """"Getting the username/password for realm from .netrc filie.

    :param realm: the server realm this user/password combination is for
    :ptype realm: str
    """
    if os.access(os.path.join(os.environ.get('HOME', '/'), ".netrc"), os.R_OK):
        auth = netrc.netrc().authenticators(realm)
    else:
        auth = False
    if not auth:
        sys.stdout.write("{0} Username: ".format(realm))
        sys.stdout.flush()
        username = sys.stdin.readline().strip('\n')
        password = getpass.getpass().strip('\n')
    else:
        username = auth[0]
        password = auth[2]
    return username, password


def _main():

    def _signal_handler(signal, frame):
        sys.stderr.write("\n")
        sys.exit(-1)

    signal.signal(signal.SIGINT, _signal_handler)

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=("Retrieve a security certificate for interation with "
                     "VOSpace. Certificate will be valid for daysValid and "
                     "stored as local file cert_filename. First looks for an "
                     "entry in the users .netrc matching the realm {0}, "
                     "the user is prompted for a username and "
                     "password if no entry is found.".format(CERT_SERVER)))

    parser.add_argument('--daysValid', type=int, default=10,
                        help='Number of days the cetificate should be valid.')
    parser.add_argument('--cert-filename',
                        default=os.path.join(
                            os.getenv('HOME', '/tmp'), '.ssl/cadcproxy.pem'),
                        help="Filesysm location to store the proxy "
                             "certifcate.")
    parser.add_argument('--cert-server',
                        default=CERT_SERVER,
                        help="Certificate server network address.")

    args = parser.parse_args()

    retry = True
    while retry:
        try:
            get_cert(cert_server=args.cert_server,
                     daysValid=args.daysValid,
                     cert_filename=args.cert_filename)
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


if __name__ == '__main__':
    sys.exit(_main())
