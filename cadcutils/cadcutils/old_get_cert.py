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

import netrc
import argparse
import os
import signal
import sys
from six.moves.urllib.parse import urlparse

from .net.auth import get_cert, CRED_RESOURCE_ID, Subject
from .net.ws import BaseWsClient, SERVICE_AVAILABILITY_ID

# CADC realms current and old
CADC_REALMS = ['www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca',
               'www.canfar.phys.uvic.ca']
"""

The code in this file is deprecated being replaced by certificate related code
in the net.auth module.
It is kept here only to support users that still rely on getCert application

"""


def _main():

    def _signal_handler(signal, frame):
        sys.stderr.write("\n")
        sys.exit(-1)

    signal.signal(signal.SIGINT, _signal_handler)

    # Note: we make the assumption that the CDP end point and its availability
    # end point are on the same host. We build a dummy anonymous client to
    # the service and retrieve the access url of the availability end point
    # which then is used to determine the actual realm of the service
    dummy = BaseWsClient(CRED_RESOURCE_ID, Subject(),
                         agent="getCert/1.0", retry=True)
    access_url = dummy._get_url((SERVICE_AVAILABILITY_ID, None))
    service_realm = urlparse(access_url).netloc

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=("Retrieve a security certificate for interaction with "
                     "VOSpace. Certificate will be valid for daysValid and "
                     "stored as local file cert_filename. First looks for an "
                     "entry in the users .netrc matching the realm {0}, "
                     "the user is prompted for a username and "
                     "password if no entry is found.".format(service_realm)))

    parser.add_argument('--version', '-V', action='version', version='1.1')
    parser.add_argument('--daysValid', type=int, default=10,
                        help='Number of days the cetificate should be valid.')
    parser.add_argument('--cert-filename', default=os.path.join(
            os.getenv('HOME'), '.ssl', 'cadcproxy.pem'),
                        help="Filesysm location to store the proxy "
                             "certifcate.")
    parser.add_argument('--cert-server', default=None,
                        help="Deprecated. Used for testing only")

    args = parser.parse_args()
    if args.cert_server:
        # update service_realm
        dummy = BaseWsClient(CRED_RESOURCE_ID, Subject(),
                             agent="getCert/1.0", retry=True,
                             host=args.cert_server)
        access_url = dummy._get_url((SERVICE_AVAILABILITY_ID, None))
        service_realm = urlparse(access_url).netloc
        print('Service realm: {}'.format(service_realm))

    if os.access(os.path.join(os.environ.get('HOME', '/'), ".netrc"), os.R_OK):
        # try the service_realm first and if that is not found in the
        # .netrc file, try any of the CADC equivalents
        auth = netrc.netrc().authenticators(service_realm)
        if not auth:
            for r in CADC_REALMS:
                auth = netrc.netrc().authenticators(r)
                if auth:
                    break
    else:
        auth = False
    if not auth:
        sys.stdout.write("{0} Username: ".format(service_realm))
        sys.stdout.flush()
        username = sys.stdin.readline().strip('\n')
        subject = Subject(username=username)
    else:
        subject = Subject(netrc=True)
        # the following prevents reading the .netrc file and uses the
        # auth already discovered.
        subject._hosts_auth[service_realm] = (auth[0], auth[2])

    retry = True
    while retry:
        try:
            cert = get_cert(subject, days_valid=args.daysValid)
            retry = False
            with open(args.cert_filename, 'w') as w:
                w.write(cert)
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
