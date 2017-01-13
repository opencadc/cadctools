# -*- coding: utf-8 -*-
# ************************************************************************
# *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
# *
# *  (c) 2015.                            (c) 2015.
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

import os
import sys
import unittest
from lxml import etree

# put local code at start of path
sys.path.insert(0, os.path.abspath('../../../'))

from cadcdata.transfer import Transfer, TransferError, TransferReader, TransferWriter, Protocol
from cadcdata.transfer import DIRECTION_PROTOCOL_MAP, VOSPACE_21, VOSPACE_20

test_target_good = 'vos://cadc.nrc.ca~vospace/file'
test_target_bad = 'ftp://garbage/target'
test_dir_put = 'pushToVoSpace'
test_dir_get = 'pullFromVoSpace'
test_dir_bad = 'push'

class TestTransferReaderWriter(unittest.TestCase):

    def test_transfer_constructor(self):
        # target not of form vos: or ad:
        with self.assertRaises( TransferError ):
            Transfer( test_target_bad, test_dir_put )

        # invalid direction
        with self.assertRaises( TransferError ):
            Transfer( test_target_good, test_dir_bad )

        # protocol inconsistent with direction
        with self.assertRaises( TransferError ):
            Transfer( test_target_good, test_dir_put,
                      protocols=[
                    Protocol( DIRECTION_PROTOCOL_MAP['pullFromVoSpace'] ) ] )

        # invalid version
        with self.assertRaises( TransferError ):
            Transfer( test_target_good, test_dir_put, version=9999 )

        # invalid property
        with self.assertRaises( TransferError ):
            Transfer( test_target_good, test_dir_put,
                      protocols=[
                    Protocol( DIRECTION_PROTOCOL_MAP['pushToVoSpace'] ) ],
                      properties = {'LENGTH':'1234'} )

        # property can be set if using VOSPACE_21
        tran = Transfer( test_target_good, test_dir_put,
                         protocols=[
                Protocol( DIRECTION_PROTOCOL_MAP['pushToVoSpace'] ) ],
                         properties = {'LENGTH':'1234'},
                         version=VOSPACE_21 )

        self.assertEqual(tran.target, test_target_good,
                         'Wrong target.')
        self.assertEqual(1, len(tran.protocols), 'Wrong number of protocols.')
        self.assertEqual( tran.protocols[0].uri,
                          DIRECTION_PROTOCOL_MAP[test_dir_put],
                          'Wrong protocol URI' )
        self.assertEqual(VOSPACE_21, tran.version)
        self.assertEqual(1, len(tran.properties), 'Wrong number of properties')
        self.assertEqual('1234',tran.properties['LENGTH'])

        # The simplest constructor for a put automatically sets protocol
        tran = Transfer( test_target_good, test_dir_put )
        self.assertEqual(1, len(tran.protocols), 'Wrong number of protocols.')
        self.assertEqual( tran.protocols[0].uri,
                          DIRECTION_PROTOCOL_MAP[test_dir_put],
                          'Wrong protocol URI' )

        # For a get constructor protocol is not set
        tran = Transfer( test_target_good, test_dir_get )
        self.assertEqual( tran.protocols[0].uri,
                          DIRECTION_PROTOCOL_MAP[test_dir_get],
                          'Wrong protocol URI')

    def test_roundtrip_put(self):
        tran = Transfer( test_target_good, test_dir_put,
                         properties = {'LENGTH':'1234'},
                         version=VOSPACE_21 )
        xml_str = TransferWriter().write(tran)
        tran2 = TransferReader(validate=True).read(xml_str)

        self.assertEqual( tran.target, tran2.target, 'Wrong target.' )
        self.assertEqual( tran.direction, tran2.direction, 'Wrong direction.' )
        self.assertEqual( tran.properties, tran2.properties,
                          'Wrong properties.' )
        self.assertEqual(len(tran.protocols), len(tran2.protocols),
                             'Wrong number of protocols.')
        for i in range(len(tran.protocols)):
            p1 = tran.protocols[i]
            p2 = tran2.protocols[i]

            self.assertEqual( p1.uri, p1.uri, 'Wrong uri, protocol %i' % i )
            self.assertEqual( p1.endpoint, p1.endpoint,
                              'Wrong endpoint, protocol %i' % i )

    def test_roundtrip_get(self):
        tran = Transfer( test_target_good, test_dir_get,
                         protocols=[
                Protocol( DIRECTION_PROTOCOL_MAP['pullFromVoSpace'],
                          endpoint='http://somewhere') ],
                         properties = {'LENGTH':'1234',
                                       'uri=ivo://ivoa.net/vospace/core#quota' :
                                           '100' },
                         version=VOSPACE_21 )

        xml_str = TransferWriter().write(tran)
        tran2 = TransferReader(validate=True).read(xml_str)

        self.assertEqual( tran.target, tran2.target, 'Wrong target.' )
        self.assertEqual( tran.direction, tran2.direction,
                          'Wrong direction.' )
        self.assertEqual( tran.properties, tran2.properties,
                          'Wrong properties.' )
        self.assertEqual(len(tran.protocols), len(tran2.protocols),
                         'Wrong number of protocols.')
        for i in range(len(tran.protocols)):
            p1 = tran.protocols[i]
            p2 = tran2.protocols[i]

            self.assertEqual( p1.uri, p1.uri, 'Wrong uri, protocol %i' % i )
            self.assertEqual( p1.endpoint, p1.endpoint,
                              'Wrong endpoint, protocol %i' % i )

    def test_validation(self):
        # VOSPACE_20
        tran = Transfer( test_target_good, test_dir_put,
                         version=VOSPACE_20 )
        xml_str = TransferWriter().write(tran)
        tran2 = TransferReader(validate=True).read(xml_str)

        # VOSPACE_21
        tran = Transfer( test_target_good, test_dir_put,
                         properties = {'LENGTH':'1234'},
                         version=VOSPACE_21 )
        xml_str = TransferWriter().write(tran)

        # introduce an error that schema validation should catch
        xml = etree.fromstring(xml_str)
        junk = etree.SubElement(xml, 'junk')
        xml_str2 = etree.tostring(xml,encoding='UTF-8',pretty_print=True)

        # should not raise exception because validation turned off by default
        tran2 = TransferReader().read(xml_str2)

        # should now raise exception with validation turned on
        with self.assertRaises( etree.DocumentInvalid ):
            tran2 = TransferReader(validate=True).read(xml_str2)

