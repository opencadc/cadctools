# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2018.                            (c) 2018.
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
#  General Public License for           Générale Publique GNU AfferoF
#  more details.                        pour plus de détails.
#
#  You should have received             Vous devriez avoir reçu une
#  a copy of the GNU Affero             copie de la Licence Générale
#  General Public License along         Publique GNU Affero avec
#  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
#  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
#                                       <http://www.gnu.org/licenses/>.
#
#  $Revision: 1 $
#
# ***********************************************************************
#

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import io
import logging
import pytest
from cadccutout.core import OpenCADCCutout, WriteOnlyStream
from cadccutout.pixel_cutout_hdu import PixelCutoutHDU

# Compatibility with Python 2.7, where there is no FileNotFoundError.
try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError

logger = logging.getLogger('cadccutout')
logger.setLevel(logging.DEBUG)


def test__parse_input():
    test_subject = OpenCADCCutout()
    inputs = ['[9][100:1000]']

    results = test_subject._parse_input(inputs)
    pixel_cutout = results[0]

    assert pixel_cutout.get_extension() == 9, 'Wrong extension found.'
    assert pixel_cutout.get_ranges() == [(100, 1000)], 'Wrong ranges found.'

    inputs = ['[500:700][SCI,8][40:58]']
    results = test_subject._parse_input(inputs)
    pixel_cutout1 = results[0]
    pixel_cutout2 = results[1]

    assert pixel_cutout1.get_extension() == 0, 'Wrong extension found for 1.'
    assert pixel_cutout1.get_ranges() == [(500, 700)], \
        'Wrong ranges found for 1.'

    assert pixel_cutout2.get_extension() == ('SCI', 8), \
        'Wrong extension found for SCI,8.'
    assert pixel_cutout2.get_ranges() == [(40, 58)], \
        'Wrong ranges found for 1.'

    inputs = ['CIRCLE=88.0 115.0 0.5']
    results = test_subject._parse_input(inputs)

    assert results[0] == 'CIRCLE=88.0 115.0 0.5', 'Wrong WCS input.'

    inputs = ['[AMP]']

    results = test_subject._parse_input(inputs)
    pixel_cutout = results[0]

    assert pixel_cutout.get_extension() == ('AMP', 1), 'Wrong extension found.'


def test__sanity_check_input():
    test_subject = OpenCADCCutout()
    input = '[9][100:1000]'

    sanity_input = test_subject._sanity_check_input(input)
    assert isinstance(sanity_input, list), 'Should be list'

    with pytest.raises(ValueError) as ve:
        test_subject._sanity_check_input(('bad', 'tuple'))
        assert ('{}'.format(ve) ==
                'Input is expected to be a string or list but was \
(u\'bad\', u\'tuple\')') or ('{}'.format(ve) ==
                             'Input is expected to be a string or list but was \
(\'bad\', \'tuple\')'), \
            'Wrong error message.'


def test_write_stream():
    output = io.BytesIO()
    test_subject = WriteOnlyStream(output)

    with pytest.raises(ValueError):
        test_subject.read()

    assert test_subject.tell() == 0, 'Nothing written yet, should be zero.'
    test_subject.write(b'You have been recruied by the Star League to defend \
            the frontier against Xur and the Kodhan Armada.')
    assert test_subject.tell() == 111, 'Message written.'


def test_construct():
    test_subject = OpenCADCCutout()

    with pytest.raises(ValueError) as ve:
        test_subject.cutout([])
    assert str(ve.value) == 'No Cutout regions specified.', \
        'Wrong error message.'

    with pytest.raises(FileNotFoundError):
        test_subject.cutout([PixelCutoutHDU([(8, 10)])],
                            input_reader=open('/no/such/file'))
