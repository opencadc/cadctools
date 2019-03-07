# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2019.                            (c) 2019.
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

import pytest
import numpy as np
from cadccutout.cutoutnd import CutoutND
from cadccutout.pixel_cutout_hdu import PixelCutoutHDU


def test_create():
    with pytest.raises(ValueError):
        CutoutND(data=None)


def test_get_position_shape():
    data_shape = (4, 4)
    data = np.random.random_sample(data_shape)
    test_subject = CutoutND(data)
    cutout_region = PixelCutoutHDU([(1, 200), (305, 600)])
    (position, shape) = test_subject._get_position_shape(data_shape,
                                                         cutout_region)

    assert shape == (296, 200), 'Wrong shape returned'
    assert position == (451, 99), 'Wrong shape returned'


def test_get_position_shape_err_shape():
    data_shape = (4, 4)
    data = np.random.random_sample(data_shape)
    test_subject = CutoutND(data)
    cutout_region = PixelCutoutHDU([(1, 200), (305, 600), (100, 155)])

    with pytest.raises(ValueError) as ve:
        test_subject._get_position_shape(data_shape, cutout_region)

    error_output = str(ve)
    ind = error_output.index('ValueError: ') + len('ValueError: ')
    assert error_output[ind:] == \
        'Invalid shape requested (tried to extract (56, 296, 200) from (4, 4)).'


def test_get_position_shape_prepend():
    data_shape = (4, 4)
    data = np.random.random_sample(data_shape)
    test_subject = CutoutND(data)
    cutout_region = PixelCutoutHDU([(10)])

    (position, shape) = \
        test_subject._get_position_shape(data_shape, cutout_region)

    assert position == (2, 9), 'Wrong position.'
    assert shape == (4, 1), 'Wrong shape.'
