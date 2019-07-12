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
from astropy.io.fits import Header
from astropy.wcs import WCS
from cadccutout.pixel_cutout_hdu import PixelCutoutHDU


def test_create():
    with pytest.raises(ValueError):
        CutoutND(data=None)


def test_extract():
    data_shape = (9, 4)
    data = np.arange(36).reshape(data_shape)
    test_subject = CutoutND(data)
    cutout_region = PixelCutoutHDU([(4, 18)])
    cutout = test_subject.extract(cutout_region.get_ranges())
    expected_data = np.array([[3],
                              [7],
                              [11],
                              [15],
                              [19],
                              [23],
                              [27],
                              [31],
                              [35]])
    np.testing.assert_array_equal(
        expected_data, cutout.data, 'Arrays do not match.')


def test_inverse_y():
    data_shape = (10, 10)
    data = np.arange(100).reshape(data_shape)
    test_subject = CutoutND(data)
    cutout_regions = [(1, 2), (8, 4)]
    cutout = test_subject.extract(cutout_regions)
    expected_data = np.array([[70, 71],
                              [60, 61],
                              [50, 51],
                              [40, 41],
                              [30, 31]])
    np.testing.assert_array_equal(
        expected_data, cutout.data, 'Arrays do not match.')


def test_inverse_y_striding():
    data_shape = (10, 10)
    data = np.arange(100).reshape(data_shape)
    test_subject = CutoutND(data)
    cutout_regions = [(1, 2), (10, 2, 2)]
    cutout = test_subject.extract(cutout_regions)
    expected_data = np.array([[90, 91],
                              [70, 71],
                              [50, 51],
                              [30, 31],
                              [10, 11]])
    np.testing.assert_array_equal(
        expected_data, cutout.data, 'Arrays do not match.')


def test_extract_striding():
    data_shape = (10, 10)
    data = np.arange(100).reshape(data_shape)
    test_subject = CutoutND(data)
    cutout_regions = [(4, 18, 5)]
    cutout = test_subject.extract(cutout_regions)
    expected_data = np.array([[3, 8],
                              [13, 18],
                              [23, 28],
                              [33, 38],
                              [43, 48],
                              [53, 58],
                              [63, 68],
                              [73, 78],
                              [83, 88],
                              [93, 98]])
    np.testing.assert_array_equal(
        expected_data, cutout.data, 'Arrays do not match.')


def test_extract_striding_wildcard():
    data_shape = (10, 10)
    data = np.arange(100).reshape(data_shape)
    test_subject = CutoutND(data)
    cutout_regions = [('*', 7)]
    cutout = test_subject.extract(cutout_regions)
    expected_data = np.array([[0, 7],
                              [10, 17],
                              [20, 27],
                              [30, 37],
                              [40, 47],
                              [50, 57],
                              [60, 67],
                              [70, 77],
                              [80, 87],
                              [90, 97]])
    np.testing.assert_array_equal(
        expected_data, cutout.data, 'Arrays do not match.')


def test_extract_invalid():
    data_shape = (10, 10)
    data = np.arange(100).reshape(data_shape)
    test_subject = CutoutND(data)
    cutout_regions = [('')]

    with pytest.raises(ValueError,
                       match=r"Should have at least two values "
                             r"\(lower, upper\)\."):
        test_subject.extract(cutout_regions)


def test_with_wcs():
    data = np.arange(100).reshape(10, 10)
    header = Header()
    wcs = WCS(fix=False)
    wcs.wcs.cd = [[0.9, 0.8], [0.7, 0.6]]
    header.set('REMAIN1', 'VALUE1')
    header.set('DQ1', 'dqvalue1')
    header.set('NAXIS', 2)

    test_subject = CutoutND(data, wcs=wcs)
    cutout_result = test_subject.extract([(1, 6, 2), (4, 10, 2)])
    result_wcs = cutout_result.wcs
    np.testing.assert_array_equal([[1.8, 1.6], [1.4, 1.2]],
                                  result_wcs.wcs.cd, 'Wrong CD output.')
