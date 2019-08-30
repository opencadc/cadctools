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

import logging
import os
import numpy as np
import pytest

from astropy.io import fits
from astropy.wcs import WCS

import context as test_context
from cadccutout.core import OpenCADCCutout
from cadccutout.pixel_cutout_hdu import PixelCutoutHDU


pytest.main(args=['-s', os.path.abspath(__file__)])
THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')
TARGET_FILE_NAME = os.path.join(TESTDATA_DIR, 'test-simple.fits')
EXPECTED_CUTOUT_FILE_NAME = os.path.join(
    TESTDATA_DIR, 'test-simple-cutout.fits')
LOGGER = logging.getLogger('cadccutout')


def test_simple_cutout():
    '''
    Perform a simple cutout from the target file.
    '''
    test_subject = OpenCADCCutout()
    cutout_file_name_path = test_context.random_test_file_name_path()
    LOGGER.info('Testing with {} and writing out to {}.'.format(
        TARGET_FILE_NAME, cutout_file_name_path))
    cutout_regions = [PixelCutoutHDU([(300, 800), (810, 1000)])]

    # Write out a test file with the test result FITS data.
    test_subject.cutout(cutout_regions, TARGET_FILE_NAME,
                        cutout_file_name_path, 'FITS')

    with fits.open(EXPECTED_CUTOUT_FILE_NAME, mode='readonly') \
            as expected_hdu_list, \
            fits.open(cutout_file_name_path, mode='readonly') \
            as result_hdu_list:
        fits_diff = fits.FITSDiff(expected_hdu_list, result_hdu_list)
        np.testing.assert_array_equal(
            (), fits_diff.diff_hdu_count, 'HDU count diff should be empty.')

        for extension, result_hdu in enumerate(result_hdu_list):
            expected_hdu = expected_hdu_list[extension]
            expected_wcs = WCS(header=expected_hdu.header, fix=False)
            result_wcs = WCS(header=result_hdu.header, fix=False)

            np.testing.assert_array_equal(
                expected_wcs.wcs.crpix, result_wcs.wcs.crpix,
                'Wrong CRPIX values.')
            np.testing.assert_array_equal(
                expected_wcs.wcs.crval, result_wcs.wcs.crval,
                'Wrong CRVAL values.')
            assert expected_hdu.header['NAXIS1'] \
                == result_hdu.header['NAXIS1'], 'Wrong NAXIS1 values.'
            assert expected_hdu.header['NAXIS2'] \
                == result_hdu.header['NAXIS2'], 'Wrong NAXIS2 values.'
            assert expected_hdu.header.get(
                'CHECKSUM') is None, 'Should not contain CHECKSUM.'
            assert expected_hdu.header.get(
                'DATASUM') is None, 'Should not contain DATASUM.'
            np.testing.assert_array_equal(
                expected_hdu.data, result_hdu.data,
                'Arrays do not match.')
