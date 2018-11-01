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
import numpy as np
import os
import sys
import pytest
import tempfile

from astropy.io import fits
from astropy.wcs import WCS

from .context import opencadc_cutout, random_test_file_name_path
from opencadc_cutout.core import OpenCADCCutout
from opencadc_cutout.pixel_cutout_hdu import PixelCutoutHDU
from opencadc_cutout.no_content_error import NoContentError


pytest.main(args=['-s', os.path.abspath(__file__)])
THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')
logger = logging.getLogger()


def _create_mef_file(dir_name='/tmp'):
    mef_file = random_test_file_name_path(dir_name=dir_name)
    hdu0 = fits.PrimaryHDU()

    data1 = np.arange(10000).reshape(100, 100)
    hdu1 = fits.ImageHDU(data=data1)

    data2 = np.arange(20000).reshape(200, 100)
    hdu2 = fits.ImageHDU(data=data2)

    data3 = np.arange(50000).reshape(100, 500)
    hdu3 = fits.ImageHDU(data=data3)

    hdulist = fits.HDUList([hdu0, hdu1, hdu2, hdu3])

    hdulist.writeto(mef_file, overwrite=True)
    return mef_file


def test_mef_cutout_no_overlap():
    test_subject = OpenCADCCutout()
    target_file_name = _create_mef_file()
    cutout_file_name_path = random_test_file_name_path()
    logger.info('Testing with {}'.format(cutout_file_name_path))
    cutout_region_str = '[1][300:800,810:1000]'

    try:
        # Write out a test file with the test result FITS data.
        with open(cutout_file_name_path, 'ab+') as output_writer, open(target_file_name, 'rb') as input_reader:
            test_subject.cutout(input_reader, output_writer,
                                cutout_region_str, 'FITS')
            output_writer.close()
            input_reader.close()
        assert False
    except NoContentError as err:
        assert str(err) == 'No content (arrays do not overlap).', 'Wrong message.'


def test_mef_cutout():
    test_subject = OpenCADCCutout()
    target_file_name = _create_mef_file()
    cutout_file_name_path = random_test_file_name_path()
    logger.info('Testing with {}'.format(cutout_file_name_path))
    cutout_region_str = '[2][20:35,40:50][3]'

    # Write out a test file with the test result FITS data.
    with open(cutout_file_name_path, 'ab+') as output_writer, open(target_file_name, 'rb') as input_reader:
        test_subject.cutout(input_reader, output_writer,
                            cutout_region_str, 'FITS')
        output_writer.close()
        input_reader.close()

    with fits.open(cutout_file_name_path, mode='readonly') as result_hdu_list:
        assert len(result_hdu_list) == 3, 'Should have 3 HDUs.'

        hdu1 = result_hdu_list[1]
        hdu2 = result_hdu_list[2]
        wcs1 = WCS(header=hdu1.header)
        wcs2 = WCS(header=hdu2.header)

        np.testing.assert_array_equal(
            wcs1.wcs.crpix, [-19.0, -39.0], 'Wrong CRPIX values.')
        np.testing.assert_array_equal(
            wcs1.wcs.crval, [0.0, 0.0], 'Wrong CRVAL values.')
        np.testing.assert_array_equal(
            wcs2.wcs.crpix, [0.0, 0.0], 'Wrong CRPIX values.')
        np.testing.assert_array_equal(
            wcs2.wcs.crval, [0.0, 0.0], 'Wrong CRVAL values.')

        assert hdu1.header['NAXIS'] == 2, 'Wrong NAXIS value.'
        assert hdu2.header['NAXIS'] == 2, 'Wrong NAXIS value.'

        assert hdu1.header.get(
            'CHECKSUM') is None, 'Should not contain CHECKSUM.'
        assert hdu2.header.get(
            'CHECKSUM') is None, 'Should not contain CHECKSUM.'

        assert hdu1.header.get(
            'DATASUM') is None, 'Should not contain DATASUM.'
        assert hdu2.header.get(
            'DATASUM') is None, 'Should not contain DATASUM.'

        expected1 = np.zeros((11, 16), dtype=hdu1.data.dtype)
        expected2 = np.arange(50000, dtype=hdu2.data.dtype).reshape(100, 500)

        for i in range(11):
            start = 3918 + (i * 100)
            expected1[i] = np.arange(start, start + 16, dtype=hdu1.data.dtype)

        np.testing.assert_array_equal(
            hdu1.data, expected1, 'Arrays 1 do not match.')
        np.testing.assert_array_equal(
            hdu2.data, expected2, 'Arrays 2 do not match.')
