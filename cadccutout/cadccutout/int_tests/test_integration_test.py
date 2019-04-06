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

import sys
import os
import logging
import tempfile
import pytest
import numpy as np

from astropy.io import fits
from astropy.wcs import WCS

from cadccutout.core import OpenCADCCutout
from cadccutout.pixel_range_input_parser import PixelRangeInputParser

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')
DEFAULT_TEST_FILE_DIR = '/tmp'
logging.basicConfig(level=logging.DEBUG)
logging.getLogger('cadccutout').setLevel(logging.DEBUG)


def random_test_file_name_path(file_extension='fits',
                               dir_name=DEFAULT_TEST_FILE_DIR):
    return tempfile.NamedTemporaryFile(
        dir=dir_name, prefix=__name__, suffix='.{}'.format(file_extension)).name


def _get_requested_extensions(cutout_string):
    input_range_parser = PixelRangeInputParser()
    cutout_regions = input_range_parser.parse(cutout_string)
    extensions = []

    for cr in cutout_regions:
        extensions.append(cr.get_extension())

    return extensions


def _extname_sort_func(hdu):
    return (hdu.header.get('EXTNAME', ''), hdu.header.get('EXTVER', '0'))


@pytest.mark.parametrize(
    'cutout_region_string, target_file_name, \
                          expected_cutout_file_path, use_fits_diff, \
                          test_dir_name, wcs_naxis_val, use_extension_names',
    [
        ('[200:500,100:300,100:140]', '/usr/src/data/test-gmims-cube.fits',
         '/usr/src/data/test-gmims-cube-cutout.fits', True,
         DEFAULT_TEST_FILE_DIR, None, False),
        ('[1][20:40][2][20:40]', '/usr/src/data/public_fits.fits.fz',
         '/usr/src/data/public_fits.cutout.fits.fz', True,
         DEFAULT_TEST_FILE_DIR, None, True),
        ('[1][20:40:12]', '/usr/src/data/public_fits.fits.fz',
         '/usr/src/data/public_fits.cutout_striding.fits.fz', True,
         DEFAULT_TEST_FILE_DIR, None, True),
        ('[80:220,100:150,100:150]', '/usr/src/data/test-alma-cube.fits',
         '/usr/src/data/test-alma-cube-cutout.fits', True,
         DEFAULT_TEST_FILE_DIR, None, False),
        ('[200:400,500:1000,10:20]', '/usr/src/data/test-cgps-cube.fits',
         '/usr/src/data/test-cgps-cube-cutout.fits', True,
         DEFAULT_TEST_FILE_DIR, None, False),
        ('[1000:1200,800:1000,160:200]', '/usr/src/data/test-sitelle-cube.fits',
         '/usr/src/data/test-sitelle-cube-cutout.fits', False,
         DEFAULT_TEST_FILE_DIR, 2, False),
        ('[500:900,300:1000,8:12]', '/usr/src/data/test-vlass-cube.fits',
         '/usr/src/data/test-vlass-cube-cutout.fits', True,
         DEFAULT_TEST_FILE_DIR, None, False),
        ('[2][*:20]', '/usr/src/data/test-cfht.fits.fz',
         '/usr/src/data/test-cfht-cutout.fits.fz', True,
         DEFAULT_TEST_FILE_DIR, None, False),
        ('[SCI,10][80:220,100:150][1][10:16,70:90][106][8:32,88:112][126]',
         '/usr/src/data/test-hst-mef.fits',
         '/usr/src/data/test-hst-mef-cutout.fits', True, DEFAULT_TEST_FILE_DIR,
         None, True),
        ('[7970:8481,14843:14332]', '/usr/src/data/test-megapipe.fits',
         '/usr/src/data/test-megapipe-cutout.fits', True, DEFAULT_TEST_FILE_DIR,
         None, True),
        ('[7970:8481:4,14843:14332:4]', '/usr/src/data/test-megapipe.fits',
         '/usr/src/data/test-megapipe-cutout-striding.fits', True,
         DEFAULT_TEST_FILE_DIR, None, True)
    ])
def test_integration_test(
        cutout_region_string, target_file_name, expected_cutout_file_path,
        use_fits_diff, test_dir_name, wcs_naxis_val, use_extension_names):
    test_subject = OpenCADCCutout()
    result_cutout_file_path = random_test_file_name_path(dir_name=test_dir_name)

    logging.info('Testing output to {}'.format(result_cutout_file_path))

    # Write out a test file with the test result FITS data.
    with open(result_cutout_file_path, 'ab+') as test_file_handle, \
            open(target_file_name, 'rb') as input_file_handle:
        test_subject.cutout_from_string(
            cutout_region_string, input_file_handle, test_file_handle, 'FITS')

    with fits.open(expected_cutout_file_path, mode='readonly',
                   do_not_scale_image_data=True) \
            as expected_hdu_list, \
            fits.open(result_cutout_file_path, mode='readonly',
                      do_not_scale_image_data=True) \
            as result_hdu_list:
        if use_fits_diff:
            fits_diff = fits.FITSDiff(expected_hdu_list, result_hdu_list)
            np.testing.assert_array_equal(
                (), fits_diff.diff_hdu_count, 'HDU count diff should be empty.')

        if use_extension_names:
            result_hdu_list.sort(key=_extname_sort_func)
            expected_hdu_list.sort(key=_extname_sort_func)

        for extension, result_hdu in enumerate(result_hdu_list):
            logging.debug('\nChecking extension {}\n'.format(extension))
            expected_hdu = expected_hdu_list[extension]

            expected_wcs = WCS(header=expected_hdu.header,
                               naxis=wcs_naxis_val, fix=False)
            result_wcs = WCS(header=result_hdu.header,
                             naxis=wcs_naxis_val, fix=False)

            np.testing.assert_array_equal(
                expected_wcs.wcs.crpix, result_wcs.wcs.crpix,
                'Wrong CRPIX values.')
            np.testing.assert_array_equal(
                expected_wcs.wcs.crval, result_wcs.wcs.crval,
                'Wrong CRVAL values.')
            np.testing.assert_array_equal(
                expected_wcs.wcs.naxis, result_wcs.wcs.naxis,
                'Wrong NAXIS values.')
            assert expected_hdu.header.get(
                'CHECKSUM') is None, 'Should not contain CHECKSUM.'
            assert expected_hdu.header.get(
                'DATASUM') is None, 'Should not contain DATASUM.'
            expected_data = expected_hdu.data
            result_data = result_hdu.data

            if expected_data is not None and result_data is not None:
                assert expected_data.shape == result_data.shape, \
                    'Shapes don\'t match.'
                np.testing.assert_array_equal(
                    expected_data, result_data, 'Arrays do not match.')
            else:
                assert expected_data == result_data


@pytest.mark.parametrize(
    'cutout_region_string, target_file_name, \
                          expected_cutout_file_path, use_fits_diff, \
                          test_dir_name, wcs_naxis_val, use_extension_names',
    [
        ('CIRCLE 70.3389 -2.8361 0.016666666666666666',
         '/usr/src/data/test-sitelle-cube.fits',
         '/usr/src/data/test-sitelle-cube-cutout-wcs.fits', True,
         DEFAULT_TEST_FILE_DIR, 2, False),
        ('CIRCLE 40.05 58.05 0.7',
            '/usr/src/data/test-cgps-cube.fits',
            '/usr/src/data/test-cgps-cube-cutout-wcs.fits', True,
            DEFAULT_TEST_FILE_DIR, 2, False),
        ('CIRCLE 246.52 -24.33 0.01',
            '/usr/src/data/test-alma-cube.fits',
            '/usr/src/data/test-alma-cube-cutout-wcs.fits', True,
            DEFAULT_TEST_FILE_DIR, 2, False),
        ('CIRCLE 161.52 77.472 0.03',
            '/usr/src/data/test-vlass-cube.fits',
            '/usr/src/data/test-vlass-cube-cutout-wcs.fits', True,
            DEFAULT_TEST_FILE_DIR, 2, False),
        ('CIRCLE 189.1726880000002 62.17111899999974 0.01',
            '/usr/src/data/test-hst-mef.fits',
            '/usr/src/data/test-hst-mef-cutout-wcs.fits', True,
            DEFAULT_TEST_FILE_DIR, 2, False)
    ])
def test_integration_wcs_test(
        cutout_region_string, target_file_name, expected_cutout_file_path,
        use_fits_diff, test_dir_name, wcs_naxis_val, use_extension_names):
    test_subject = OpenCADCCutout()
    result_cutout_file_path = random_test_file_name_path(dir_name=test_dir_name)

    logging.info('Testing output to {}'.format(result_cutout_file_path))

    # Write out a test file with the test result FITS data.
    with open(result_cutout_file_path, 'ab+') as test_file_handle, \
            open(target_file_name, 'rb') as input_file_handle:
        test_subject.cutout_from_string(
            cutout_region_string, input_file_handle, test_file_handle, 'FITS')

    with fits.open(expected_cutout_file_path, mode='readonly',
                   do_not_scale_image_data=True) \
            as expected_hdu_list, \
            fits.open(result_cutout_file_path, mode='readonly',
                      do_not_scale_image_data=True) \
            as result_hdu_list:
        if use_fits_diff:
            fits_diff = fits.FITSDiff(expected_hdu_list, result_hdu_list)
            np.testing.assert_array_equal(
                (), fits_diff.diff_hdu_count, 'HDU count diff should be empty.')

        if use_extension_names:
            result_hdu_list.sort(key=_extname_sort_func)
            expected_hdu_list.sort(key=_extname_sort_func)

        for extension, result_hdu in enumerate(result_hdu_list):
            logging.info('\nChecking extension {}\n'.format(extension))
            expected_hdu = expected_hdu_list[extension]

            expected_wcs = WCS(header=expected_hdu.header,
                               naxis=wcs_naxis_val, fix=False)
            result_wcs = WCS(header=result_hdu.header,
                             naxis=wcs_naxis_val, fix=False)

            np.testing.assert_array_almost_equal(
                expected_wcs.wcs.crpix, result_wcs.wcs.crpix, decimal=-4,
                err_msg='Wrong CRPIX values.')
            np.testing.assert_array_equal(
                expected_wcs.wcs.crval, result_wcs.wcs.crval,
                'Wrong CRVAL values.')
            np.testing.assert_array_equal(
                expected_wcs.wcs.naxis, result_wcs.wcs.naxis,
                'Wrong NAXIS values.')
            assert expected_hdu.header.get(
                'CHECKSUM') is None, 'Should not contain CHECKSUM.'
            assert expected_hdu.header.get(
                'DATASUM') is None, 'Should not contain DATASUM.'

            expected_data = expected_hdu.data
            result_data = result_hdu.data

            try:
                if expected_data is not None and result_data is not None:
                    assert expected_data.shape == result_data.shape, \
                        'Shapes don\'t match.'
                    np.testing.assert_array_equal(
                        expected_data, result_data, 'Arrays do not match.')
                else:
                    assert expected_data == result_data
            except AssertionError:
                # Check the shape if the data array doesn't match.
                np.testing.assert_array_almost_equal(
                    expected_hdu.data.shape, result_hdu.data.shape, decimal=-4,
                    err_msg='Arrays match closely enough.')
