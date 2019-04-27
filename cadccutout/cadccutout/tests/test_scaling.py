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
import context as test_context
from astropy.io import fits

from cadccutout.core import OpenCADCCutout
from cadccutout.pixel_cutout_hdu import PixelCutoutHDU


THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')
target_file_name = os.path.join(TESTDATA_DIR, 'testscaling.fits.fz')
logger = logging.getLogger()


def test_astropy_scaling():
    test_subject = OpenCADCCutout()
    cutout_file_name_path = test_context.random_test_file_name_path()
    cutout_regions = [PixelCutoutHDU([], "2")]
    # Write out a test file with the test result FITS data.
    with open(cutout_file_name_path, 'wb') as output_writer, \
            open(target_file_name, 'rb') as input_reader:
        test_subject.cutout(cutout_regions, input_reader, output_writer,
                            'FITS')

    # now check that BZERO and BSCALE have not been changed
    expected = fits.open(target_file_name, do_not_scale_image_data=True)
    actual = fits.open(cutout_file_name_path, do_not_scale_image_data=True)

    # check headers and data not changed. Except ...
    # expected missing keywords from actual: PCOUNT, XTENSION and GCOUNT
    # added to actual: SIMPLE
    del expected[2].header['PCOUNT']
    del expected[2].header['XTENSION']
    del expected[2].header['GCOUNT']
    assert 'SIMPLE' in actual[0].header
    del actual[0].header['SIMPLE']

    assert len(expected[2].header) == len(actual[0].header)
    for key in expected[2].header.keys():
        assert expected[2].header[key] == actual[0].header[key]
    np.testing.assert_array_equal(
        expected[2].data, actual[0].data,
        'Arrays do not match.')

    # do a cutout
    cutout_regions = [PixelCutoutHDU([(1, 100), (1, 100)], "3")]
    # Write out a test file with the test result FITS data.
    with open(cutout_file_name_path, 'wb') as output_writer, \
            open(target_file_name, 'rb') as input_reader:
        test_subject.cutout(cutout_regions, input_reader, output_writer,
                            'FITS')

    # now check that BZERO and BSCALE have not been changed
    expected = fits.open(target_file_name, do_not_scale_image_data=True)
    actual = fits.open(cutout_file_name_path, do_not_scale_image_data=True)

    # check only expected headers changed
    # changed headers
    del expected[3].header['PCOUNT']
    del expected[3].header['XTENSION']
    del expected[3].header['GCOUNT']
    assert 'SIMPLE' in actual[0].header
    del actual[0].header['SIMPLE']
    assert len(expected[3].header) == len(actual[0].header)
    for key in expected[3].header.keys():
        if key == 'NAXIS1' or key == 'NAXIS2':
            assert actual[0].header[key] == 100
        else:
            assert expected[3].header[key] == actual[0].header[key]

    # used for debugging...
    # changed_headers = {}
    # for key in expected[3].header.keys():
    #     if key not in actual[0].header:
    #         changed_headers[key] = 'Missing from actual'
    #     elif expected[3].header[key] != actual[0].header[key]:
    #         changed_headers[key] = (expected[3].header[key],
    #                                 actual[0].header[key])
    # for key in actual[0].header.keys():
    #     if key not in expected[3].header:
    #         changed_headers[key] = "Added to actual"
    #
    # print("*******************Differences*****************")
    # for key in changed_headers:
    #     print('***{}***: {}'.format(key, changed_headers[key]))
    # print("*********************************************")


def test_multiple_ext_cutouts():
    test_subject = OpenCADCCutout()
    cutout_file_name_path = test_context.random_test_file_name_path()
    cutout_regions = [PixelCutoutHDU([(1, 100), (1, 100)], "1"),
                      PixelCutoutHDU([(1, 100), (1, 100)], "3")]
    with open(cutout_file_name_path, 'wb') as output_writer, \
            open(target_file_name, 'rb') as input_reader:
        test_subject.cutout(cutout_regions, input_reader, output_writer,
                            'FITS')
    expected = fits.open(target_file_name, do_not_scale_image_data=True)
    actual = fits.open(cutout_file_name_path, do_not_scale_image_data=True)

    assert len(actual) == 3
    # test primary header unchanged
    assert len(expected[0].header) == len(actual[0].header)
    for key in expected[0].header.keys():
        assert expected[0].header[key] == actual[0].header[key]

    # check BSCALE and BZERO correct in cutout file
    assert expected[1].header['BSCALE'] == actual[1].header['BSCALE']
    assert expected[1].header['BZERO'] == actual[1].header['BZERO']
    assert expected[3].header['BSCALE'] == actual[2].header['BSCALE']
    assert expected[3].header['BZERO'] == actual[2].header['BZERO']


def test_multiple_cutouts_single_ext():
    test_subject = OpenCADCCutout()
    cutout_file_name_path = test_context.random_test_file_name_path()
    cutout_regions = [PixelCutoutHDU([(1, 100), (1, 100)], "1"),
                      PixelCutoutHDU([(200, 300), (2, 300)], "1")]
    with open(cutout_file_name_path, 'wb') as output_writer, \
            open(target_file_name, 'rb') as input_reader:
        test_subject.cutout(cutout_regions, input_reader, output_writer,
                            'FITS')
    expected = fits.open(target_file_name, do_not_scale_image_data=True)
    actual = fits.open(cutout_file_name_path, do_not_scale_image_data=True)

    # cutouts in the same extension => no extra primary HDU
    assert len(actual) == 2
    # test primary header changed
    assert len(expected[0].header) != len(actual[0].header)

    # check BSCALE and BZERO correct in cutout file
    assert expected[1].header['BSCALE'] == actual[0].header['BSCALE']
    assert expected[1].header['BZERO'] == actual[0].header['BZERO']
    assert expected[1].header['BSCALE'] == actual[1].header['BSCALE']
    assert expected[1].header['BZERO'] == actual[1].header['BZERO']
