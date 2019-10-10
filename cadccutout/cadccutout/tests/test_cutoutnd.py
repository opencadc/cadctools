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

import os
import logging
import tempfile
import pytest
import numpy as np

from astropy.wcs import WCS
from cadccutout.utils import to_astropy_header
from cadccutout.cutoutnd import CutoutND
from cadccutout.pixel_cutout_hdu import PixelCutoutHDU

import fitsio

LOGGER = logging.getLogger('cadccutout')
THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')
DEFAULT_TEST_FILE_DIR = '/tmp'


def random_test_file_name_path(file_extension='fits',
                               dir_name=DEFAULT_TEST_FILE_DIR):
    '''
    Create a random file and return the path of it.
    '''
    return tempfile.NamedTemporaryFile(
        dir=dir_name, prefix=__name__, suffix='.{}'.format(
            file_extension)).name


def test_create():
    '''
    Test the constructor with a None input.
    '''
    with pytest.raises(ValueError):
        CutoutND(hdu=None)

    with pytest.raises(ValueError):
        CutoutND(hdu=[])


def test_get_parameters():
    '''
    Test getting parameters.
    '''
    fname = random_test_file_name_path()
    with fitsio.FITS(fname, 'rw', clobber=True) as fits:
        data_shape = (9, 4)
        data = np.arange(36).reshape(data_shape)

        fits.create_image_hdu(dims=data.shape, dtype=data.dtype)

        fits[-1].write(data)
        hdu = fits[-1]

        test_subject = CutoutND(hdu=hdu)
        cutout_region = PixelCutoutHDU([(3, 18)])
        cutout = test_subject.get_parameters(cutout_region.get_ranges())
        expected_cutout = (slice(9), slice(2, 18, 1))
        assert expected_cutout == cutout.cutout, 'Arrays do not match.'


def test_inverse_y():
    '''
    Test reversing the y values (i.e. stop < start).
    '''
    fname = random_test_file_name_path()
    with fitsio.FITS(fname, 'rw', clobber=True) as fits:
        data_shape = (10, 10)
        data = np.arange(100).reshape(data_shape)

        fits.create_image_hdu(dims=data.shape, dtype=data.dtype)

        fits[-1].write(data)
        hdu = fits[-1]

        test_subject = CutoutND(hdu=hdu)
        cutout_regions = [(1, 2), (8, 4)]
        cutout = test_subject.get_parameters(cutout_regions)
        expected_cutout = (slice(7, 4, 1), slice(0, 2, 1))
        assert expected_cutout == cutout.cutout, \
            'Arrays do not match in {}.'.format(fname)


def test_inverse_y_striding():
    '''
    Test reversing the y values (i.e. stop < start) with a step value.
    '''
    fname = random_test_file_name_path()
    with fitsio.FITS(fname, 'rw', clobber=True) as fits:
        data_shape = (10, 10)
        data = np.arange(100).reshape(data_shape)

        fits.create_image_hdu(dims=data.shape, dtype=data.dtype)

        fits[-1].write(data)
        hdu = fits[-1]

        test_subject = CutoutND(hdu=hdu)
        cutout_regions = [(1, 2), (10, 2, 2)]
        cutout = test_subject.get_parameters(cutout_regions)
        expected_cutout = (slice(9, 2, 2), slice(0, 2, 1))
        assert expected_cutout == cutout.cutout, \
            'Arrays do not match in {}.'.format(fname)


def test_extract_striding():
    '''
    Test extraction with a step value.
    '''
    fname = random_test_file_name_path()
    with fitsio.FITS(fname, 'rw', clobber=True) as fits:
        data_shape = (10, 10)
        data = np.arange(100).reshape(data_shape)

        fits.create_image_hdu(dims=data.shape, dtype=data.dtype)

        fits[-1].write(data)
        hdu = fits[-1]

        test_subject = CutoutND(hdu=hdu)
        cutout_regions = [(4, 18, 5)]
        cutout = test_subject.get_parameters(cutout_regions)
        expected_cutout = (slice(10), slice(3, 18, 5))
        assert expected_cutout == cutout.cutout, \
            'Arrays do not match in {}.'.format(fname)


def test_extract_striding_wildcard():
    '''
    Test extraction with a step value using a wildcard.
    '''
    fname = random_test_file_name_path()
    with fitsio.FITS(fname, 'rw', clobber=True) as fits:
        data_shape = (10, 10)
        data = np.arange(100).reshape(data_shape)

        fits.create_image_hdu(dims=data.shape, dtype=data.dtype)

        fits[-1].write(data)
        hdu = fits[-1]

        test_subject = CutoutND(hdu=hdu)
        cutout_regions = [('*', 7)]
        cutout = test_subject.get_parameters(cutout_regions)
        expected_cutout = (slice(10), slice(None, 10, 7))
        assert expected_cutout == cutout.cutout, \
            'Arrays do not match for file {}.'.format(fname)


def test_extract_invalid():
    '''
    Test for an invalid extraction.
    '''
    fname = random_test_file_name_path()
    with fitsio.FITS(fname, 'rw', clobber=True) as fits:
        data_shape = (10, 10)
        data = np.arange(100).reshape(data_shape)

        fits.create_image_hdu(dims=data.shape, dtype=data.dtype)

        fits[-1].write(data)
        hdu = fits[-1]

        test_subject = CutoutND(hdu)
        cutout_regions = [('')]

        with pytest.raises(ValueError,
                           match=r"Should have at least two values "
                           r"\(lower, upper\)\."):
            test_subject.get_parameters(cutout_regions)
