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

import pytest
from astropy.io import fits
from cadccutout.transform import Transform, Shape

from cadccutout.no_content_error import NoContentError

pytest.main(args=['-s', os.path.abspath(__file__)])
THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

# IRIS 3D spectral cube I212B2H0.fits
IRIS_3D_CUBE_HEADER = 'iris-3d-cube.hdr'

# CGPS 4D cube CGPS_MA1_HI_line_image.fits
CGPS_4D_CUBE_HEADER = 'cgps-4d-cube.hdr'

# VLASS 4D cube VLASS1.1.cc.T29t05.J110448+763000.10.2048.v1.fits
VLASS_4D_CUBE_HEADER = 'vlass-4d-cube.hdr'

# JCMT 3D spectral cube JCMT-jcmth20181022_00048_01_reduced001_obs_000.fits
JCMT_3D_CUBE_HEADER = 'jcmt-3d-cube.hdr'


@pytest.mark.skip
def test_axis_type():
    from cadccutout.transform import AxisType
    header_filename = os.path.join(TESTDATA_DIR, VLASS_4D_CUBE_HEADER)
    header = fits.Header.fromtextfile(header_filename)

    test_subject = AxisType(header)

    spatial\
        = test_subject.get_spatial_axes()
    assert spatial is not None
    assert len(spatial) == 2
    assert spatial[0] == 1
    assert spatial[1] == 2
    spectral = test_subject.get_spectral_axis()
    assert spectral is not None
    assert spectral == 3
    temporal = test_subject.get_temporal_axis()
    assert temporal is None
    polarization = test_subject.get_polarization_axis()
    assert polarization is not None
    assert polarization == 4


@pytest.mark.skip
def test_parse_world_to_shapes():
    test_subject = Transform()

    cutout = "CIRCLE=1.0+2.0+3.0&CIRCLE=4.0+5.0+6.0"
    try:
        shapes = test_subject.parse_world_to_shapes(cutout)
        assert False, 'Should raise ValueError'
    except ValueError:
        assert True

    cutout = "CIRCLE=1.0+2.0+3.0&POLYGON=1.0 2.0 3.0 4.0 5.0 6.0 7.0 8.0"
    try:
        shapes = test_subject.parse_world_to_shapes(cutout)
        assert False, 'Should raise ValueError'
    except ValueError:
        assert True

    cutout = "CIRCLE=1.0+2.0+3.0&CIRCLE="
    try:
        shapes = test_subject.parse_world_to_shapes(cutout)
        assert False, 'Should raise ValueError'
    except ValueError:
        assert True

    cutout = "POL=I&POL=V"
    shapes = test_subject.parse_world_to_shapes(cutout)
    assert len(shapes) == 1
    shape = shapes[0]
    assert shape[0] == Shape.CIRCLE
    coordinates = shape[1]
    assert len(coordinates) == 3
    assert coordinates[0] == '1.0'
    assert coordinates[1] == '2.0'
    assert coordinates[2] == '3.0'

    cutout = "CIRCLE=1.0 2.0 3.0"
    shapes = test_subject.parse_world_to_shapes(cutout)
    assert len(shapes) == 1
    shape = shapes[0]
    assert shape[0] == Shape.CIRCLE
    coordinates = shape[1]
    assert len(coordinates) == 3
    assert coordinates[0] == '1.0'
    assert coordinates[1] == '2.0'
    assert coordinates[2] == '3.0'

    cutout = "POLYGON=1.0 2.0 3.0 4.0 5.0 6.0 7.0 8.0"
    shapes = test_subject.parse_world_to_shapes(cutout)
    assert len(shapes) == 1
    shape = shapes[0]
    assert shape[0] == Shape.POLYGON
    coordinates = shape[1]
    assert len(coordinates) == 8
    assert coordinates[0] == '1.0'
    assert coordinates[1] == '2.0'
    assert coordinates[2] == '3.0'
    assert coordinates[3] == '4.0'
    assert coordinates[4] == '5.0'
    assert coordinates[5] == '6.0'
    assert coordinates[6] == '7.0'
    assert coordinates[7] == '8.0'

    cutout = "BAND=1.0 2.0"
    shapes = test_subject.parse_world_to_shapes(cutout)
    assert len(shapes) == 1
    shape = shapes[0]
    assert shape[0] == Shape.BAND
    coordinates = shape[1]
    assert len(coordinates) == 2
    assert coordinates[0] == '1.0'
    assert coordinates[1] == '2.0'

    cutout = "TIME=1.0 2.0"
    shapes = test_subject.parse_world_to_shapes(cutout)
    assert len(shapes) == 1
    shape = shapes[0]
    assert shape[0] == Shape.TIME
    coordinates = shape[1]
    assert len(coordinates) == 2
    assert coordinates[0] == '1.0'
    assert coordinates[1] == '2.0'

    cutout = "POL=LL"
    shapes = test_subject.parse_world_to_shapes(cutout)
    assert len(shapes) == 1
    shape = shapes[0]
    assert shape[0] == Shape.POL
    coordinates = shape[1]
    assert len(coordinates) == 1
    assert coordinates[0] == 'LL'

    cutout = "POLYGON=1.0 2.0 3.0 4.0 5.0 6.0 7.0 8.0&BAND=1.0 2.0&\
        TIME=1.0 2.0&POL=LL"
    shapes = test_subject.parse_world_to_shapes(cutout)
    assert len(shapes) == 4

    shape = shapes[0]
    assert shape[0] == Shape.POLYGON
    coordinates = shape[1]
    assert len(coordinates) == 8
    assert coordinates[0] == '1.0'
    assert coordinates[1] == '2.0'
    assert coordinates[2] == '3.0'
    assert coordinates[3] == '4.0'
    assert coordinates[4] == '5.0'
    assert coordinates[5] == '6.0'
    assert coordinates[6] == '7.0'
    assert coordinates[7] == '8.0'

    shape = shapes[1]
    assert shape[0] == Shape.BAND
    coordinates = shape[1]
    assert len(coordinates) == 2
    assert coordinates[0] == '1.0'
    assert coordinates[1] == '2.0'

    shape = shapes[2]
    assert shape[0] == Shape.TIME
    coordinates = shape[1]
    assert len(coordinates) == 2
    assert coordinates[0] == '1.0'
    assert coordinates[1] == '2.0'

    shape = shapes[3]
    assert shape[0] == Shape.POL
    coordinates = shape[1]
    assert len(coordinates) == 1
    assert coordinates[0] == 'LL'


@pytest.mark.skip
def test_world_to_pixels_no_content():
    header_filename = os.path.join(TESTDATA_DIR, VLASS_4D_CUBE_HEADER)
    header = fits.Header.fromtextfile(header_filename)

    # circle no content
    query = 'circle=-168.34719985367971+-76.18699791158396+0.01&\
        BAND=0.04456576+0.11662493&POL=I'

    test_subject = Transform()
    try:
        test_subject.world_to_pixels(query, header)
        assert False, 'Should raise NoContentError'
    except NoContentError:
        assert True

    # polygon no content
    query = 'Polygon=-168.34 -76.18 -168.34 -76.19 -168.35 -76.19&\
        BAND=0.04456576+0.11662493&POL=I'

    test_subject = Transform()
    try:
        test_subject.world_to_pixels(query, header)
        assert False, 'Should raise NoContentError'
    except NoContentError:
        assert True

    # energy no content
    query = 'circle=168.34719985367971+76.18699791158396+0.01&\
        BAND=0.14456576+0.21662493&POL=I'

    test_subject = Transform()
    try:
        test_subject.world_to_pixels(query, header)
        assert False, 'Should raise NoContentError'
    except NoContentError:
        assert True

    # polarization no content
    query = 'circle=168.34719985367971+76.18699791158396+0.01&\
        BAND=0.04456576+0.11662493&POL=LL'

    test_subject = Transform()
    try:
        test_subject.world_to_pixels(query, header)
        assert False, 'Should raise NoContentError'
    except NoContentError:
        assert True


@pytest.mark.skip
def test_get_circle_cutout_pixels_vlass():
    header_filename = os.path.join(TESTDATA_DIR, VLASS_4D_CUBE_HEADER)
    header = fits.Header.fromtextfile(header_filename)

    coords = [168.34719985367971, 76.18699791158396, 0.01]

    test_subject = Transform()
    pixels = test_subject.get_circle_cutout_pixels(header, 1, 2, coords)

    assert pixels is not None
    assert len(pixels) == 4
    assert pixels[0] == 2940
    assert pixels[1] == 3061
    assert pixels[2] == 4193
    assert pixels[3] == 4314


@pytest.mark.skip
def test_get_circle_cutout_pixels_cgps_galactic():
    header_filename = os.path.join(TESTDATA_DIR, CGPS_4D_CUBE_HEADER)
    header = fits.Header.fromtextfile(header_filename)

    coords = [25.0, 60.0, 0.5]

    test_subject = Transform()
    pixels = test_subject.get_circle_cutout_pixels(header, 1, 2, coords)

    # SODA returns [0][350:584,136:370]
    assert pixels is not None
    assert pixels[0] == 367
    assert pixels[1] == 568
    assert pixels[2] == 152
    assert pixels[3] == 353


@pytest.mark.skip
def test_get_circle_cutout_pixels_iris_no_overlap():
    header_filename = os.path.join(TESTDATA_DIR, IRIS_3D_CUBE_HEADER)
    header = fits.Header.fromtextfile(header_filename)

    coords = [20.0, 20.0, 0.1]

    test_subject = Transform()
    try:
        test_subject.get_circle_cutout_pixels(header, 1, 2, coords)
        assert False, 'Should raise NoContentError.'
    except NoContentError:
        assert True


@pytest.mark.skip
def test_get_circle_cutout_pixels_iris_all_overlap():
    header_filename = os.path.join(TESTDATA_DIR, IRIS_3D_CUBE_HEADER)
    header = fits.Header.fromtextfile(header_filename)

    coords = [140.0, 0.0, 10.0]

    test_subject = Transform()
    pixels = test_subject.get_circle_cutout_pixels(header, 1, 2, coords)

    # cutout pixels: -125:676, -143:659
    # cutout returning entire image returns empty list
    assert pixels is not None
    assert len(pixels) == 4
    assert pixels[0] == 1
    assert pixels[1] == 500
    assert pixels[2] == 1
    assert pixels[3] == 500


@pytest.mark.skip
def test_get_polygon_cutout_pixels_vlass():
    header_filename = os.path.join(TESTDATA_DIR, VLASS_4D_CUBE_HEADER)
    header = fits.Header.fromtextfile(header_filename)

    coords = [168.34, 76.18, 168.34, 76.19, 168.35, 76.19]

    test_subject = Transform()
    pixels = test_subject.get_polygon_cutout_pixels(header, 1, 2, coords)

    # SODA returns cutout=[0][2997:3011,4211:4272,*,*]
    assert pixels is not None
    assert len(pixels) == 4
    assert pixels[0] == 2996
    assert pixels[1] == 3012
    assert pixels[2] == 4211
    assert pixels[3] == 4272


@pytest.mark.skip
def test_get_energy_cutout_pixels_vlass():
    """
    BAND 0.04456576 0.11662493
    cutout=[1:2]
    """
    header_filename = os.path.join(TESTDATA_DIR, VLASS_4D_CUBE_HEADER)
    header = fits.Header.fromtextfile(header_filename)

    coords = [0.04456576, 0.11662493]

    test_subject = Transform()
    pixels = test_subject.get_energy_cutout_pixels(header, 3, coords)

    # SODA returns cutout=[0][*,*,1:2,*]
    # library returns (before clipping) [2.79849082, 0.79277332] pixels
    assert pixels is not None
    assert len(pixels) == 2
    assert pixels[0] == 1
    assert pixels[1] == 3


@pytest.mark.skip
def test_get_energy_cutout_pixels_cgps_raises_error():
    """
        CPGS cube lacks rest wavelength or frequency for wcslib
        to do the transform from VELO-LFR to WAVE-???, and the
        python wcslib wrapper will raise a ValueError.

    """
    header_filename = os.path.join(TESTDATA_DIR, CGPS_4D_CUBE_HEADER)
    header = fits.Header.fromtextfile(header_filename)

    coords = [211.0e-3, 211.05e-3]

    test_subject = Transform()
    try:
        test_subject.get_energy_cutout_pixels(header, 3, coords)
        assert False, 'Should raise ValueError.'
    except ValueError:
        assert True


# Skip test
# always returns the ref pixel, possibly the header is incomplete???
@pytest.mark.skip
def test_get_energy_cutout_pixels_jcmt():
    header_filename = os.path.join(TESTDATA_DIR, JCMT_3D_CUBE_HEADER)
    header = fits.Header.fromtextfile(header_filename)

    # cdelt3 from caom2: 3.05140426249E-5
    header.append(('CDELT3', 3.05140426249E-5))

    coords = [0.00091067,  0.00091012]

    test_subject = Transform()
    pixels = test_subject.get_energy_cutout_pixels(header, 3, coords)

    # caom2ops returns cutout=[0][*,*,290:6810]&cutout=[1][*,*,290:6810]
    assert pixels is not None
    assert len(pixels) == 2
    assert pixels[0] == 290
    assert pixels[1] == 6810


@pytest.mark.skip
def test_get_polarization_cutout_pixels_vlass():
    """
    Polarization states for header are I, Q, U, V (1, 2, 3, 4)
    """
    header_filename = os.path.join(TESTDATA_DIR, VLASS_4D_CUBE_HEADER)
    header = fits.Header.fromtextfile(header_filename)

    coords = ['I']

    test_subject = Transform()
    pixels = test_subject.get_polarization_cutout_pixels(header, 4, coords)

    # SODA returns [*,*,*,1:1]
    assert pixels is not None
    assert len(pixels) == 2
    assert pixels[0] == 1
    assert pixels[1] == 1

    coords = ['I', 'Q']

    pixels = test_subject.get_polarization_cutout_pixels(header, 4, coords)

    # should return [1:2]
    assert pixels is not None
    assert len(pixels) == 2
    assert pixels[0] == 1
    assert pixels[1] == 2

    coords = ['I', 'Q', 'U']

    pixels = test_subject.get_polarization_cutout_pixels(header, 4, coords)

    # should return [1:3]
    assert pixels is not None
    assert len(pixels) == 2
    assert pixels[0] == 1
    assert pixels[1] == 3

    coords = ['I', 'Q', 'U', 'V']

    pixels = test_subject.get_polarization_cutout_pixels(header, 4, coords)

    # should return [1:4]
    assert pixels is not None
    assert len(pixels) == 2
    assert pixels[0] == 1
    assert pixels[1] == 4

    coords = ['I', 'V']

    pixels = test_subject.get_polarization_cutout_pixels(header, 4, coords)

    # should return [1:4]
    assert pixels is not None
    assert len(pixels) == 2
    assert pixels[0] == 1
    assert pixels[1] == 4


@pytest.mark.skip
def test_get_polarization_cutout_pixels_cgps():
    """
    CTYPE4  = 'STOKES  '           / 4TH COORDINATE TYPE
    CRVAL4  =   1.000000000000E+00 / REF. COORD. 1-4=I,Q,U,V
    CRPIX4  =                 1.00 / REF. PIXEL
    CDELT4  =        1.0000000E+00 / DELTA COORD.
    CROTA4  =                 0.00 / ROTATION ANGLE (DEG)
    """
    header_filename = os.path.join(TESTDATA_DIR, CGPS_4D_CUBE_HEADER)
    header = fits.Header.fromtextfile(header_filename)

    coords = ['I']

    test_subject = Transform()
    pixels = test_subject.get_polarization_cutout_pixels(header, 4, coords)

    # SODA returns [*,*,*,1:1]
    assert pixels is not None
    assert len(pixels) == 2
    assert pixels[0] == 1
    assert pixels[1] == 1


@pytest.mark.skip
def test_world_to_pixels_vlass():
    """
    CIRCLE 168.34719985367971 76.18699791158396 0.01 BAND 0.04456576 0.11662493
    POL I cutout=[0][2938:3062,4191:4316,1:2,1:1]
    """
    header_filename = os.path.join(TESTDATA_DIR, VLASS_4D_CUBE_HEADER)
    header = fits.Header.fromtextfile(header_filename)

    query = 'circle=168.34719985367971+76.18699791158396+0.01&\
        BAND=0.04456576+0.11662493&POL=I'

    test_subject = Transform()
    pixel_cutout_hdu = test_subject.world_to_pixels(query, header)

    assert pixel_cutout_hdu is not None
    ranges = pixel_cutout_hdu.get_ranges()
    assert len(ranges) == 4
    assert ranges[0] == (2940, 3061)
    assert ranges[1] == (4193, 4314)
    assert ranges[2] == (1, 3)
    assert ranges[3] == (1, 1)
