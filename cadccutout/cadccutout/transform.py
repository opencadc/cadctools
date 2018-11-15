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
import math
import sys

from astropy import units as u
from astropy.coordinates import SkyCoord, Longitude, Latitude, ICRS
from astropy.wcs import WCS
from regions.shapes.circle import CircleSkyRegion
from regions.shapes.polygon import PolygonSkyRegion

from .no_content_error import NoContentError
from .pixel_cutout_hdu import PixelCutoutHDU
from .shape import Circle, Polygon, Energy, Time, Polarization, \
                   PolarizationState

logger = logging.getLogger(__name__)

__all__ = ['AxisType', 'Transform']

_DEFAULT_ENERGY_CTYPE = 'WAVE-???'
_DEFAULT_ENERGY_CUNIT = 'm'


class AxisType(object):
    """
    Extracts the axis number for each coordinate type.
    """

    COORDINATE_TYPE = 'coordinate_type'
    SPATIAL_KEYWORDS = ['celestial']
    SPECTRAL_KEYWORDS = ['spectral']
    TEMPORAL_KEYWORDS = []
    POLARIZATION_KEYWORDS = ['stokes']

    def __init__(self, header):
        self.spatial_1 = None
        self.spatial_2 = None
        self.spectral = None
        self.temporal = None
        self.polarization = None

        # wcs from header
        wcs = WCS(header)

        # get list of dict of axis types
        axis_types = wcs.get_axis_types()

        # for each coordinate dict extract type and axis number
        for i in range(len(axis_types)):
            axis_type = axis_types[i]
            coordinate_type = axis_type.get(self.COORDINATE_TYPE)
            if coordinate_type in self.SPATIAL_KEYWORDS:
                if not self.spatial_1:
                    self.spatial_1 = i + 1
                else:
                    self.spatial_2 = i + 1
                    logger.info('Spatial naxis{} naxis{}'.format(
                        self.spatial_1, self.spatial_2))
            elif coordinate_type in self.SPECTRAL_KEYWORDS:
                self.spectral = i + 1
                logger.info('Spectral naxis{}'.format(self.get_spectral_axis()))
            elif coordinate_type in self.TEMPORAL_KEYWORDS:
                self.temporal = i + 1
                logger.info('Temporal naxis{}'.format(self.get_temporal_axis()))
            elif coordinate_type in self.POLARIZATION_KEYWORDS:
                self.polarization = i + 1
                logger.info('Polarization naxis{}'.format(
                    self.get_polarization_axis()))
            else:
                raise ValueError('Unknown axis keyword {}'.format(
                    coordinate_type))

    def get_spatial_axes(self):
        """
        Get a list of the spatial axis numbers.
        :return: List[int], the two spatial axis numbers,
                            or None if no spatial axes found.
        """
        return [self.spatial_1, self.spatial_2]

    def get_spectral_axis(self):
        """
        Get the spectral axis number.
        :return: int, the spectral axis number,
                      or None if no spectral axis found.
        """
        return self.spectral

    def get_temporal_axis(self):
        """
        Get the temporal axis number.
        :return: int, the temporal axis number,
                      or None if no temporal axis found.
        """
        return self.temporal

    def get_polarization_axis(self):
        """
        Get the polarization axis number.
        :return: int, the polarization axis number,
                      or None if no polarization axis found.
        """
        return self.polarization


class Transform(object):

    def world_to_pixels(self, world_coords, header):
        """
        Convert a list of world coordinates to pixel coordinates
        for the given FITS header.

        :param world_coords: List List of world coordinate strings
        :param header: Header   The FITS header
        :return: PixelCutoutHDU containing the pixel coordinates
        """
        logger.info('Shapes: {}'.format(world_coords))

        # coordinate axis numbers
        axis_types = AxisType(header)

        # get the Shape object for the world coordinates
        shapes = self.world_to_shape(world_coords)

        # list of NAXIS length, with 1 to NAXIS[1|2|3|4] pixel coordinates,
        # add a dummy object to the start of the list so the axes
        # align with the list index, remove it at the end.
        cutouts = [(0, 0)]
        axes = header.get('NAXIS')
        for i in range(1, axes + 1):
            length = header.get('NAXIS{}'.format(i))
            cutouts.append((1, length))

        # accumulated NoContentErrors
        no_content_errors = []

        # get the pixel coordinates for each type of shape
        # do try except for each call to catch NoContentError for no overlap
        # raise no content error if an axis has no overlap
        for shape in shapes:
            if isinstance(shape, Circle):
                try:
                    # length of the two axes
                    naxis1 = axis_types.get_spatial_axes()[0]
                    naxis2 = axis_types.get_spatial_axes()[1]

                    # get the cutout pixels
                    pixels = self.get_circle_cutout_pixels(
                        shape, header, naxis1, naxis2)
                    logger.info('CIRCLE pixels [{}:{}, {}:{}]'.format(*pixels))

                    # remove default cutouts and add query cutout
                    cutouts.pop(naxis1)
                    cutouts.insert(naxis1, (pixels[0], pixels[1]))
                    cutouts.pop(naxis2)
                    cutouts.insert(naxis2, (pixels[2], pixels[3]))
                except NoContentError as e:
                    no_content_errors.append(repr(e))
            elif isinstance(shape, Polygon):
                try:
                    # length of the two axes
                    naxis1 = axis_types.get_spatial_axes()[0]
                    naxis2 = axis_types.get_spatial_axes()[1]

                    # get the cutout pixels
                    pixels = self.get_polygon_cutout_pixels(
                        shape, header, naxis1, naxis2)
                    logger.info('POLYGON pixels [{}:{}, {}:{}]'.format(*pixels))

                    # remove default cutouts and add query cutout
                    cutouts.pop(naxis1)
                    cutouts.insert(naxis1, (pixels[0], pixels[1]))
                    cutouts.pop(naxis2)
                    cutouts.insert(naxis2, (pixels[2], pixels[3]))
                except NoContentError as e:
                    no_content_errors.append(repr(e))
            elif isinstance(shape, Energy):
                try:
                    # length of the spectral axis
                    naxis = axis_types.get_spectral_axis()

                    # get the cutout pixels
                    pixels = self.get_energy_cutout_pixels(shape, header, naxis)
                    logger.info('BAND pixels [{}:{}]'.format(*pixels))

                    # remove default cutouts and add query cutout
                    cutouts.pop(naxis)
                    cutouts.insert(naxis, (pixels[0], pixels[1]))
                except NoContentError as e:
                    no_content_errors.append(repr(e))
            elif isinstance(shape, Time):
                try:
                    # length of the temporal axis
                    naxis = axis_types.get_temporal_axis()

                    # get the cutout pixels
                    pixels = self.get_time_cutout_pixels(shape, header, naxis)
                    logger.info('TIME pixels [{}:{}]'.format(*pixels))

                    # remove default cutouts and add query cutout
                    cutouts.pop(naxis)
                    cutouts.insert(naxis, (pixels[0], pixels[1]))
                except NoContentError as e:
                    no_content_errors.append(repr(e))
            elif isinstance(shape, Polarization):
                try:
                    # length of the polarization axis
                    naxis = axis_types.get_polarization_axis()

                    # get the cutout pixels
                    pixels = self.get_polarization_cutout_pixels(
                        shape, header, naxis)
                    logger.info('POL pixels [{}:{}]'.format(*pixels))

                    # remove default cutouts and add query cutout
                    cutouts.pop(naxis)
                    cutouts.insert(naxis, (pixels[0], pixels[1]))
                except NoContentError as e:
                    no_content_errors.append(repr(e))

        # check for no content errors
        if no_content_errors:
            raise NoContentError('\n'.join(no_content_errors))

        # remove the dummy first list item
        cutouts.pop(0)

        return PixelCutoutHDU(cutouts)

    def world_to_shape(self, world_coords):
        """
        Parse the world coordinates into a list of Shapes.

        :param world_coords: List List of world coordinate strings.
        :return: List A list of Shapes.
        """

        # list of Shape
        shapes = []

        # track names of shapes to check for duplicates
        shape_types = []
        for world in world_coords:

            # get the Shape for this world coordinate
            shape = self.parse_world(world)

            # check for duplicate shapes, one shape per axis allowed
            if shape.AXIS_TYPE in shape_types:
                raise ValueError('One cutout per axis supported, '
                                 'found duplicate world axis for {}'
                                 .format(shape.NAME))

            shapes.append(shape)
            shape_types.append(shape.AXIS_TYPE)

        return shapes

    def parse_world(self, world):
        """

            :param world: str   World coordinate string.
            :returns:
        """

        if not world.strip():
            raise ValueError('Empty or None world coordinate')

        parameters = world.upper().split()
        name = parameters[0].strip()
        values = list(filter(None, parameters[1:]))
        values = ' '.join(values)

        if name == Circle.NAME:
            return Circle.parse_circle(values)
        elif name == Polygon.NAME:
            return Polygon.parse_polygon(values)
        elif name == Energy.NAME:
            return Energy.parse_energy(values)
        elif name == Time.NAME:
            return Time.parse_time(values)
        elif name == Polarization.NAME:
            return Polarization.parse_polarization(values)
        else:
            raise ValueError('Unknown world coordinate {}'.format(world))

    def get_circle_cutout_pixels(self, circle, header, naxis1, naxis2):
        """
        Get the pixels coordinates for a Circle query
        using the given FITS header.

        :param circle: Circle Circle instance
        :param header: Header   FITS extension header
        :param naxis1: int  First spatial axis
        :param naxis2: int  Second spatial axis
        :return: List[int] The x and y pairs of the pixel coordinates
        """

        # sky coordinates from the Circle
        ra = Longitude(circle.ra, unit=u.deg)
        dec = Latitude(circle.dec, unit=u.deg)
        radius = u.Quantity(circle.radius, unit=u.deg)
        sky_coords = SkyCoord(ra, dec, frame=ICRS)

        # WCS from the header, extract only the spatial axes wcs, the sky to pix
        # transform will want to convert each axis in the wcs, and we only have
        # spatial data.
        wcs = WCS(header, naxis=[naxis1, naxis2])

        # Circle region with radius
        sky_region = CircleSkyRegion(sky_coords, radius=radius)

        # convert to pixel coordinates
        pixels = sky_region.to_pixel(wcs)
        try:
            x_min = pixels.bounding_box.ixmin
            x_max = pixels.bounding_box.ixmax
            y_min = pixels.bounding_box.iymin
            y_max = pixels.bounding_box.iymax
            logger.info('Circle bounding box [{}, {}, {}, {}]'.format(
                x_min, x_max, y_min, y_max))
        except ValueError as e:
            # bounding_box raises ValueError if the cutout
            # doesn't intersect the image
            raise NoContentError(repr(e))

        # do clip check
        x_axis = header.get('NAXIS{}'.format(naxis1))
        y_axis = header.get('NAXIS{}'.format(naxis2))
        return self.do_position_clip_check(
            x_axis, y_axis, x_min, x_max, y_min, y_max)

    def get_polygon_cutout_pixels(self, polygon, header, naxis1, naxis2):
        """
        Get the pixels coordinates for a Circle query
        using the given FITS header.

        :param polygon: Polygon  Polygon instance
        :param header: Header   FITS extension header
        :param naxis1: int  First spatial axis
        :param naxis2: int  Second spatial axis
        :return: List[int] The x and y pairs of the cutout pixel coordinates
        """

        # query sky position
        even_vertices = []
        odd_vertices = []
        for vertice in polygon.vertices:
            even_vertices.append(vertice[0])
            odd_vertices.append(vertice[1])
        sky_coords = SkyCoord(even_vertices, odd_vertices,
                              unit=u.deg, frame=ICRS)

        # WCS from the header, extract only the spatial axes,
        # because the sky to pix transform will want to convert
        # every axis in the wcs, and we only have spatial data to convert.
        wcs = WCS(header, naxis=[naxis1, naxis2])

        # Polygon region
        sky_region = PolygonSkyRegion(sky_coords)

        # convert to pixel coordinates
        pixels = sky_region.to_pixel(wcs)
        try:
            x_min = pixels.bounding_box.ixmin
            x_max = pixels.bounding_box.ixmax
            y_min = pixels.bounding_box.iymin
            y_max = pixels.bounding_box.iymax
            logger.info('Polygon bounding box [{}, {}, {}, {}]'.format(
                x_min, x_max, y_min, y_max))
        except ValueError as e:
            # bounding_box raises ValueError if the cutout
            # doesn't intersect the image
            raise NoContentError(repr(e))

        # do clip check
        x_axis = header.get('NAXIS{}'.format(naxis1))
        y_axis = header.get('NAXIS{}'.format(naxis2))
        return self.do_position_clip_check(x_axis, y_axis,
                                           x_min, x_max, y_min, y_max)

    def get_energy_cutout_pixels(self, energy, header, naxis):
        """
        Get the pixels coordinates for a spectral query
        using the given FITS header.

        :param energy: Energy   Energy instance
        :param header: Header   FITS extension header
        :param naxis: int   Spectral axis number
        :return: List[int] The two cutout pixel coordinates
        """

        # WCS from the header
        wcs = WCS(header)

        # if the spectral wcs isn't a wavelength, transform the
        # spectral axis to wavelength.
        ctype = header.get('CTYPE{}'.format(naxis))
        if ctype and not ctype.startswith('WAVE'):
            try:
                wcs.wcs.sptr(_DEFAULT_ENERGY_CTYPE)
            except ValueError as e:
                error = 'wcslib error transforming from {} to {}: {}'\
                    .format(ctype, _DEFAULT_ENERGY_CTYPE, repr(e))
                raise ValueError(error)

        # slice out the spectral wcs
        energy_wcs = wcs.sub([naxis])

        # convert from world to pixel coordinates
        pixels = energy_wcs.wcs_world2pix([energy.lower, energy.upper], 1)

        # clip check
        length = header.get('NAXIS{}'.format(naxis))
        lower = min(pixels[0][0], pixels[0][1])
        upper = max(pixels[0][0], pixels[0][1])
        logger.info('Energy interval: {}:{}'.format(lower, upper))

        return self.do_energy_clip_check(length, lower, upper)

    def get_time_cutout_pixels(self, time, header, naxis):
        """
        Get the pixels coordinates for a temporal query
        using the given FITS header.

        :param time: Time  Time instance
        :param header: Header   FITS extension header
        :param naxis: int   Temporal axis number
        :return: List[int] The two cutout pixel coordinates
        """

        raise ValueError('Temporal cutouts not supported')

    def get_polarization_cutout_pixels(self, polarization, header, naxis):
        """
        Get the pixels coordinates for a polarization query
        using the given FITS header.

        :param polarization: Polarization Polarization instance
        :param header: Header   FITS extension header
        :param naxis: int   Polarization axis number
        :return: List[int] The two cutout pixel coordinates
        """

        crval = header.get('CRVAL{}'.format(naxis))
        crpix = header.get('CRPIX{}'.format(naxis))
        cdelt = header.get('CDELT{}'.format(naxis))

        if not crval and not crpix and not cdelt:
            raise ValueError('Unable to do a polarization cutout, '
                             'incomplete WCS, missing one of {} {} {}'
                             .format('CRVAL{}'.format(naxis), 'CRPIX{}'
                                     .format(naxis), 'CDELT{}'.format(naxis)))

        # polarization states in the file
        wcs_states = self.get_wcs_polarization_states(header, naxis)
        if not wcs_states:
            raise NoContentError('Polarization states not found '
                                 'in the FITS header')

        # list of states in both the cutout query and header
        cutout_states = []
        for state in polarization.states:
            if state in wcs_states:
                cutout_states.append(state)

        # no overlap, raise NoContentError
        if not cutout_states:
            raise NoContentError('No polarization states in the query {} '
                                 'match the states in the FITS header {}'
                                 .format(str(polarization.states),
                                         str(wcs_states)))

        # calculate pixels for the states to cutout
        pix1 = sys.maxsize
        pix2 = -sys.maxsize - 1
        for state in cutout_states:
            value = PolarizationState(state).value
            pix = crpix + (value - crval) / cdelt
            pix1 = min(pix1, pix)
            pix2 = max(pix2, pix)
        logger.info('Polarization states: {}:{}'.format(pix1, pix2))

        # do clip check
        pixels = self.do_polarization_clip_check(naxis, pix1, pix2)
        return pixels

    def do_position_clip_check(self, w, h, x1, x2, y1, y2):
        """
        Trim the cutout coordinates to fit within the image bounds.

        :param w: int   Width of the image
        :param h: int   Height of the image
        :param x1: int  Lower x cutout coordinate
        :param x2: int  Upper x cutout coordinate
        :param y1: int  Lower y cutout coordinate
        :param y2: int  Upper y cutout coordinate
        :return: List[int] The coordinates pixels within the images bounds
        """

        # bounds check
        if x1 < 1:
            x1 = 1
        if x2 > w:
            x2 = w
        if y1 < 1:
            y1 = 1
        if y2 > h:
            y2 = h

        # cutout pixels
        logger.info('Position clip check: [{}, {}, {}, {}]'.format(
            x1, x2, y1, y2))
        return [x1, x2, y1, y2]

    def do_energy_clip_check(self, naxis, lower, upper):
        """
        Trim the cutout coordinates to fit within the spectral axis.

        :param naxis: int   Length of the spectral axis
        :param lower: float Lower cutout bounds
        :param upper: float Upper cutout bounds
        :return: List[int]  The coordinate pixels within the spectral axis.
        """

        # round floats to individual pixels
        z1 = math.floor(lower)
        z2 = math.ceil(upper)

        # bounds check
        if z1 < 1:
            z1 = 1
        if z2 > naxis:
            z2 = naxis

        # validity check, no pixels included
        if z1 >= naxis or z2 <= 1:
            error = 'pixels coordinates {}:{} do not intersect {} to {}'\
                .format(z1, z2, 1, naxis)
            raise NoContentError(error)

        # an actual cutout
        logger.info('Energy clip check: [{}:{}]'.format(z1, z2))
        return [z1, z2]

    def do_time_clip_check(self, naxis, lower, upper):
        """
        Trim the cutout coordinates to fit within the temporal axis.

        :param naxis: int   Length of the temporal axis
        :param lower: float Lower cutout bounds
        :param upper: float Upper cutout bounds
        :return: List[int]  The coordinate pixels within the temporal axis.
        """

        return []

    def do_polarization_clip_check(self, naxis, lower, upper):
        """
        Trim the cutout coordinates to fit within the polarization axis.

        :param naxis: int   Length of the polarization axis
        :param lower: float Lower cutout bounds
        :param upper: float Upper cutout bounds
        :return: List[int]  The coordinate pixels within the polarization axis.
        """

        # round floats to individual pixels
        p1 = math.floor(lower)
        p2 = math.ceil(upper)

        # bounds check
        if p1 < 1:
            p1 = 1
        if p2 > naxis:
            p2 = naxis

        # validity check, no pixels included
        if p1 > naxis or p2 < 1:
            error = 'pixels coordinates {}:{} do not intersect {} to {}'\
                .format(p1, p2, 1, naxis)
            raise NoContentError(error)

        # an actual cutout
        return [p1, p2]

    def get_wcs_polarization_states(self, header, naxis):
        """
        Get the polarization states from the FITS header.

        :param header: Header   The FITS header
        :param naxis: int   The polarization axis number
        :return: List[PolarizationState]    List of PolarizationState
                                            in the header
        """

        crval = header.get('CRVAL{}'.format(naxis))
        crpix = header.get('CRPIX{}'.format(naxis))
        cdelt = header.get('CDELT{}'.format(naxis))

        polarization_states = []
        if crval and crpix and cdelt:
            for i in range(1, naxis + 1):
                pix = crpix + (i - crval) / cdelt
                polarization_states.append(PolarizationState(pix))

        return polarization_states
