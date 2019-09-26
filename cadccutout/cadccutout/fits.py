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

from __future__ import (absolute_import, division,
                        print_function, unicode_literals)

import logging

import fitsio
from fitsio.hdu import ImageHDU

from cadccutout.utils import get_header_value
from cadccutout.transform import Transform
from cadccutout.pixel_cutout_hdu import PixelCutoutHDU
from cadccutout.cutoutnd import CutoutND
from cadccutout.no_content_error import NoContentError


__all__ = ['cutout']

LOGGER = logging.getLogger(__name__)


def _recalculate_crpix(slc, crpix):
    # Calculate the new CRPIXn value
    start = slc.start
    step = slc.step
    if crpix is None:
        curr_crp = 0
    else:
        curr_crp = crpix

    if start:
        if start <= slc.stop:
            crp = (curr_crp - start) / step + 1.0
        else:
            crp = (start - curr_crp) / step + 1.0
    else:
        crp = None

    return crp


def _post_sanitize_header(cutout_result, header):
    '''
    Fix the WCS values in the header.
    '''
    cutout_shape = cutout_result.cutout_shape
    has_c_d = 'CD1_1' in header
    has_p_c = 'PC1_1' in header
    naxis = header['NAXIS']
    axis_count = naxis + 1
    for idx, cutout_region in enumerate(cutout_shape):
        crpix_key = 'CRPIX{}'.format(idx + 1)
        step = cutout_region.step

        if crpix_key in header:
            crpix = header[crpix_key]
            crp = _recalculate_crpix(cutout_region, crpix)

            if crp:
                header[crpix_key] = crp

            if step and not has_c_d:
                header['CDELT{}'.format(idx + 1)] *= step

            LOGGER.debug('Adjusted {} val from {} to {}'.format(
                crpix_key, crpix, crp))

    if has_p_c and naxis > 0:
        for i in range(1, axis_count):
            for j in range(1, axis_count):
                step = cutout_shape[j - 1].step
                p_c_key = 'PC{}_{}'.format(i, j)
                if step and p_c_key in header:
                    header[p_c_key] *= step
    elif has_c_d and naxis > 0:
        for i in range(1, axis_count):
            for j in range(1, axis_count):
                step = cutout_shape[j - 1].step
                c_d_key = 'CD{}_{}'.format(i, j)
                if step and c_d_key in header:
                    header[c_d_key] *= step


def _pixel_cutout_params(hdu, cutout_dimension):
    extension = cutout_dimension.get_extension()
    LOGGER.debug('Cutting out from extension {}'.format(extension))

    cutout_nd = CutoutND(hdu)
    return cutout_nd.get_parameters(cutout_dimension.get_ranges())


def _is_image(hdu):
    return isinstance(hdu, ImageHDU)


def _write_out(result_hdu_list, hdu, header, cutout_slice=None):
    if cutout_slice is not None:
        result_hdu_list.write(
            hdu[cutout_slice],
            extname=get_header_value(header, 'EXTNAME'),
            header=header)
    else:
        result_hdu_list.write(
            hdu.read(),
            extname=get_header_value(header, 'EXTNAME'),
            header=header)


def _require_primary_hdu(cutout_dimensions):
    # returns True if resulting cutout requires primary HDU from the
    # original hdu list. This is the case when cutouts are done in
    # different extensions of the file
    last_ext = -1
    for c_d in cutout_dimensions:
        LOGGER.debug('Last Extension is {} and comparing to {}'.format(
            last_ext, c_d.get_extension()))
        if last_ext != -1 and last_ext != c_d.get_extension():
            return True
        last_ext = c_d.get_extension()
    return False


def _write_pixel_cutout(primary_header, hdu_list, result_hdu_list,
                        cutout_dimensions):
    count = 0
    if 'NEXTEND' in primary_header:
        primary_header['NEXTEND'] = len(cutout_dimensions)

    if _require_primary_hdu(cutout_dimensions) \
            and primary_header['NAXIS'] == 0:
        # add the Primary HDU from the original HDU list
        LOGGER.debug('Setting primary HDU.')
        result_hdu_list.write(None, header=primary_header)

    lcd = len(cutout_dimensions)
    for idx, cutout_dimension in enumerate(cutout_dimensions):
        LOGGER.debug('Next cutout dimension is {}'.format(cutout_dimension))
        ext = cutout_dimension.get_extension()
        hdu = hdu_list[ext]

        if hdu.get_dims() == ():
            # We probably should raise a NoOverlapError, but an empty
            # header is what fcat produces now...
            LOGGER.warning('Extension {} was requested but has no\
 data to cutout from.'.format(ext))
        else:
            header = hdu.read_header()
            hdu.ignore_scaling = True
            # Entire extension was requested.
            if not cutout_dimension.get_ranges():
                LOGGER.debug('Appending entire extension {}'.format(ext))
                _write_out(result_hdu_list, hdu, header)
                count += 1
            else:
                cutout_params = _pixel_cutout_params(hdu, cutout_dimension)
                _post_sanitize_header(cutout_params, header)
                _write_out(result_hdu_list, hdu, header, cutout_params.cutout)
                count += 1
                LOGGER.debug('Successfully cutout from {}'.format(ext))

        LOGGER.debug(
            'Finished dimension {} of {}.'.format(idx + 1, lcd))

    return count


def _write_wcs_cutout(primary_header, hdu_list, result_hdu_list,
                      cutout_dimensions):
    count = 0
    # Write out the primary header, if needed.
    requires_primary_hdu = len(hdu_list) > 1 and primary_header['NAXIS'] == 0
    if requires_primary_hdu:
        # add the Primary HDU from the original HDU list
        LOGGER.debug('Setting primary HDU.')
        result_hdu_list.write(None, header=primary_header)

    for idx, hdu in enumerate(hdu_list):
        if idx == 0 and requires_primary_hdu:
            continue

        if hdu.get_dims() != ():
            LOGGER.debug('\nTrying extension {}...\n'.format(idx))
            transform = Transform()
            hdu.ignore_scaling = True
            header = hdu.read_header()
            LOGGER.debug('Transforming {}'.format(cutout_dimensions))
            try:
                transformed_cutout_dimension = \
                    transform.world_to_pixels(cutout_dimensions,
                                              header)
                LOGGER.debug('Transformed {} into {} pixels.'.format(
                    cutout_dimensions, transformed_cutout_dimension))

                cutout_params = _pixel_cutout_params(
                    hdu, transformed_cutout_dimension)
                _post_sanitize_header(cutout_params, header)
                _write_out(result_hdu_list, hdu, header, cutout_params.cutout)
                count += 1
            except NoContentError:
                LOGGER.debug(
                    'Skipping non-overlapping cutout {}'.format(
                        cutout_dimensions))
        else:
            LOGGER.debug('Skipping extension {}'.format(idx))

    return count


def _check_hdu_list(cutout_dimensions, hdu_list, output_writer):
    len_cutout_dimensions = len(cutout_dimensions)
    nextend = 0
    if len_cutout_dimensions > 0:
        result_hdu_list = fitsio.FITS(output_writer, 'rw')
        primary_hdu = hdu_list[0]
        primary_header = primary_hdu.read_header()

        # Check for a pixel cutout
        if isinstance(cutout_dimensions[0], PixelCutoutHDU):
            nextend += _write_pixel_cutout(primary_header, hdu_list,
                                           result_hdu_list, cutout_dimensions)
        else:
            nextend += _write_wcs_cutout(primary_header, hdu_list,
                                         result_hdu_list, cutout_dimensions)
    else:
        raise NoContentError('No overlap found (No cutout specified).')

    return nextend > 0


def cutout(cutout_dimensions, input_stream, output_writer):
    '''
    Perform a cutout and write out the results.
    '''

    # Start with the first extension
    source_hdu_list = fitsio.FITS(input_stream, 'r')

    LOGGER.debug('Calling check_hdu_list().')

    # Keep a tally of whether at least one HDU matched.
    matches = _check_hdu_list(cutout_dimensions, source_hdu_list,
                              output_writer)

    LOGGER.debug('Has match in list? -- {}'.format(matches))

    if not matches:
        raise NoContentError('No overlap found.')
    else:
        logging.debug('Successful cutout.')
