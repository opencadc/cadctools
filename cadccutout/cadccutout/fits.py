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


def _post_sanitize_header(cutout_result):
    '''
    Fix the CRPIX offset
    '''
    header = cutout_result.header

    if cutout_result.wcs is not None:
        wcs = cutout_result.wcs.wcs
        if wcs.crpix is not None:
            cutout_crpix = wcs.crpix
            for idx, val in enumerate(cutout_crpix):
                header_key = 'CRPIX{}'.format(idx + 1)
                if header_key in header:
                    curr_header_val = header[header_key]
                    LOGGER.debug(
                        'Adjusting {} from {} to {}'.format(
                            header_key, curr_header_val, val))
                    header[header_key] = val

        if wcs.has_pc():
            LOGGER.debug('Handling PC values.')
            p_c = wcs.pc
            for i in range(wcs.naxis):
                for j in range(wcs.naxis):
                    p_c_key = 'PC{}_{}'.format(i + 1, j + 1)
                    if p_c_key in header:
                        header[p_c_key] = p_c[i][j]
        elif wcs.has_cd():
            LOGGER.debug('Handling CD values.')
            c_d = wcs.cd
            for i in range(wcs.naxis):
                for j in range(wcs.naxis):
                    c_d_key = 'CD{}_{}'.format(i + 1, j + 1)
                    if c_d_key in header:
                        header[c_d_key] = c_d[i][j]


def _pixel_cutout_params(hdu, cutout_dimension):
    extension = cutout_dimension.get_extension()
    LOGGER.debug('Cutting out from extension {}'.format(extension))

    cutout_nd = CutoutND(hdu)
    return cutout_nd.get_parameters(cutout_dimension.get_ranges())


def _is_image(hdu):
    return isinstance(hdu, ImageHDU)


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


def _check_hdu_list(cutout_dimensions, hdu_list, output_writer):
    len_cutout_dimensions = len(cutout_dimensions)
    nextend = 0
    if len_cutout_dimensions > 0:
        result_hdu_list = fitsio.FITS(output_writer, 'rw')
        primary_hdu = hdu_list[0]
        primary_header = primary_hdu.read_header()

        # Check for a pixel cutout
        if isinstance(cutout_dimensions[0], PixelCutoutHDU):
            if 'NEXTEND' in primary_header:
                primary_header['NEXTEND'] = len_cutout_dimensions

            if _require_primary_hdu(cutout_dimensions) \
                    and primary_header['NAXIS'] == 0:
                # add the Primary HDU from the original HDU list
                LOGGER.debug('Setting primary HDU.')

                result_hdu_list.write(None, header=primary_header)

            lcd = len(cutout_dimensions)
            for idx, cutout_dimension in enumerate(cutout_dimensions):
                LOGGER.debug(
                    'Next cutout dimension is {}'.format(cutout_dimension))
                ext = cutout_dimension.get_extension()
                hdu = hdu_list[ext]

                if hdu.get_dims() == ():
                    # We probably should raise a NoOverlapError, but an empty
                    # header is what fcat produces now...
                    LOGGER.warn('Extension {} was requested but has no\
 data to cutout from.'.format(ext))
                else:
                    header = hdu.read_header()
                    hdu.ignore_scaling = True
                    # Entire extension was requested.
                    if not cutout_dimension.get_ranges():
                        LOGGER.debug(
                            'Appending entire extension {}'.format(ext))
                        result_hdu_list.write(
                            hdu.read(),
                            extname=get_header_value(header, 'EXTNAME'),
                            header=header)
                        nextend += 1
                    else:
                        cutout_params = _pixel_cutout_params(hdu,
                                                             cutout_dimension)
                        _post_sanitize_header(cutout_params)
                        header = cutout_params.header
                        cut = hdu[cutout_params.cutout]
                        LOGGER.debug('Cut {} from image.'.format(cut.shape))
                        result_hdu_list.write(cut, extname=get_header_value(
                            header, 'EXTNAME'), header=header)
                        LOGGER.debug('Done writing results.')
                        nextend += 1
                        LOGGER.debug('Successfully cutout from {}'.format(ext))

                LOGGER.debug(
                    'Finished dimension {} of {}.'.format(idx + 1, lcd))
        else:
            # Write out the primary header.
            result_hdu_list.write(None, header=primary_header)

            for ext_idx, hdu in enumerate(hdu_list):
                if _is_image(hdu) and hdu.read is not None:
                    transform = Transform()
                    header = hdu.read_header()
                    LOGGER.debug(
                        'Next HDU to check {} from {}'.format(hdu, ext_idx))
                    LOGGER.debug('Transforming {}'.format(cutout_dimensions))
                    try:
                        transformed_cutout_dimension = \
                            transform.world_to_pixels(cutout_dimensions,
                                                      header)
                        LOGGER.debug('Transformed {} into {}'.format(
                            cutout_dimensions, transformed_cutout_dimension))

                        cutout_params = _pixel_cutout_params(
                            hdu, transformed_cutout_dimension)
                        result_hdu_list.write(hdu[cutout_params.cutout],
                                              header=cutout_params.header)
                        nextend += 1
                    except NoContentError:
                        LOGGER.debug(
                            'Skipping non-overlapping cutout {}'.format(
                                cutout_dimensions))
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
