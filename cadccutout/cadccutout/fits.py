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

import fitsio
from fitsio.hdu import ImageHDU

from cadccutout.utils import to_astropy_header
from cadccutout.transform import Transform
from cadccutout.pixel_cutout_hdu import PixelCutoutHDU
from cadccutout.cutoutnd import CutoutND
from cadccutout.no_content_error import NoContentError

from astropy.wcs import WCS
from astropy.io import fits
from astropy.io.fits import PrimaryHDU


import logging


__all__ = ['cutout']

logger = logging.getLogger(__name__)


def _post_sanitize_header(header, cutout_result):
    """
    Fix the CRPIX offset
    """
    if cutout_result.wcs is not None:
        wcs = cutout_result.wcs.wcs
        if wcs.crpix is not None:
            cutout_crpix = wcs.crpix

            for idx, val in enumerate(cutout_crpix):
                header_key = 'CRPIX{}'.format(idx + 1)

                if val != 1:
                    new_val = val + 1
                else:
                    new_val = val

                logger.debug(
                    'Adjusting {} from {} to {}'.format(
                        header_key, val, new_val))

                header['CRPIX{}'.format(idx + 1)] = new_val

        if wcs.has_pc():
            logger.debug('Handling PC values.')
            pc = wcs.pc
            for i in range(wcs.naxis):
                for j in range(wcs.naxis):
                    header['PC{}_{}'.format(i + 1, j + 1)] = pc[i][j]
        elif wcs.has_cd():
            logger.debug('Handling CD values.')
            cd = wcs.cd
            for i in range(wcs.naxis):
                for j in range(wcs.naxis):
                    header['CD{}_{}'.format(i + 1, j + 1)] = cd[i][j]


def _get_wcs(header):
    naxis_value = header['NAXIS']

    if naxis_value is not None and int(naxis_value) > 0:
        for nax in range(1, naxis_value + 1):
            next_ctype_key = 'CTYPE{0}'.format(nax)
            if next_ctype_key in header:
                ctype = header['CTYPE{0}'.format(nax)]
                if ctype is not None and ctype.endswith('-SIP'):
                    naxis = 2
                    break
                else:
                    naxis = None
            else:
                naxis = None
    else:
        naxis = naxis_value

    return WCS(header=to_astropy_header(header), naxis=naxis, fix=False)


def _pixel_cutout(hdu, cutout_dimension, header):
    extension = cutout_dimension.get_extension()
    logger.debug('Cutting out from extension {}'.format(extension))

    c = CutoutND(hdu, wcs=_get_wcs(header))
    return c.extract(cutout_dimension.get_ranges())


def _is_image(hdu):
    return isinstance(hdu, ImageHDU)


def _require_primary_hdu(cutout_dimensions):
    # returns True if resulting cutout requires primary HDU from the
    # original hdu list. This is the case when cutouts are done in
    # different extensions of the file
    last_ext = -1
    for c in cutout_dimensions:
        logger.debug('Last Extension is {} and comparing to {}'.format(
            last_ext, c.get_extension()))
        if last_ext != -1 and last_ext != c.get_extension():
            return True
        last_ext = c.get_extension()
    return False


def _check_hdu_list(cutout_dimensions, hdu_list, output_writer):
    has_match = False
    len_cutout_dimensions = len(cutout_dimensions)
    if len_cutout_dimensions > 0:
        result_hdu_list = fitsio.FITS(output_writer, 'rw')
        primary_hdu = hdu_list[0]
        primary_header = primary_hdu.read_header()

        # Check for a pixel cutout
        if isinstance(cutout_dimensions[0], PixelCutoutHDU):
            if _require_primary_hdu(cutout_dimensions) and primary_header['NAXIS'] == 0:
                # add the PrimaryHDU from the original HDU list
                logger.debug('Setting primary HDU.')
                result_hdu_list.write(None, header=primary_header)

            lcd = len(cutout_dimensions)
            for idx, cutout_dimension in enumerate(cutout_dimensions):
                logger.debug(
                    'Next cutout dimension is {}'.format(cutout_dimension))
                ext = cutout_dimension.get_extension()
                hdu = hdu_list[ext]

                # Entire extension was requested.
                if not cutout_dimension.get_ranges():
                    logger.debug('Appending entire extension {}'.format(ext))
                    result_hdu_list.write(hdu.read(), header=hdu.read_header())
                    has_match = True
                else:
                    try:
                        header = hdu.read_header()
                        cutout_result = _pixel_cutout(hdu, cutout_dimension, header)
                        logger.debug('Sanitizing header.')
                        _post_sanitize_header(header, cutout_result)
                        logger.debug('Writing results for {}'.format(cutout_result.data.shape))
                        result_hdu_list.write(cutout_result.data, header=header)
                        logger.debug('Done writing results.')
                        has_match = True
                        logger.debug('Successfully cutout from {}'.format(ext))
                    except NoContentError:
                        logger.debug(
                            'Skipping non-overlapping cutout {}'.format(cutout_dimension))

                logger.debug('Finished dimension {} of {}.'.format(idx + 1, lcd))
        else:
            # Write out the primary header.
            result_hdu_list.write(None, header=primary_header)

            for ext_idx, hdu in enumerate(hdu_list):
                if _is_image(hdu) and hdu.read is not None:
                    transform = Transform()
                    header = hdu.read_header()
                    logger.debug('Next HDU to check {} from {}'.format(hdu, ext_idx))
                    logger.debug('Transforming {}'.format(cutout_dimensions))
                    try:
                        transformed_cutout_dimension = \
                            transform.world_to_pixels(cutout_dimensions, header)
                        logger.debug('Transformed {} into {}'.format(
                            cutout_dimensions, transformed_cutout_dimension))

                        if _require_primary_hdu([transformed_cutout_dimension]) and header['NAXIS'] == 0:
                            result_hdu_list.write(None, header=header)

                        cutout_result = _pixel_cutout(hdu, transformed_cutout_dimension, header)
                        result_hdu_list.write(cutout_result.data, header=header)

                        has_match = True
                    except NoContentError:
                        logger.debug(
                            'Skipping non-overlapping cutout {}'.format(cutout_dimensions))
    else:
        raise NoContentError('No overlap found (No cutout specified).')

    return has_match


def cutout(cutout_dimensions, input_stream, output_writer):
    """
    Perform a cutout and write out the results.
    """

    # Start with the first extension
    source_hdu_list = fitsio.FITS(input_stream, 'r')

    logger.debug('Calling check_hdu_list().')

    # Keep a tally of whether at least one HDU matched.
    matches = _check_hdu_list(cutout_dimensions, source_hdu_list, output_writer)

    logger.debug('Has match in list? -- {}'.format(matches))

    if not matches:
        raise NoContentError('No overlap found.')
    else:
        logging.debug('Successful cutout.')
