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

from cadccutout.transform import Transform
from cadccutout.pixel_cutout_hdu import PixelCutoutHDU
from cadccutout.no_content_error import NoContentError
from cadccutout.file_helpers.base_file_helper import BaseFileHelper
from astropy.wcs import WCS
from astropy.io import fits
from astropy.io.fits import PrimaryHDU


import logging


__all__ = ['FITSHelper']

logger = logging.getLogger(__name__)


class FITSHelper(BaseFileHelper):

    def __init__(self, input_stream, output_writer):
        """
        Create a new BaseFileHelper used for different file types.  Concrete
         instances are expected to extend this
        class to provide some common state.

        :param input_stream:    The Reader to read the file data from.
        :param output_writer:   The Writer to write the cutout to.
        """
        super(FITSHelper, self).__init__(input_stream, output_writer)

    def _post_sanitize_header(self, header, cutout_result):
        """
        Fix the CRPIX offset
        """
        if cutout_result.wcs is not None:
            wcs = cutout_result.wcs.wcs
            if wcs.crpix is not None:
                cutout_crpix = wcs.crpix

                for idx, val in enumerate(cutout_crpix):
                    header_key = 'CRPIX{}'.format(idx + 1)
                    curr_val = header.get(header_key)
                    logger.debug(
                        'Adjusting {} from {} to {}'.format(
                            header_key, curr_val, val))
                    header.set('CRPIX{}'.format(idx + 1), val)

            if wcs.has_pc():
                pc = wcs.pc
                for i in range(wcs.naxis):
                    for j in range(wcs.naxis):
                        header.set('PC{}_{}'.format(i + 1, j + 1), pc[i][j])
            elif wcs.has_cd():
                cd = wcs.cd
                for i in range(wcs.naxis):
                    for j in range(wcs.naxis):
                        header.set('CD{}_{}'.format(i + 1, j + 1), cd[i][j])

    def _get_wcs(self, header):
        naxis_value = header.get('NAXIS')

        if naxis_value is not None and int(naxis_value) > 0:
            for nax in range(1, naxis_value + 1):
                ctype = header.get('CTYPE{0} '.format(nax))
                if ctype is not None and ctype.endswith('-SIP'):
                    naxis = 2
                    break
                else:
                    naxis = None
        else:
            naxis = naxis_value

        return WCS(header=header, naxis=naxis, fix=False)

    def _pixel_cutout(self, hdu, cutout_dimension):
        extension = cutout_dimension.get_extension()
        header = hdu.header
        wcs = self._get_wcs(header)

        logger.debug('Cutting out from extension {}'.format(extension))

        cutout_result = self.do_cutout(
            data=hdu.data, cutout_dimension=cutout_dimension, wcs=wcs)

        self._post_sanitize_header(header, cutout_result)
        result = fits.ImageHDU(header=header, data=cutout_result.data)
        self._fix_header(result.header, header)
        return result

    def _fix_header(self, target, original):
        # overcome astropy's tendency to remove keywords from original headers
        if 'BSCALE' in original:
            target['BSCALE'] = original['BSCALE']
        if 'BZERO' in original:
            target['BZERO'] = original['BZERO']

    def _add_primary_hdu(self, hdu_list, result_hdu_list):
        if isinstance(hdu_list[0], PrimaryHDU):
            logger.debug('Appending Primary HDU.')
            return fits.HDUList([hdu_list[0]])
        else:
            logger.debug('HDU List does NOT contain a Primary HDU.'
                         'Skipping.')
            return result_hdu_list

    def _require_primary_hdu(self, cutout_dimensions):
        # returns True if resulting cutout requires primary HDU from the
        # original hdu list. This is the case when cutouts are done in
        # different extensions of the file
        last_ext = -1
        for c in cutout_dimensions:
            if last_ext != -1 and last_ext != c._extension:
                return True
            last_ext = c._extension
        return False

    def _check_hdu_list(self, cutout_dimensions, hdu_list):
        has_match = False
        len_cutout_dimensions = len(cutout_dimensions)
        if len_cutout_dimensions > 0:
            result_hdu_list = None
            if self._require_primary_hdu(cutout_dimensions) and \
               hdu_list[0].header['NAXIS'] == 0:
                # add the PrimaryHDU from the original HDU list
                result_hdu_list = fits.HDUList([hdu_list[0]])
            # Check for a pixel cutout
            if isinstance(cutout_dimensions[0], PixelCutoutHDU):
                for cutout_dimension in cutout_dimensions:
                    ext = cutout_dimension.get_extension()
                    ext_idx = hdu_list.index_of(ext)
                    hdu = hdu_list[ext_idx]

                    # Entire extension was requested.
                    if not cutout_dimension.get_ranges():
                        logger.debug(
                            'Appending entire extension {}'.format(ext))
                        result_hdu_list = self._add_hdu(hdu, result_hdu_list)
                        has_match = True
                    else:
                        try:
                            result_hdu_list = self._add_hdu(
                                self._pixel_cutout(hdu, cutout_dimension),
                                result_hdu_list)
                            has_match = True
                            logger.debug(
                                'Successfully cutout from {} ({})'.format(
                                    ext, ext_idx))
                        except NoContentError:
                            logger.debug(
                                'Skipping non-overlapping cutout {}'.format(
                                    cutout_dimension))
            else:
                # Skip the primary as it should be written out already.
                for hdu in hdu_list:
                    if hdu.is_image and hdu.data is not None:
                        transform = Transform()
                        logger.debug(
                            'Transforming {}'.format(cutout_dimensions))
                        transformed_cutout_dimension = \
                            transform.world_to_pixels(
                                cutout_dimensions, hdu.header)
                        logger.debug('Transformed {} into {}'.format(
                            cutout_dimensions, transformed_cutout_dimension))
                        try:
                            result_hdu_list = self._add_hdu(self._pixel_cutout(
                                hdu,
                                transformed_cutout_dimension), result_hdu_list)
                            has_match = True
                        except NoContentError:
                            logger.debug(
                                'Skipping non-overlapping cutout {}'.format(
                                     cutout_dimensions))

            # time to write the cutout file
            result_hdu_list.writeto(
                self.output_writer, output_verify='ignore', checksum='remove')
        else:
            raise NoContentError('No overlap found (No cutout specified).')

        logger.debug('Has match in list? -- {}'.format(has_match))
        return has_match

    def _add_hdu(self, hdu, result_hdu_list):
        if result_hdu_list:
            result_hdu_list.append(hdu)
        else:
            primary_hdu = fits.PrimaryHDU(header=hdu.header, data=hdu.data)
            # astropy might remove BSCALE and BZERO. Put them back if true
            self._fix_header(primary_hdu.header, hdu.header)
            result_hdu_list = fits.HDUList([primary_hdu])
        return result_hdu_list

    def cutout(self, cutout_dimensions):
        # Start with the first extension
        hdu_list = fits.open(
            name=self.input_stream, memmap=True, mode='readonly',
            do_not_scale_image_data=True, ignore_missing_end=True)

        # Keep a tally of whether at least one HDU matched.
        has_match = self._check_hdu_list(cutout_dimensions, hdu_list)

        if not has_match:
            raise NoContentError('No overlap found.')
