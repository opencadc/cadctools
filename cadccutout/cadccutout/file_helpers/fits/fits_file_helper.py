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

from astropy.io import fits
from astropy.io.fits import PrimaryHDU, ImageHDU
from astropy.wcs import WCS
from astropy.nddata import NoOverlapError
from cadccutout.utils import is_integer
from cadccutout.file_helpers.base_file_helper import BaseFileHelper
from cadccutout.no_content_error import NoContentError
from cadccutout.pixel_cutout_hdu import PixelCutoutHDU
from cadccutout.transform import Transform


__all__ = ['FITSHelper']


# Remove the DQ1 and DQ2 headers until the issue with wcslib is resolved:
# https://github.com/astropy/astropy/issues/7828
UNDESIREABLE_HEADER_KEYS = ['DQ1', 'DQ2']


class FITSHelper(BaseFileHelper):

    def __init__(self, input_stream, output_writer):
        """
        Create a new BaseFileHelper used for different file types.  Concrete
         instances are expected to extend this
        class to provide some common state.

        :param input_stream:    The Reader to read the file data from.
        :param output_writer:   The Writer to write the cutout to.
        """
        self.logger = logging.getLogger(__name__)
        super(FITSHelper, self).__init__(input_stream, output_writer)

    def _post_sanitize_header(self, header, cutout_result):
        """
        Remove headers that don't belong in the cutout output.
        """
        # Remove known keys
        for x in UNDESIREABLE_HEADER_KEYS:
            header.remove(x, ignore_missing=True, remove_all=True)

        # If a WCSAXES card exists, ensure that it comes before the CTYPE1 card.
        wcsaxes_keyword = 'WCSAXES'
        ctype1_keyword = 'CTYPE1'

        # Only proceed with this if both the WCSAXES and CTYPE1 cards are
        # present.
        if header.get(wcsaxes_keyword) and header.get(ctype1_keyword):
            wcsaxes_index = header.index(wcsaxes_keyword)
            ctype1_index = header.index(ctype1_keyword)

            if wcsaxes_index > ctype1_index:
                existing_wcsaxes_value = header.get(wcsaxes_keyword)
                header.remove(wcsaxes_keyword)
                header.insert(
                    ctype1_index, (wcsaxes_keyword, existing_wcsaxes_value))

        if cutout_result.wcs is not None:
            naxis = header.get('NAXIS', 0)
            cutout_wcs = cutout_result.wcs
            cutout_wcs_header = cutout_wcs.to_header(relax=True)
            header.update(cutout_wcs_header)

            if cutout_wcs.sip is not None:
                cutout_crpix = cutout_result.wcs_crpix

                for idx, val in enumerate(cutout_crpix):
                    header.set('CRPIX{}'.format(idx + 1), val)

            # Remove the CDi_j headers in favour of the PCi_j equivalents
            for i in range(naxis):
                for j in range(naxis):
                    idx_val = '{}_{}'.format(i + 1, j + 1)
                    cd_key = 'CD{}'.format(idx_val)
                    cd_val = header.get(cd_key)

                    if cd_val is not None:
                        pc_key = 'PC{}'.format(idx_val)
                        if header.get(pc_key) is None:
                            header.set(pc_key, cd_val)

                        header.remove(
                            cd_key, ignore_missing=True, remove_all=True)

            # Is this necessary?
            header.set('WCSAXES', naxis)

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

        return WCS(header=header, naxis=naxis)

    def _write_cutout(self, header, data, cutout_dimension, wcs):
        try:
            cutout_result = self.do_cutout(
                data=data, cutout_dimension=cutout_dimension, wcs=wcs)

            self._post_sanitize_header(header, cutout_result)

            fits.append(filename=self.output_writer, header=header,
                        data=cutout_result.data, overwrite=False,
                        output_verify='silentfix', checksum='remove')
            self.output_writer.flush()
        except NoContentError:
            self.logger.warn(
                'No cutout possible on extension {}.  Skipping...'.format(
                    cutout_dimension.get_extension()))

    def _pixel_cutout(self, header, data, cutout_dimension):
        extension = cutout_dimension.get_extension()
        wcs = self._get_wcs(header)
        try:
            self._write_cutout(header=header, data=data,
                               cutout_dimension=cutout_dimension, wcs=wcs)
            self.logger.debug(
                'Cutting out from extension {}'.format(extension))
        except NoOverlapError:
            self.logger.error(
                'No overlap found for extension {}'.format(extension))
            raise NoContentError('No content (arrays do not overlap).')

    def _is_extension_requested(
            self, extension_idx, ext_name_ver, cutout_dimension):
        requested_extension = cutout_dimension.get_extension()

        matches = False

        if ext_name_ver is not None:
            matches = (
                ext_name_ver == requested_extension or requested_extension ==
                ext_name_ver[0])

        if not matches and is_integer(requested_extension) and is_integer(
                extension_idx):
            matches = (int(requested_extension) == int(extension_idx))

        if not matches:
            matches = (requested_extension == extension_idx)

        return matches

    def _check_hdu_list(self, cutout_dimensions, hdu_list):
        has_match = False
        for curr_extension_idx, hdu in enumerate(hdu_list):
            # If we encounter a PrimaryHDU, write it at the top and continue.
            if isinstance(hdu, PrimaryHDU):
                self.logger.debug('Primary at {}'.format(curr_extension_idx))
                fits.append(
                    filename=self.output_writer, header=hdu.header, data=None,
                    overwrite=False, output_verify='silentfix',
                    checksum='remove')
            elif isinstance(hdu, ImageHDU):
                header = hdu.header
                ext_name = header.get('EXTNAME')
                ext_ver = header.get('EXTVER', 0)
                curr_ext_name_ver = None

                if ext_name is not None:
                    curr_ext_name_ver = (ext_name, ext_ver)

                try:
                    if isinstance(cutout_dimensions[0], PixelCutoutHDU):
                        for cutout_dimension in cutout_dimensions:
                            if self._is_extension_requested(
                                    curr_extension_idx, curr_ext_name_ver,
                                    cutout_dimension):
                                self.logger.debug(
                                    '*** Extension {} does match ({} | {})'
                                    .format(
                                        cutout_dimension.get_extension(),
                                        curr_extension_idx, curr_ext_name_ver))
                                self._pixel_cutout(
                                    header, hdu.data, cutout_dimension)
                    else:
                        # Handle WCS transform.
                        transform = Transform()
                        transformed_cutout_dimension = \
                            transform.world_to_pixels(cutout_dimensions, header)
                        self._pixel_cutout(header, hdu.data,
                                           transformed_cutout_dimension)
                    has_match = True
                except NoContentError:
                    # Skip for now as we're iterating the loop.
                    pass
            else:
                self.logger.warn(
                    'Unsupported HDU at extension {}.'.format(
                        curr_extension_idx))

        return has_match

    def _iterate_hdu_list(self, cutout_dimensions):
        # Start with the first extension
        hdu_list = fits.open(
            name=self.input_stream, memmap=True, mode='readonly',
            do_not_scale_image_data=True)

        # Keep a tally of whether at least one HDU matched.
        has_match = self._check_hdu_list(cutout_dimensions, hdu_list)

        if not has_match:
            raise NoContentError('No overlap found.')

    def _quick_pixel_cutout(self, cutout_dimension):
        hdu_list = fits.open(self.input_stream, memmap=True,
                             mode='readonly', do_not_scale_image_data=True)
        ext_idx = hdu_list.index_of(cutout_dimension.get_extension())
        if ext_idx >= 0:
            hdu = hdu_list.pop(ext_idx)
            self._pixel_cutout(hdu.header, hdu.data, cutout_dimension)

    def _is_single_hdu_cutout(self, cutout_dimensions):
        return cutout_dimensions is not None \
            and len(cutout_dimensions) == 1 \
            and isinstance(cutout_dimensions[0], PixelCutoutHDU)

    def cutout(self, cutout_dimensions):
        if self._is_single_hdu_cutout(cutout_dimensions):
            self._quick_pixel_cutout(cutout_dimensions[0])
        else:
            self._iterate_hdu_list(cutout_dimensions)
