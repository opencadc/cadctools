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

from copy import deepcopy
from math import ceil

from astropy.wcs import Sip
from cadccutout.no_content_error import NoContentError

__all__ = ['CutoutResult', 'CutoutND']

logger = logging.getLogger(__name__)


class CutoutResult(object):
    """
    Just a DTO to move results of a cutout.  It's more readable than a plain
    tuple.
    """

    def __init__(self, data, wcs=None):
        self.data = data
        self.wcs = wcs


class CutoutND(object):
    def __init__(self, data, wcs=None):
        """
        Parameters
        ----------
        data : `~numpy.ndarray`
            The N-dimensional data array from which to extract the cutout array.
        cutout_region : `PixelCutoutHDU`
            The Pixel HDU Cutout description.  See
            cadccutout.pixel_cutout_hdu.py.
        wcs : `~astropy.wcs.WCS` or `None`
            A WCS object associated with the cutout array.  If it's specified,
            reset the WCS values for the cutout.

        Returns
        -------
        CutoutResult instance
        """
        if data is None:
            raise NoContentError('Nothing to cutout from.')

        self.data = data
        self.wcs = wcs

    def to_slice(self, idx, cutout_region):
        """
        Convert a region (tuple) into a slice to be used in a data array.  Some
        fenangling is added here to adjust the values based on a striding step,
        or to handle a two-dimensional cutout where y2 < y1.

        Some math is translated (or invented) to match what cfitsio (imcopy) is
        producing.

        Parameters
        ----------
        idx:        Index of the current region requested.  Used to lookup the
                    shape of the data to "pad" the cutout if necessary.
        cutout_region:  The requested region to be cutout.
        """
        len_region = len(cutout_region)

        if len_region > 0:
            low_bound = cutout_region[0]

            if low_bound == '*':
                lower_bound = 0
                upper_bound = self.data.shape[idx]
                step = int(cutout_region[1])
            else:
                lower_bound = int(low_bound)
                if lower_bound > 0:
                    lower_bound -= 1
                upper_bound = int(cutout_region[1])
                if (len_region == 3):
                    step = int(cutout_region[2])
                    if lower_bound > upper_bound:
                        upper_bound -= 2
                        if step > 0:
                            step *= -1
                elif lower_bound > upper_bound:
                    upper_bound -= 2
                    step = -1
                else:
                    step = 1

            logger.debug('Bounds are {}:{}:{}'.format(
                lower_bound, upper_bound, step))
            return slice(lower_bound, upper_bound, step)
        else:
            raise ValueError('Should have at least two values (lower, upper).')

    def _pad_cutout(self, cutout_shape):
        len_shape = len(cutout_shape)
        data_shape = self.data.shape
        logger.debug('Data shape is {} with length {}'.format(
            data_shape, len(self.data)))
        len_data = len(data_shape)
        if len_data > len_shape:
            missing_shape_bounds = data_shape[:len_data - len_shape]
            logger.debug('Missing shape bounds are {} for length {}'.format(
                missing_shape_bounds, len_data - len_shape))

            for val in missing_shape_bounds:
                cutout_shape.append(slice(val))

    def format_wcs(self, cutout_shape):
        """
        Re-calculate the CRPIX values for the WCS.  The SIP values are also
        re-calculated, if present.

        CRPIX values are tricky and there exists some black magic math in here,
        not to mention some values are set differently to accommodate the subtle
        variations with Python 2/3 float values.

        Parameters
        ----------
        cutout_shape:   The tuple containing the bounding values (shape) of the
        resulting cutout.

        Returns
        -------
        The formatted WCS object.
        """
        output_wcs = deepcopy(self.wcs)
        wcs_crpix = output_wcs.wcs.crpix
        l_wcs_crpix = len(wcs_crpix)

        logger.debug('Adjusting WCS.')

        for idx, cutout_region in enumerate(cutout_shape):
            if idx < l_wcs_crpix:
                curr_val = wcs_crpix[idx]
                start = cutout_region.start
                step = cutout_region.step

                if start:
                    wcs_crpix[idx] -= float(ceil(start + 0.5))

                if step is not None:
                    logger.debug('Taking step {} into account.'.format(step))
                    wcs_crpix[idx] /= step

                if start:
                    wcs_crpix[idx] += 1.0

                logger.debug(
                    'Adjusted wcs_crpix val from {} to {}'.format(
                        curr_val, wcs_crpix[idx]))

        if output_wcs.wcs.has_pc():
            pc = output_wcs.wcs.pc
            for i in range(output_wcs.wcs.naxis):
                for j in range(output_wcs.wcs.naxis):
                    step = cutout_shape[j].step
                    if step:
                        pc[i][j] *= step
        elif output_wcs.wcs.has_cd():
            cd = output_wcs.wcs.cd
            for i in range(output_wcs.wcs.naxis):
                for j in range(output_wcs.wcs.naxis):
                    step = cutout_shape[j].step
                    if step:
                        cd[i][j] *= step

        if self.wcs.sip:
            curr_sip = self.wcs.sip
            output_wcs.sip = Sip(curr_sip.a, curr_sip.b,
                                 curr_sip.ap, curr_sip.bp,
                                 wcs_crpix[0:2])

        logger.debug('WCS adjusted.')

        return output_wcs

    def extract(self, cutout_regions):
        """
        Perform the extraction from the data for the provided region.  If the
        provided region is smaller than the data, it will be padded with the
        values from the data.

        :param cutout_regions:    List of region tuples.
        """
        try:
            cutout_shape = [
                self.to_slice(idx, cutout_region) for idx, cutout_region in
                enumerate(cutout_regions)]
            self._pad_cutout(cutout_shape)

            cutout = tuple(reversed(cutout_shape))
            logger.debug('Cutout is {}'.format(cutout))
            cutout_data = self.data[cutout]
        except IndexError:
            raise NoContentError('No content (arrays do not overlap).')

        logger.debug('Extracted {} of data from {}.'.format(cutout_data.shape,
                                                            self.data.shape))

        if self.wcs:
            output_wcs = self.format_wcs(cutout_shape)
        else:
            logger.debug('No WCS present.')
            output_wcs = None

        return CutoutResult(data=cutout_data, wcs=output_wcs)
