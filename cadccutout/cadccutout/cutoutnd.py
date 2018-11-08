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

from copy import deepcopy

from astropy.wcs import Sip
from astropy.nddata.utils import extract_array
from cadccutout.no_content_error import NoContentError

__all__ = ['CutoutResult', 'CutoutND']


class CutoutResult(object):
    """
    Just a DTO to move results of a cutout.  It's more readable than a plain
    tuple.
    """

    def __init__(self, data, wcs=None, wcs_crpix=None):
        self.data = data
        self.wcs = wcs
        self.wcs_crpix = wcs_crpix


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
        self.logger = logging.getLogger(__name__)
        self.data = data
        self.wcs = wcs

    def _get_position_shape(self, data_shape, cutout_region):
        requested_shape = cutout_region.get_shape()
        requested_position = cutout_region.get_position()

        # reverse position because extract_array uses reverse ordering
        # (i.e. x,y -> y,x).
        r_position = tuple(reversed(requested_position))
        r_shape = tuple(reversed(requested_shape))

        len_data = len(data_shape)
        len_pos = len(r_position)
        len_shape = len(r_shape)

        if len_shape > len_data:
            raise NoContentError('Invalid shape requested (tried to extract {} \
            from {}).'.format(r_shape, data_shape))

        if r_shape:
            shape = (data_shape[:(len_data - len_shape)]) + r_shape
        else:
            shape = None

        if len_pos > len_data:
            raise NoContentError('Invalid position requested (tried to extract \
             {} from {}).'.format(
                r_position, data_shape))

        if r_position:
            position = (data_shape[:(len_data - len_pos)]) + r_position
        else:
            position = None

        return (position, shape)

    def extract(self, cutout_region):
        data = self.data
        data_shape = data.shape
        position, shape = self._get_position_shape(data_shape, cutout_region)
        self.logger.debug('Position {} and Shape {}'.format(position, shape))

        # No pixels specified, so return the entire HDU
        if (not position and not shape) or shape == data_shape:
            self.logger.debug('Returning entire HDU data for {}'.format(
                cutout_region.get_extension()))
            cutout_data = data
        else:
            self.logger.debug('Cutting out {} at {} for extension {} from  \
            {}.'.format(
                shape, position, cutout_region.get_extension(), data.shape))
            cutout_data, position = extract_array(
                data, shape, position, mode='partial', return_position=True)

        if self.wcs is not None:
            cutout_shape = cutout_data.shape
            output_wcs = deepcopy(self.wcs)
            wcs_crpix = output_wcs.wcs.crpix
            ranges = cutout_region.get_ranges()
            l_ranges = len(ranges)

            while len(wcs_crpix) < l_ranges:
                wcs_crpix = np.append(wcs_crpix, 1.0)

            for idx, _ in enumerate(ranges):
                wcs_crpix[idx] -= (ranges[idx][0] - 1)

            output_wcs._naxis = list(cutout_shape)

            if self.wcs.sip is not None:
                curr_sip = self.wcs.sip
                output_wcs.sip = Sip(curr_sip.a, curr_sip.b,
                                     curr_sip.ap, curr_sip.bp,
                                     wcs_crpix[0:2])
        else:
            output_wcs = None

        return CutoutResult(data=cutout_data, wcs=output_wcs,
                            wcs_crpix=wcs_crpix)
