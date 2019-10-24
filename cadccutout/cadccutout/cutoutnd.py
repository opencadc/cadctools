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

from cadccutout.no_content_error import NoContentError

__all__ = ['CutoutParameters', 'CutoutND']

LOGGER = logging.getLogger(__name__)


class CutoutParameters(object):
    '''
    Just a DTO to move results of a cutout.  It's more readable than a plain
    tuple.
    '''

    def __init__(self, cutout, cutout_shape):
        self.cutout = cutout
        self.cutout_shape = cutout_shape


class CutoutND(object):
    '''
    Class to contain cutting out from the NumPy array and to provide
    convenience methods to prepare the data to be cutout.
    '''

    def __init__(self, hdu):
        '''
        Parameters
        ----------
        hdu : fitsio HDU
            The HDU from fitsio containing the header and data block.

        Returns
        -------
        CutoutParameters instance
        '''
        if not hdu:
            raise NoContentError('Nothing to cutout from.')

        self.hdu = hdu

    def to_slice(self, idx, cutout_region):
        '''
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
        '''
        len_region = len(cutout_region)

        if len_region > 0:
            low_bound = cutout_region[0]

            if low_bound == '*':
                lower_bound = None
                upper_bound = self.hdu.get_dims()[idx + 1]
                if len_region > 1:
                    step = int(cutout_region[1])
                else:
                    step = 1
            else:
                # The lower bound is extended by 1 to support CFITSIO indexes.
                lower_bound = int(low_bound) - 1
                upper_bound = int(cutout_region[1])

                if len_region == 3:
                    step = int(cutout_region[2])
                else:
                    step = 1

                if upper_bound < lower_bound:
                    # A little black magic here.  The stop is offset by 2 to
                    # accommodate the 1-offset of CFITSIO, and to move past the
                    # end pixel to get the complete set after it is flipped
                    # along the axis.  Maybe there is a clearer way to
                    # accomplish what this offset is glossing over.
                    upper_bound -= 2

                    if step > 0:
                        step *= -1

            LOGGER.debug('Bounds are {}:{}:{}'.format(
                lower_bound, upper_bound, step))
            return slice(lower_bound, upper_bound, step)
        else:
            raise ValueError('Should have at least two values (lower, upper).')

    def pad_cutout(self, cutout_shape):
        '''
        Pad the cutout's shape with that of the data's shape to create a
        shape that matches what the numpy array is expecting.
        '''
        len_shape = len(cutout_shape)
        data_shape = self.hdu.get_dims()
        len_data = len(data_shape)
        LOGGER.debug(
            'Data shape is {} and cutout shape is {}.'.format(
                data_shape, cutout_shape))
        if len_data == 0:
            raise NoContentError('No data to cutout from.')
        elif len_shape > len_data:
            raise ValueError(
                'Shape of cutout ({}) exceeds shape of data ({})'.format(
                    cutout_shape, data_shape))
        else:
            missing_shape_bounds = data_shape[:(len_data - len_shape)]
            LOGGER.debug('Missing shape bounds are {} for length {}'.format(
                missing_shape_bounds, len_data - len_shape))

            for val in missing_shape_bounds:
                cutout_shape.append(slice(val))

    def get_parameters(self, cutout_regions):
        '''
        Perform the extraction from the data for the provided region.  If the
        provided region is smaller than the data, it will be padded with the
        values from the data.

        :param cutout_regions:    List of region tuples.
        '''

        LOGGER.debug('Inspecting regions {}'.format(cutout_regions))

        cutout_shape = [
            self.to_slice(idx, cutout_region) for idx,
            cutout_region in enumerate(cutout_regions)
        ]
        self.pad_cutout(cutout_shape)
        cutout = tuple(reversed(cutout_shape))
        LOGGER.debug('Cutout is {}'.format(cutout))

        return CutoutParameters(cutout=cutout, cutout_shape=cutout_shape)
