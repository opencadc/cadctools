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
import re
from cadccutout.pixel_cutout_hdu import PixelCutoutHDU
from cadccutout.utils import to_num

__all__ = ['PixelRangeInputParserError', 'PixelRangeInputParser']


RANGE_BEGIN_CHAR = '['
RANGE_END_CHAR = ']'


class PixelRangeInputParserError(ValueError):
    pass


class PixelRangeInputParser(object):
    """
    Parse the pixel input as it was delivered.
    """

    def __init__(self, delimiter=':', separator=','):
        self.logger = logging.getLogger(__name__)
        self.delimiter = delimiter
        self.separator = separator
        self.match_pattern = re.compile(
            r'[\[?[\w]*,?\d*\]?]?[\[?[\d*:?\d*,?]*\]?]')

    def is_pixel_cutout(self, input_str):
        return input_str and input_str.count(RANGE_BEGIN_CHAR) > 0

    def _to_range_tuple(self, rs):
        if self.delimiter not in rs:
            return (to_num(rs), to_num(rs))  # Turns 7 into 7:7
        else:
            start, end = rs.split(self.delimiter)

            if not start or not end:
                raise PixelRangeInputParserError(
                    'Incomplete range specified {}'.format(rs))
            else:
                return (to_num(start), to_num(end))

    def parse(self, pixel_range_input_str):
        """
        Parse a string range.
        :param  pixel_range_input_str: The string to parse.
        :return List of PixelCutoutHDU instances

        Example:

        rp = PixelRangeInputParser()
        rp.parse('[0][1]')
        => [PixelCutoutHDU((1,1), extension='0')]

        rp.parse('[99:112]')
        => [PixelCutoutHDU((99,112), extension='0')]

        rp.parse('[SCI][99:112][5]')
        => [PixelCutoutHDU((99,112), extension=SCI),
            PixelCutoutHDU(extension='5')]

        rp.parse('[IMG,2][100:112][6][300:600]')
        => [PixelCutoutHDU((100,112), extension='IMG,2'),
            PixelCutoutHDU((300,600), extension='6')]
        """
        rs = pixel_range_input_str.strip()

        if not self.is_pixel_cutout(rs):
            raise PixelRangeInputParserError(
                'Not a valid pixel cutout string "{}".'.format(rs))

        # List of ranges in format [ext][pixel ranges]
        ranges = re.findall(self.match_pattern, rs)

        if not ranges:
            raise PixelRangeInputParserError(
                'Invalid range specified.  Should be in the format of {}  \
                (i.e.[0][8:35]), or single digit(i.e. 9). '.format(
                    self.match_pattern))

        parsed_items = []

        for r in ranges:
            pixel_ranges = []
            extension = '0'
            split_items = list(map(lambda x: x.split(
                '[')[1], list(filter(None, r.split(']')))))
            l_items = len(split_items)

            if l_items == 2:
                extension = split_items[0]
                pixel_ranges = list(map(self._to_range_tuple, list(
                    filter(None, split_items[1].split(self.separator)))))
            elif l_items == 1:
                item = split_items[0]
                if item.count(self.delimiter) > 0:
                    pixel_ranges = list(map(self._to_range_tuple, list(
                        filter(None, item.split(self.separator)))))
                else:
                    extension = item
            else:
                raise PixelRangeInputParserError(
                    'Nothing usable for range {}'.format(r))

            parsed_items.append(PixelCutoutHDU(
                dimension_ranges=pixel_ranges, extension=extension))

        return parsed_items
