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

import logging
import numpy as np

from math import ceil
from cadccutout.utils import is_integer

__all__ = ['PixelCutoutHDU']


def fix_tuple(t):
    if np.isscalar(t):
        return (t, t)
    elif len(t) < 2:
        raise ValueError('Unusable dimension range {}'.format(t))
    else:
        return t


class PixelCutoutHDU(object):
    def __init__(self, dimension_ranges=[], extension='0'):
        """
        A Pixel cutout.
        :param dimension_ranges: list    Dimension ranges expressed as tuples
        (i.e. (lower,upper)).
        :param extension: tuple, int, string
            The Extension specification to use.  If tuple, use (str, int) to
             get the nth count of the EXTNAME=str
            extension.  If string, use the first extension with EXTNAME=string,
             or use int to get the extension[int].
            This is zero (0) based.
        """
        self.logger = logging.getLogger(__name__)
        self.dimension_ranges = list(map(fix_tuple, dimension_ranges))
        self._extension = str(extension)  # For consistency.

    def get_ranges(self):
        """
        Obtain the range tuples.
        """
        acc = []
        for range_tuple in self.dimension_ranges:
            acc.append(
                (int(np.round(range_tuple[0])), int(np.round(range_tuple[1]))))

        return acc

    def get_shape(self):
        """
        Convert the given dimensions to a shape.
        """
        acc = []
        for range_tuple in self.dimension_ranges:
            acc.append(int(np.round((range_tuple[1] - range_tuple[0]) + 1)))

        return tuple(acc)

    def get_position(self):
        """
        Convert the given dimensions to a position to extract.
        """
        acc = []
        for range_tuple in self.dimension_ranges:
            acc.append(int(ceil(
                range_tuple[0] - 0.5) + int(ceil(((range_tuple[1]
                                                   - range_tuple[0]) / 2)
                                                 - 0.5))) - 1)

        return tuple(acc)

    def get_extension(self):
        ext = self._extension
        if is_integer(ext):
            return int(ext)
        elif ext.count(',') == 1:
            es = ext.split(',')
            ext_int = int(es[1])

            # EXTNAME and EXTVER are 1-based.
            if ext_int == 0:
                ext_int = 1

            return (es[0], ext_int)
        else:
            return ext
