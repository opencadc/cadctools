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

from cadccutout.cutoutnd import CutoutND
from cadccutout.pixel_range_input_parser import PixelRangeInputParser

__all__ = ['BaseFileHelper']


class BaseFileHelper(object):
    def __init__(self, input_stream, output_writer,
                 input_range_parser=PixelRangeInputParser()):
        """
        Create a new BaseFileHelper used for different file types.  Concrete
        instances are expected to extend this class to provide some common
        state.

        :param input_stream:    The Reader to read the file data from.
        :param output_writer:   The Writer to write the cutout to.
        :param input_range_parser:  Parser instance to parse the range of
        inputs.
        """
        self.logger = logging.getLogger(__name__)
        if input_stream is None:
            raise ValueError(
                'An input stream(file-like object or io/stream) is required \
                 to read from.')
        else:
            self.input_stream = input_stream

        if output_writer is None:
            raise ValueError('An output stream(file-like object or io/stream) \
             is required to write to.')
        else:
            self.output_writer = output_writer

        self.input_range_parser = input_range_parser

    def do_cutout(self, data, cutout_dimension, wcs):
        """
        Perform a Cutout of the given data at the given position and size.
        :param data:  The data to cutout from
        :param cutout_dimension:  `PixelCutoutHDU`       Cutout object.
        :param wcs:    The WCS object to use with the cutout to return a copy
         of the WCS object.

        :return: CutoutND instance
        """

        # Sanitize the array by removing the single-dimensional entries.
        sanitized_data = np.squeeze(data)
        c = CutoutND(data=sanitized_data, wcs=wcs)
        return c.extract(cutout_dimension)
