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

import io
import sys
import os
import tempfile
import logging
import numpy as np

import fitsio

from cadccutout.fits import cutout as fits_cutout
from cadccutout.pixel_cutout_hdu import PixelCutoutHDU

logging.getLogger('cadccutout').setLevel(level=logging.DEBUG)

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')
DEFAULT_TEST_FILE_DIR = '/tmp'


def random_test_file_name_path(file_extension='fits',
                               dir_name=DEFAULT_TEST_FILE_DIR):
    '''
    Create a new random test file and return the name.
    '''
    return tempfile.NamedTemporaryFile(
        dir=dir_name, prefix=__name__, suffix='.{}'.format(
            file_extension)).name


def _create_hdu_list():
    fits_file_name = random_test_file_name_path()
    fits = fitsio.FITS(fits_file_name, 'rw')

    fits.write(None)

    data1 = np.arange(10000).reshape(100, 100)
    fits.write(data1)

    data2 = np.arange(20000).reshape(200, 100)
    fits.write(data2)

    data3 = np.arange(50000).reshape(100, 500)
    fits.write(data3)

    return fitsio.FITS(fits_file_name)


def test_factory_cutout():
    '''
    Bare cutout tests.
    '''
    fname = random_test_file_name_path()
    with fitsio.FITS(fname, 'rw', clobber=True) as fits:
        fits.write(None)

        data1 = np.arange(10000).reshape(100, 100)
        fits.write(data1)

        data2 = np.arange(20000).reshape(200, 100)
        fits.write(data2)

        data3 = np.arange(50000).reshape(100, 500)
        fits.write(data3)

        out_io = io.BytesIO()
        dimensions = [PixelCutoutHDU([(20, 35), (40, 50)], extension=1)]

        fits_cutout(dimensions, fname, out_io)

        assert True, 'Should not throw an error.'
