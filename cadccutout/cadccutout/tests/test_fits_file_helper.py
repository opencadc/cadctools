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

import io
import logging
import numpy as np

from astropy.io import fits
from astropy.io.fits import Header
from astropy.wcs import WCS
from cadccutout.cutoutnd import CutoutResult
from cadccutout.file_helpers.fits.fits_file_helper import FITSHelper
from cadccutout.pixel_cutout_hdu import PixelCutoutHDU

logging.getLogger('cadccutout').setLevel(level=logging.DEBUG)


def _create_hdu_list():
    hdu0 = fits.PrimaryHDU()

    data1 = np.arange(10000).reshape(100, 100)
    hdu1 = fits.ImageHDU(data=data1)

    data2 = np.arange(20000).reshape(200, 100)
    hdu2 = fits.ImageHDU(data=data2)

    data3 = np.arange(50000).reshape(100, 500)
    hdu3 = fits.ImageHDU(data=data3)

    return fits.HDUList([hdu0, hdu1, hdu2, hdu3])


def test__check_hdu_list():
    in_io = io.BytesIO(b'TEST STRING HERE')
    out_io = io.BytesIO()
    test_subject = FITSHelper(in_io, out_io)
    dimensions = [PixelCutoutHDU([(20, 35), (40, 50)], extension=1)]
    hdu_list = _create_hdu_list()

    has_match = test_subject._check_hdu_list(dimensions, hdu_list)

    logging.debug('Output from _check_hdu_list() is {}'.format(has_match))

    assert has_match, 'Should match.'


def test_post_sanitize_header():
    test_subject = FITSHelper(io.BytesIO(), io.BytesIO())
    data = np.arange(10000).reshape(100, 100)
    header = Header()
    wcs = WCS(fix=False)
    header.set('REMAIN1', 'VALUE1')
    header.set('DQ1', 'dqvalue1')
    header.set('NAXIS', 2)
    header.set('NAXIS1', 88)
    header.set('NAXIS2', 212)

    result = CutoutResult(data, wcs=wcs)

    test_subject._post_sanitize_header(header, result)

    assert 'VALUE1' == header.get('REMAIN1'), 'REMAIN1 should still be there.'


def test_post_sanitize_header_crpix():
    test_subject = FITSHelper(io.BytesIO(), io.BytesIO())
    data = np.arange(10000).reshape(100, 100)
    header = Header()
    wcs = WCS(fix=False)
    header.set('REMAIN1', 'VALUE1')
    header.set('DQ1', 'dqvalue1')
    header.set('NAXIS', 2)
    header.set('NAXIS1', 88)
    header.set('NAXIS2', 212)
    header.set('CRPIX1', 77.0)
    header.set('WCSAXES', 2)
    header.set('CTYPE1', 'ctype1value')

    result = CutoutResult(data, wcs=wcs)

    assert header.index('WCSAXES') > header.index('CRPIX1'), \
        'Start with bad indexes...'

    test_subject._post_sanitize_header(header, result)

    assert 'VALUE1' == header.get('REMAIN1'), 'REMAIN1 should still be there.'


def test_post_sanitize_header_cd():
    test_subject = FITSHelper(io.BytesIO(), io.BytesIO())
    data = np.arange(10000).reshape(100, 100)
    header = Header()
    wcs = WCS(fix=False)
    wcs.wcs.cd = [[0.9, 0.8], [0.7, 0.6]]
    header.set('REMAIN1', 'VALUE1')
    header.set('DQ1', 'dqvalue1')
    header.set('NAXIS', 2)
    header.set('NAXIS1', 88)
    header.set('NAXIS2', 212)
    header.set('CRPIX1', 77.0)
    header.set('WCSAXES', 2)
    header.set('CTYPE1', 'ctype1value')

    assert header.get('CD1_1') is None, 'Should not contain CD1_1'
    assert header.get('CD2_1') is None, 'Should not contain CD2_1'
    assert header.get('CD1_2') is None, 'Should not contain CD1_2'
    assert header.get('CD2_2') is None, 'Should not contain CD2_2'

    result = CutoutResult(data, wcs=wcs)

    assert header.index('WCSAXES') > header.index('CRPIX1'), \
        'Start with bad indexes...'

    test_subject._post_sanitize_header(header, result)

    assert header.get('CD1_1') == 0.9, 'Wrong CD1_1 value.'
    assert header.get('CD2_1') == 0.7, 'Wrong CD2_1 value.'
    assert header.get('CD1_2') == 0.8, 'Wrong CD1_2 value.'
    assert header.get('CD2_2') == 0.6, 'Wrong CD2_2 value.'

    assert 'VALUE1' == header.get('REMAIN1'), 'REMAIN1 should still be there.'


def test_post_sanitize_header_pc():
    test_subject = FITSHelper(io.BytesIO(), io.BytesIO())
    data = np.arange(10000).reshape(100, 100)
    header = Header()
    wcs = WCS(fix=False)
    wcs.wcs.pc = [[0.9, 0.8], [0.7, 0.6]]
    header.set('REMAIN1', 'VALUE1')
    header.set('DQ1', 'dqvalue1')
    header.set('NAXIS', 2)
    header.set('NAXIS1', 88)
    header.set('NAXIS2', 212)
    header.set('CRPIX1', 77.0)
    header.set('WCSAXES', 2)
    header.set('CTYPE1', 'ctype1value')

    assert header.get('PC1_1') is None, 'Should not contain PC1_1'
    assert header.get('PC2_1') is None, 'Should not contain PC2_1'
    assert header.get('PC1_2') is None, 'Should not contain PC1_2'
    assert header.get('PC2_2') is None, 'Should not contain PC2_2'

    result = CutoutResult(data, wcs=wcs)

    assert header.index('WCSAXES') > header.index('CRPIX1'), \
        'Start with bad indexes...'

    test_subject._post_sanitize_header(header, result)

    assert header.get('PC1_1') == 0.9, 'Wrong PC1_1 value.'
    assert header.get('PC2_1') == 0.7, 'Wrong PC2_1 value.'
    assert header.get('PC1_2') == 0.8, 'Wrong PC1_2 value.'
    assert header.get('PC2_2') == 0.6, 'Wrong PC2_2 value.'

    assert 'VALUE1' == header.get('REMAIN1'), 'REMAIN1 should still be there.'
