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

from __future__ import (absolute_import, division, print_function, unicode_literals)

import logging
import os
import pytest

from cadccutout.shape import Circle, Polygon, Energy, Time, Polarization, PolarizationState

pytest.main(args=['-s', os.path.abspath(__file__)])
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def test_circle():
    coordinates = [1.0, 2.0, 3.0]
    test_subject = Circle(*coordinates)
    assert test_subject is not None
    assert test_subject.ra == 1.0
    assert test_subject.dec == 2.0
    assert test_subject.radius == 3.0

    coordinates = ''
    try:
        test_subject = Circle.parse_circle(coordinates)
        assert False, 'Should raise ValueError'
    except ValueError:
        assert True

    coordinates = '1.0 2.0'
    try:
        test_subject = Circle.parse_circle(coordinates)
        assert False, 'Should raise ValueError'
    except ValueError:
        assert True

    coordinates = '1.0 2.0 3.0'
    test_subject = Circle.parse_circle(coordinates)
    assert test_subject is not None
    assert test_subject.ra == 1.0
    assert test_subject.dec == 2.0
    assert test_subject.radius == 3.0


def test_polygon():
    coordinates = []
    try:
        test_subject = Polygon(coordinates)
        assert False, 'Should raise ValueError'
    except ValueError:
        assert True

    coordinates = [[1.0, 2.0], [3.0, 4.0]]
    try:
        test_subject = Polygon(coordinates)
        assert False, 'Should raise ValueError'
    except ValueError:
        assert True

    coordinates = [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]
    test_subject = Polygon(coordinates)
    assert test_subject is not None
    vertices = test_subject.vertices
    assert len(vertices) == 3
    assert vertices[0][0] == 1.0
    assert vertices[0][1] == 2.0
    assert vertices[1][0] == 3.0
    assert vertices[1][1] == 4.0
    assert vertices[2][0] == 5.0
    assert vertices[2][1] == 6.0

    coordinates = ''
    try:
        test_subject = Polygon.parse_polygon(coordinates)
        assert False, 'Should raise ValueError'
    except ValueError:
        assert True

    coordinates = '1.0 2.0 3.0 4.0'
    try:
        test_subject = Polygon.parse_polygon(coordinates)
        assert False, 'Should raise ValueError'
    except ValueError:
        assert True

    coordinates = '1.0 2.0 3.0 4.0 5.0 6.0'
    test_subject = Polygon.parse_polygon(coordinates)
    assert test_subject is not None
    vertices = test_subject.vertices
    assert len(vertices) == 3
    assert vertices[0][0] == 1.0
    assert vertices[0][1] == 2.0
    assert vertices[1][0] == 3.0
    assert vertices[1][1] == 4.0
    assert vertices[2][0] == 5.0
    assert vertices[2][1] == 6.0


def test_energy():
    coordinates = [1.0, 2.0]
    test_subject = Energy(*coordinates)
    assert test_subject is not None
    assert test_subject.lower is not None
    assert test_subject.upper is not None
    assert test_subject.lower == 1.0
    assert test_subject.upper == 2.0

    coordinates = ''
    try:
        test_subject = Energy.parse_energy(coordinates)
        assert False, 'Should raise ValueError'
    except ValueError:
        assert True

    coordinates = '1.0'
    try:
        test_subject = Energy.parse_energy(coordinates)
        assert False, 'Should raise ValueError'
    except ValueError:
        assert True

    coordinates = '1.0 2.0'
    test_subject = Energy.parse_energy(coordinates)
    assert test_subject is not None
    assert test_subject.lower is not None
    assert test_subject.upper is not None
    assert test_subject.lower == 1.0
    assert test_subject.upper == 2.0


def test_time():
    coordinates = [1.0, 2.0]
    test_subject = Time(*coordinates)
    assert test_subject is not None
    assert test_subject.lower is not None
    assert test_subject.upper is not None
    assert test_subject.lower == 1.0
    assert test_subject.upper == 2.0

    coordinates = ''
    try:
        test_subject = Time.parse_time(coordinates)
        assert False, 'Should raise ValueError'
    except ValueError:
        assert True

    coordinates = '1.0'
    try:
        test_subject = Time.parse_time(coordinates)
        assert False, 'Should raise ValueError'
    except ValueError:
        assert True

    coordinates = '1.0 2.0'
    test_subject = Time.parse_time(coordinates)
    assert test_subject is not None
    assert test_subject.lower is not None
    assert test_subject.upper is not None
    assert test_subject.lower == 1.0
    assert test_subject.upper == 2.0


def test_polarization():
    arguments = []
    try:
        test_subject = Polarization(arguments)
        assert False, 'Should raise ValueError'
    except ValueError:
        assert True

    arguments = ['A']
    try:
        test_subject = Polarization(arguments)
        assert False, 'Should raise ValueError'
    except ValueError:
        assert True

    arguments = [PolarizationState.I, PolarizationState.V]
    test_subject = Polarization(arguments)
    assert test_subject is not None
    states = test_subject.states
    assert states is not None
    assert len(states) == 2
    assert states[0] == PolarizationState.I
    assert states[1] == PolarizationState.V

    arguments = ''
    try:
        test_subject = Polarization.parse_polarization(arguments)
        assert False, 'Should raise ValueError'
    except ValueError:
        assert True

        arguments = 'A'
    try:
        test_subject = Polarization.parse_polarization(arguments)
        assert False, 'Should raise ValueError'
    except ValueError:
        assert True

    arguments = 'I V'
    test_subject = Polarization.parse_polarization(arguments)
    assert test_subject is not None
    states = test_subject.states
    assert states is not None
    assert len(states) == 2
    assert states[0] == PolarizationState.I
    assert states[1] == PolarizationState.V
