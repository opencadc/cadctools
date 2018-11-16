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

from aenum import Enum

logger = logging.getLogger(__name__)

__all__ = ['Circle', 'Polygon', 'Energy', 'Time', 'Polarization',
           'PolarizationState']


class Circle(object):

    AXIS_TYPE = 'POSITION'
    NAME = 'CIRCLE'

    def __init__(self, ra, dec, radius):
        """
        Circle must have 3 parameters, RA, Dec, and radius.

        :param ra: float    Right Ascension
        :param dec: float   Declination
        :param radius: float    Radius of the circle
        """

        self.ra = ra
        self.dec = dec
        self.radius = radius

    @staticmethod
    def parse_circle(coordinates):
        """
        Parse a string of RA, Dec, and radius into a Circle.

        :param coordinates: str Space delimited RA, Dec, and radius
        :return: Circle instance
        """

        if not coordinates:
            raise ValueError('Empty or None coordinate value')
        values = coordinates.split()

        # Circle must have 3 values
        if len(values) < 3:
            raise ValueError('Circle requires a RA, Dec, and radius')

        return Circle(float(values[0]), float(values[1]), float(values[2]))


class Polygon(object):

    AXIS_TYPE = 'POSITION'
    NAME = 'POLYGON'

    def __init__(self, vertices):
        """
        A Polygon is at a minimum 3 pairs of coordinates (vertices).

        :param vertices: List   List of float of coordinate pairs
        """

        # Polygon must have a minimum of 6 values (3 pairs of coordinates)
        if not vertices or len(vertices) < 3:
            raise ValueError(
                'Polygon requires a minimum of 3 vertices (3 coordinate pairs)')

        self.vertices = vertices

    @staticmethod
    def parse_polygon(coordinates):
        """
        Parse a string of coordinates into a Polygon.

        :param coordinates: str   Space delimited string of coordinates
        :return: Polygon instance
        """

        if not coordinates:
            raise ValueError('Empty or None coordinate value')

        # Polygon must have a minimum of 6 values (3 pairs of coordinates)
        values = coordinates.split()
        if len(values) < 6:
            raise ValueError('Polygon requires a minimum of 6 coordinates')

        # parse coordinates into pairs
        vertices = []
        for i in range(0, len(values), 2):
            vertices.append([float(values[i]), float(values[i + 1])])
        return Polygon(vertices)


class Energy(object):

    AXIS_TYPE = 'SPECTRAL'
    NAME = 'BAND'

    def __init__(self, lower, upper):
        """
        Create an Energy instance from an interval
        with the given lower and upper bounds.

        :param lower: float Lower energy value
        :param upper: float Upper energy value
        """

        self.lower = lower
        self.upper = upper

    @staticmethod
    def parse_energy(bounds):
        """
        Parse a string of lower and upper bounds into an Energy.

        :param bounds: str   Space delimited string of lower and upper bounds
        :return: Energy instance
        """

        if not bounds:
            raise ValueError('Empty or None bounds value')

        values = bounds.split()
        if len(values) != 2:
            raise ValueError('Energy requires a lower and upper bounds')

        return Energy(float(values[0]), float(values[1]))


class Time(object):

    AXIS_TYPE = 'TEMPORAL'
    NAME = 'TIME'

    def __init__(self, lower, upper):
        """
        Create a Time instance from an interval
        with the given lower and upper bounds.

        :param lower: float Lower time value
        :param upper: float Upper time value
        """

        self.lower = lower
        self.upper = upper

    @staticmethod
    def parse_time(bounds):
        """
        Parse a string of lower and upper bounds into a Time.

        :param bounds: str   Space delimited string of lower and upper bounds
        :return: Time instance
        """

        if not bounds:
            raise ValueError('Empty or None bounds value')

        values = bounds.split()
        if len(values) != 2:
            raise ValueError('Time requires a lower and upper bounds')

        return Time(float(values[0]), float(values[1]))


class Polarization(object):

    AXIS_TYPE = 'POLARIZATION'
    NAME = 'POL'

    def __init__(self, states):
        """
        Create a Polarization instance.

        :param states: List List of PolarizationState
        """

        # Polarization must have at least one state
        if not states:
            raise ValueError('Polarization must have one or more states')

        # validate states
        for state in states:
            if not isinstance(state, PolarizationState):
                raise ValueError('Invalid PolarizationState {}'.format(state))

        self.states = states

    @staticmethod
    def parse_polarization(polarization_states):
        """
        Parse a string of polarization states into a Polarization.

        :param polarization_states: str   Space delimited string
                                          of polarization states
        :return: Polarization instance
        """

        if not polarization_states:
            raise ValueError('Empty or None polarization_states value')

        # validate states
        states = polarization_states.split()
        known_states = []
        unknown_states = []
        valid_states = [e.name for e in PolarizationState]
        for state in states:
            if state not in valid_states:
                unknown_states.append(state)
            else:
                known_states.append(PolarizationState[state])
        if unknown_states:
            raise ValueError('Unknown polarization states {}'
                             .format(unknown_states))

        # Polarization must have at least one state
        if not known_states:
            raise ValueError('Polarization must have one or more states')

        return Polarization(known_states)


class PolarizationState(Enum):
    """
    Enum of polarization states.
    """
    I = 1   # noqa: E741
    Q = 2
    U = 3
    V = 4
    POLI = 5
    FPOLI = 6
    POLA = 7
    EPOLI = 8
    CPOLI = 9
    NPOLI = 10
    RR = -1
    LL = -2
    RL = -3
    LR = -4
    XX = -5
    YY = -6
    XY = -7
    YX = -8
