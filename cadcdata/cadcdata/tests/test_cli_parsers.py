# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2026.                            (c) 2026.
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
#  General Public License for           Générale Publique GNU Affero pour
#  more details.                        plus de détails.
#
#  You should have received             Vous devriez avoir reçu une
#  a copy of the GNU Affero             copie de la Licence Générale
#  General Public License along         Publique GNU Affero avec
#  with OpenCADC.  If not, see          OpenCADC ; si ce n’est pas le cas,
#  <http://www.gnu.org/licenses/>.      consultez :
#                                       <http://www.gnu.org/licenses/>.
#
# ***********************************************************************

"""Contract tests for cadcdata CLI parsers."""

import pytest

from cadcdata.storageinv import (
    build_cadcget_parser, build_cadcput_parser,
    build_cadcinfo_parser, build_cadcremove_parser,
    DEFAULT_RESOURCE_ID,
)
from cadcutils.util.tests.parser_helpers import (
    assert_has_base_dests, assert_has_dests, assert_epilog_contains,
    assert_help_contains,
)


@pytest.mark.parametrize('build_parser,extra_dests,epilog_snippets', [
    (build_cadcget_parser, ('output', 'identifier', 'fhead'), (
        'Examples:',
        'cadcget GEMINI/N20220825S0383.fits',
        'cadcget --cert ~/.ssl/cadcproxy.pem',
        'cutout=[1][10:120,20:30]',
    )),
    (build_cadcput_parser, ('type', 'encoding', 'replace', 'identifier', 'src'), (
        'Examples:',
        'cadcput --cert ~/.ssl/cadcproxy.pem',
        'cadcput -v -n cadc:TEST/',
        'cadcput -v -u auser cadc:TEST/',
    )),
    (build_cadcinfo_parser, ('identifier',), (
        'Examples:',
        'cadcinfo CFHT/1000003f.fits.fz',
        'cadcinfo cadc:CFHT/1000003f.fits.fz',
    )),
    (build_cadcremove_parser, ('identifier',), (
        'Examples:',
        'cadcremove --cert ~/.ssl/cadcproxy.pem',
        'cadc:CFHT/700000o.fz',
    )),
])
def test_storage_cli_parser_contract(build_parser, extra_dests, epilog_snippets):
    parser = build_parser()
    assert_has_base_dests(parser, use_service=True)
    assert_has_dests(parser, *extra_dests)
    assert_help_contains(parser, DEFAULT_RESOURCE_ID)
    assert_epilog_contains(parser, *epilog_snippets)
