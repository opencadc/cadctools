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

"""Contract tests for cadc-tap CLI parsers."""

import warnings

import pytest

from cadctap.core import DEFAULT_SERVICE_ID, build_parser
from cadcutils.util.tests.parser_helpers import (
    assert_has_base_dests, assert_has_dests, assert_description_contains,
    assert_epilog_contains, assert_help_contains, get_subparser,
    subparser_names,
)

_SUBCOMMANDS = (
    'schema', 'query', 'create', 'delete', 'index', 'load', 'permission',
)

_AUTH_SNIPPETS = (
    'cadc-tap will determine the',
    '~/.netrc',
    'cadcproxy.pem',
    '--anon',
)


@pytest.fixture
def root_parser():
    return build_parser()


def test_root_parser_contract(root_parser):
    assert set(subparser_names(root_parser)) == set(_SUBCOMMANDS)
    assert_help_contains(
        root_parser,
        'TAP protocol',
        'Canadian Astronomy Data Centre',
    )


@pytest.mark.parametrize('subcmd,extra_dests', [
    ('schema', ('tablename',)),
    ('query', ('output_file', 'maxrec', 'QUERY', 'input_file', 'timeout',
               'format', 'tmptable')),
    ('create', ('format', 'TABLENAME', 'TABLEDEFINITION')),
    ('delete', ('TABLENAME',)),
    ('index', ('unique', 'TABLENAME', 'COLUMN')),
    ('load', ('format', 'TABLENAME', 'SOURCE')),
    ('permission', ('MODE', 'TARGET', 'GROUPS')),
])
def test_subcommand_parser_contract(root_parser, subcmd, extra_dests):
    parser = get_subparser(root_parser, subcmd)
    assert_has_base_dests(parser, use_service=True)
    assert_has_dests(parser, *extra_dests)
    assert_help_contains(parser, DEFAULT_SERVICE_ID)
    if subcmd != 'permission':
        for snippet in _AUTH_SNIPPETS:
            assert snippet in (parser.description or '')


def test_subcommand_descriptions(root_parser):
    for subcmd, expected in (
        ('schema', ('Print the tables available for querying',)),
        ('query', ('Run an adql query',)),
        ('create', ('Create a table',)),
        ('delete', ('Delete a table',)),
        ('index', ('create a table index',)),
        ('load', ('load data to a table',)),
        ('permission', (
            'Update access permissions',
            'schema command to display the existing permissions',
        )),
    ):
        parser = get_subparser(root_parser, subcmd)
        assert_description_contains(parser, *expected)


def test_query_epilog(root_parser):
    parser = get_subparser(root_parser, 'query')
    assert_epilog_contains(
        parser,
        'Examples:',
        'Anonymously run a query string',
        'cadc-tap query -a -s tap',
        'Use certificate to run a query',
        '--cert ~/.ssl/cadcproxy.pem',
        'Use username/password to run a query',
        'ivo://cadc.nrc.ca/tap',
        'Use netrc file to run a query',
        'ivo://cadc.nrc.ca/ams/mast',
        'caom2.Observation',
    )


def test_permission_parser_dests(root_parser):
    parser = get_subparser(root_parser, 'permission')
    assert_has_dests(parser, 'anon')

def test_query_format_deprecated_votable_warns(root_parser):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        args = root_parser.parse_args(
            ['query', '-a', 'SELECT 1', '-f', 'VOTable'])
    assert args.format == 'votable'
    assert len(caught) == 1
    assert issubclass(caught[0].category, DeprecationWarning)
    assert 'VOTable' in str(caught[0].message)
