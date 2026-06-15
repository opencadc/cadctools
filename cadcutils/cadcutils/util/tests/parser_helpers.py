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

"""Helpers for testing argparse CLI contracts without golden help files."""

from __future__ import annotations

import argparse


def option_dests(parser):
    """
    Return ``dest`` names for user-facing parser actions.
    """
    return {
        action.dest
        for action in parser._actions
        if action.dest not in (None, argparse.SUPPRESS, 'help')
    }


def get_subparser(root_parser, name):
    """
    Return a named subparser from a root parser built with subcommands.
    """
    for action in root_parser._actions:
        choices = getattr(action, 'choices', None)
        if choices is not None and name in choices:
            return choices[name]
    raise KeyError('No subparser named {!r}'.format(name))


def subparser_names(root_parser):
    """
    Return sorted subcommand names registered on the root parser.
    """
    for action in root_parser._actions:
        choices = getattr(action, 'choices', None)
        if choices is not None:
            return sorted(choices.keys())
    return []


def assert_has_dests(parser, *dests):
    """
    Assert that the parser exposes all expected option/positional dests.
    """
    available = option_dests(parser)
    missing = [dest for dest in dests if dest not in available]
    assert not missing, (
        'Missing parser dest(s) {} (have {})'.format(missing, sorted(available)))


def assert_help_contains(parser, *strings):
    """
    Assert that formatted help includes each expected substring.
    """
    help_text = parser.format_help()
    missing = [text for text in strings if text not in help_text]
    assert not missing, (
        'Help text missing: {}\n--- help ---\n{}'.format(
            missing, help_text))


def assert_description_contains(parser, *strings):
    """
    Assert that the parser description includes each expected substring.
    """
    description = parser.description or ''
    missing = [text for text in strings if text not in description]
    assert not missing, (
        'Description missing: {}\n--- description ---\n{}'.format(
            missing, description))


def assert_epilog_contains(parser, *strings):
    """
    Assert that the parser epilog (via format_help) includes each substring.
    """
    epilog = parser.epilog or ''
    help_text = parser.format_help()
    missing = [
        text for text in strings
        if text not in epilog and text not in help_text
    ]
    assert not missing, (
        'Epilog/help missing: {}\n--- epilog ---\n{}\n--- help ---\n{}'.format(
            missing, epilog, help_text))


_INSECURE_HELP_SNIPPET = 'skip SSL server certificate verification'


def assert_has_base_dests(parser, *, use_service=False, usecert=True):
    """
    Assert dests and -k/--insecure help from :func:`cadcutils.util.utils.get_base_parser`.
    """
    expected = ['n', 'netrc_file', 'user', 'token', 'host', 'insecure',
                'debug', 'quiet', 'verbose']
    if usecert:
        expected.append('cert')
    if use_service:
        expected.append('service')
    else:
        expected.append('resource_id')
    assert_has_dests(parser, *expected)
    assert_help_contains(
        parser, _INSECURE_HELP_SNIPPET, 'not recommended')
