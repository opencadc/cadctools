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
#  General Public License for           Générale Publique GNU Affero
#  more details.                        pour plus de détails.
#
#  You should have received             Vous devriez avoir reçu une
#  a copy of the GNU Affero             copie de la Licence Générale
#  General Public License along         Publique GNU Affero avec
#  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
#  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
#                                       <http://www.gnu.org/licenses/>.
#
#
# ***********************************************************************

#
# Acknowledgment: This code is based on the code developed by the
# East Asian Observatory for the JCMT Science Archive
#

import logging
import os.path
import re

from lxml import etree

logger = logging.getLogger(__name__)

namecheck_section = set(('RAW', 'PROCESSED'))
namecheck_pattern = None


def check_file_name(namecheck_file, filename, return_section=False):
    """Test whether the file name has an acceptable name.

    The upper-cased suffix-removed file name is matched against
    all of the namecheck patterns.  If a match is found, True
    is returned.  Otherwise False is returned.

    If the return_section argument is set then the matching
    section of the namecheck configuration or None is returned
    instead.
    :param namecheck_file -- file containing the name check rules
    :param filename -- name of the file to check
    :param return_section -- if True, the matching section is returned
    """

    (base, ext) = os.path.splitext(filename)

    for (key, patterns) in _get_namecheck_pattern(namecheck_file).items():
        for pattern in patterns:
            if pattern.match(base):
                # Pattern matched: decide whether to return the section
                # key or just True.
                if return_section:
                    return key
                return True

    if return_section:
        return None

    return False


def _get_namecheck_pattern(namecheck_file):
    """Get a list of namecheck patterns.

    Returns a cached version if available, otherwise reads the namecheck
    configuration file.  The patterns are listed as regular expression
    objects.
    :param namecheck_file -- File containing the name check rules
    """

    global namecheck_pattern

    # If we already read the configuration, return it.
    if namecheck_pattern is not None:
        return namecheck_pattern
    # Otherwise read the namecheck XML file.
    namecheck_pattern = {}
    tree = etree.parse(namecheck_file)
    root = tree.getroot()
    for outerlist in root.iter('list'):
        for struct in outerlist.iter('struct'):
            for list in struct.iter('list'):
                key = list.get('key')
                if key in namecheck_section:
                    logger.debug('Reading namecheck section {}'.format(key))
                    namecheck_pattern[key] = []
                    for value in list.iter('value'):
                        namecheck_pattern[key].append(
                            re.compile('^{0}$'.format(value.text)))
                else:
                    logger.debug('Skipping namecheck section {}'.format(key))
    return namecheck_pattern
