# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2022.                            (c) 2022.
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

import configparser
import os
import tempfile
import shutil
import uuid
import unittest
from cadcutils.util import config

_ROOT = os.path.abspath(os.path.dirname(__file__))


def get_test_data_file_path(filename):
    return os.path.join(_ROOT, 'data', filename)


class TestConfig(unittest.TestCase):
    """Test the vos Config class.
    """

    def test_single_section_config(self):

        self.do_test('single-section-config', 'single-section-default-config')

    def test_multi_section_config(self):

        self.do_test('multi-section-config', 'multi-section-default-config')

    def do_test(self, config_filename, default_config_filename):

        default_config_path = tempfile.gettempdir() + '/' + str(uuid.uuid4())
        test_default_config = get_test_data_file_path(default_config_filename)
        shutil.copy(test_default_config, default_config_path)

        # no existing config file
        config_path = tempfile.gettempdir() + '/' + str(uuid.uuid4())

        config.Config.write_config(config_path, default_config_path)

        self.assertTrue(os.path.isfile(config_path))
        self.cmp_configs(config_path, default_config_path)

        # existing config file same as default config file
        config_path = tempfile.gettempdir() + '/' + str(uuid.uuid4())
        shutil.copy(test_default_config, config_path)

        config.Config.write_config(config_path, default_config_path)

        self.cmp_configs(config_path, default_config_path)

        # merge default and existing config files
        config_path = tempfile.gettempdir() + '/' + str(uuid.uuid4())

        test_config = get_test_data_file_path(config_filename)
        shutil.copy(test_config, config_path)

        config.Config.write_config(config_path, default_config_path)

        self.cmp_configs(config_path, default_config_path)

        # test get non-existing option
        self.assertEqual(None,
                         config.Config(config_path, default_config_path).
                         get('blah', 'blah'))

        # test error paths
        with self.assertRaises(IOError):
            config.Config('/non/existent/path')
        with self.assertRaises(IOError):
            config.Config(test_default_config,
                          default_config_path='/non/existen/path')

    def cmp_configs(self, config_path, default_config_path):

        parser = configparser.ConfigParser()
        parser.read(config_path)

        default_parser = configparser.ConfigParser()
        default_parser.read(default_config_path)

        for section in default_parser.sections():
            self.assertTrue(parser.has_section(section))
            for option in default_parser.options(section):
                self.assertTrue(parser.has_option(section, option))
