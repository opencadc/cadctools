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

import errno
import logging
import os
import sys
import configparser
from shutil import copyfile


logger = logging.getLogger('config')
logger.setLevel(logging.INFO)

if sys.version_info[1] > 6:
    logger.addHandler(logging.NullHandler())


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


class Config(object):

    def __init__(self, config_path, default_config_path=None):
        logger.info("Using config file {0}.".format(config_path))

        # check config file exists and can be read
        if not os.path.isfile(config_path) and not os.access(config_path,
                                                             os.R_OK):
            error = "Can not read {0}.".format(config_path)
            logger.debug(error)
            raise IOError(error)

        self.parser = configparser.ConfigParser()
        if default_config_path:
            try:
                self.parser.read_file(open(default_config_path))
            except configparser.Error as exc:
                logger.debug("Error opening {0} because {1}.".format(
                    default_config_path, exc.message))

        try:
            self.parser.read(config_path)
        except configparser.Error as exc:
            logger.debug("Error opening {0} because {1}.".format(
                config_path, exc.message))

    def get(self, section, option):
        try:
            return self.parser.get(section, option)
        except (configparser.NoOptionError, configparser.NoSectionError):
            pass
        return None

    @staticmethod
    def write_config(config_path, default_config_path):
        """
        :param config_path:
        :param default_config_path:
        :return:
        """

        # if not local config file then write the default config file
        if not os.path.isfile(config_path):
            mkdir_p(os.path.dirname(config_path))
            copyfile(default_config_path, config_path)
            return

        # read local config file
        parser = configparser.ConfigParser()
        try:
            parser.read(config_path)
        except configparser.Error as exc:
            logger.debug("Error opening {0} because {1}.".format(config_path,
                                                                 exc.message))
            return

        # read default config file
        default_parser = configparser.RawConfigParser()
        try:
            default_parser.read(default_config_path)
        except configparser.Error as exc:
            logger.debug("Error opening {0} because {1}.".format(
                default_config_path, exc.message))
            return

        # update config file with new options from the default config
        updated = False
        for section in default_parser.sections():
            default_items = default_parser.items(section)
            for option, value in default_items:
                if not parser.has_section(section):
                    parser.add_section(section)
                if not parser.has_option(section, option):
                    parser.set(section, option, value)
                    updated = True

        # remove old options not in the default config file?
        # for section in default_parser.sections():
        #     options = parser.options(section)
        #     for option in options:
        #         if not default_parser.has_option(section, option):
        #             parser.remove_option(section, option)

        # write updated config file
        if updated:
            try:
                config_file = open(config_path, 'w')
                parser.write(config_file)
                config_file.close()
            except Exception as exc:
                print(exc.message)
