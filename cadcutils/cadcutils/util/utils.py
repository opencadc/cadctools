# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2016.                            (c) 2016.
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
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import logging
import os
import sys
import inspect
from argparse import ArgumentParser
from datetime import datetime
from six.moves.urllib.parse import urlparse

__all__ = ['IVOA_DATE_FORMAT', 'date2ivoa', 'str2ivoa',
           'get_logger', 'get_log_level','get_base_parser']

# TODO both these are very bad, implement more sensibly
IVOA_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"

DEFAULT_LOG_FORMAT = "%(levelname)s: %(name)s %(message)s"
DEBUG_LOG_FORMAT = "%(levelname)s: @(%(asctime)s) %(name)s %(module)s.%(funcName)s:%(lineno)d - %(message)s"


def date2ivoa(d):
    """
    Takes a datetime and returns a string formatted
    to the IVOA date format yyyy-MM-dd'T'HH:mm:ss.SSS
    
    """

    if d is None:
        return None
    return d.strftime(IVOA_DATE_FORMAT)[:23]


def str2ivoa(s):
    """
    Takes a IVOA date formatted string and returns a datetime.
    
    """

    if s is None:
        return None
    return datetime.strptime(s, IVOA_DATE_FORMAT)


def get_logger(namespace=None, log_level=logging.ERROR):
    """
    Create a logger with a standard format.
    :param namespace: The namespace to which to attach the logger.
        default: the name of the calling module.
    :ptype log_level: The initial log level.
        default: logging.ERROR
    :return the logger object.
    
    """
    
    if namespace is None:
        frm = inspect.stack()[1]
        mod = inspect.getmodule(frm[0])
        if mod is None:
            namespace =__name__
        else:
            namespace = mod.__name__ 
    
    logger = logging.getLogger(namespace)
    
    if not len(logger.handlers):
        handler = logging.StreamHandler(sys.stdout)
        logger.addHandler(handler)
    else:
        handler = logger.handlers[0]
    
    log_format = DEFAULT_LOG_FORMAT
    if log_level == logging.DEBUG:
        log_format = DEBUG_LOG_FORMAT

    logger.setLevel(log_level)
    handler.setFormatter(logging.Formatter(fmt=log_format))
    
    return logger


def get_log_level(args):
    """
    Obtain a single logger level from parsed args.

    :param args The list of arguments.

    """

    log_level = ((args.debug and logging.DEBUG) or
                 (args.verbose and logging.INFO) or
                 (args.quiet and logging.FATAL) or
                 logging.ERROR)
    return log_level


def parse_resource_id(resource_id):
    """
    Parses a resource identifier and returns its components as a tuple:
    (scheme, netloc, path, params, query, fragment).
    :param resource_id: the resource identifier
    :return: (scheme, netloc, path, params, query, fragment)
    """
    result = urlparse(resource_id)
    if len(result.netloc) < 2 or len(result.path) < 2 or (result.scheme != 'ivo'):
        raise ValueError('Invalid resourceID: {}'.format(resource_id))
    return resource_id


def get_base_parser(version=None, usecert=True, default_resource_id=None):
    """
    An ArgumentParser with some common things most CADC clients will want.

    :param version: A version number if desired.
    :param usecert: If True add '--certfile' argument.
    :param default_resource_id: default resource identifier to use
    :return: An ArgumentParser instance.
    """
    parser = ArgumentParser(add_help=False)
    if usecert:
        parser.add_argument('--certfile', type=str,
                            help="location of your CADC certificate "
                            + "file (default: $HOME/.ssl/cadcproxy.pem, " +
                            "otherwise uses $HOME/.netrc for name/password)",
                            default=os.path.join(os.getenv("HOME", "."),
                                                 ".ssl/cadcproxy.pem"))
    parser.add_argument('--anonymous', action="store_true",
                        help='Force anonymous connection')
    parser.add_argument('--host', help="Base hostname for services - used mainly for testing " +
                                       "(default: www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca)",
                        default='www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca')
    if default_resource_id is None:
        parser.add_argument('--resourceID', type=urlparse, required=True,
                            help="resource identifier (e.g. ivo://cadc.nrc.ca/caom2repo")
    else:
        parser.add_argument('--resourceID', type=parse_resource_id,
                            default = default_resource_id,
                            help="resource identifier (default {})".format(default_resource_id))
    parser.add_argument('--verbose', action="store_true",
                        help='verbose messages')
    parser.add_argument('--debug', action="store_true",
                        help='debug messages')
    parser.add_argument('--quiet', action="store_true",
                        help='run quietly')

    if version is not None:
        parser.add_argument('--version', action='version', version=version)

    return parser
