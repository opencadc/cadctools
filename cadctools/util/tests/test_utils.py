#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2014.                            (c) 2014.
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

from cadctools.util import *
import unittest
import logging
import sys
from six import StringIO


class UtilTests(unittest.TestCase):

    ''' Class for testing cadc utilities'''


    def test_ivoa_dates(self):
        ''' Test the ivoa date formats functions date2ivoa and str2ivoa'''

        expect_date = '2016-11-07T11:22:33.123'
        self.assertEquals(expect_date, date2ivoa(str2ivoa(expect_date)))

        with self.assertRaises(ValueError):
            str2ivoa('2011-01-01')

        # handling of None arguments
        self.assertEquals(None, date2ivoa(None))
        self.assertEquals(None, str2ivoa(None))
        
    def test_get_log_level(self):
        ''' Test the handling of logging level control from command line arguments '''
        
        parser = BaseParser(description="UtilTests")
        args = parser.parse_args(["--debug"])        
        self.assertEqual(logging.DEBUG, parser.get_log_level(args))
        
        args = parser.parse_args(["--verbose"])        
        self.assertEqual(logging.INFO, parser.get_log_level(args))
        
        args = parser.parse_args(["--quiet"])        
        self.assertEqual(logging.FATAL, parser.get_log_level(args))
        
        args = parser.parse_args([])        
        self.assertEqual(logging.ERROR, parser.get_log_level(args))
        
        print ("passed log level tests")

        
    def test_init_logging_debug(self):
        ''' Test the init_logging function '''
        logger1 = get_logger('namspace1', log_level=logging.DEBUG)
        self.assertEquals(logging.DEBUG, logger1.getEffectiveLevel())
        logger2 = get_logger('namspace2')
        self.assertEquals(logging.ERROR, logger2.getEffectiveLevel())
        
        
    def test_shared_logger(self):
        ''' Loggers with the same namespace share the
            same logging instances '''
        logger1 = get_logger('namspace1', log_level=logging.DEBUG)
        self.assertEquals(logging.DEBUG, logger1.getEffectiveLevel())
        logger2 = get_logger('namspace1', log_level=logging.WARN)
        self.assertEquals(logging.WARN, logger1.getEffectiveLevel())
        self.assertEquals(logging.WARN, logger2.getEffectiveLevel())
        logger3 = get_logger('namspace2', log_level=logging.INFO)
        self.assertEquals(logging.INFO, logger3.getEffectiveLevel())
        self.assertEquals(logging.WARN, logger1.getEffectiveLevel())
        self.assertEquals(logging.WARN, logger2.getEffectiveLevel())
        
        
    def test_modify_log_level(self):
        logger = get_logger('test_modify_log_level', log_level=logging.INFO)
        self.assertEquals(logging.INFO, logger.getEffectiveLevel())
        logger = get_logger('test_modify_log_level', log_level=logging.DEBUG)
        self.assertEquals(logging.DEBUG, logger.getEffectiveLevel())
        logger.setLevel(logging.ERROR)
        self.assertEquals(logging.ERROR, logger.getEffectiveLevel())
        
        
    def test_modname_log_format(self):
        
        try:
            stdout_pointer = sys.stdout
            sys.stdout = StringIO()     # capture output
            logger = get_logger()
            logger.error('Test message')
            out = sys.stdout.getvalue() # release output
            self.assertTrue('test_utils' in out)
        finally:
            sys.stdout.close()  # close the stream 
            sys.stdout = stdout_pointer # restore original stdout
        
        
    def test_info_log_format(self):
        
        try:
            stdout_pointer = sys.stdout
            sys.stdout = StringIO()     # capture output
            logger = get_logger('test_info_log_format', log_level=logging.INFO)
            logger.info('Test message')
            out = sys.stdout.getvalue() # release output
            self.assertTrue('INFO' in out)
            self.assertTrue('test_info_log_format' in out)
            self.assertTrue('Test message' in out)
        finally:
            sys.stdout.close()  # close the stream 
            sys.stdout = stdout_pointer # restore original stdout
        
        
    def test_debug_log_format(self):
        
        try:
            stdout_pointer = sys.stdout
            sys.stdout = StringIO()     # capture output
            logger = get_logger('test_debug_log_format_namespace', log_level=logging.DEBUG)
            logger.debug('Test message')
            out = sys.stdout.getvalue() # release output
            self.assertTrue('DEBUG' in out)
            self.assertTrue('test_debug_log_format_namespace' in out)
            self.assertTrue('test_utils.test_debug_log_format:' in out)
            self.assertTrue('Test message' in out)
        finally:
            sys.stdout.close()  # close the stream 
            sys.stdout = stdout_pointer # restore original stdout