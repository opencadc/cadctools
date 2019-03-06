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
import sys
import io
from io import BufferedRandom
import argparse

from cadccutout.utils import is_string
from cadccutout import version
from cadccutout.file_helper import FileHelperFactory
from cadccutout.pixel_range_input_parser import PixelRangeInputParser

logger = logging.getLogger(__name__)

__all__ = ['OpenCADCCutout', 'WriteOnlyStream']


class OpenCADCCutout(object):
    """
    Main cutout class.  This is mainly used as a parent class for concrete
    instances, like from a FITS file, but can be called by itself if need be.

    Parameters
    ----------
    helper_factory : `.file_helper.FileHelperFactory`
        The Helper Factory instance to load a file helper appropriate to the
        file type.  Defaults to file_helper.FileHelperFactory().

    input_range_parser : `.pixel_range_input_parser.PixelRangeInputParser`
        Parser to parse the input string.  This defaults to the provided
        pixel_range_input_parser.PixelRangeInputParser() class.

    Example 1
    --------
    from cadccutout import OpenCADCCutout

    cutout = OpenCADCCutout()
    output_file = tempfile.mkstemp(suffix='.fits')
    input_file = '/path/to/file.fits'

    # Cutouts are in cfitsio format.
    cutout_region_string = '[300:800,810:1000]'  # HDU 0 along two axes.

    # Needs to have 'append' flag set.  The cutout() method will write out the
    # data.
    with open(output_file, 'ab+') as output_writer, open(input_file, 'rb') as
     input_reader:
        test_subject.cutout(input_reader, output_writer, cutout_region_string,
        'FITS')


    Example 2 (CADC)
    --------
    from cadccutout import OpenCADCCutout
    from cadcdata import CadcDataClient

    cutout = OpenCADCCutout()
    anonSubject = net.Subject()
    data_client = CadcDataClient(anonSubject)
    output_file = tempfile.mkstemp(suffix='.fits')
    archive = 'HST'
    file_name = 'n8i311hiq_raw.fits'
    input_stream = data_client.get_file(archive, file_name)

    # Cutouts are in cfitsio format.
    cutout_region_string = '[SCI,10][80:220,100:150]'  # SCI version 10, along
    two axes.

    # Needs to have 'append' flag set.  The cutout() method will write out the
    # data.
    with open(output_file, 'ab+') as output_writer:
        test_subject.cutout(input_stream, output_writer, cutout_region_string,
        'FITS')
    """

    def __init__(self, helper_factory=FileHelperFactory(),
                 input_range_parser=PixelRangeInputParser()):
        self.helper_factory = helper_factory
        self.input_range_parser = input_range_parser

    def cutout(self, cutout_dimensions, input_reader=None,
               output_writer=None, file_type='FITS'):
        """
        Perform a Cutout of the given data at the given position and size.

        Parameters
        ----------
        input_reader: File-like object, Reader stream
            The file location.  The file extension is important as it's used to
            determine how to process it.

        output_writer: File-like object, Writer stream
            The writer to push the cutout array to.

        cutout_dimensions: List of PixelCutoutHDU or WCS Shape objects.
            The requested dimensions expressed as PixelCutoutHDU objects.

        file_type: string
            The file type, in upper case.  Will usually be 'FITS'.
        """

        if not cutout_dimensions or len(cutout_dimensions) == 0:
            raise ValueError('No Cutout regions specified.')

        if input_reader is None:
            # Python 3 uses the buffer property to treat stream data as binary.
            # Python 2 (sometimes) requires the -u command line switch.
            if hasattr(sys.stdin, 'buffer'):
                input_stream = sys.stdin.buffer
            else:
                input_stream = sys.stdin
        else:
            input_stream = input_reader

        if output_writer is None:
            # Python 3 uses the buffer property to treat stream data as binary.
            # Python 2 (sometimes) requires the -u command line switch.
            if hasattr(sys.stdout, 'buffer'):
                output_stream = sys.stdout.buffer
            else:
                output_stream = sys.stdout
        else:
            output_stream = output_writer

        file_helper = self._get_file_helper(
            file_type, input_stream, output_stream)

        try:
            file_helper.cutout(cutout_dimensions)
        except OSError as oe:
            raise ValueError(
                'Output target or input source unusable (Did you specify an '
                'input and output?).\n{}'.format(str(oe)))

    def _parse_input(self, input_cutout_dimensions):
        if self.input_range_parser.is_pixel_cutout(input_cutout_dimensions[0]):
            parsed_cutout_dimensions = self.input_range_parser.parse(
                input_cutout_dimensions[0])
        else:
            # Parse WCS into appropriate objects.
            parsed_cutout_dimensions = input_cutout_dimensions

        return parsed_cutout_dimensions

    def _sanity_check_input(self, cutout_dimensions_str):
        if is_string(cutout_dimensions_str):
            input_cutout_dimensions = [cutout_dimensions_str]
        elif not isinstance(cutout_dimensions_str, list) \
                or not cutout_dimensions_str:
            raise ValueError(
                'Input is expected to be a string or list but was {}'.format(
                    cutout_dimensions_str))
        else:
            input_cutout_dimensions = cutout_dimensions_str

        return input_cutout_dimensions

    def cutout_from_string(self, cutout_dimensions_str, input_reader=None,
                           output_writer=None, file_type='FITS'):
        """
        Perform a Cutout of the given data at the given position and size.

        Parameters
        ----------
        input_reader: File-like object, Reader stream
            The file location.  The file extension is important as it's used to
            determine how to process it.

        output_writer: File-like object, Writer stream
            The writer to push the cutout array to.

        cutout_dimensions_str: string or list of WCS coordinates, or extension
                            and pixel coordinates.  The requested dimensions
                            expressed as PixelCutoutHDU objects.

        file_type: string
            The file type, in upper case.  Will usually be 'FITS'.
        """

        input_cutout_dimensions = self._sanity_check_input(
            cutout_dimensions_str)

        self.cutout(self._parse_input(input_cutout_dimensions), input_reader,
                    output_writer, file_type)

    def _get_file_helper(self, file_type, input_reader, output_writer):
        return self.helper_factory.get_instance(file_type, input_reader,
                                                output_writer)


class WriteOnlyStream(BufferedRandom):
    """
    Stream implementation to seem like a seekable stream.  It is meant to wrap
    the sys.stdout stream so that when Astropy calls the tell() method it
    will have an accurate place to start writing the stream.

    :param raw: file or file-like object.  The Raw underlying stream.
    """

    def __init__(self, raw):
        super(WriteOnlyStream, self).__init__(io.BytesIO())
        self._raw = raw
        self.write_offset = 0
        self.read_offset = 0

    def read(self, size=1):
        raise ValueError('Unreadable stream.  This is write only.')

    def write(self, data):
        written = self._raw.write(data)
        if written:
            self.write_offset += written
        return self.write_offset

    def tell(self):
        return self.write_offset

    def seek(self, offset):
        raise ValueError('Unseekable stream.  This is write only.')


def main_app(argv=None):
    # Execute only if run as a script.
    parser = argparse.ArgumentParser()

    parser.description = ('Cutout library to extract an N-Dimension array.')
    parser.formatter_class = argparse.RawTextHelpFormatter

    parser.add_argument('-d', '--debug', action='store_true',
                        help='debug messages')
    parser.add_argument('-q', '--quiet', action='store_true',
                        help='run quietly')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='verbose messages')
    parser.add_argument('--version', action='version', version=version.version)

    parser.add_argument('--type', '-t', choices=['FITS'], default='FITS',
                        help='Optional file type.  Defaults to FITS.')
    parser.add_argument('--infile', '-i', type=argparse.FileType(mode='rb'),
                        nargs='?', default=None,
                        help='Optional input file.  Defaults to stdin.')
    parser.add_argument('--outfile', '-o', type=argparse.FileType(mode='ab+'),
                        nargs='?', default=None,
                        help='Optional output file.  Defaults to stdout.')

    parser.add_argument(
        'cutout', help='The cutout region string.\n[0][200:400] for a cutout \
        of the 0th extension along the first axis', nargs='+')

    args = parser.parse_args(args=argv)

    if not args:
        parser.print_usage(file=sys.stderr)
        sys.stderr.write("{}: error: too few arguments\n".format(__name__))
        sys.exit(-1)
    if args.verbose:
        level = logging.INFO
    elif args.debug:
        level = logging.DEBUG
    else:
        level = logging.WARN

    logging.basicConfig(level=level)
    logging.getLogger().setLevel(level)

    c = OpenCADCCutout()

    logging.info('Start cutout.')

    # Support multiple strings.  This will write out as many cutouts as
    # it finds.
    c.cutout_from_string(
        args.cutout, input_reader=args.infile,
        output_writer=args.outfile,
        file_type=args.type)


if __name__ == "__main__":
    try:
        main_app()
        sys.exit(0)
    except Exception as e:
        logging.error('{}'.format(str(e)))
        sys.exit(-1)
