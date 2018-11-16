from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import argparse
import logging
import sys

from cadccutout.core import OpenCADCCutout, APP_NAME


def main_app():
    # Execute only if run as a script.
    parser = argparse.ArgumentParser()
    parser.description = ('Cutout library to extract an N-Dimension array.')
    parser.formatter_class = argparse.RawTextHelpFormatter

    # Python 3 uses the buffer property to treat stream data as binary.
    # Python 2 requires the -u command line switch.
    if hasattr(sys.stdin, 'buffer'):
        default_input = sys.stdin.buffer
    else:
        default_input = sys.stdin

    if hasattr(sys.stdout, 'buffer'):
        default_output = sys.stdout.buffer
    else:
        default_output = sys.stdout

    parser.add_argument('-d', '--debug', action='store_true',
                        help='debug messages')
    parser.add_argument('-q', '--quiet', action='store_true',
                        help='run quietly')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='verbose messages')

    parser.add_argument('--type', '-t', choices=['FITS'],
                        default='FITS',
                        help='Optional file type.  Defaults to FITS.')
    parser.add_argument('--infile', '-i', type=argparse.FileType(mode='rb+'),
                        default=default_input, nargs='?',
                        help='Optional input file.  Defaults to stdin.')
    parser.add_argument('--outfile', '-o', type=argparse.FileType(mode='ab+'),
                        default=default_output, nargs='?',
                        help='Optional output file.  Defaults to stdout.')

    parser.add_argument(
        'cutout', help='The cutout region string.\n[0][200:400] for a cutout \
        of the 0th extension along the first axis', nargs='+')

    args = parser.parse_args()
    if len(sys.argv) < 1:
        parser.print_usage(file=sys.stderr)
        sys.stderr.write("{}: error: too few arguments\n".format(APP_NAME))
        sys.exit(-1)
    if args.verbose:
        level = logging.INFO
    elif args.debug:
        level = logging.DEBUG
    else:
        level = logging.WARN

    logging.getLogger('cadccutout').setLevel(level)

    c = OpenCADCCutout()

    # Support multiple strings.  This will write out as many cutouts as
    # it finds.
    c.cutout_from_string(args.cutout, input_reader=args.infile,
                         output_writer=args.outfile, file_type=args.type)


if __name__ == "__main__":
    main_app()
