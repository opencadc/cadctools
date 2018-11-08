import argparse
import logging
import sys
import io
from cadccutout.core import OpenCADCCutout, APP_NAME


def main_app():
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

    parser.add_argument('--type', '-t', choices=['FITS'],
                        default='FITS',
                        help='Optional file type.  Defaults to FITS.')
    parser.add_argument('--infile', '-i', type=argparse.FileType(mode='rb+'),
                        default=sys.stdin.buffer, nargs='?',
                        help='Optional input file.  Defaults to stdin.')
    parser.add_argument('--outfile', '-o', type=argparse.FileType(mode='ab+'),
                        default=sys.stdout.buffer, nargs='?',
                        help='Optional output file.  Defaults to stdout.')

    parser.add_argument(
        'cutout', help='The cutout region string.\n[0][200:400] for a cutout \
        of the 0th extension along the first axis', nargs=1)

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
    c.cutout_from_string(args.infile, args.outfile, args.cutout[0], args.type)


if __name__ == "__main__":
    main_app()
