import logging
import os
import re
import traceback
import subprocess
from urllib import parse

HTTP_200_OK = '200 OK'
HTTP_400_BAD_REQUEST = '400 BAD_REQUEST'
HTTP_404_NOT_FOUND = '404 NOT_FOUND'
HTTP_500_INTERNAL_SERVER_ERROR = '500 SERVER_ERROR'
LOGGER = logging.getLogger('cadccutout')


def application(env, start_response):
    request_uri = env['REQUEST_URI']
    param_source = dict(parse.parse_qs(parse.urlsplit(request_uri).query))

    try:
        infile = param_source['file'][0]
    except KeyError:
        return b'Missing Parameter "file".\n\nThe "file" parameter is \
required (i.e. file=file.fits).\n\n', HTTP_400_BAD_REQUEST
    debug_flag = param_source.get('debug', False)
    verbose_flag = param_source.get('verbose', False)
    logical_filename = param_source.get('fileid', '')
    head_request = param_source.get('head', False)
    cutouts = param_source.get('cutout', [])

    if verbose_flag and verbose_flag not in [
            'False', 'false', 'no', 'n', False]:
        level = logging.INFO
    elif debug_flag and debug_flag not in ['False', 'false', 'no', 'n', False]:
        level = logging.DEBUG
    else:
        level = logging.WARN

    LOGGER.setLevel(level)

    if logical_filename.endswith('.gz') or logical_filename.endswith('.fz'):
        output_file = logical_filename[:-3]
    else:
        output_file = logical_filename

    # Make the output file name:
    #  {everything up to the last dot}.{hashvalue}.{everthing after the last
    #   dot}
    # This should turn something like x.fits into x.<hash>.fits
    # where hash is the cutout with all non-alphanumeric replaced with '_'
    # and the leading and trailing '_' removed.
    if cutouts:
        cutout_key = re.sub('[^0-9a-zA-Z]', '_', ' '.join(cutouts))

        # Remove the leading and end underscores.
        cutout_key = re.sub('^_|_$', '', cutout_key)
        path_items = os.path.splitext(output_file)
        if path_items and len(path_items) > 1:
            output_file = '{}.{}{}'.format(
                path_items[0], cutout_key, path_items[1])
        else:
            output_file = '{}.{}'.format(path_items[0], cutout_key)

        try:
            if head_request is True:
                LOGGER.info('HEAD REQUEST.')
                return [b'HEAD Request'], HTTP_200_OK
            else:
                cutout_str = ''.join(cutouts)
                LOGGER.debug('Cutting {} out of {}.'.format(cutout_str,
                                                            infile))
                start_response(
                    HTTP_200_OK, [('Content-Type', 'application/fits')])

                prog = ['python', '-m', 'cadccutout.core', '--infile',
                        '{}'.format(infile), '{}'.format(cutout_str)]
                proc = subprocess.Popen(prog, stdout=subprocess.PIPE)
                try:
                    outs, _errs = proc.communicate(timeout=15)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    outs, _errs = proc.communicate()
                return outs

        except FileNotFoundError as fnfe:
            msg = 'FileNotFoundError: {}'.format(str(fnfe))
            LOGGER.error(msg)
            return msg, HTTP_404_NOT_FOUND
        except ValueError as ve:
            msg = 'ValueError: {}'.format(str(ve))
            LOGGER.error(msg)
            traceback.print_exc()
            return msg, HTTP_400_BAD_REQUEST
        except Exception as e:
            msg = 'Exception: {}'.format(str(e))
            LOGGER.error(msg)
            return msg, HTTP_500_INTERNAL_SERVER_ERROR
    else:
        return '\n\nNo cutouts specified.  \
Use cutout=[XXX]&cutout=[YYY].\n\n', HTTP_400_BAD_REQUEST
