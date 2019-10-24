import logging
import os
import re
from io import BufferedRandom, BytesIO
import traceback
import subprocess
from ntpath import basename
from urllib import parse
from cadccutout.core import OpenCADCCutout

HTTP_200_OK = '200 OK'
HTTP_400_BAD_REQUEST = '400 BAD_REQUEST'
HTTP_404_NOT_FOUND = '404 NOT_FOUND'
HTTP_500_INTERNAL_SERVER_ERROR = '500 SERVER_ERROR'
LOGGER = logging.getLogger('cadccutout')


class BlockedOutputWriter(BufferedRandom):
    """
    Stream implementation to seem like a seekable stream.  It is meant to wrap
    the sys.stdout stream so that when Astropy calls the tell() method it
    will have an accurate place to start writing the stream.

    :param raw: file or file-like object.  The Raw underlying stream.
    """

    BLOCK_SIZE = 16384

    def __init__(self, raw):
        super(BlockedOutputWriter, self).__init__(BytesIO())
        self._raw = raw
        self.write_offset = 0
        self.read_offset = 0
        self.block_buffer = []

    def read(self, size=1):
        raise ValueError('Unreadable stream.  This is write only.')

    def _write_buffer(self):
        print('BLOCK {}'.format(len(self.block_buffer)))
        if callable(getattr(self._raw, 'write', None)):
            written = self._raw.write(bytes(self.block_buffer))
        else:
            self._raw = self._raw + bytes(self.block_buffer)
            written = len(self.block_buffer)
        if written:
            self.write_offset += written
        self.block_buffer = []

    def write(self, data):
        if data is None:
            # Should never happen, but it could indicate EOF.
            self._write_buffer()
        else:
            data_byte_arr = bytearray(data)
            for b in data_byte_arr:
                self.block_buffer.append(b)

                if len(self.block_buffer) == self.BLOCK_SIZE:
                    self._write_buffer()

        return self.write_offset

    def close(self):
        self._write_buffer()
        if callable(getattr(super, 'close', None)):
            super.close()
        else:
            self._raw = None

    def tell(self):
        return self.write_offset

    def seek(self, offset):
        raise ValueError('Unseekable stream.  This is write only.')


def to_bool(val):
    print('Checking {}'.format(val))
    return val is not None and val in [
        'True', 'true', 't', 'T', '1', 'y', 'Y', 'yes', 'on', 1]


def get_first_value(param_source, key, default_value=None):
    # Enforce a KeyError when the key is required.
    if default_value is None:
        key_value = param_source[key]
    else:
        key_value = param_source.get(key, default_value)

    if key_value and isinstance(key_value, list):
        return key_value[0]
    else:
        return key_value


def application(env, start_response):
    request_uri = env['REQUEST_URI']
    param_source = dict(parse.parse_qs(parse.urlsplit(request_uri).query))

    try:
        infile = get_first_value(param_source, 'file')
    except KeyError:
        start_response(HTTP_400_BAD_REQUEST, [])
        return b'\n\nMissing Parameter "file".\n\nThe "file" parameter is \
required (i.e. file=file.fits).\n\n'

    debug_flag = to_bool(get_first_value(param_source, 'debug', False))
    verbose_flag = to_bool(get_first_value(param_source, 'verbose', False))
    logical_filename = get_first_value(param_source, 'fileid',
                                       basename(infile))
    use_stream_flag = to_bool(get_first_value(param_source, 'streamout', False))
    head_request = to_bool(get_first_value(param_source, 'head', False))
    cutouts = get_first_value(param_source, 'cutout', [])

    if verbose_flag and verbose_flag not in [
            'False', 'false', 'no', 'n', False]:
        level = logging.INFO
    elif debug_flag and debug_flag not in ['False', 'false', 'no', 'n', False]:
        level = logging.DEBUG
    else:
        level = logging.WARN

    logging.basicConfig(level=level)
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
                    HTTP_200_OK, [('Content-Type', 'application/fits'),
                                  ('Content-Disposition', output_file)])

                # prog = ['python', '-m', 'cadccutout.core']
                # if debug_flag is True:
                #     prog.append('-d')
                # elif verbose_flag is True:
                #     prog.append('-v')
                # prog += ['--infile', '{}'.format(infile)]
                # if use_stream_flag is True:
                #     prog += ['--outfile', 'stream://']
                # prog.append('{}'.format(cutout_str))
                # print('Running {}'.format(' '.join(prog)))
                # proc = subprocess.Popen(prog, stdout=subprocess.PIPE)
                # try:
                #     outs, errs = proc.communicate(timeout=15)
                #     LOGGER.error(
                #         'Error while trying to cutout: {}'.format(errs))
                # except subprocess.TimeoutExpired:
                #     proc.kill()
                #     outs, _errs = proc.communicate()
                # return outs

        except FileNotFoundError as fnfe:
            msg = 'FileNotFoundError: {}'.format(str(fnfe))
            start_response(HTTP_404_NOT_FOUND, [])
            LOGGER.error(msg)
            return msg
        except ValueError as ve:
            msg = 'ValueError: {}'.format(str(ve))
            LOGGER.error(msg)
            traceback.print_exc()
            start_response(HTTP_400_BAD_REQUEST, [])
            return msg
        except Exception as e:
            msg = 'Exception: {}'.format(str(e))
            LOGGER.error(msg)
            start_response(HTTP_500_INTERNAL_SERVER_ERROR, [])
            return msg
    else:
        start_response(HTTP_400_BAD_REQUEST, [])
        return b'\n\nNo cutouts specified. Use cutout=[XXX]&cutout=[YYY].\n\n'
