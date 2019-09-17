import math
from astropy.io.fits import Header
from astropy.wcs import WCS

__all__ = ['to_num', 'is_integer', 'is_string', 'to_astropy_header']


def to_num(s):
    '''
    Convert the given value to an integer, if applicable, or a float.
    '''
    try:
        return int(s)
    except ValueError:
        return float(s)


def is_string(s):
    '''
    Determine if the given argument is a String.
    '''
    return s == str(s)


def is_integer(s):
    '''
    Determine if the given argument is an Integer.
    '''
    try:
        int(s)
        return True
    except ValueError:
        return False


def to_astropy_header(header_dict):
    '''
    Build an AstroPy header instance, filtering out empty header cards or
    empty values.

    This function will support a fitsio FITSHDR object or a regular dictionary.

    This assumes a FITS file.
    '''

    comment_keyword = 'COMMENT'
    history_keyword = 'HISTORY'

    astropy_header = Header()

    for key in header_dict:
        if key:
            value = header_dict[key]

            if value:
                if key == comment_keyword:
                    str_value = str(value).replace('\n', '  ')
                    astropy_header.add_comment(str_value)
                elif key == history_keyword:
                    str_value = str(value).replace('\n', '  ')
                    astropy_header.add_history(str_value)
                else:
                    astropy_header[key] = value

    return astropy_header


def get_header_value(hdr, key):
    if key in hdr:
        return hdr[key]
    else:
        return None


def to_astropy_wcs(header_dict):
    '''
    Obtain an AstroPy WCS instance for the given header dictionary.
    '''
    naxis_value = header_dict['NAXIS']

    if naxis_value is not None and int(naxis_value) > 0:
        for nax in range(1, naxis_value + 1):
            next_ctype_key = 'CTYPE{0}'.format(nax)
            if next_ctype_key in header_dict:
                ctype = header_dict['CTYPE{0}'.format(nax)]
                if ctype is not None and ctype.endswith('-SIP'):
                    naxis = 2
                    break
                else:
                    naxis = None
            else:
                naxis = None
    else:
        naxis = naxis_value

    # return WCS(header=to_astropy_header(header_dict), naxis=naxis, fix=False)
    return WCS(header=header_dict, naxis=naxis, fix=False)
