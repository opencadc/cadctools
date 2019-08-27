
from astropy.io.fits import Header

__all__ = ['to_num', 'is_integer', 'is_string', 'to_astropy_header']


def to_num(s):
    try:
        return int(s)
    except ValueError:
        return float(s)


def is_string(s):
    return s == str(s)


def is_integer(s):
    if isinstance(s, tuple):
        return False

    try:
        int(s)
        return True
    except ValueError:
        return False

def to_astropy_header(header_dict):
    _COMMENT_KEYWORD = 'COMMENT'
    _HISTORY_KEYWORD = 'HISTORY'

    """
    Build an AstroPy header instance, filtering out empty header cards or empty values.

    This function will support a fitsio FITSHDR object or a regular dictionary.

    This assumes a FITS file.
    """
    astropy_header = Header()

    for key in header_dict:
        if key:
            value = header_dict[key]

            if value:
                if key == _COMMENT_KEYWORD:
                    str_value = str(value).replace('\n', '  ')
                    astropy_header.add_comment(str_value)
                elif key == _HISTORY_KEYWORD:
                    str_value = str(value).replace('\n', '  ')
                    astropy_header.add_history(str_value)
                else:
                    astropy_header[key] = value

    return astropy_header
