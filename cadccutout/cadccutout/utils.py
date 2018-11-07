
__all__ = ['to_num', 'is_integer']

def to_num(s):
    try:
        return int(s)
    except ValueError:
        return float(s)

def is_integer(s):
    if isinstance(s, tuple):
        return False

    try:
        int(s)
        return True
    except ValueError:
        return False
