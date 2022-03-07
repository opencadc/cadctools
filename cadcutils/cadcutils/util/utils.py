# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2022.                            (c) 2022.
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
import sys
import inspect
from argparse import ArgumentParser, RawDescriptionHelpFormatter, SUPPRESS, \
    Action
from datetime import datetime
from six.moves.urllib.parse import urlparse
from operator import attrgetter
import hashlib
import os

__all__ = ['IVOA_DATE_FORMAT', 'date2ivoa', 'str2ivoa',
           'get_logger', 'get_log_level', 'get_base_parser', 'Md5File']

# TODO both these are very bad, implement more sensibly
IVOA_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"

DEFAULT_LOG_FORMAT = "%(levelname)s: %(name)s %(message)s"
DEBUG_LOG_FORMAT = "%(levelname)s: @(%(asctime)s) %(name)s " \
                   "%(module)s.%(funcName)s:%(lineno)d - %(message)s"


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
    :param log_level: logging level
    :return the logger object.
    """

    if namespace is None:
        frm = inspect.stack()[1]
        mod = inspect.getmodule(frm[0])
        if mod is None:
            namespace = __name__
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
    if len(result.netloc) < 2 or len(result.path) < 2 or \
            (result.scheme != 'ivo'):
        raise ValueError('Invalid resourceID: {}'.format(resource_id))
    return resource_id


###############################################################################
# Common command line options and customized format
###############################################################################

class SingleMetavarHelpFormatter(RawDescriptionHelpFormatter):
    """
    Class the customizes the argparse help formatter. It does 2 things:
        - the display of an option with short and long format is shorter
            e.g '-o, --output OUTPUT' instead of the default '-o OUTPUT,
            --output OUTPUT'
        - options are sorted in alphabetical order in command line usage
    """

    def _format_action_invocation(self, action):
        """
        Customized version of the function in HelpFormatter to shorten the
        display of option with long and short options
        :param action:
        :return:
        """
        if not action.option_strings:
            metavar, = self._metavar_formatter(action, action.dest)(1)
            return metavar

        else:
            parts = []

            # if the Optional doesn't take a value, format is:
            #    -s, --long
            if action.nargs == 0:
                parts.extend(action.option_strings)

            # if the Optional takes a value, format is:
            #    -s ARGS, --long ARGS
            else:
                default = action.dest.upper()
                args_string = self._format_args(action, default)

                parts.extend(action.option_strings)
                parts[-1] += ' %s' % args_string
            return ', '.join(parts)

    def add_arguments(self, actions):
        """
        Customized version to sort options in alphabetical order before
        displaying them
        :param actions:
        :return:
        """
        if (len(actions) > 0) and \
           (actions[0].container.title == 'optional arguments'):
            actions = sorted(actions, key=attrgetter('dest'))
        super(SingleMetavarHelpFormatter, self).add_arguments(actions)


class _AugmentAction(object):
    """
    This automatically adds parents and the formatter class when a new
    subparser is created in the client code
    """

    def __init__(self, parser, action):
        self.action = action
        self.parser = parser

    def add_parser(self, name, **kwargs):
        kwargs['parents'] = [self.parser]
        kwargs['formatter_class'] = SingleMetavarHelpFormatter
        return self.action.add_parser(name, **kwargs)


class _CustomArgParser(ArgumentParser):
    """
    Custom arg parses to sort options in alphabetical order before displaying
    them
    """

    def __init__(self, subparsers=True, common_parser=None, version=None,
                 **kwargs):
        self.common_parser = common_parser
        self.subparsers = subparsers
        self._subparsers_added = False
        self.kwargs = kwargs
        kwargs['formatter_class'] = SingleMetavarHelpFormatter
        if not self.subparsers:
            super(_CustomArgParser, self).__init__(parents=[common_parser],
                                                   **kwargs)
        else:
            super(_CustomArgParser, self).__init__(**kwargs)
        if version is not None:
            self.add_argument('-V', '--version', action='version',
                              version=version)

    def add_subparsers(self, **kwargs):
        if not self.subparsers:
            raise RuntimeError('Parser created to run without subparsers')
        self._subparsers_added = True
        return _AugmentAction(self.common_parser,
                              super(_CustomArgParser, self).add_subparsers(
                                  **kwargs))

    def parse_args(self, args=None, namespace=None):
        if self.subparsers and not self._subparsers_added:
            raise RuntimeError('No subparsers added. Change the parsers flag?')
        return super(_CustomArgParser, self).parse_args(args=args,
                                                        namespace=namespace)


def get_base_parser(subparsers=True, version=None, usecert=True,
                    default_resource_id=None, auth_required=False,
                    service=None):
    """
    An ArgumentParser with some common things most CADC clients will want.
    There are two modes to use this parser: with or without subparsers.
    With supbarsers (subparsers=True), separate subparsers are created for
    each subcommand and the common CADC options show up in each subcommand
    (and not on the parent parser). Without subparsers (subparsers=False)
    the common options are automatically added to the base parser.

    :param subparsers: True if the parser will use subparsers (subcommands)
    otherwise False
    :param version: A version number if desired.
    :param usecert: If True add '--cert' argument.
    :param default_resource_id: default resource identifier to use. (deprecated
    in favour of service argument)
    :param auth_required: At least one of the authentication options is
    required
    :param service: Alias of resource_id
    :return: An ArgumentParser instance.
    """
    cparser = ArgumentParser(add_help=False,
                             formatter_class=SingleMetavarHelpFormatter)

    auth_group = cparser.add_mutually_exclusive_group(required=auth_required)
    if usecert:
        auth_group.add_argument(
            '--cert', type=str,
            help='location of your X509 certificate to use for ' +
                 'authentication (unencrypted, in PEM format)')
    auth_group.add_argument('-n', action='store_true',
                            help='use .netrc in $HOME for authentication')
    auth_group.add_argument('--netrc-file',
                            help='netrc file to use for authentication')
    auth_group.add_argument('-u', '--user',
                            help='name of user to authenticate. ' +
                                 'Note: application prompts for the '
                                 'corresponding password!')
    cparser.add_argument('--host', help=SUPPRESS)
    cparser.add_argument('-k', '--insecure', action='store_true',
                         help=SUPPRESS)
    if service is None:
        if default_resource_id is None:
            cparser.add_argument('--resource-id',
                                 type=urlparse, required=True,
                                 help='resource identifier '
                                      '(e.g. ivo://cadc.nrc.ca/service)')
        else:
            cparser.add_argument('--resource-id', type=parse_resource_id,
                                 default=default_resource_id,
                                 help='resource identifier (default {})'.
                                 format(default_resource_id))
    else:
        cparser.add_argument(
            '-s', '--service', action=_ServiceAction,
            default=service,
            help='service this command accesses. Both IDs in short '
                 'form (<service>) or the complete one '
                 '(ivo://cadc.nrc.ca/<service>) as well as actual URLs to the '
                 'root of the service (https://someurl/service) are accepted.'
                 ' Default is: {}'.format(service))
    log_group = cparser.add_mutually_exclusive_group()
    log_group.add_argument('-d', '--debug', action='store_true',
                           help=SUPPRESS)
    log_group.add_argument('-q', '--quiet', action='store_true',
                           help='run quietly')
    log_group.add_argument('-v', '--verbose', action='store_true',
                           help='verbose messages')

    argparser = _CustomArgParser(subparsers=subparsers, common_parser=cparser,
                                 version=version)
    return argparser


class _ServiceAction(Action):
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        if nargs is not None:
            raise ValueError("nargs not allowed")
        super(_ServiceAction, self).__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        if not (values.startswith('ivo://') or (values.startswith('http'))):
            values = 'ivo://cadc.nrc.ca/{}'.format(values)
        setattr(namespace, self.dest, values)


class Md5File(object):
    """
    A wrapper to a file object that calculates the MD5 sum of the bytes
    that are being read or written. It allows allows seeking to a particular
    position in the file and it limits the legth of the data to be read,
    essentially enabling chunking/segmentation of large files.
    """

    def __init__(self, f, mode, offset=0, length=None):
        """

        :param f: location of the file
        :param mode: open mode (must be binary)
        :param offset: offset to start from
        :param length: data length to read from
        """
        self.file = open(f, mode)
        self._offset = offset
        self._length = length
        self._end_offset = os.stat(f).st_size
        if offset:
            if offset > self._end_offset:
                raise AttributeError(
                    '{} offset greater that file size: {} vs {}'.format(
                        f, offset, self._end_offset))
            self.file.seek(offset)
        if length:
            self._end_offset = min(offset + length, self._end_offset)
        self._md5_checksum = hashlib.md5()
        self._total_read_length = 0

    def __enter__(self):
        return self

    def read(self, size):
        batch_read_size = \
            min(size, self._end_offset-self._total_read_length-self._offset)
        buffer = self.file.read(batch_read_size)
        self._total_read_length += len(buffer)
        self._md5_checksum.update(buffer)
        return buffer

    def write(self, buffer):
        self._md5_checksum.update(buffer)
        self.file.write(buffer)
        self.file.flush()

    def __exit__(self, *args, **kwargs):
        if not self.file.closed:
            self.file.close()
        # clean up
        exit = getattr(self.file, '__exit__', None)
        if exit is not None:
            return exit(*args, **kwargs)
        else:
            exit = getattr(self.file, 'close',
                           None)
            if exit is not None:
                exit()

    def __getattr__(self, attr):
        if attr not in ['__len__', 'len']:
            # requests or other libraries might want to use `tell` and `seek`
            # to retry etc. Do not allow it as it might interfere with the
            # md5 checksum calculations
            raise AttributeError('Unsupported attribute: {}'.format(attr))

    def __len__(self):
        """
        :return: size meant to be seen by the clients (entire file or just
        a segment/chunk)
        """
        sz = self._end_offset - self._offset
        return sz

    def len(self):
        return self.__len__()

    def __iter__(self):
        return iter(self.file)

    @property
    def md5_checksum(self):
        return self._md5_checksum.hexdigest()
