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
import logging
import sys
import inspect
from argparse import ArgumentParser, RawDescriptionHelpFormatter, SUPPRESS, \
    Action
from datetime import datetime
from urllib.parse import urlparse
from operator import attrgetter
import hashlib
import os
from packaging.version import Version
import requests
import time
import json
import warnings
from pathlib import Path
from cadcutils import exceptions

__all__ = ['IVOA_DATE_FORMAT', 'date2ivoa', 'str2ivoa', 'get_url_content',
           'get_logger', 'get_log_level', 'get_base_parser', 'Md5File',
           'check_version', 'VersionWarning']

# TODO both these are very bad, implement more sensibly
IVOA_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"

CADC_CACHE_DIR = os.path.join(os.path.expanduser("~"), '.config')
VERSION_REFRESH_INTERVAL = 24 * 60 * 60  # 24h

DEFAULT_LOG_FORMAT = "%(levelname)s: %(name)s %(message)s"
DEBUG_LOG_FORMAT = "%(levelname)s: @(%(asctime)s) %(name)s " \
                   "%(module)s.%(funcName)s:%(lineno)d - %(message)s"

logger = logging.getLogger(__name__)


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
           (actions[0].container.title in ['optional arguments', 'options']):
            actions[0].container.title = 'optional arguments'
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
        self._version = version
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
        result = super(_CustomArgParser, self).parse_args(args=args,
                                                          namespace=namespace)
        if hasattr(result, 'verbose') and result.verbose:
            logging.basicConfig(level=logging.INFO, stream=sys.stdout)
            logger.setLevel(logging.INFO)
        elif hasattr(result, 'debug') and result.debug:
            logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
            logger.setLevel(logging.DEBUG)
        elif hasattr(result, 'quiet') and result.quiet:
            logging.basicConfig(level=logging.FATAL, stream=sys.stdout)
            logger.setLevel(logging.FATAL)
        else:
            logging.basicConfig(level=logging.WARNING, stream=sys.stdout)

        # print package version info
        with warnings.catch_warnings():
            warnings.simplefilter("error", category=VersionWarning)
            try:
                check_version(self._version)
            except VersionWarning as e:
                logger.warning('{}'.format(str(e)))
            except Exception as e:
                logger.debug('Unexpected exception in check_version: {}'.format(str(e)))
                pass
        return result


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


def get_url_content(url, cache_file, refresh_interval, verify=True):
    """
     Return content of a url from a cache file or directly from source if the
     cache is stale (file is older than refresh_interval). For now, the access
     to the url is anonymous.
     :param url: URL of the source
     :param cache_file: cache file location
     :param refresh_interval: how long (in sec) to consider the cache stale
     :param verify: verify the HTTPS server certificate. This should be
     set to False only for testing purposes

     :returns content of the url from source or cache
    """
    content = None
    if os.path.exists(cache_file):
        last_accessed = os.path.getmtime(cache_file)
    else:
        last_accessed = None
    if (last_accessed and (time.time() - last_accessed) < refresh_interval):
        # get content from the cached file
        logger.debug(
            'Read cached content of {}'.format(cache_file))
        try:
            with open(cache_file, 'r') as f:
                content = f.read()
        except Exception:
            # will download it
            pass
    # config dirs if they don't exist yet
    if not os.path.exists(os.path.dirname(cache_file)):
        os.makedirs(os.path.dirname(cache_file))

    if not content:
        # get content from source URL
        try:
            session = requests.Session()
            # do not allow requests to use .netrc file
            session.trust_env = False
            rsp = session.get(url, verify=verify)
            rsp.raise_for_status()
            content = rsp.text
            if content is None or len(content.strip(' ')) == 0:
                # workaround for a problem with CADC servers
                raise exceptions.HttpException('Received empty content')
            with open(cache_file, 'w') as f:
                f.write(content)
        except Exception as e:
            # problems with the source. Try to use the old
            # local one regardless of how old it is
            logger.debug("Cannot read content from {}: {}".format(url, str(e)))
            if os.path.exists(cache_file):
                with open(cache_file, 'r') as f:
                    content = f.read()
                # touch the cache file to reduce the remote calls to the frequency dictated
                # by the refresh_interval even if errors are later encountered.
                Path(cache_file).touch()
    if content is None:
        raise RuntimeError('Cannot get content from {}'.format(url))
    return content


class VersionWarning(Warning):
    # category of warnings when current version falls behind the released version
    pass


def check_version(version):
    """
    Function to test the version of a package against PyPI and issue
    a warning when a newer version exists on PyPI. It will check only against
    versions of format "major.minor.micro" and ignore the others including pre-releases
    or dev releases.

    Note: Only the first check of a package is performed. Subsequent calls
    for the same package are ignored.

    :param version: package version of the form "<package_name> <version>
    e.g. "cadcutils 1.2.3"

    :return: raises a VersionWarning when PyPI version is ahead.
    """
    strict_versions = None
    current_version = None
    try:
        package, pkg_version = version.split(' ')
        if package in check_version.checked:
            return
        check_version.checked.append(package)
        cache_file = os.path.join(CADC_CACHE_DIR, package, 'caches/.pypi_versions.json')
        content = get_url_content(
            url='https://pypi.org/pypi/{}/json'.format(package),
            cache_file=cache_file,
            refresh_interval=VERSION_REFRESH_INTERVAL)
        data = json.loads(content)
        versions = list(data['releases'].keys())
        current_version = Version(pkg_version)
        strict_versions = []
        for v in versions:
            try:
                tempv = Version(v)  # version
                if not tempv.is_devrelease and not tempv.is_prerelease:
                    strict_versions.append(
                        Version('{}.{}.{}'.format(tempv.major, tempv.minor, tempv.micro)))
            except ValueError:
                continue
        strict_versions.sort()
    except Exception as e:
        logger.debug(
            'Unexpected exception in PyPI version checking: {}'.format(str(e)))
        pass
    if strict_versions and current_version < strict_versions[-1]:
        current_warn_formatting = warnings.formatwarning
        warnings.formatwarning = lambda message, *ignore: 'WARNING: {}\n'.format(message)
        warnings.warn('Current version {}. A newer version, {}, '
                      'is available on PyPI'.format(version,
                                                    strict_versions[-1]),
                      category=VersionWarning)
        sys.stdout.flush()
        sys.stderr.flush()
        warnings.formatwarning = current_warn_formatting


check_version.checked = []  # packages already checked
