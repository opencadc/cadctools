# -*- coding: utf-8 -*-

# ************************************************************************
# *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
# *
# *  (c) 2015.                            (c) 2015.
# *  Government of Canada                 Gouvernement du Canada
# *  National Research Council            Conseil national de recherches
# *  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
# *  All rights reserved                  Tous droits réservés
# *
# *  NRC disclaims any warranties,        Le CNRC dénie toute garantie
# *  expressed, implied, or               énoncée, implicite ou légale,
# *  statutory, of any kind with          de quelque nature que ce
# *  respect to the software,             soit, concernant le logiciel,
# *  including without limitation         y compris sans restriction
# *  any warranty of merchantability      toute garantie de valeur
# *  or fitness for a particular          marchande ou de pertinence
# *  purpose. NRC shall not be            pour un usage particulier.
# *  liable in any event for any          Le CNRC ne pourra en aucun cas
# *  damages, whether direct or           être tenu responsable de tout
# *  indirect, special or general,        dommage, direct ou indirect,
# *  consequential or incidental,         particulier ou général,
# *  arising from the use of the          accessoire ou fortuit, résultant
# *  software.  Neither the name          de l'utilisation du logiciel. Ni
# *  of the National Research             le nom du Conseil National de
# *  Council of Canada nor the            Recherches du Canada ni les noms
# *  names of its contributors may        de ses  participants ne peuvent
# *  be used to endorse or promote        être utilisés pour approuver ou
# *  products derived from this           promouvoir les produits dérivés
# *  software without specific prior      de ce logiciel sans autorisation
# *  written permission.                  préalable et particulière
# *                                       par écrit.
# *
# *  This file is part of the             Ce fichier fait partie du projet
# *  OpenCADC project.                    OpenCADC.
# *
# *  OpenCADC is free software:           OpenCADC est un logiciel libre ;
# *  you can redistribute it and/or       vous pouvez le redistribuer ou le
# *  modify it under the terms of         modifier suivant les termes de
# *  the GNU Affero General Public        la “GNU Affero General Public
# *  License as published by the          License” telle que publiée
# *  Free Software Foundation,            par la Free Software Foundation
# *  either version 3 of the              : soit la version 3 de cette
# *  License, or (at your option)         licence, soit (à votre gré)
# *  any later version.                   toute version ultérieure.
# *
# *  OpenCADC is distributed in the       OpenCADC est distribué
# *  hope that it will be useful,         dans l’espoir qu’il vous
# *  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
# *  without even the implied             GARANTIE : sans même la garantie
# *  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
# *  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
# *  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
# *  General Public License for           Générale Publique GNU Affero
# *  more details.                        pour plus de détails.
# *
# *  You should have received             Vous devriez avoir reçu une
# *  a copy of the GNU Affero             copie de la Licence Générale
# *  General Public License along         Publique GNU Affero avec
# *  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
# *  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
# *                                       <http://www.gnu.org/licenses/>.
# *
# *
# ************************************************************************
"""
Exceptions used in the cadcutils package
"""
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import errno
import logging
import traceback
import requests

__all__ = ['UnauthorizedException', 'ForbiddenException', 'NotFoundException',
           'BadRequestException', 'ByteLimitException',
           'InternalServerException', 'UnexpectedException']


class HttpException(Exception):
    """
    Generic HTTP exception that is the base class of all the other
    HTTP related exceptions
    """
    def __init__(self, msg=None, orig_exception=None):
        self.orig_exception = orig_exception
        self._msg = msg
        if (msg is None) and (self.orig_exception is not None):
            if isinstance(self.orig_exception, requests.HTTPError):
                self._msg = self.orig_exception.response.text
            else:
                self._msg = str(self.orig_exception)

        if logging.getLogger().isEnabledFor(logging.DEBUG):
            self._msg = '{}\n{}'.format(
                self._msg, ''.join(traceback.format_stack()))

    @property
    def msg(self):
        return self._msg

    def __str__(self):
        return self.msg if self.msg is not None else ''


class UnauthorizedException(HttpException):
    """User requires authentication to perform the requested action.
    Attributes:
        msg  -- explanation of why the specific transition is not allowed
    """
    def __init__(self, msg=None, orig_exception=None):
        HttpException.__init__(self, msg, orig_exception)
        self.errno = errno.EACCES


class ForbiddenException(HttpException):
    """Authenticated user is not authorized to perform an action.
    Attributes:
        msg  -- explanation of why the specific transition is not allowed
    """
    def __init__(self, msg=None, orig_exception=None):
        HttpException.__init__(self, msg, orig_exception)


class NotFoundException(HttpException):
    """Resource not found.
    Attributes:
        msg
    """
    def __init__(self, msg=None, orig_exception=None):
        self.errno = errno.ENOENT
        HttpException.__init__(self, msg, orig_exception)


class BadRequestException(HttpException):
    """Operation is not permitted or the argument is illegal
    Attributes:
        msg
    """
    def __init__(self, msg=None, orig_exception=None):
        HttpException.__init__(self, msg, orig_exception)


class AlreadyExistsException(HttpException):
    """Resource already exists
    Attributes:
        msg
    """
    def __init__(self, msg=None, orig_exception=None):
        HttpException.__init__(self, msg, orig_exception)
        self.errno = errno.EEXIST


class ByteLimitException(HttpException):
    """Request is too large
    Attributes:
        msg
    """
    def __init__(self, msg=None, orig_exception=None):
        HttpException.__init__(self, msg, orig_exception)
        self.errno = errno.E2BIG


class InternalServerException(HttpException):
    """Server encounters an internal error
    Attributes:
        msg
    """
    def __init__(self, msg=None, orig_exception=None):
        HttpException.__init__(self, msg, orig_exception)


class UnexpectedException(HttpException):
    """Unexpected error
    Attributes:
        msg
    """
    def __init__(self, msg=None, orig_exception=None):
        HttpException.__init__(self, msg, orig_exception)


class TransferException(HttpException):
    """A transfer exception was encountered. Client should either try another
    mirror URL or re-try this if possible (data is still accessible - not
    streamed)
    Attributes:
        msg
    """
    def __init__(self, msg=None, orig_exception=None):
        HttpException.__init__(self, msg, orig_exception)
