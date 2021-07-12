# -*- coding: utf-8 -*-
# ************************************************************************
# *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
# *
# *  (c) 2021.                            (c) 2021.
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
# ************************************************************************

import pytest
import os
from urllib.parse import urlparse
import hashlib
import filecmp
from os.path import expanduser
import random
import requests

from cadcdata import StorageInventoryClient
from cadcutils.net import Subject
from cadcutils.util import str2ivoa
from cadcutils import exceptions

REG_HOST = 'https://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca'

HOME = expanduser("~")
CERT = os.path.join(HOME, '.ssl/cadcproxy.pem')


@pytest.mark.intTests
def test_client_public():
    # file info - NOTE: Test relies on an existing file not to be updated.
    client = StorageInventoryClient(Subject())
    file_info = client.cadcinfo('cadc:IRIS/I429B4H0.fits')
    assert 'cadc:IRIS/I429B4H0.fits' == file_info.id
    assert 1008000 == file_info.size
    assert 'I429B4H0.fits' == file_info.name
    assert 'e3922d47243563529f387ebdf00b66da' == file_info.md5sum
    timestamp = str2ivoa('2012-06-20T12:31:00.000')
    assert timestamp == file_info.lastmod
    assert 'application/fits' == file_info.file_type
    assert file_info.encoding is None

    # download file
    dest = '/tmp/inttest_I429B4H0.fits'
    if os.path.isfile(dest):
        os.remove(dest)
    try:
        client.cadcget('cadc:IRIS/I429B4H0.fits', dest=dest)
        assert os.path.isfile(dest)
        assert file_info.size == os.stat(dest).st_size
        hash_md5 = hashlib.md5()
        with open(dest, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        assert file_info.md5sum == hash_md5.hexdigest()
    finally:
        # clean up
        if os.path.isfile(dest):
            os.remove(dest)

    # TODO cutouts


@pytest.mark.intTests
@pytest.mark.skipif(not os.path.isfile(CERT),
                    reason='CADC credentials required in '
                           '$HOME/.ssl/cadcproxy.pem')
def test_client_authenticated():
    """ uses $HOME/.ssl/cadcproxy.pem certificates"""
    # create a random root for file IDs
    test_file = '/tmp/cadcdata-inttest.txt'
    id_root = 'cadc:TEST/cadcdata-intttest-{}'.format(random.randrange(100000))
    global_id = id_root + '/global'
    file_name = global_id.split('/')[-1]
    dest_file = os.path.join('/tmp', file_name)
    try:

        subject = Subject(certificate=CERT)

        if os.path.isfile(test_file):
            os.remove(test_file)
        with open(test_file, 'a') as f:
            f.write('THIS IS A TEST')

        md5 = hashlib.md5()
        with open(test_file, 'rb') as f:
            md5.update(f.read())
        md5sum = md5.hexdigest()
        file_size = os.stat(test_file).st_size
        # find out locations
        reg = requests.get('{}/reg/resource-caps'.format(REG_HOST)).text
        location_resource_ids = []
        for line in reg.split('\n'):
            line.strip()
            if not line.startswith('#') and ('minoc' in line) and (
                    '/ad/minoc' not in line):
                location_resource_ids.append(line.split('=')[0].strip())

        # test all operations on a location
        for resource_id in location_resource_ids:
            location_operations(subject=subject, resource_id=resource_id,
                                file=test_file, id_root=id_root,
                                md5sum=md5sum, size=file_size)

        # to test global without waiting for the eventual consistency to occur,
        # put the file to global and look for it in at least one of the
        # location. get it and then remove it from that location
        client = StorageInventoryClient(subject=subject)

        client.cadcput(id=global_id, src=test_file)

        file_info = None
        for resource_id in location_resource_ids:
            try:
                location_client = StorageInventoryClient(
                    subject=subject, resource_id=resource_id)
                file_info = location_client.cadcinfo(global_id)
                break
            except exceptions.NotFoundException:
                continue
        assert file_info, 'File not found on any location'
        assert global_id == file_info.id
        assert file_size == file_info.size
        assert md5sum == file_info.md5sum

        if os.path.isfile(dest_file):
            os.remove(dest_file)
        location_client.cadcget(global_id, dest=dest_file)
        assert filecmp.cmp(test_file, dest_file)

        # remove the file
        location_client.cadcremove(global_id)

        with pytest.raises(exceptions.NotFoundException):
            location_client.cadcinfo(global_id)

        with pytest.raises(exceptions.NotFoundException):
            location_client.cadcget(global_id)
    finally:
        # cleanup
        if os.path.isfile(dest_file):
            os.remove(dest_file)
        if os.path.isfile(test_file):
            os.remove(test_file)


def location_operations(subject, resource_id, file, id_root, md5sum, size):
    # tests cadcput, cadcinfo, cadcget and cadcremove on a specific location
    location = urlparse(resource_id).path.replace('/', '_')
    si_id = id_root + '/' + location
    file_name = si_id.split('/')[-1]
    dest_file = os.path.join('/tmp', file_name)
    try:

        location_client = StorageInventoryClient(subject=subject,
                                                 resource_id=resource_id)
        # put the file
        location_client.cadcput(id=si_id, src=file)

        # get info about the file
        file_info = location_client.cadcinfo(id=si_id)

        assert si_id == file_info.id
        assert file_name == file_info.name
        assert size == file_info.size
        assert md5sum == file_info.md5sum

        # get the file

        if os.path.isfile(dest_file):
            os.remove(dest_file)

        location_client.cadcget(id=si_id, dest='/tmp')
        assert filecmp.cmp(file, dest_file)

        # remove the file
        location_client.cadcremove(si_id)

        with pytest.raises(exceptions.NotFoundException):
            location_client.cadcinfo(si_id)

        with pytest.raises(exceptions.NotFoundException):
            location_client.cadcget(si_id)
    finally:
        # cleanup
        if os.path.isfile(dest_file):
            os.remove(dest_file)
