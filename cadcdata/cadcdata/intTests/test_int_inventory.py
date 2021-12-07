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
from os.path import expanduser
import random
import requests
import filecmp
from io import BytesIO

from cadcdata import StorageInventoryClient
from cadcutils.net import Subject, ws
from cadcutils.util import str2ivoa
from cadcutils import exceptions

REG_HOST = 'www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca'

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')

HOME = expanduser("~")
CERT = os.path.join(HOME, '.ssl/cadcproxy.pem')


def check_file(file_name, size, md5):
    assert os.path.isfile(file_name)
    assert size == os.stat(file_name).st_size
    hash_md5 = hashlib.md5()
    with open(file_name, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    assert md5 == hash_md5.hexdigest()


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
        check_file(dest, file_info.size, file_info.md5sum)
    finally:
        # clean up
        if os.path.isfile(dest):
            os.remove(dest)

    # download just the headers
    fhead_dest = '/tmp/inttest_I429B4H0.fits.txt'
    if os.path.isfile(fhead_dest):
        os.remove(fhead_dest)
    try:
        client.cadcget('cadc:IRIS/I429B4H0.fits', dest=fhead_dest, fhead=True)
        assert os.path.isfile(fhead_dest)
        assert filecmp.cmp(fhead_dest,
                           os.path.join(TESTDATA_DIR, 'I429B4H0.fits.txt'),
                           shallow=False)

        # read in a memory buffer
        header_content = BytesIO()
        client.cadcget('cadc:IRIS/I429B4H0.fits',
                       dest=header_content, fhead=True)
        expected = open(fhead_dest, 'rb').read()
        assert expected == header_content.getvalue()

    finally:
        # clean up
        if os.path.isfile(fhead_dest):
            os.remove(fhead_dest)


@pytest.mark.intTests
def test_cadcget_resume():
    # file info - NOTE: Test relies on an existing file not to be updated.
    client = StorageInventoryClient(Subject())
    file_id = 'cadc:IRIS/I429B4H0.fits'
    file_info = client.cadcinfo(file_id)
    assert 1008000 == file_info.size
    assert 'I429B4H0.fits' == file_info.name
    assert 'e3922d47243563529f387ebdf00b66da' == file_info.md5sum
    # download file
    dest = '/tmp/inttest_I429B4H0.fits'
    if os.path.isfile(dest):
        os.remove(dest)
    try:
        client.cadcget(file_id, dest=dest)
        check_file(dest, file_info.size, file_info.md5sum)

        # to mimic resume create the temporary with incomplete content
        final_dest, temp_dest = client._cadc_client._resolve_destination_file(
            dest=dest, src_md5=file_info.md5sum, default_file_name=None)
        os.rename(dest, temp_dest)
        # truncate the last 10 bytes
        with open(temp_dest, 'r+') as f:
            f.seek(0, os.SEEK_END)
            f.seek(f.tell()-10, os.SEEK_SET)
            f.truncate()
        assert os.stat(temp_dest).st_size < file_info.size
        # download the file again to use the resume
        client.cadcget(file_id, dest=dest)
        check_file(dest, file_info.size, file_info.md5sum)
        assert not os.path.isfile(temp_dest)

        # create an empty temporary file to trigger another full download
        os.remove(dest)
        open(temp_dest, 'w')
        assert os.stat(temp_dest).st_size == 0
        client.cadcget(file_id, dest=dest)
        check_file(dest, file_info.size, file_info.md5sum)
        assert not os.path.isfile(temp_dest)

        # make the temporary file the same as the final. This is a BUG
        # scenario which should trigger a complete download of the file
        os.rename(dest, temp_dest)
        assert os.stat(temp_dest).st_size == file_info.size
        client.cadcget(file_id, dest=dest)
        check_file(dest, file_info.size, file_info.md5sum)
        assert not os.path.isfile(temp_dest)

        # make the temporary file larger. This is also a BUG case that
        # shouldn't happen because the md5 of source changes when size
        # of the file changes. The test is just to make sure that the
        # application recovers from such a case
        os.rename(dest, temp_dest)
        # add some text to the file
        with open(temp_dest, 'ab') as f:
            f.write(b'beef')
        assert os.stat(temp_dest).st_size > file_info.size
        # temporary file should be overriden
        client.cadcget(file_id, dest=dest)
        check_file(dest, file_info.size, file_info.md5sum)
        assert not os.path.isfile(temp_dest)
    finally:
        # clean up
        if os.path.isfile(dest):
            os.remove(dest)


@pytest.mark.intTests
@pytest.mark.skipif(not os.path.isfile(CERT),
                    reason='CADC credentials required in '
                           '$HOME/.ssl/cadcproxy.pem')
def test_client_authenticated():
    """ uses $HOME/.ssl/cadcproxy.pem certificates"""
    # create a random root for file IDs
    # Note: "+" in the file name is testing the special character in URI
    test_file = '/tmp/cadcdata+inttest.txt'
    id_root = 'cadc:TEST/cadcdata+intttest-{}'.format(random.randrange(100000))
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
        reg = requests.get(
            'https://{}/reg/resource-caps'.format(REG_HOST)).text
        location_resource_ids = []
        for line in reg.split('\n'):
            line.strip()
            if not line.startswith('#') and ('minoc' in line) and (
                    '/ad/minoc' not in line) and ('ws-sf' not in line):
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


@pytest.mark.intTests
@pytest.mark.skipif(not os.path.isfile(CERT),
                    reason='CADC credentials required in '
                           '$HOME/.ssl/cadcproxy.pem')
@pytest.mark.skip('To enable when put-txn is released')
def test_put_transactions():
    # very similar with the test_client_authenticated except that threshold
    # is set to a minimum value such that the test file is considered large
    # and its md5 checksum is not pre-computed which forces the use of trans
    orig_max_md5_size = ws.MAX_MD5_COMPUTE_SIZE
    global REG_HOST
    # TODO remove after put-txn released
    orig_reg_host = REG_HOST
    try:
        REG_HOST = 'localhost.cadc.dao.nrc.ca'
        ws.MAX_MD5_COMPUTE_SIZE = 5
        """ uses $HOME/.ssl/cadcproxy.pem certificates"""
        # create a random root for file IDs
        # Note: "+" in the file name is testing the special character in URI
        test_file = '/tmp/cadcdata+inttest.txt'
        id_root = 'cadc:TEST/cadcdata+intttest-{}'.format(
            random.randrange(100000))
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
            reg = requests.get('https://{}/reg/resource-caps'.format(REG_HOST),
                               verify=False).text
            location_resource_ids = []
            for line in reg.split('\n'):
                line.strip()
                if not line.startswith('#') and ('minoc' in line) and (
                        '/ad/minoc' not in line):
                    location_resource_ids.append(line.split('=')[0].strip())

            # TODO remove line after put-txn released
            location_resource_ids = ['ivo://cadc.nrc.ca/minoc']

            # test all operations on a location
            for resource_id in location_resource_ids:
                location_operations(subject=subject, resource_id=resource_id,
                                    file=test_file, id_root=id_root,
                                    md5sum=md5sum, size=file_size)
            # pick up one location to test a transaction rollback

            client = StorageInventoryClient(
                subject=subject,
                resource_id=location_resource_ids[0],
                host=REG_HOST,
                insecure=True)
            # file should not be on the storage to start with
            with pytest.raises(exceptions.NotFoundException):
                client.cadcinfo(id=id_root)

            orig_put = client._cadc_client._get_session().put

            # simulate a rollback
            def tamper_md5(url, **kwargs):
                # tamper with the md5 checksum returned from the server
                # so that the code finds a mismatch and rollbacks the
                # transaction
                response = orig_put(url, **kwargs)
                if 'Digest' in response.headers:
                    response.headers['Digest'] = 'md5=YmVlZg=='
                return response

            client._cadc_client._get_session().put = tamper_md5
            with pytest.raises(exceptions.TransferException) as te:
                client.cadcput(id=id_root, src=test_file)
            assert 'MD5 checksum mismatch. Transaction rolled back' in str(te)

            # transaction should be rolled back at this point so the file
            # should not be there
            with pytest.raises(exceptions.NotFoundException):
                client.cadcinfo(id=id_root)

        finally:
            # cleanup
            if os.path.isfile(dest_file):
                os.remove(dest_file)
            if os.path.isfile(test_file):
                os.remove(test_file)

    finally:
        ws.MAX_MD5_COMPUTE_SIZE = orig_max_md5_size
        REG_HOST = orig_reg_host


def location_operations(subject, resource_id, file, id_root, md5sum, size):
    # tests cadcput, cadcinfo, cadcget and cadcremove on a specific location
    location = urlparse(resource_id).path.replace('/', '_')
    si_id = id_root + '/' + location
    file_name = si_id.split('/')[-1]
    dest_file = os.path.join('/tmp', file_name)
    try:

        location_client = StorageInventoryClient(subject=subject,
                                                 resource_id=resource_id,
                                                 host=REG_HOST,
                                                 insecure=True)
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
