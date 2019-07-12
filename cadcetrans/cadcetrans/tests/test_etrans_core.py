# -*- coding: utf-8 -*-

# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2018.                            (c) 2018.
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

import os
import pytest
from cadcutils.net import Subject
from cadcetrans.etrans_core import transfer, main_app, FileInfo
from cadcetrans import etrans_core
from cadcetrans.utils import CommandError
import tempfile
import shutil
from mock import patch, Mock, call
from six import StringIO
import sys
from datetime import datetime

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')
TESTDATA_INPUT_DIR = os.path.join(TESTDATA_DIR, 'input')

# create a work directory
PROC_DIR = tempfile.mkdtemp(prefix='cadcetrans')
dest = os.path.join(PROC_DIR, 'new')
os.mkdir(dest)


class MyExitError(Exception):
    pass


def config_get(section, key):
    # this is a mock of the
    config = {'max_files': 20, 'ad_stream': 'RAW:raw PROCESSED:product'}
    return config[key]


@patch('cadcetrans.etrans_core.etrans_config')
@patch('cadcetrans.utils.CadcDataClient')
@patch('cadcetrans.etrans_core._get_files_to_transfer')
def test_transfer_dryrun(get_files_mock, data_client_mock, config_mock):
    config_mock.get = config_get
    data_client_mock.return_value.get_file_info.return_value = None

    # no files to transfer
    transfer(PROC_DIR, 'new', True, Subject())

    # copy all the files including the invalid ones:
    src_files = os.listdir(TESTDATA_INPUT_DIR)
    # mock _get_files_to_transfer so all the files are picked up with no
    # staging time dalay
    get_files_mock.return_value = [FileInfo(f, 'new') for f in src_files]
    for file_name in src_files:
        full_file_name = os.path.join(TESTDATA_INPUT_DIR, file_name)
        if (os.path.isfile(full_file_name)):
            shutil.copy(full_file_name, dest)

    invalid_files = [f for f in os.listdir(dest) if 'invalid' in f]

    with pytest.raises(CommandError) as e:
        transfer(PROC_DIR, 'new', True, Subject())
    assert 'Errors occurred during transfer ({} error(s))'\
           .format(len(invalid_files)) in str(e.value)

    # remove the "invalid" files
    for f in invalid_files:
        os.unlink(os.path.join(dest, f))
        src_files.remove(f)
    get_files_mock.return_value = [FileInfo(f, 'new') for f in src_files]
    transfer(PROC_DIR, 'new', True, Subject())

    with patch('cadcetrans.etrans_core.put_cadc_file'):
        transfer(PROC_DIR, 'new', False, Subject())  # no more errors
    # all files processed
    assert not os.listdir(dest)

    # stream required in dryrun mode
    with pytest.raises(CommandError):
        transfer(PROC_DIR, None, True, Subject())


@patch('cadcetrans.etrans_core.etrans_config')
@patch('cadcetrans.utils.CadcDataClient')
@patch('cadcetrans.etrans_core.put_cadc_file')
@patch('cadcetrans.etrans_core._get_files_to_transfer')
def test_transfer(get_files_mock, put_mock, data_client_mock, config_mock):
    config_mock.get = config_get
    data_client_mock.return_value.get_file_info.return_value = None
    # cleanup the test directory (dest)
    for f in os.listdir(dest):
        ff = os.path.join(dest, f)
        if os.path.isfile(ff):
            os.unlink(ff)

    # copy all the files including the invalid ones:
    src_files = os.listdir(TESTDATA_INPUT_DIR)
    # mock _get_files_to_transfer so all the files are picked up with no
    # staging time dalay
    get_files_mock.return_value = [FileInfo(f, 'new') for f in src_files]
    for file_name in src_files:
        full_file_name = os.path.join(TESTDATA_INPUT_DIR, file_name)
        if (os.path.isfile(full_file_name)):
            shutil.copy(full_file_name, dest)

    invalid_files = [f for f in os.listdir(dest)
                     if 'invalid' in f or 'bad' in f]
    valid_files = [f for f in os.listdir(dest) if 'invalid' not in f]

    subject = Subject()
    with pytest.raises(CommandError) as e:
        transfer(PROC_DIR, 'new', False, subject,
                 namecheck_file=os.path.join(TESTDATA_DIR,
                                             'namecheck.xml'))
    assert 'Errors occurred during transfer ({} error(s))'\
           .format(len(invalid_files)) in str(e.value)
    assert put_mock.call_count == len(src_files) - len(invalid_files), 'Calls'
    calls = []
    for f in valid_files:
        calls.append(call(os.path.join(dest, 'new', f), None, subject,
                          mime_type=None, mime_encoding=None))
    put_mock.asses_has_calls(calls, any_order=True)
    # check that the left files are all invalid
    for f in os.listdir(dest):
        assert f.startswith('invalid')
    # check to see if rejected files have been moved to the right place
    for f in invalid_files:
        if f.startswith('bad'):
            assert os.path.isfile(os.path.join(PROC_DIR, 'reject', 'name', f))
        else:
            _, file_extension = os.path.splitext(f)
            file_extension = file_extension.strip('.')
            if file_extension == 'gz':
                # this corresponds to a tar file with a invalid fits file
                file_extension = 'fits'
            if file_extension == 'jpg':
                file_extension = 'jpeg'
            assert os.path.isfile(os.path.join(PROC_DIR, 'reject',
                                               '{} verify'.format(
                                                   file_extension), f))
    # run again with the invalid files only
    put_mock.reset_mock()
    transfer(PROC_DIR, 'new', False, subject)
    assert 'Errors occurred during transfer ({} error(s))'\
           .format(len(invalid_files)) in str(e.value)
    assert put_mock.call_count == 0


@patch('sys.exit', Mock(side_effect=[MyExitError, MyExitError, MyExitError,
                                     MyExitError, MyExitError, MyExitError,
                                     MyExitError, MyExitError]))
def test_help():
    """ Tests the helper displays for commands and subcommands in main"""

    # expected helper messages
    with open(os.path.join(TESTDATA_DIR, 'help.txt'), 'r') as myfile:
        usage = myfile.read()
    with open(
            os.path.join(TESTDATA_DIR, 'data_help.txt'), 'r') as myfile:
        data_usage = myfile.read()
    with open(os.path.join(TESTDATA_DIR, 'status_help.txt'), 'r') as myfile:
        status_usage = myfile.read()

    # maxDiff = None  # Display the entire difference
    # --help
    with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
        sys.argv = ["cadc-etrans", "--help"]
        with pytest.raises(MyExitError):
            main_app()
        assert usage == stdout_mock.getvalue()

    with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
        sys.argv = ["cadc-etrans", "data", "--help"]
        with pytest.raises(MyExitError):
            main_app()
        assert data_usage == stdout_mock.getvalue()

    with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
        sys.argv = ["cadc-etrans", "status", "--help"]
        with pytest.raises(MyExitError):
            main_app()
        assert status_usage == stdout_mock.getvalue()


@patch('cadcetrans.etrans_core.net.Subject.from_cmd_line_args')
@patch('cadcetrans.etrans_core.transfer')
@patch('cadcetrans.etrans_core.clean_up')
@patch('cadcetrans.etrans_core.print_status')
@patch('cadcetrans.etrans_core.update_backup')
def test_main(backup_mock, status_mock, cleanup_mock, transfer_mock,
              subject_mock):
    """
    Tests the main function etrans_core
    :return:
    """

    # data transfer
    subject = Subject()
    subject_mock.return_value = subject
    sys.argv = ['cadc-etran', 'data', PROC_DIR]
    main_app()
    transfer_mock.assert_called_with(PROC_DIR, stream_name=None, dry_run=False,
                                     subject=subject, namecheck_file=None)
    cleanup_mock.assert_called_with(PROC_DIR, dry_run=False)
    backup_mock.assert_not_called()
    status_mock.assert_not_called()

    # status print
    transfer_mock.reset_mock()
    cleanup_mock.reset_mock()
    sys.argv = ['cadc-etran', 'status', PROC_DIR]
    main_app()
    status_mock.assert_called_with(PROC_DIR)
    cleanup_mock.assert_not_called()
    transfer_mock.assert_not_called()
    backup_mock.assert_not_called()

    # status backup
    status_mock.reset_mock()
    sys.argv = ['cadc-etran', 'status', '-b', PROC_DIR]
    main_app()
    backup_mock.assert_called_with(subject, PROC_DIR)
    cleanup_mock.assert_not_called()
    transfer_mock.assert_not_called()
    status_mock.assert_not_called()


def test_proc():
    # create a proc timestamp file
    tmpdir = tempfile.mkdtemp()
    etrans_core._write_proc_info(tmpdir)
    config, stampfile = etrans_core._get_proc_info(tmpdir)
    start = datetime.strptime(config.get('transfer', 'start'),
                              '%Y-%m-%d %H:%M:%S')
    assert (datetime.utcnow() - start).seconds < 2
    assert int(config.get('transfer', 'pid')) == os.getpid()
    assert stampfile == os.path.join(tmpdir, etrans_core.PROC_TIMESTAMP_FILE)

    assert etrans_core._get_proc_info(tempfile.mkdtemp()) is None


def test_cleanup():
    # mimic the input directory
    tmpdir = tempfile.mkdtemp()
    orig_new = os.path.join(tmpdir, 'new')
    os.mkdir(orig_new)
    orig_replace = os.path.join(tmpdir, 'replace')
    os.mkdir(orig_replace)
    orig_any = os.path.join(tmpdir, 'any')
    os.mkdir(orig_any)
    procdirs = os.path.join(tmpdir, 'proc')
    os.mkdir(procdirs)
    procdir = os.path.join(procdirs, 'proc123')
    os.mkdir(procdir)
    proc_new = os.path.join(procdir, 'new')
    os.mkdir(proc_new)
    proc_replace = os.path.join(procdir, 'replace')
    os.mkdir(proc_replace)
    proc_any = os.path.join(procdir, 'any')
    os.mkdir(proc_any)
    with open(os.path.join(proc_new, 'new.fits'), 'w+') as f:
        f.write('new')
    with open(os.path.join(proc_replace, 'replace.fits'), 'w+') as f:
        f.write('replace')
    with open(os.path.join(proc_any, 'any.fits'), 'w+') as f:
        f.write('any')

    stamp_file = etrans_core._write_proc_info(procdir)
    etrans_core.clean_up(tmpdir)

    # because the proc is still "current" there is no cleanup
    assert len(os.listdir(orig_new)) == 0
    assert len(os.listdir(orig_replace)) == 0
    assert len(os.listdir(orig_any)) == 0
    assert os.path.isfile(os.path.join(proc_new, 'new.fits'))
    assert os.path.isfile(os.path.join(proc_replace, 'replace.fits'))
    assert os.path.isfile(os.path.join(proc_any, 'any.fits'))

    # move timestamp in the past and test again
    config = etrans_core._get_proc_info(procdir)[0]
    config.set('transfer', 'start', '2000-01-01 00:00:00')
    with open(stamp_file, 'w+') as f:
        config.write(f)

    etrans_core.clean_up(tmpdir)

    # check files have moved to the original directories and proc dir
    # has been removed
    assert os.path.isfile(os.path.join(orig_new, 'new.fits'))
    assert os.path.isfile(os.path.join(orig_replace, 'replace.fits'))
    assert os.path.isfile(os.path.join(orig_any, 'any.fits'))
    assert not os.path.isdir(procdir)


def test_get_rejected():
    tmpdir = tempfile.mkdtemp()
    reject_dir = os.path.join(tmpdir, 'reject')
    os.mkdir(reject_dir)
    assert len(etrans_core.get_rejected_files(tmpdir)) == 0

    # add an empty directory namecheck
    name_reject = os.path.join(reject_dir, 'namecheck')
    os.mkdir(name_reject)
    rejected_files = etrans_core.get_rejected_files(tmpdir)
    assert len(rejected_files) == 1
    assert rejected_files['namecheck'] == []

    # add an error namecheck file
    with open(os.path.join(name_reject, 'file1'), 'w+') as f:
        f.write('test1')
    rejected_files = etrans_core.get_rejected_files(tmpdir)
    assert len(rejected_files) == 1
    assert len(rejected_files['namecheck']) == 1
    assert 'file1' in rejected_files['namecheck']

    # add a second and a fits verify error file too
    with open(os.path.join(name_reject, 'file2'), 'w+') as f:
        f.write('test2')
    name_reject = os.path.join(reject_dir, 'fits verify')
    os.mkdir(name_reject)
    with open(os.path.join(name_reject, 'file.fits'), 'w+') as f:
        f.write('fits')
    rejected_files = etrans_core.get_rejected_files(tmpdir)
    assert len(rejected_files) == 2
    assert len(rejected_files['namecheck']) == 2
    assert 'file1' in rejected_files['namecheck']
    assert 'file2' in rejected_files['namecheck']
    assert len(rejected_files['fits']) == 1
    assert 'file.fits' in rejected_files['fits']


def test_get_trans_files():
    tmpdir = tempfile.mkdtemp()
    trans_files = etrans_core.get_trans_files(tmpdir)
    assert len(trans_files) == 1
    assert trans_files[0]['error'] == 'No proc directory'

    procdirs = os.path.join(tmpdir, 'proc')
    os.mkdir(procdirs)
    assert not etrans_core.get_trans_files(tmpdir)
    # create an unexpected directory - (only proc* directories expected)
    wrong_dir = os.path.join(procdirs, 'foo')
    os.mkdir(wrong_dir)
    trans_files = etrans_core.get_trans_files(tmpdir)
    assert len(trans_files) == 1
    assert trans_files[0]['error'] == 'Unknown file/dir in proc directory: foo'
    os.rmdir(wrong_dir)

    # create correct proc dir
    procdir = os.path.join(procdirs, 'proc123')
    os.mkdir(procdir)
    proc_new = os.path.join(procdir, 'new')
    os.mkdir(proc_new)
    with open(os.path.join(proc_new, 'new.fits'), 'w+') as f:
        f.write('new')
    etrans_core._write_proc_info(procdir)
    trans_files = etrans_core.get_trans_files(tmpdir)
    assert len(trans_files) == 1
    assert trans_files[0]['started'] is not None
    assert trans_files[0]['files'] == 1
    assert trans_files[0]['pid'] == os.getpid()


@patch('cadcetrans.etrans_core._get_last_week_logs')
@patch('cadcetrans.etrans_core.get_rejected_files')
@patch('cadcetrans.etrans_core.get_trans_files')
def test_print_status(trans_mock, rejected_mock, logs_mock):
    trans_mock.return_value = [{'pid': 123, 'files': 2,
                                'started': '2018-11-21 10:10:10'}]
    rejected_mock.return_value = {'namecheck': ['a', 'b', 'c'],
                                  'fits': []}
    # create a log file with 2 entries
    logfile = os.path.join('/tmp', '#cadc_etrans_unittest.log')
    with open(logfile, 'w+') as f:
        f.write('2010-11-11 00:38:08,480 [11965] put_cadc_file - '
                '{"path": "input.fits", "time": 6.799309968948364, '
                '"speed": 0.1, "success": true, "size": 0.65}\n')
        f.write('2010-11-11 01:38:08,480 [11965] put_cadc_file - '
                '{"path": "input2.fits", "message": "Error on transfer", '
                '"success": false}\n')
    logs_mock.return_value = [logfile]
    with open(os.path.join(TESTDATA_DIR, 'status_test.txt'), 'r') as myfile:
        status = myfile.read()
    with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
        etrans_core.print_status('/tmp')
    assert status == stdout_mock.getvalue()


def test_get_files_to_transfer():
    # empty directory
    tmpdir = tempfile.mkdtemp()
    stream_names = ('new', 'replace')
    assert not etrans_core._get_files_to_transfer(stream_names, tmpdir)

    # create input files - still no returned files due to minimum staging
    # time
    new_dir = os.path.join(tmpdir, 'new')
    os.mkdir(new_dir)
    new_file = os.path.join(new_dir, 'new.fits')
    with open(new_file, 'w+') as f:
        f.write('new')
    replace_dir = os.path.join(tmpdir, 'replace')
    os.mkdir(replace_dir)
    with open(os.path.join(replace_dir, 'replace.fits'), 'w+') as f:
        f.write('replace')
    assert not etrans_core._get_files_to_transfer(stream_names, tmpdir)

    # mock files mtime in the past
    past = int(datetime.now().strftime('%s')) - \
        etrans_core.MIN_STAGE_TIME*60 - 1
    with patch('cadcetrans.etrans_core.os.path.getctime') as ctime_mock:
        ctime_mock.return_value = past
        files = etrans_core._get_files_to_transfer(stream_names, tmpdir)
    assert len(files) == 2
    assert etrans_core.FileInfo('new.fits', 'new') in files
    assert etrans_core.FileInfo('replace.fits', 'replace') in files
