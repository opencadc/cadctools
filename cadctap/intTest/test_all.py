# Integration tests for cadc-tap

import sys
import os
import logging
from io import StringIO, BytesIO
from mock import patch
from astropy.io import fits
import tempfile
import numpy as np
import cadctap
from cadcutils.net import Subject
from astropy.io.votable import parse_single_table
import random

from cadctap.core import main_app
THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')


logger = logging.getLogger('test')
logging.basicConfig()
logger.setLevel(logging.DEBUG)

try:
    DB_SCHEMA_NAME = os.environ['CADCTAP_SCHEMA']
    logger.info('Set SCHEMA to {} (CADCTAP_SCHEMA env)'.format(DB_SCHEMA_NAME))
except KeyError:
    DB_SCHEMA_NAME = 'cadcauthtest1'
    logger.error(
        'Using the default schema {}'.format(DB_SCHEMA_NAME))
    sys.exit(-1)

try:
    CERT_DIR = os.environ['CADC_CERT_DIR']
except KeyError:
    logger.error(
        '$CADC_CERT environment variable required to run the int tests')
    sys.exit(-1)
CERT1 = os.path.join(CERT_DIR, 'x509_CADCAuthtest1.pem')
assert os.path.exists(CERT1), 'Cannot find {}'.format(CERT1)
DN1 = 'CN=cadcauthtest1_24c,OU=cadc,O=hia,C=ca'  # TODO extract from cert
CERT2 = os.path.join(CERT_DIR, 'x509_CADCAuthtest2.pem')
DN2 = 'CN=cadcauthtest2_ff0,OU=cadc,O=hia,C=ca' # TODO extract from cert
assert os.path.exists(CERT2), 'Cannot find {}'.format(CERT2)
# not member of any group
CERTREG = os.path.join(CERT_DIR, 'x509_CADCRegtest1.pem')

# group CADC_TEST2-Staff has members: CADCAuthtest1, CADCAuthtest2
GROUP = "CADC_TEST2-Staff"

try:
    TAP_HOST = os.environ['TAP_HOST']
    logger.info('Set TAP_HOST to {}'.format(TAP_HOST))
except KeyError:
    TAP_HOST = 'www.cadc-ccda.hia-iha.nrc-cnrg.gc.ca'
    logger.info(
        'Using the default TAP_HOST'.format(TAP_HOST))

TABLE_NAME = 'testtable'
TABLE = '{}.{}'.format(DB_SCHEMA_NAME, TABLE_NAME)
TABLE_DEF = '{}/createTable.vosi'.format(TESTDATA_DIR)
HOST = '--host ' + TAP_HOST

def test_astropytable():
    # example of how to integrate with astropy table. Not sure it belongs here
    client = cadctap.CadcTapClient(Subject(),
                                   resource_id='ivo://cadc.nrc.ca/argus')
    buffer = BytesIO()
    client.query('select top 1000 * from caom2.Observation', output_file=buffer)
    tb = parse_single_table(buffer).to_table()
    assert len(tb) == 1000


def test_commands(monkeypatch):
    # test cadc TAP service with anonymous access
    sys.argv = ['cadc-tap', 'query', '-d', '-a', '-s', 'ivo://cadc.nrc.ca/argus', '-f',
                'VOTable', 'select observationID FROM caom2.Observation '
                'where observationID=\'dao_c122_2018_003262\'']
    with patch('sys.stdout', new_callable=BytesIO) as stdout_mock:
        main_app()
    assert b'<INFO name="QUERY_STATUS" value="OK" />' in stdout_mock.getvalue()
    assert b'<TD>dao_c122_2018_003262</TD>' in stdout_mock.getvalue()

    # monkeypatch required for the "user interaction"
    sys.argv = 'cadc-tap delete --cert {} {} {}'.format(CERT1, HOST,
                                                        TABLE).split()
    try:
        monkeypatch.setattr('cadctap.core.input', lambda x: "yes")
        with patch('cadctap.core.sys.exit'):
            main_app()
        logger.debug('Deleted table {}'.format(TABLE))
    except Exception as e:
        logger.debug(
            'Cannot delete table {}. Reason: {}'.format(TABLE, str(e)))

    # create table
    sys.argv = 'cadc-tap create --cert {} {} {} {}'.format(
        CERT1, HOST, TABLE, TABLE_DEF).split()
    try:
        main_app()
        logger.debug('Created table {}'.format(TABLE))
    except Exception as e:
        logger.debug(
            'Cannot create table {}. Reason: {}'.format(TABLE, str(e)))
        raise e

    sys.argv = 'cadc-tap query -f VOTable --cert {} {}'.format(CERT1,
                                                               HOST).split()
    sys.argv.append('select * from {}'.format(TABLE))
    with patch('sys.stdout', new_callable=BytesIO) as stdout_mock:
        main_app()
    assert b'<INFO name="QUERY_STATUS" value="OK" />' in stdout_mock.getvalue()
    assert b'<TABLEDATA />' in stdout_mock.getvalue()

    # create index
    sys.argv = 'cadc-tap index --cert {} {} {} article'.format(
        CERT1, HOST, TABLE).split()
    try:
        main_app()
        logger.debug('Create index on {}.article'.format(TABLE))
    except Exception as e:
        logger.debug(
            'Cannot create index {}.article. Reason: {}'.format(TABLE, str(e)))
        raise e

    # load data csv format
    sys.argv = 'cadc-tap load -f csv --cert {} {} {} {}'.format(
        CERT1, HOST, TABLE,
        os.path.join(TESTDATA_DIR, 'loadTable_csv.txt')).split()
    try:
        main_app()
        logger.debug('Load table {} (csv)'.format(TABLE))
    except Exception as e:
        logger.debug(
            'Cannot load table csv {}. Reason: {}'.format(TABLE, str(e)))
        raise e

    sys.argv = 'cadc-tap query -f csv --cert {} {} '.format(CERT1, HOST).split()
    sys.argv.append('select count, article from {}'.format(TABLE))
    with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
        main_app()
    result = stdout_mock.getvalue()
    assert result == \
           'count,article\n-----------------------\n1,art1\n2,' \
           'art2\n3,art3\n\n(3 rows affected)\n'

    # load data tsv format
    sys.argv = 'cadc-tap load --cert {} {} {} {}'.format(
        CERT1, HOST, TABLE,
        os.path.join(TESTDATA_DIR, 'loadTable_tsv.txt')).split()
    try:
        main_app()
        logger.debug('Load table {} (tsv)'.format(TABLE))
    except Exception as e:
        logger.debug(
            'Cannot load tsv table {}. Reason: {}'.format(TABLE, str(e)))
        raise e

    sys.argv = 'cadc-tap query -f VOTable --cert {} {}'.format(CERT1,
                                                               HOST).split()
    sys.argv.append('select * from {}'.format(TABLE))
    with patch('sys.stdout', new_callable=BytesIO) as stdout_mock:
        main_app()
    result = stdout_mock.getvalue()
    assert b'<INFO name="QUERY_STATUS" value="OK" />' in result
    # 6 rows
    assert 6 == result.count(b'<TD>art')
    for i in range(1, 6):
        assert '<TD>art{}</TD>'.format(i).encode('utf-8') in result
        assert '<TD>{}</TD>'.format(i).encode('utf-8') in result

    # load data BINTABLE format
    hdu0 = fits.PrimaryHDU()
    hdu0.header['COMMENT'] = 'Sample BINTABLE'

    c1 = fits.Column(name='article', array=np.array(['art6', 'art7', 'art8']),
                     format='5A')
    c2 = fits.Column(name='count', array=np.array([6, 7, 8]), format='K')
    hdu1 = fits.BinTableHDU.from_columns([c1, c2])
    new_hdul = fits.HDUList([hdu0, hdu1])

    tempdir = tempfile.mkdtemp()
    bintb_file = os.path.join(tempdir, 'bintable.fits')
    new_hdul.writeto(bintb_file)

    sys.argv = 'cadc-tap load -f FITSTable --cert {} {} {} {}'.format(
        CERT1, HOST, TABLE, bintb_file).split()
    try:
        main_app()
        logger.debug('Load table {} (bintable)'.format(TABLE))
    except Exception as e:
        logger.debug(
            'Cannot load table bintable {}. Reason: {}'.format(TABLE, str(e)))
        raise e

    sys.argv = 'cadc-tap query -f VOTable --cert {} {}'.format(CERT1,
                                                               HOST).split()
    sys.argv.append('select * from {}'.format(TABLE))
    with patch('sys.stdout', new_callable=BytesIO) as stdout_mock:
        main_app()
    result = stdout_mock.getvalue()
    assert b'<INFO name="QUERY_STATUS" value="OK" />' in result
    # 9 rows
    assert 9 == result.count(b'<TD>art')
    for i in range(1, 9):
        assert '<TD>art{}</TD>'.format(i).encode('utf-8') in result
        assert '<TD>{}</TD>'.format(i).encode('utf-8') in result

    # TODO query with temporary table
    # cleanup
    sys.argv = 'cadc-tap delete --cert {} {} {}'.format(CERT1, HOST,
                                                        TABLE).split()
    try:
        monkeypatch.setattr('cadctap.core.input', lambda x: "yes")
        main_app()
        logger.debug('Deleted table {}'.format(TABLE))
    except Exception as e:
        logger.debug(
            'Cannot delete table {}. Reason: {}'.format(TABLE, str(e)))
        raise e

def _get_permissions(cert, resource):
    """
    parse permissions from the output of the schema command
    :return: array with permissions elements: owner, others, g-read, g-rwrite
    """
    sys.argv = 'cadc-tap schema --cert {} {} {}'.format(cert, HOST,
                                                        resource).split()
    with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
        main_app()
    return stdout_mock.getvalue().split('\n')[-4].split()

def _create_table(monkeypatch, owner_cert):
    sys.argv = 'cadc-tap delete --cert {} {} {}'.format(CERT1, HOST,
                                                        TABLE).split()
    try:
        monkeypatch.setattr('cadctap.core.input', lambda x: "yes")
        with patch('cadctap.core.sys.exit'):
            main_app()
        logger.debug('Deleted table {}'.format(TABLE))
    except Exception as e:
        logger.debug(
            'Cannot delete table {}. Reason: {}'.format(TABLE, str(e)))

    # create table
    sys.argv = 'cadc-tap create --cert {} {} {} {}'.format(
        owner_cert, HOST, TABLE, TABLE_DEF).split()
    try:
        main_app()
        logger.debug('Created table {}'.format(TABLE))
    except Exception as e:
        logger.debug(
            'Cannot create table {}. Reason: {}'.format(TABLE, str(e)))
        raise e

def test_permission_settings(monkeypatch):
    sys.argv = 'cadc-tap permission --cert {} {} og-rw {}'.format(
        CERT1, HOST, DB_SCHEMA_NAME).split()
    main_app()
    # create a table
    _create_table(monkeypatch, CERT1)

    # schema permissions at the minimum - overrides table permissions
    assert [DN1, 'false', '-', '-'] == _get_permissions(CERT1, DB_SCHEMA_NAME)
    _check_read_permissions(TABLE, True, False, False, False)

    sys.argv = 'cadc-tap permission --cert {} {} g+r {} {}'.format(
        CERT1, HOST, TABLE, GROUP).split()
    main_app()
    assert [DN1, 'false', GROUP, '-'] == _get_permissions(CERT1, TABLE)
    _check_read_permissions(TABLE, True, False, False, False)

    sys.argv = 'cadc-tap permission --cert {} {} g+w {} {}'.format(
        CERT1, HOST, TABLE, GROUP).split()
    main_app()
    assert [DN1, 'false', GROUP, GROUP] == \
           _get_permissions(CERT1, TABLE)
    _check_read_permissions(TABLE, True, False, False, False)

    sys.argv = 'cadc-tap permission --cert {} {} o+r {}'.format(
        CERT1, HOST, TABLE).split()
    main_app()
    assert [DN1, 'true', GROUP, GROUP] == \
           _get_permissions(CERT1, TABLE)
    _check_read_permissions(TABLE, True, False, False, False)

    # change the schema permissions - table has all permissions
    sys.argv = 'cadc-tap permission --cert {} {} g+r {} {}'.format(
        CERT1, HOST, DB_SCHEMA_NAME, GROUP).split()
    main_app()
    assert [DN1, 'false', GROUP, '-'] == \
           _get_permissions(CERT1, DB_SCHEMA_NAME)
    _check_read_permissions(TABLE, True, True, False, False)

    sys.argv = 'cadc-tap permission --cert {} {} g+w {} {}'.format(
        CERT1, HOST, DB_SCHEMA_NAME, GROUP).split()
    main_app()
    assert [DN1, 'false', GROUP, GROUP] == \
           _get_permissions(CERT1, DB_SCHEMA_NAME)
    _check_read_permissions(TABLE, True, True, False, False)

    sys.argv = 'cadc-tap permission --cert {} {} o+r {}'.format(
        CERT1, HOST, DB_SCHEMA_NAME).split()
    main_app()
    assert [DN1, 'true', GROUP, GROUP] == \
           _get_permissions(CERT1, DB_SCHEMA_NAME)
    _check_read_permissions(TABLE, True, True, True, True)

    # remove table and create a new one owned by CERT2 user
    _create_table(monkeypatch, CERT2)
    _check_read_permissions(TABLE, False, True, False, False)

    sys.argv = 'cadc-tap permission --cert {} {} g+r {} {}'.format(
        CERT2, HOST, TABLE, GROUP).split()
    main_app()
    assert [DN2, 'false', GROUP, '-'] == \
           _get_permissions(CERT2, TABLE)
    _check_read_permissions(TABLE, True, True, False, False)

    sys.argv = 'cadc-tap permission --cert {} {} o+r {}'.format(
        CERT2, HOST, TABLE).split()
    main_app()
    assert [DN2, 'true', GROUP, '-'] == \
           _get_permissions(CERT2, TABLE)
    _check_read_permissions(TABLE, True, True, True, True)

    sys.argv = 'cadc-tap permission --cert {} {} og-r {}'.format(
        CERT2, HOST, TABLE).split()
    main_app()
    assert [DN2, 'false', '-', '-'] == \
           _get_permissions(CERT2, TABLE)
    # try to delete as CERT1
    _create_table(monkeypatch, CERT1)


def test_write_delete_permissions(monkeypatch):
    # monkeypatch required for the "user interaction"

    # set schema permissions
    sys.argv = 'cadc-tap permission --cert {} {} og-rw {}'.format(
        CERT1, HOST, DB_SCHEMA_NAME).split()
    main_app()
    sys.argv = 'cadc-tap permission --cert {} {} og+r {} {}'.format(
        CERT1, HOST, DB_SCHEMA_NAME, GROUP).split()
    main_app()
    sys.argv = 'cadc-tap schema --cert {} {} {}'.format(CERT1, HOST,
                                                        DB_SCHEMA_NAME).split()
    with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
        main_app()
    result = stdout_mock.getvalue()

    # create table with no credentials (Fail)
    sys.argv = 'cadc-tap create {} {} {}'.format(
        HOST, TABLE, TABLE_DEF).split()
    try:
        with patch('sys.stderr', new_callable=StringIO) as stdout_mock:
            main_app()
            assert False, 'Application should have aborted due to access error'
    except SystemExit as e:
        assert e.code == -1
        assert 'permission denied' in stdout_mock.getvalue()

    # create table with no write permission in schema (Fail)
    for cert in [CERT2, CERTREG]:
        sys.argv = 'cadc-tap create --cert {} {} {} {}'.format(
            cert, HOST, TABLE, TABLE_DEF).split()
        try:
            with patch('sys.stderr', new_callable=StringIO) as stdout_mock:
                main_app()
                assert False,\
                    'Application should have aborted due to access error'
        except SystemExit as e:
            assert e.code == -1
            assert 'permission denied' in stdout_mock.getvalue()

    _create_table(monkeypatch, CERT1)

    # -------------------- test write access ---------------
    hdu0 = fits.PrimaryHDU()
    hdu0.header['COMMENT'] = 'Sample BINTABLE'

    c1 = fits.Column(name='article', array=np.array(['art1']),
                     format='5A')
    c2 = fits.Column(name='count', array=np.array([1]), format='K')
    hdu1 = fits.BinTableHDU.from_columns([c1, c2])
    new_hdul = fits.HDUList([hdu0, hdu1])

    tempdir = tempfile.mkdtemp()
    bintb_file = os.path.join(tempdir, 'bintable.fits')
    new_hdul.writeto(bintb_file)

    sys.argv = 'cadc-tap load -f FITSTable --cert {} {} {} {}'.format(
        CERT2, HOST, TABLE, bintb_file).split()
    try:
        with patch('sys.stderr', new_callable=StringIO) as stdout_mock:
            main_app()
            assert False, 'Application should have aborted due to access error'
    except SystemExit as e:
        assert e.code == -1
        assert 'permission denied' in stdout_mock.getvalue()

    # give write access and try again
    sys.argv = 'cadc-tap permission --cert {} {} g+w {} {}'.format(
        CERT1, HOST, TABLE, GROUP).split()
    main_app()

    sys.argv = 'cadc-tap load -f FITSTable --cert {} {} {} {}'.format(
        CERT2, HOST, TABLE, bintb_file).split()
    try:
        main_app()
        logger.debug('Load table {} (bintable)'.format(TABLE))
    except Exception as e:
        logger.debug(
            'Cannot load table bintable {}. Reason: {}'.format(TABLE, str(e)))
        raise e

    sys.argv = 'cadc-tap query -f VOTable --cert {} {}'.format(CERT1,
                                                               HOST).split()
    sys.argv.append('select * from {}'.format(TABLE))
    with patch('sys.stdout', new_callable=BytesIO) as stdout_mock:
        main_app()
    result = stdout_mock.getvalue()
    assert b'<INFO name="QUERY_STATUS" value="OK" />' in result
    # 1 row
    assert 1 == result.count(b'<TD>art')
    for i in range(1, 1):
        assert '<TD>art{}</TD>'.format(i).encode('utf-8') in result
        assert '<TD>{}</TD>'.format(i).encode('utf-8') in result

    # REG user still failing
    sys.argv = 'cadc-tap load -f FITSTable --cert {} {} {} {}'.format(
        CERTREG, HOST, TABLE, bintb_file).split()

    try:
        with patch('sys.stderr', new_callable=StringIO) as stdout_mock:
            main_app()
            assert False, 'Application should have aborted due to access error'
    except SystemExit as e:
        assert e.code == -1
        assert 'permission denied' in stdout_mock.getvalue()

    # test remove with anon user
    sys.argv = 'cadc-tap delete {} {}'.format(HOST, TABLE).split()
    try:
        with patch('sys.stderr', new_callable=StringIO) as stdout_mock:
            monkeypatch.setattr('cadctap.core.input', lambda x: "yes")
            main_app()
            assert False, 'Application should have aborted due to access error'
    except SystemExit as e:
        assert e.code == -1
        assert 'permission denied' in stdout_mock.getvalue()

    # same thing with user in no groups or in write group
    for cert in [CERT2, CERTREG]:
        sys.argv = 'cadc-tap delete --cert {} {} {}'.format(
            CERTREG, HOST, TABLE).split()
        try:
            with patch('sys.stderr', new_callable=StringIO) as stdout_mock:
                monkeypatch.setattr('cadctap.core.input', lambda x: "yes")
                main_app()
                assert False, 'Application should have aborted due to access error'
        except SystemExit as e:
            assert e.code == -1
            assert 'permission denied' in stdout_mock.getvalue()

    # success if user is the owner
    sys.argv = 'cadc-tap delete --cert {} {} {}'.format(CERT1, HOST,
                                                        TABLE).split()
    try:
        monkeypatch.setattr('cadctap.core.input', lambda x: "yes")
        main_app()
        logger.debug('Deleted table {}'.format(TABLE))
    except Exception as e:
        logger.debug(
            'Cannot delete table {}. Reason: {}'.format(TABLE, str(e)))
        raise e


def _check_read_permissions(table, auth1=False, auth2=False, reg=False,
                            anon=False):
    formats = ['VOTable', 'tsv', 'csv']
    for cert, read_access in [(CERT1, auth1), (CERT2, auth2), (CERTREG, reg),
                 (None, anon)]:
        logger.debug("User cert: {}".format(cert))
        format = formats[random.randrange(0, 3)]
        if format == 'VOTable':
            out_format = BytesIO
        else:
            out_format = StringIO
        cert_opt = '--cert {}'.format(cert) if cert else ''
        sys.argv = \
            'cadc-tap query -f {} {} {}'.format(format, cert_opt, HOST).split()
        sys.argv.append('select * from {}'.format(TABLE))
        if read_access:
            with patch('sys.stdout', new_callable=out_format) as stdout_mock:
                main_app()
            result = stdout_mock.getvalue()
            if out_format == BytesIO:
                assert b'<INFO name="QUERY_STATUS" value="OK" />' in result
            else:
                assert 'rows affected)' in result
        else:
            try:
                with patch('sys.stderr', new_callable=StringIO) as stdout_mock:
                    main_app()
                    assert False,\
                        'Application should have aborted due to access error'
            except SystemExit as e:
                assert e.code == -1
                assert 'is not found in TapSchema. Possible reasons: table ' \
                       'does not exist or permission is denied.' in \
                    stdout_mock.getvalue() or 'permission denied on table' in \
                    stdout_mock.getvalue()

