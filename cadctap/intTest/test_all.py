# Integration tests for cadc-tap

import sys
import os
import logging
from six import StringIO
from mock import patch, Mock
from astropy.io import fits
import tempfile
import numpy as np

from cadctap.core import main_app
THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')


logger = logging.getLogger('test')
logging.basicConfig()
logger.setLevel(logging.DEBUG)

try:
    DB_SCHEMA_NAME = os.environ['CADCTAP_SCHEMA']
except KeyError:
    logger.error(
        '$CADCTAP_SCHEMA environment variable required to run the int tests')
    sys.exit(-1)

try:
    CERT = os.environ['CADC_CERT']
except KeyError:
    logger.error(
        '$CADC_CERT environment variable required to run the int tests')
    sys.exit(-1)

TABLE_NAME = 'testtable'
TABLE = '{}.{}'.format(DB_SCHEMA_NAME, TABLE_NAME)
TABLE_DEF = '{}/createTable.vosi'.format(TESTDATA_DIR)



def test_commands(monkeypatch):
    # test cadc TAP service with anonymous access
    sys.argv = ['cadc-tap', 'query', '-s', 'ivo://cadc.nrc.ca/tap', '-f', 'VOTable',
                'select observationID FROM caom2.Observation '
                'where observationID=\'dao_c122_2018_003262\'']
    with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
        main_app()
    assert '<INFO name="QUERY_STATUS" value="OK" />' in stdout_mock.getvalue()
    assert '<TD>dao_c122_2018_003262</TD>' in stdout_mock.getvalue()

    # monkeypatch required for the "user interaction"
    sys.argv = 'cadc-tap delete --cert {} {}'.format(CERT, TABLE).split()
    try:
        monkeypatch.setattr('cadctap.core.input', lambda x: "yes")
        with patch('cadctap.core.sys.exit'):
            main_app()
        logger.debug('Deleted table {}'.format(TABLE))
    except Exception as e:
        logger.debug(
            'Cannot delete table {}. Reason: {}'.format(TABLE, str(e)))

    # create table
    sys.argv = 'cadc-tap create --cert {} {} {}'.format(
        CERT, TABLE, TABLE_DEF).split()
    try:
        main_app()
        logger.debug('Created table {}'.format(TABLE))
    except Exception as e:
        logger.debug(
            'Cannot create table {}. Reason: {}'.format(TABLE, str(e)))
        raise e

    sys.argv = 'cadc-tap query -f VOTable --cert {}'.format(CERT).split()
    sys.argv.append('select * from {}'.format(TABLE))
    with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
        main_app()
    assert '<INFO name="QUERY_STATUS" value="OK" />' in stdout_mock.getvalue()
    assert '<TABLEDATA />' in stdout_mock.getvalue()

    # create index
    sys.argv = 'cadc-tap index --cert {} {} article'.format(
        CERT, TABLE).split()
    try:
        main_app()
        logger.debug('Create index on {}.article'.format(TABLE))
    except Exception as e:
        logger.debug(
            'Cannot create index {}.article. Reason: {}'.format(TABLE, str(e)))
        raise e

    # load data csv format
    sys.argv = 'cadc-tap load -f csv --cert {} {} {}'.format(
        CERT, TABLE, os.path.join(TESTDATA_DIR, 'loadTable_csv.txt')).split()
    try:
        main_app()
        logger.debug('Load table {} (csv)'.format(TABLE))
    except Exception as e:
        logger.debug(
            'Cannot load table csv {}. Reason: {}'.format(TABLE, str(e)))
        raise e

    sys.argv = 'cadc-tap query -f VOTable --cert {}'.format(CERT).split()
    sys.argv.append('select * from {}'.format(TABLE))
    with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
        main_app()
    result = stdout_mock.getvalue()
    assert '<INFO name="QUERY_STATUS" value="OK" />' in result
    # 3 rows
    assert 3 == result.count('<TD>art')
    for i in range(1, 3):
        assert '<TD>art{}</TD>'.format(i) in result
        assert '<TD>{}</TD>'.format(i) in result

    # load data tsv format
    sys.argv = 'cadc-tap load --cert {} {} {}'.format(
        CERT, TABLE, os.path.join(TESTDATA_DIR, 'loadTable_tsv.txt')).split()
    try:
        main_app()
        logger.debug('Load table {} (tsv)'.format(TABLE))
    except Exception as e:
        logger.debug(
            'Cannot load tsv table {}. Reason: {}'.format(TABLE, str(e)))
        raise e

    sys.argv = 'cadc-tap query -f VOTable --cert {}'.format(CERT).split()
    sys.argv.append('select * from {}'.format(TABLE))
    with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
        main_app()
    result = stdout_mock.getvalue()
    assert '<INFO name="QUERY_STATUS" value="OK" />' in result
    # 6 rows
    assert 6 == result.count('<TD>art')
    for i in range(1, 6):
        assert '<TD>art{}</TD>'.format(i) in result
        assert '<TD>{}</TD>'.format(i) in result

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

    sys.argv = 'cadc-tap load -f FITSTable --cert {} {} {}'.format(
        CERT, TABLE, bintb_file).split()
    try:
        main_app()
        logger.debug('Load table {} (bintable)'.format(TABLE))
    except Exception as e:
        logger.debug(
            'Cannot load table bintable {}. Reason: {}'.format(TABLE, str(e)))
        raise e

    sys.argv = 'cadc-tap query -f VOTable --cert {}'.format(CERT).split()
    sys.argv.append('select * from {}'.format(TABLE))
    with patch('sys.stdout', new_callable=StringIO) as stdout_mock:
        main_app()
    result = stdout_mock.getvalue()
    assert '<INFO name="QUERY_STATUS" value="OK" />' in result
    # 9 rows
    assert 9 == result.count('<TD>art')
    for i in range(1, 9):
        assert '<TD>art{}</TD>'.format(i) in result
        assert '<TD>{}</TD>'.format(i) in result

    #TODO query with temporary table

    # cleanup
    sys.argv = 'cadc-tap delete --cert {} {}'.format(CERT, TABLE).split()
    try:
        monkeypatch.setattr('cadctap.core.input', lambda x: "yes")
        main_app()
        logger.debug('Deleted table {}'.format(TABLE))
    except Exception as e:
        logger.debug(
            'Cannot delete table {}. Reason: {}'.format(TABLE, str(e)))
        raise e



