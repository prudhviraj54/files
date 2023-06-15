from unittest.mock import patch, mock_open, Mock, MagicMock
import unittest
import json
import logging
from src.utils import databricks_widget
from src.utils.databricks_widget import SFTPWork
from pathlib import Path
import paramiko
import pytest
import sys
import pgpy
from pgpy.constants import PubKeyAlgorithm, KeyFlags, HashAlgorithm, SymmetricKeyAlgorithm, CompressionAlgorithm

settings_text = '''{
    "spn_id" : "4cc4b64b-7440-4555-91b3-296d7f748776",
    "secret_scope" : "cdo-cac-dl-csandbox",
    "secret_key" : "storage-key",
    "tenant_id" : "5d3e2773-e07f-4432-a630-1a0f68a28a05",
    "storage_account" : "cdncacnprodcdologdl",

    "log_storage_acc" : "cdncacnprodcdologdl",
    "log_container_name" : "nifimigration-logs",
    "log_dir" : "SFTP_logs",

    "host": "azcedledgop02.op01caedl.manulife.com",
    "user" : "cddlsmdmsvcprd",
    "sftp_secret_key" : "cddlsmdmsvcprd",
    "source_file" : "/data-01/apps/cddl/CDDL_Triggers/smdm",
    "target_file" : "/data-01/apps/cddl/CDDL_Triggers/smdm/a_test_file",
    
    "logging_level" : "INFO",
    "table_name" : "SFTP_Test",
    "Container": {
        "staging": {
            "container": "staging",
            "path": "staging/"
        },
        "logs": {
            "container": "logs",
            "path": "logs/"
        }
	}
    
}'''

table_text = '''
{
		"src_path": "/dbfs/mnt/ma-stg-bankccnifi-pii/ma_extract/cadb_ma_curation_prd/bank_model_filteredequifaxout",
		"file_type": "txt",
		"extract_location": "ma_extract/extract_file",
		"target_file_pattern": "eqxds.exmanulifepd.ds.ccdailycrosssell.%s.%s",
		"column_name": "equifax_payload",
		"encryption_info": {
			"user_id": "Manulife_EDL PROD <manulife_edl_prod@manulife.com>",
			"key": "/Users/linborc/OneDrive - Manulife/test/test_projects/pgp/MLBEDLPROD_public_b.asc"
		}
	}'''

def encrypt_file(table, extracted_file, logger):
    try:
        
        encrypted_root = ('%s/%s') % ('/'.join(extracted_file.split('/')[:-1]), 'encrypted')
        encrypted_file = '%s/%s.pgp' % (encrypted_root, extracted_file.split('/')[-1:][0])
        Path(encrypted_root).mkdir(parents=True, exist_ok=True)
        logger.info('Encrypting [%s], file staged at [%s]...' % (extracted_file, encrypted_file))
        encrypted_message = databricks_widget.encrypt(table['encryption_info']['key']
                                                      , table['encryption_info']['user_id']
                                                      , extracted_file
                                                      , encrypted_file, logger)
    except Exception as e:
        logger.error('Error type: %s, Error: %s' % (type(e), e))
        return False
    return encrypted_message

class SFTPTestAttr:
    def __init__(self):
        self._flags = 0
        self.st_size = None
        self.st_uid = None
        self.st_gid = None
        self.st_mode = 0o100000
        self.st_atime = None
        self.st_mtime = None
        self.attr = {}
        self.filename = 'testing_file.txt'
        self.loggername = 'sftp.under.test'
        self.mkdir_module = 'pathlib.Path.mkdir'
        
class TestSFTPWork(unittest.TestCase):
    SFTPWork_module = 'src.utils.databricks_widget.SFTPWork'
    @patch('paramiko.SSHClient', autospec=True)
    def test_connect_target(self, mock_sftp_constructor):
        sftp_test_attr = SFTPTestAttr()
        mock_sftp = mock_sftp_constructor.return_value
        logger = logging.getLogger(sftp_test_attr.loggername)
        _ = SFTPWork("Test", "Test", "Test", logger)
        self.assertTrue(mock_sftp.connect.called)

    @patch(f'{SFTPWork_module}.connect', autospec=True)
    def test_put_files(self, mock_sftp_constructor):
        sftp_test_attr = SFTPTestAttr()
        settings = json.loads(settings_text)
        logger = logging.getLogger(sftp_test_attr.loggername)
        sftp = SFTPWork("Test", "Test", "Test", logger)
        mock_sftp = mock_sftp_constructor.return_value
        
        mock_sftp.listdir_attr = MagicMock(return_value=[sftp_test_attr])
        mock_sftp.put = MagicMock(return_value=None)

        with patch(sftp_test_attr.mkdir_module, MagicMock(autospec=True)):
            val = sftp.put_files(settings['source_file'], settings['target_file'])
        self.assertTrue(val)
        
    @patch(f'{SFTPWork_module}.connect', autospec=True)
    def test_get_files(self, mock_sftp_constructor):
        sftp_test_attr = SFTPTestAttr()
        settings = json.loads(settings_text)
        logger = logging.getLogger(sftp_test_attr.loggername)
        sftp = SFTPWork("Test", "Test", "Test", logger)
        mock_sftp = mock_sftp_constructor.return_value
        
        mock_sftp.listdir_attr = MagicMock(return_value=[sftp_test_attr])
        mock_sftp.get = MagicMock(return_value=None)

        with patch(sftp_test_attr.mkdir_module, MagicMock(autospec=True)):
            val = sftp.get_files(settings['source_file'], settings['target_file'])
        self.assertTrue(val)

    @patch(f'{SFTPWork_module}.connect', autospec=True)
    def test_get_files_dir(self, mock_sftp_constructor):
        sftp_test_attr = SFTPTestAttr()
        settings = json.loads(settings_text)
        logger = logging.getLogger(sftp_test_attr.loggername)
        sftp = SFTPWork("Test", "Test", "Test", logger)
        mock_sftp = mock_sftp_constructor.return_value
        
        sftp_test_attr.st_mode = 0o040000
        mock_sftp.listdir_attr = MagicMock(return_value=[sftp_test_attr])

        with patch(sftp_test_attr.mkdir_module, MagicMock(autospec=True, side_effect=Exception)):
            val = sftp.get_files(settings['source_file'], settings['target_file'])
        self.assertFalse(val)

    @patch(f'{SFTPWork_module}.connect', autospec=True)
    def test_get_files_exception(self, mock_sftp_constructor):
        sftp_test_attr = SFTPTestAttr()
        settings = json.loads(settings_text)
        logger = logging.getLogger(sftp_test_attr.loggername)
        sftp = SFTPWork("Test", "Test", "Test", logger)
        mock_sftp = mock_sftp_constructor.return_value
        
        mock_sftp.listdir_attr = MagicMock(return_value=[sftp_test_attr], side_effect=Exception)

        val = sftp.get_files(settings['source_file'], settings['target_file'])
        self.assertFalse(val)


        
class TestPGP(unittest.TestCase):
    class PGPAttrib:
        def __init__(self, userid, passcode):
            self.userid = userid
            self.passcode = passcode
            self.private_monkey_key = self.create_key()
            self.public_monkey_key = self.private_monkey_key.pubkey

        def create_key(self):
            _private_monkey_key = pgpy.PGPKey.new(PubKeyAlgorithm.RSAEncryptOrSign, 4096)
            uid = pgpy.PGPUID.new(self.userid)  
            _private_monkey_key.add_uid(uid, usage={KeyFlags.Sign, KeyFlags.EncryptCommunications, KeyFlags.EncryptStorage},
                hashes=[HashAlgorithm.SHA256, HashAlgorithm.SHA384, HashAlgorithm.SHA512, HashAlgorithm.SHA224],
                ciphers=[SymmetricKeyAlgorithm.AES256, SymmetricKeyAlgorithm.AES192, SymmetricKeyAlgorithm.AES128],
                compression=[CompressionAlgorithm.ZLIB, CompressionAlgorithm.BZ2, CompressionAlgorithm.ZIP, CompressionAlgorithm.Uncompressed])
            _private_monkey_key.protect(self.passcode, SymmetricKeyAlgorithm.AES256, HashAlgorithm.SHA256)
            
            return _private_monkey_key
        
    def test_encrypt(self):
        logger = logging.getLogger('nifi.migration.test')
        pgp_attrib = self.PGPAttrib('PGP Test', 'T0p$3cr3t')
        with patch('pgpy.PGPKey.from_file', MagicMock(autospec=True)) as mock_pgpkey:
            with patch('builtins.open',  mock_open()) as m_open:
                m_file = m_open.return_value.__enter__.return_value
                reads = ['monkey_message', 'monkey write message']
                m_file.read.side_effect = lambda: reads.pop(0)
                mock_pgpkey.return_value = pgp_attrib.public_monkey_key, None

                val = databricks_widget.encrypt('key', 'PGP Test', 'source_file', 'encrypted_file', logger)
                
                with pgp_attrib.private_monkey_key.unlock(pgp_attrib.passcode):
                    decrypted = pgp_attrib.private_monkey_key.decrypt(val).message

        assert decrypted == 'monkey_message'
        
    def test_decrypt(self):
        logger = logging.getLogger('nifi.migration.test')
        pgp_attrib = self.PGPAttrib('PGP Test', 'T0p$3cr3t')
        with patch('pgpy.PGPKey.from_file', MagicMock(autospec=True)) as mock_pgpkey:
            with patch('pgpy.PGPMessage.from_file',  MagicMock(autospec=True)) as mock_message:
                mock_pgpkey.return_value = pgp_attrib.private_monkey_key, None
                message = pgpy.PGPMessage.new('decrypted_monkey_message')
                mock_message.return_value = pgp_attrib.public_monkey_key.encrypt(message, user=pgp_attrib.userid)
                
                val = databricks_widget.decrypt(pgp_attrib.passcode, pgp_attrib.private_monkey_key, 'encrypted_file', logger)

        assert val == 'decrypted_monkey_message'
def test_config_logger():
    properties = json.loads('{"Log_Level" : "INFO", "Log_Path" : "/test/", "Log_FileName" : "sftp_get_test" }')

    with patch('src.utils.databricks_widget.Path.mkdir', MagicMock(autospec=True)) as mock_dir:
        with patch('logging.FileHandler', MagicMock(autospec=True)):
            logger = databricks_widget.config_logger(properties)
    assert logger.level == logging.INFO


def test_application_setup():
    testargs = ["/home/test", "nifi_migration_setup.json"]
    read_json = json.dumps({'a': 1, 'b': 2, 'c': 3, "table_list": 'Table1'})

    expected = (
        {'a': 1, 'b': 2, 'c': 3, 'table_list': 'Table1'}, {'a': 1, 'b': 2, 'c': 3, 'table_list': 'Table1'})
    with patch('os.path.exists', MagicMock(return_value=True)):
        with patch.object(sys, 'argv', testargs):
            with patch('builtins.open', mock_open(read_data=read_json)):
                with patch('src.utils.databricks_widget.is_safe_path', MagicMock(return_value=True)):
                    val = databricks_widget.application_setup("/home/test", "test.json")

    assert val == expected
    
def test_application_setup_no_tablelist():
    testargs = ["/home/test", "nifi_migration_setup.json"]
    read_json = json.dumps({'a': 1, 'b': 2, 'c': 3})

    expected = ({'a': 1, 'b': 2, 'c': 3})
    with patch('os.path.exists', MagicMock(return_value=True)):
        with patch.object(sys, 'argv', testargs):
            with patch('builtins.open', mock_open(read_data=read_json)):
                with patch('src.utils.databricks_widget.is_safe_path', MagicMock(return_value=True)):
                    val, _ = databricks_widget.application_setup("/home/test", "test.json")

    assert val == expected

def test_check_argvs():
    with patch.object(sys, 'argv', ["python","ingestion-history.py", "common.properties"]):
        assert databricks_widget.check_argvs() == ('ingestion-history.py','common.properties')
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        with patch.object(sys, 'argv', ["ingestion-history.py"]):
            databricks_widget.check_argvs()
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 1
    
def test_check_extra_argvs():
    with patch.object(sys, 'argv', ["python","ingestion-history2.py", "common2.properties","staging"]) as mock_argv:
        assert databricks_widget.check_argvs(4) == ('ingestion-history2.py','common2.properties')
        assert mock_argv[3] == 'staging'

def test_encrypt_file():
    logger = logging.getLogger('nifi.migration.test')
    table = properties = json.loads(table_text)
    pubkey, _ = pgpy.PGPKey.from_file(table['encryption_info']['key'])
    # public_binary = '/Users/linborc/OneDrive - Manulife/test/test_projects/pgp/MLBEDLPROD_public_b.asc'
    
    # with open (public_binary, 'wb') as binary_file:
    #     binarykeybytes = bytes(pubkey)
    #     binary_file.write(binarykeybytes)
    extracted_file = '/Users/linborc/OneDrive - Manulife/test/test_projects/data/encrypt_this.txt'
    encrypted_message = encrypt_file(table, extracted_file, logger)

    assert encrypted_message != False
