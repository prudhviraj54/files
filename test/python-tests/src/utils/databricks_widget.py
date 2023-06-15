from operator import sub
from pyspark.sql import functions as F
import json
import os
import logging
from datetime import datetime
from pathlib import Path
import paramiko
from stat import S_ISDIR, S_ISREG
import pgpy
import sys
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
import shutil

class SFTPWork:
    def __init__(self, host, username, password, logger):
        self.host = host
        self.username = username
        self.password = password
        self.connector = self.connect()
        self.logger = logger
        self.file_get = 0
        self.file_remove = 0
        self.file_put = 0
    
    def connect(self):
        # paramiko.transport.Transport._preferred_kex = (
        # 'ecdh-sha2-nistp256', 'ecdh-sha2-nistp384', 'ecdh-sha2-nistp521', 'diffie-hellman-group16-sha512'
        # , 'diffie-hellman-group-exchange-sha256', 'diffie-hellman-group14-sha256', 'diffie-hellman-group-exchange-sha1'
        # , 'diffie-hellman-group14-sha1', 'diffie-hellman-group1-sha1')
        paramiko.transport.Transport._preferred_ciphers = ('aes256-ctr', 'aes192-ctr', 'aes128-ctr')
        paramiko.transport.Transport._preferred_macs = ('hmac-sha2-512', 'hmac-sha2-256')
        paramiko.transport.Transport._preferred_key_types = ('ssh-ed25519')
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=self.host, username=self.username, password=self.password)
        sftp = ssh.open_sftp()
        
        return sftp 
    
    def put_files(self, source_dir, target_dir):
        status = False
        try:            
            remote_path = target_dir
            self.logger.info('Start processing: %s' % (source_dir))
            for cur_path, directories, files in os.walk(source_dir):
                if files:
                    for file in files:
                        self.logger.info('Sending: %s/%s' % (cur_path, file))
                        self.connector.put((cur_path + '/' + file), (remote_path + '/' + file))
                        self.file_put += 1
            status = True
        except Exception as e:
            self.logger.error("%s" % e)

        return status
        
    def remove_files(self, root_directory):
        self.connector.chdir(root_directory)

        try:
            self.logger.info('processing: %s' % root_directory)

            for entry in self.connector.listdir_attr(root_directory):
                # Directory or file check
                mode = entry.st_mode
                if S_ISDIR(mode):

                    sub_directory = root_directory + entry.filename + '/'

                    self.logger.info(sub_directory + " is folder")

                    # sub_sub_directory_list = self.connector.listdir_attr(sub_directory)
                    self.remove_files(sub_directory)

                elif S_ISREG(mode):
                    
                    self.logger.info('Remove: %s/%s ...' % (root_directory, entry.filename))
                    # self.connector.get(source_directory + entry.filename, target_directory + entry.filename)
                    self.connector.remove(root_directory + '/' + entry.filename)
                    self.file_remove += 1

        except Exception as e:
            self.logger.error("%s" % e)
            self.logger.info("connection close")
            return False
                
        return True
        
    def get_files(self, source_directory, target_directory):
        try:
            if source_directory[-1] != '/': source_directory = source_directory + '/'
            if target_directory[-1] != '/': target_directory = target_directory + '/'
            for entry in self.connector.listdir_attr(source_directory):
                # Directory or file check
                mode = entry.st_mode
                if S_ISDIR(mode):

                    sub_directory = source_directory + entry.filename + '/'
                    sub_target_directory = target_directory + entry.filename + '/'

                    self.logger.info(sub_directory + " is folder")

                    # Create Target directory
                    Path(sub_target_directory).mkdir(parents=True, exist_ok=True)

                    # Loop through the directory and fetch files
                    self.get_files(sub_directory, sub_target_directory)

                elif S_ISREG(mode):
                    
                    self.logger.info('Pulling: %s/%s -> %s/%s' % (source_directory, entry.filename, target_directory, entry.filename))
                    self.connector.get(source_directory + entry.filename, target_directory + entry.filename)
                    self.file_get += 1

        except Exception as e:
            self.logger.error("%s" % e)
            return False

        return True

    def __del__(self):
        self.connector.close()

def get_dbutils(spark):
    """
    Get dbutils object
    :return: DBUtils
    """
    from pyspark.dbutils import DBUtils #NOSONAR
    return DBUtils(spark)

def check_argvs(num_argv = 3):
    """Ensure we have the proper args passed in
    """
    if (num_argv == 3) and (len(sys.argv) != 3):
        print(sys.argv)
        sys.exit(1)
    _setting_path = sys.argv[1]
    _setting_name = sys.argv[2]

    return _setting_path, _setting_name

def is_safe_path(basedir, path, follow_symlinks=True):
    # resolves symbolic links
    if os.path.exists(os.path.dirname(basedir)):
        if follow_symlinks:
            matchpath = os.path.realpath(path)
        else:
            matchpath = os.path.abspath(path)
        return basedir == os.path.commonpath((basedir, matchpath))

def clean_char(chr_code):
    return  (((48<=chr_code<58) and chr(chr_code)) or 
            ((65<=chr_code<91) and chr(chr_code)) or 
            ((97<=chr_code<123) and chr(chr_code)) or 
            ((45<=chr_code<48) and chr(chr_code)) or 
            ((95==chr_code) and chr(chr_code)) or chr(37))
        
def file_normalizer(filename):
    if not filename:  return None #NOSONAR
    string_code = [ ord(c) for c in filename ]
    return_filename = ""
    for code in string_code: return_filename += clean_char(code) #NOSONAR
    return return_filename

def create_date_subdir(staging_path, subdir=None):
    if subdir:
        staging_path = ('%s/%s') % (staging_path, subdir)
        now = datetime.utcnow()
        if subdir not in ['transport', 'ma_extract']:
            date_path = ('%s/%s/%s/%s') % (staging_path, now.strftime('%Y'), now.strftime('%m'), now.strftime('%d'))
        else:
            date_path = staging_path
        Path(date_path).mkdir(parents=True, exist_ok=True)
    else:
        date_path = staging_path

    return date_path
            
def application_setup(_setting_path, _setting_name):

    setting_file = file_normalizer('%s/%s' % (_setting_path,_setting_name))
    _settings = None
    if os.path.exists(os.path.dirname(setting_file)):
        if is_safe_path(_setting_path, setting_file):
            with open(setting_file) as config_file:
                _settings = json.loads(config_file.read())

    _tables_list = None
    if 'table_list' in _settings:
        table_list_file = file_normalizer('%s/%s' % (_setting_path, _settings['table_list']))
        if os.path.exists(os.path.dirname(table_list_file)):
            if is_safe_path(_setting_path, table_list_file):
                with open(table_list_file) as config_file:
                    _tables_list = json.loads(config_file.read())

    return _settings, _tables_list

def config_logger(_settings): 
    logger = logging.getLogger('databricks.nifimigration.ingestion')
    level = logging.getLevelName(_settings['Log_Level']) if 'Log_Level' in _settings else logging.getLevelName('INFO')
    logger.setLevel(level)

    log_formater = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(process)d - %(message)s')
    # Configure a console output
    console_andler = logging.StreamHandler()

    now = datetime.utcnow()
    logpath = _settings['Log_Path'] + now.strftime('%Y') + '/' + now.strftime('%m') + '/'

    Path(logpath).mkdir(parents=True, exist_ok=True)

    log_filename = _settings['Log_FileName']

    dfn = "%s_%s.log" % (logpath + log_filename, now.strftime('%Y_%m_%d_%H-%M-%S'))

    file_handler = logging.FileHandler(dfn)

    console_andler.setFormatter(log_formater)
    file_handler.setFormatter(log_formater)

    if not len(logger.handlers):
        logger.addHandler(file_handler)
        logger.addHandler(console_andler)

    return logger

def adls_passthrough(spark, _storage_account, _secret_key, _settings, _dbutils): 
    
    secret = _dbutils.secrets.get(scope=_settings['secret_scope'], key=_secret_key)
    spark.conf.set("fs.azure.account.auth.type", "OAuth")
    spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set("fs.azure.account.oauth2.client.id", _settings['spn_id'])
    spark.conf.set("fs.azure.account.oauth2.client.secret", secret)          #NOSONAR
    spark.conf.set("fs.azure.account.oauth2.client.endpoint"
                   , ("https://login.microsoftonline.com/%s/oauth2/token" %_settings['tenant_id']))
    
    credential = ClientSecretCredential(_settings['tenant_id'], _settings['spn_id'], secret)
    adls_service_client = DataLakeServiceClient(account_url="https://%s.dfs.core.windows.net"%(_storage_account), credential=credential)
    
    return adls_service_client

def load_mount_point(_containers, _settings, dbutils): #NOSONAR
    # Blob Configuration
    mount_path = "/mnt/"
    configs = ({"fs.azure.account.auth.type": "OAuth",
                "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "fs.azure.account.oauth2.client.id": _settings['spn_id'],
                "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope=_settings['secret_scope'],
                                                                             key=_settings['mount_secret_key']),
                "fs.azure.account.oauth2.client.endpoint": ("https://login.microsoftonline.com/%s/oauth2/token" % _settings['tenant_id'])})
    _mountpoint = {}

    for id, container in _containers.items():
        if not container['container'].startswith("dbfs:"):
            _mountpoint[id] = mount_path + container['container']
            if not any(mount.mountPoint == _mountpoint[id] for mount in dbutils.fs.mounts()):
                print('mounting [%s]...'%(mount_path + container['container']))
                dbutils.fs.mount(
                    source="abfss://" + container['container'] + "@%s.dfs.core.windows.net/" % container['storage_account'],
                    mount_point=mount_path + container['container'],
                    extra_configs=configs)
            _containers[id]['path'] = mount_path + container['container']
    return _containers

def clean_up(logger, extracted_file, table_list):
    logger.info('cleaning up staging files...')
    try:
        if extracted_file:
            logger.info('remove extracted file: %s...' % extracted_file)
            shutil.rmtree('/'.join(extracted_file.split('/')[:-1]))
        for id, table in table_list.items():
            logger.info('remove extracted path: %s...' % table['src_path'])
            if Path(table['src_path']).exists():
                shutil.rmtree(table['src_path'])
        logger.info('cleanup successful!')
    except Exception as e:
        logger.warning(e)
        
def setup_application(_spark, num_argv = 3):
    setting_path, setting_name = check_argvs(num_argv)
    _dbutils = get_dbutils(_spark)
    _settings, _tables_list = application_setup(setting_path, setting_name)
    
    _containers = _settings['Container']
    _containers = load_mount_point(_containers, _settings, _dbutils)

    _logger = config_logger(_settings)
    _logger.info('DATABRICKS_PROJECT_ROOT_PATH: %s' % os.environ.get("DATABRICKS_PROJECT_ROOT_PATH", ""))
    
    return _dbutils, _settings, _tables_list, _containers, _logger

def encrypt(key, userid, source_file, encrypted_file, logger):
    try:
        pubkey, _ = pgpy.PGPKey.from_file(key)
 
        with open(source_file, 'r') as f:
            message = pgpy.PGPMessage.new(bytearray(str(f.read()), 'utf-8'))
            encrypted_message = pubkey.encrypt(message, userid=userid)
            enc_message = bytearray(str(encrypted_message), 'utf-8')
            with open(encrypted_file, 'wb') as fw:
                fw.write(enc_message)
        
    except Exception as e:
        logger.error('Exception: %s' % e)
        return False
    return encrypted_message

def decrypt(passcode, key, encrypted_file, logger):
    decrypted_message = None
    try:
        private_key, _ = pgpy.PGPKey.from_file(key)
        
        with private_key.unlock(passcode):
            encrypted_message = pgpy.PGPMessage.from_file(encrypted_file)
            decrypted_message = private_key.decrypt(encrypted_message).message

    except Exception as e:
        logger.error('Exception: %s' % e)
        
    return decrypted_message