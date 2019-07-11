#!/usr/bin/python
# -*- coding:utf-8 -*-
import os

KB, MB, GB, TB = [1024 ** (i + 1) for i in range(4)]
UNIT_DICT = {
    'K': KB, 'M': MB, 'G': GB, 'T': TB
}

# config folder
CONFIG_DIR = '/usr/etc/obssftp'
# receive client data larger than 10MB, will sync upload
SEND_BUF_SIZE = 10 * MB
# default read buffer for client read
READ_BUF_SIZE = 64*1024
BACKLOG = 10
# default thread nums for every user copy or update task
DEFAULT_THREAD_NUMS = 1
DEFAULT_SCAN_THREAD_NUMS = 1
OBS_SDK_LOG_CONFIG = os.path.join(CONFIG_DIR, 'sdk_log.conf')
OBS_SDK_LOG_NAME_PREFIX='obsclient-'
CONTENT_MD5_HEADER = 'Content-MD5'
# default client read connection idle time with obs service
CONNECTION_IDLE_TIME_S = 31
AUTH_CONFIG_FILE = os.path.join(CONFIG_DIR, 'auth.json')
LOG_CONFIG_FILE = os.path.join(CONFIG_DIR, 'log.json')
STATE_CONFIG_FILE = os.path.join(CONFIG_DIR, 'state.json')
HOST_KEY_DIR = os.path.join(CONFIG_DIR, '.ssh')
HOST_PRIVATE_KEY_PATH= os.path.join(HOST_KEY_DIR, 'id_rsa')

# mode of log file
log_file_mode = 0o640

# mode of rotate log file
rotate_log_file_mode = 0o440


OBS_SFTP_AUTH_ECHO_OFF = 1
OBS_SFTP_AUTH_ECHO_ON = 2
OBS_SFTP_AUTH_ERROR_MSG = 3
OBS_SFTP_AUTH_TEXT_INFO = 4
OBS_SFTP_AUTH_REINITIALIZE_CRED = 8
