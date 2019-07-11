#! /bin/python
# -*- coding: utf-8 -*-
import json
import sys
import os
import functools
import logging
import base64
import re
from subprocess import run, DEVNULL, STDOUT
if sys.platform == 'linux':
    import fcntl

from Crypto.Cipher import AES

from obssftp import const

log = logging.getLogger('obssftp')


def getTraceStack():
    co_list = []
    frame = sys._getframe(1)
    while frame:
        co_list.append(frame.f_code.co_name)
        frame = frame.f_back
    return co_list


class Lock:
    def __init__(self, lock_file, lock_type):
        self._lock_file = lock_file
        self._lock_type = lock_type
        if not os.path.exists(os.path.dirname(self._lock_file)):
            os.makedirs(os.path.dirname(self._lock_file))
        self._handle = open(self._lock_file, 'w')

    def acquire(self):
        log.debug(
            'acquire lock [lock_type=%s lock_file=%s]' %
            (self._lock_type, self._lock_file))
        fcntl.lockf(self._handle, self._lock_type)

    def release(self):
        log.debug(
            'release lock [lock_type=%s lock_file=%s]' %
            (self._lock_type, self._lock_file))
        fcntl.lockf(self._handle, fcntl.LOCK_UN)

    def __del__(self):
        self._handle.close()
        # os.remove(self._lock_file)


def lock_decorator():
    def decorator(func):
        @functools.wraps(func)
        def _wrapper(self, *args, **kwargs):
            locker = Lock('/tmp/obssftp.lock', fcntl.LOCK_EX)
            while True:
                try:
                    locker.acquire()
                except Exception:
                    log.debug('acquire fail, wait...')
                    continue
                finally:
                    rst = func(self, *args, **kwargs)
                    locker.release()
                    break
            return rst
        return _wrapper
    return decorator


class AESCoder():
    def __init__(self):
        self._encryptKey = "HwrPVG2t7a5Y9Sl1XUEi8RnsQAMNuovJ"
        self._key = base64.b64decode(self._encryptKey)

    def encrypt(self, data):
        BS = 16
        def pad(s): return s + (BS - len(s) % BS) * chr(BS - len(s) % BS)
        cipher = AES.new(self._key, AES.MODE_ECB)
        endata = cipher.encrypt(pad(data))
        endata = base64.b64encode(endata)
        return endata.decode()

    def decrypt(self, endata):
        endata = base64.b64decode(endata.encode())
        def unpad(s): return s[0:-s[-1]]
        cipher = AES.new(self._key, AES.MODE_ECB)
        dedata = unpad(cipher.decrypt(endata))
        return dedata.decode()


class Config:
    def __init__(self):
        self._auth = {}
        self._log = {}
        self._state = {}
        self._reload()
        self._regex = {
            'auth': {
                'auth_type': '^password$',
                'obs_endpoint': '^.*$',
                'ak': '^.*$',
                'sk': '^.*$',
                'perm': '^get,put$|^put,get$|^get$|^put$|^$'},
            'log': {
                'logpath': '^.+$',
                'level': '^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$',
                'logrotate_num': '^[1-9]\d*$',
                'logrotate_size': '^[1-9]\d*[mMkK]$',
                'auditlogpath': '^.+$',
                'auditlog_obs_endpoint': '^.+$',
                'auditlog_obs_bucket': '^.+$',
                'auditlog_obs_ak': '^.*$',
                'auditlog_obs_sk': '^.*$'},
            'state': {
                'listen_address': '^((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?)$',
                'listen_port': '[1-65535]',
                'user': '^.+$',
                'client_num': '^\d+$',
                'timeout': '^\d+$',
                'buffer_size': '^[1-9]\d*[gGmMkK]$'}
        }

    def _reload(self):
        with open(const.AUTH_CONFIG_FILE, 'r') as f:
            self._auth = json.load(f)
            # if for CLI, return encrypt, else return decrypt
            if 'take_action' not in getTraceStack():
                for user, _ in self._auth.items():
                    sk = self._auth.get(user).get('sk')
                    if len(sk) > 0:
                        self._auth[user]['sk'] = self._decrypt(sk)

        with open(const.LOG_CONFIG_FILE, 'r') as f:
            self._log = json.load(f)
            # if for CLI, return encrypt, else return decrypt
            if 'take_action' not in getTraceStack():
                auditlog_obs_sk = self._log.get('auditlog_obs_sk')
                if len(auditlog_obs_sk) > 0:
                    self._log['auditlog_obs_sk'] = self._decrypt(auditlog_obs_sk)

        with open(const.STATE_CONFIG_FILE, 'r') as f:
            self._state = json.load(f)

    def _encrypt(self, msg):
        if msg is not None and len(msg) > 0:
            aes = AESCoder()
            return aes.encrypt(msg)

    def _decrypt(self, msg):
        if msg is not None and len(msg) > 0:
            aes = AESCoder()
            return aes.decrypt(msg)

    @lock_decorator()
    def _dump(self):
        with open(const.CONFIG_FILE, 'r') as f:
            config_data = json.load(f)
            config_data['state'] = self._stat
            config_data['auth'] = self._auth
            config_data['log'] = self._log

    @property
    def auth(self):
        self._reload()
        return self._auth

    @property
    def log(self):
        self._reload()
        return self._log

    @property
    def state(self):
        self._reload()
        return self._state

    def _check_format(self, section, key, value):
        errInfo = '{} format error!\n'
        if value is None:
            return errInfo.format(key)
        regex = re.compile(self._regex[section][key])
        if not regex.match(value):
            return errInfo.format(key)
        else:
            return ''

    @lock_decorator()
    def delAuth(self, user):
        errMsg = []
        try:
            if user not in self._auth.keys():
                return False
            self._auth.pop(user)
            with open(const.AUTH_CONFIG_FILE, 'w') as f:
                json.dump(self._auth, f)
        except Exception as e:
            log.exception(e)
            errMsg.append(repr(e))
            return False, errMsg
        else:
            return True, None

    @lock_decorator()
    def setState(self, state_dict):
        errMsg = []
        try:
            for key, value in state_dict.items():
                # if value is None, use current value
                if value is None:
                    value = self.state.get(key)
                errInfo = self._check_format('state', key, value)
                if len(errInfo) > 0:
                    errMsg.append(errInfo)

            if len(errMsg) > 0:
                return False, errMsg
            for key, value in state_dict.items():
                if value is not None:
                    self._state[key] = value
            with open(const.STATE_CONFIG_FILE, 'w') as f:
                json.dump(self._state, f)
        except Exception as e:
            log.exception(e)
            errMsg.append(repr(e))
            return False, errMsg
        else:
            return True, None

    @lock_decorator()
    def AddAuth(self, user, auth_type, obs_endpoint, ak, sk, perm):
        kwargs = locals()
        kwargs.pop('self')
        kwargs.pop('user')
        errMsg = []
        try:
            if run("grep -w '^{}' /etc/passwd".format(user), shell=True, stdout=DEVNULL, stderr=STDOUT).returncode != 0:
                errInfo = 'user {} not exists!\n'.format(user)
                errMsg.append(errInfo)
            for key, value in kwargs.items():
                errInfo = self._check_format('auth', key, value)
                if len(errInfo) > 0:
                    errMsg.append(errInfo)

            if len(errMsg) > 0:
                return False, errMsg

            self._auth[user] = {}
            self._auth[user]['auth_type'] = auth_type
            if obs_endpoint is not None and len(obs_endpoint) > 0:
                self._auth[user]['obs_endpoint'] = obs_endpoint
            else:
                self._auth[user]['obs_endpoint'] = ''

            if sk is not None and len(sk) > 0:
                self._auth[user]['sk'] = self._encrypt(sk)
            else:
                self._auth[user]['sk'] = ''

            self._auth[user]['ak'] = ak
            self._auth[user]['perm'] = perm
            with open(const.AUTH_CONFIG_FILE, 'w') as f:
                json.dump(self._auth, f)
        except Exception as e:
            log.exception(e)
            errMsg.append(repr(e))
            return False, errMsg
        else:
            return True, None

    @lock_decorator()
    def setAuth(self, user, auth_dict):
        errMsg = []
        try:
            for key, value in auth_dict.items():
                # if value is None, use current value
                if value is None:
                    value = self.auth.get(user).get(key)
                errInfo = self._check_format('auth', key, value)
                if len(errInfo) > 0:
                    errMsg.append(errInfo)

            if len(errMsg) > 0:
                return False, errMsg

            sk = auth_dict.get('sk')
            if sk is not None and len(sk) > 0:
                auth_dict['sk'] = self._encrypt(sk)

            for key, value in auth_dict.items():
                if value is not None:
                    self._auth[user][key] = value

            with open(const.AUTH_CONFIG_FILE, 'w') as f:
                json.dump(self._auth, f)
        except Exception as e:
            log.exception(e)
            errMsg.append(repr(e))
            return False, errMsg
        else:
            return True, None

    @lock_decorator()
    def setLog(self, log_dict):
        errMsg = []
        try:
            for key, value in log_dict.items():
                # if value is None, use current value
                if value is None:
                    value = self.log.get(key)
                errInfo = self._check_format('log', key, value)
                if len(errInfo) > 0:
                    errMsg.append(errInfo)

            if len(errMsg) > 0:
                return False, errMsg

            # encrypt sk
            for key, value in log_dict.items():
                if key == 'auditlog_obs_sk':
                    if value is not None and len(value) > 0:
                        value = self._encrypt(value)
                    else:
                        value = ''
                if value is not None:
                    self._log[key] = value
            with open(const.LOG_CONFIG_FILE, 'w') as f:
                json.dump(self._log, f)
        except Exception as e:
            log.exception(e)
            errMsg.append(repr(e))
            return False, errMsg
        else:
            return True, None


_CFG = Config()


def getConfig():
    _CFG._reload()
    return _CFG
