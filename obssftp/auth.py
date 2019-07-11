#! /bin/python
# -*- coding:utf-8 -*-
import logging
import sys
import threading

if sys.platform == 'linux':
    from ctypes import CDLL, POINTER, Structure, CFUNCTYPE, cast, byref, sizeof
    from ctypes import c_void_p, c_size_t, c_char_p, c_char, c_int
    from ctypes import memmove
    from ctypes.util import find_library

from paramiko import ServerInterface, AUTH_SUCCESSFUL, OPEN_SUCCEEDED, AUTH_FAILED

from obssftp.const import OBS_SFTP_AUTH_ECHO_OFF, OBS_SFTP_AUTH_ECHO_ON, OBS_SFTP_AUTH_ERROR_MSG, \
    OBS_SFTP_AUTH_REINITIALIZE_CRED, OBS_SFTP_AUTH_TEXT_INFO

log = logging.getLogger(__name__)


class ObsServerAuth(ServerInterface):

    def __init__(self, connIp, connPort, transport):
        super(ObsServerAuth, self).__init__()
        self._pam = Auth()
        self._username = ''
        self._connIp = connIp
        self._connPort = connPort
        self._session = ''
        self._transport = transport

    @property
    def transport(self):
        return self._transport

    @property
    def session(self):
        return self._session

    @property
    def connIp(self):
        return self._connIp

    @property
    def connPort(self):
        return self._connPort

    @property
    def username(self):
        return self._username

    def check_auth_password(self, username, password):
        # just support for linux
        if sys.platform != 'linux':
            self._username = username
            return AUTH_SUCCESSFUL
        log.info('User login with username [%s] - thread name [%s]' % (username, threading.current_thread().name))
        self._username = username
        self._pam.authenticate(username, password)
        if self._pam.code == 0:
            log.debug('auth success. user=%s' % username)
            return AUTH_SUCCESSFUL
        else:
            log.error(
                'authenticate user [%s] fail[code=%s, reason=%s], maybe the user name or password wrong.' %
                (username, self._pam.code, self._pam.reason))
            return AUTH_FAILED

    def check_auth_publickey(self, username, key):
        log.error('not support publickey - thread name [%s]' % threading.current_thread().name)
        return AUTH_FAILED

    def check_channel_request(self, kind, chanid):
        log.info('[get_allowed_auths] kind [%s], chanid [%s] - thread name [%s]' % (
        kind, chanid, threading.current_thread().name))
        self._session = chanid
        return OPEN_SUCCEEDED

    def get_allowed_auths(self, username):
        """List availble auth mechanisms."""
        threading.current_thread().name = 'trans_%s_%s' % (self._connIp, self._connPort)
        log.info('[get_allowed_auths] username [%s] - thread name [%s]' % (username, threading.current_thread().name))
        return 'password'


class ObsSftpAuthHandle(Structure):
    """ Obs sftp auth handle

    """
    _fields_ = [("handle", c_void_p)]

    def __init__(self):
        Structure.__init__(self)
        self.handle = 0


class ObsSftpAuthMessage(Structure):
    _fields_ = [("msg_style", c_int), ("msg", c_char_p)]

    def __repr__(self):
        return "<AuthMessage %i '%s'>" % (self.msg_style, self.msg)


class ObsSftpAuthResponse(Structure):
    """wrapper class for pam_response structure"""
    _fields_ = [("resp", c_char_p), ("resp_retcode", c_int)]

    def __repr__(self):
        return "<AuthResponse %i '%s'>" % (self.resp_retcode, self.resp)


convertFunc = CFUNCTYPE(
    c_int, c_int, POINTER(
        POINTER(ObsSftpAuthMessage)), POINTER(
        POINTER(ObsSftpAuthResponse)), c_void_p)


class ObsSftpAuthConverter(Structure):
    """ Auth converter
   """
    _fields_ = [("conv", convertFunc), ("appdata_ptr", c_void_p)]


cLib = CDLL(find_library("c"))
authLib = CDLL(find_library("pam"))

cMethodAlloc = cLib.calloc
cMethodAlloc.restype = c_void_p
cMethodAlloc.argtypes = [c_size_t, c_size_t]

# bug #6 (@NIPE-SYSTEMS), some libpam versions don't include this function
if hasattr(authLib, 'pam_end'):
    obsSftpAuthEnd = authLib.pam_end
    obsSftpAuthEnd.restype = c_int
    obsSftpAuthEnd.argtypes = [ObsSftpAuthHandle, c_int]

obsSftpAuthStart = authLib.pam_start
obsSftpAuthStart.restype = c_int
obsSftpAuthStart.argtypes = [c_char_p, c_char_p, POINTER(ObsSftpAuthConverter), POINTER(ObsSftpAuthHandle)]

obsSftpAuthSetcred = authLib.pam_setcred
obsSftpAuthSetcred.restype = c_int
obsSftpAuthSetcred.argtypes = [ObsSftpAuthHandle, c_int]

obsSftpAuthStrerror = authLib.pam_strerror
obsSftpAuthStrerror.restype = c_char_p
obsSftpAuthStrerror.argtypes = [ObsSftpAuthHandle, c_int]

obsSftpAuthenticate = authLib.pam_authenticate
obsSftpAuthenticate.restype = c_int
obsSftpAuthenticate.argtypes = [ObsSftpAuthHandle, c_int]


class Auth(object):
    code = 0
    reason = None

    def encode(self, value, encoding):
        if isinstance(value, str):
            return value.encode(encoding)
        return value

    def authenticate(
            self,
            username,
            password,
            service='system-auth',
            encoding='utf-8',
            resetcreds=True):

        @convertFunc
        def obsSftpAuthConv(n_messages, messages, p_response, app_data):

            addr = cMethodAlloc(n_messages, sizeof(ObsSftpAuthResponse))
            response = cast(addr, POINTER(ObsSftpAuthResponse))
            p_response[0] = response
            for i in range(n_messages):
                if messages[i].contents.msg_style == OBS_SFTP_AUTH_ECHO_OFF:
                    dst = cMethodAlloc(len(password) + 1, sizeof(c_char))
                    memmove(dst, cpassword, len(password))
                    response[i].resp = dst
                    response[i].resp_retcode = 0
            return 0

        username = self.encode(username, encoding)
        password = self.encode(password, encoding)
        service = self.encode(service, encoding)

        if b'\x00' in username or b'\x00' in password or b'\x00' in service:
            self.code = 4
            self.reason = 'strings may not contain NUL'
            return False

        cpassword = c_char_p(password)

        handle = ObsSftpAuthHandle()
        conv = ObsSftpAuthConverter(obsSftpAuthConv, 0)
        start_retval = obsSftpAuthStart(service, username, byref(conv), byref(handle))
        if start_retval != 0:
            self.code = start_retval
            self.reason = "pam_start() failed"
            return False

        auth_retval = obsSftpAuthenticate(handle, 0)
        auth_success = auth_retval == 0

        if auth_success and resetcreds:
            auth_retval = obsSftpAuthSetcred(handle, OBS_SFTP_AUTH_REINITIALIZE_CRED)

        self.code = auth_retval
        self.reason = obsSftpAuthStrerror(handle, auth_retval)
        if sys.version_info >= (3,):
            self.reason = self.reason.decode(encoding)

        if hasattr(authLib, 'pam_end'):
            obsSftpAuthEnd(handle, auth_retval)

        return auth_success
