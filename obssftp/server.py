# Copyright (C) 2003-2007  Robey Pointer <robeypointer@gmail.com>
#
# This file is part of paramiko.
#
# Paramiko is free software; you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation; either version 2.1 of the License, or (at your option)
# any later version.
#
# Paramiko is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with Paramiko; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA.

import os
import time
import functools
import traceback
import logging
import threading
from stat import S_IFDIR

from paramiko import SFTP_OP_UNSUPPORTED, SFTP_OK, SFTP_FAILURE, SFTPHandle, SFTPServerInterface, SFTPAttributes, \
    SFTP_NO_SUCH_FILE
from obs import ObsClient
from obs import LogConf

from obssftp.util import ObsSftpUtil
from obssftp import obsadpter, config, logmgr
from obssftp.const import OBS_SDK_LOG_CONFIG, OBS_SDK_LOG_NAME_PREFIX

logClient = logging.getLogger(__name__)
auditLogger = logmgr.getAuditLogger()


class ObsSftpServer(SFTPServerInterface):
    ROOT = ''

    def __init__(self, server, *largs, **kwargs):
        """ OBS SFTP server initiate method when auth successfully. includes some step:
            1. obtain ak\sk from config;
            2. initiate obs client and obs client log;
            3. initiate a thread to close idle time greater than idle timeout;
            4. if login user has not ak\sk setting, will close resource.

        :param server: auth object
        :param largs:
        :param kwargs:
        """
        super(SFTPServerInterface, self).__init__(*largs, **kwargs)
        # read ak sk and build client
        self._userName = server.username
        self._session = server.session
        self._transport = server.transport
        try:
            cfg = config.getConfig()
            self._timeout = int(cfg.state.get('timeout'))
            self._connHost = '%s_%s' % (server.connIp, server.connPort)
            self._endpoint = cfg.auth.get(self._userName).get('obs_endpoint')
            ak = cfg.auth.get(self._userName).get('ak')
            sk = cfg.auth.get(self._userName).get('sk')
            listen_address = cfg.state.get('listen_address')
            listen_port = int(cfg.state.get('listen_port'))
            self._closeLock = threading.Lock()
            self._closed = False
            self._ak = ak
            self._listenHost = '%s:%s' % (listen_address, listen_port)
            self._client = ObsClient(access_key_id=ak, secret_access_key=sk, server=self._endpoint, path_style=True)
            self._client.initLog(LogConf(OBS_SDK_LOG_CONFIG), '%s[%s]' % (OBS_SDK_LOG_NAME_PREFIX, self._userName))
            self._logClient = logClient
            self._logClient.log(logging.INFO, 'Initiate a sftp server for [%s] - ak [%s]', self._userName, ak)
            self._ar = {}
            self._activeTime = time.time()
            self._cleanThread = threading.Thread(target=self._clean, name='trans_%s' % self._connHost).start()
            self._obsAdapters = None
        except Exception as e:
            logClient.log(logging.ERROR, 'Initialize sftp server failed. error message [%s] - %s.', str(e),
                          traceback.format_exc())
            try:
                self._transport.close()
                if self._cleanThread:
                    self._cleanThread.close()
            except Exception as e:
                # ignore
                self._logClient.log(logging.WARNING, 'Ignore close error. error message [%s] - %s', str(e),
                                    traceback.format_exc())

    def _realpath(self, path):
        return self.ROOT + self.canonicalize(path)

    def setActiveTime(self):
        self._activeTime = time.time()

    def close(self):
        """ close relate resource when user auto close, or timeout.

        :return:
        """
        try:
            if self._client:
                self._logClient.log(logging.INFO, 'Close ObsClient, clean socket resource.')
                self._client.close()
            if self._cleanThread:
                self._cleanThread.close()
            if self._obsAdapters:
                for obsAdpter in self._obsAdapters:
                    obsAdpter.cleanThreadPool()
        except Exception as e:
            # ignore clean resource exception
            pass
        finally:
            self._closeLock.acquire()
            try:
                self._closed = True
            finally:
                self._closeLock.release()

    def _clean(self):
        self._logClient.log(logging.DEBUG, 'Schedule clean obs client and release connection.')
        try:
            while time.time() - self._activeTime < self._timeout:
                self._closeLock.acquire()
                try:
                    if self._closed:
                        self._logClient.log(logging.INFO, 'User initiative close. so do not close transport again.')
                        return
                finally:
                    self._closeLock.release()

                time.sleep(1)
            self._logClient.log(logging.DEBUG, 'Idle time gt [%d]s. Start to clean obs client and release connection.',
                                self._timeout)
            self._transport.close()
        except Exception as e:
            # ignore
            self._logClient.log(logging.WARNING, 'Ignore close transport failed. error message [%s]- %s', str(e),
                                traceback.format_exc())
        finally:
            try:
                if self._cleanThread:
                    self._cleanThread.close()
            except Exception as e:
                self._logClient.log(logging.WARNING, 'Ignore close idle clean thread failed. error message [%s] - %s.', str(e),
                                    traceback.format_exc())

    def _obsAuditRecord(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            sftpServer = args[0] if isinstance(args[0], ObsSftpServer) else None
            result = None
            try:
                return func(*args, **kwargs)
            finally:
                end = time.time()
                totalTimeMs = int((end - start) * 1000)
                auditDict = {'obs_ret_detail': 'success' if result not in [SFTP_FAILURE] else 'failed',
                             'obs_start_time': ObsSftpUtil.utcFormater(start),
                             'obs_end_time': ObsSftpUtil.utcFormater(end), 'obs_total_time': totalTimeMs}
                sftpServer._ar.update(auditDict)

        return wrapper

    @_obsAuditRecord
    def _listUserBuckets(self):
        resp = self._client.listBuckets()
        if resp is None:
            self._logClient.log(logging.ERROR, 'List buckets failed. error message [response is None].')
            self._ar.update({'obs_ret_detail': str(resp)})
            return SFTP_FAILURE
        if resp.status < 300:
            self._logClient.log(logging.INFO, 'List buckets successfully. %s.', ObsSftpUtil.makeResponseMessage(resp))
            buckets = []
            for bucket in resp.body.buckets:
                attr = SFTPAttributes()
                attr.st_mode = S_IFDIR
                createTime = bucket.create_date
                attr.st_mtime = ObsSftpUtil.strToTimestamp(createTime)
                attr.filename = bucket.name
                buckets.append(attr)
            self._ar.update({'obs_ret_detail': str(resp)})
            return buckets
        self._ar.update({'obs_ret_detail': str(resp)})
        self._logClient.log(logging.ERROR,
                            'List buckets failed. %s.', ObsSftpUtil.makeErrorMessage(resp))
        return SFTP_FAILURE

    def _errorCatch(func):
        """ a wrapper on every callback function, includes
            [open|read|mkdir|rmdir...]
            1. the function will record entering time and filter some method
            (mkdir|open|rmdir|remove...) with root path.
            2. when the callback finnished will record running log and audit log.

        :return:
        """
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            sftpServer = args[0] if isinstance(args[0], ObsSftpServer) else None
            result = None
            _path = ''
            try:
                if sftpServer and sftpServer._logClient:
                    sftpServer._ar = {}
                    # record active time for clear connection
                    sftpServer._activeTime = time.time()
                    if len(args) < 1:
                        raise Exception('Function must has a path for operation.')
                    path = args[1]
                    _path = sftpServer._realpath(path)
                    funcName = func.__name__
                    if funcName in ['rename']:
                        newPath = args[2]
                        sftpServer._logClient.log(logging.INFO,
                                                  'Entering [%s] for old path [%s] to new path [%s]  ...' % (
                                                      func.__name__, _path, newPath))
                    else:
                        sftpServer._logClient.log(logging.INFO,
                                                  'Entering [%s] for path [%s] ...', func.__name__, _path)

                    if funcName in ['open', 'rename', 'mkdir', 'rmdir', 'remove']:
                        if ObsSftpUtil.isRoot(_path) or ObsSftpUtil.isBucket(_path):
                            sftpServer._ar.update({'obs_key': _path})
                            sftpServer._logClient.log(logging.ERROR,
                                                      'Sftp operation [%s] is not supported for path [%s]' , func.__name__, _path)
                            result = SFTP_OP_UNSUPPORTED
                            return SFTP_OP_UNSUPPORTED
                result = func(*args, **kwargs)
            except Exception as e:
                result = SFTP_FAILURE
                if sftpServer and sftpServer._logClient:
                    sftpServer._logClient.log(logging.ERROR, "Operation [%s] failed, error message [%s] - %s.",
                                              func.__name__, str(e), traceback.format_exc())
            finally:
                end = time.time()
                totalTimeMs = int((end - start) * 1000)
                operationResult = 'success' if result not in [SFTP_FAILURE, SFTP_OP_UNSUPPORTED, SFTP_NO_SUCH_FILE] else 'failed'
                auditDict = {'session_id': sftpServer._session, 'client_ip_port': sftpServer._connHost,
                             'server_ip_port': sftpServer._listenHost, 'user': sftpServer._userName,
                             'obs_endpoint': sftpServer._endpoint, 'sftp_opt': func.__name__,
                             'result': operationResult, 'client_ak': sftpServer._ak,
                             'sftp_start_time': ObsSftpUtil.utcFormater(start),
                             'sftp_end_time': ObsSftpUtil.utcFormater(end), 'sftp_total_time': totalTimeMs}
                sftpServer._ar.update(auditDict)
                if sftpServer and sftpServer._logClient:
                    sftpServer._logClient.log(logging.INFO,
                                              'End operation [%s] for path [%s] - result [%s]%s - cost [%s] ms', _path,
                                              func.__name__, operationResult, (' - count [%d]' % len(result)) if all(
                            [operationResult == 'success', func.__name__ == 'list_folder']) else '', totalTimeMs)
                try:
                    # record active time for clear connection
                    auditLogger.log(sftpServer._ar)
                    sftpServer._activeTime = time.time()
                except Exception as e:
                    sftpServer._logClient.log(logging.ERROR, 'Log audit failed. error message [%s] - %s', str(e),
                                              traceback.format_exc())
                sftpServer._ar = {}

            return result

        return wrapper

    @_errorCatch
    def list_folder(self, path):
        _path = self._realpath(path)
        if ObsSftpUtil.isRoot(_path):
            self._ar.update({'obs_bucket': '-', 'obs_key': _path})
            return self._listUserBuckets()
        bucketName, key = ObsSftpUtil.getBucketAndKey(_path)
        obsAdapter = obsadpter.ObsAdapter(client=self._client, bucketName=bucketName, key=key,
                                          logClient=self._logClient, ar=self._ar)
        return obsAdapter.listDir()

    @_obsAuditRecord
    def _bucketExists(self, bucketName):
        resp = self._client.headBucket(bucketName)
        if resp is None:
            noRespError = 'Head bucket [%s] failed. error message [response is None].' % bucketName
            self._logClient.log(logging.ERROR, noRespError)
            self._ar.update({'obs_ret_detail': str(resp)})
            raise Exception(noRespError)
        if resp.status < 300:
            self._logClient.log(logging.INFO, 'Head bucket [%s] successfully. %s.', bucketName,
                                ObsSftpUtil.makeResponseMessage(resp))
            return True
        if resp.status == 404:
            self._logClient.log(logging.DEBUG, 'Head bucket [%s] failed. error message [bucket is not exists] - %s.',
                                bucketName, ObsSftpUtil.makeResponseMessage(resp))
            return False
        self._ar.update({'obs_ret_detail': str(resp)})
        respError = 'Head bucket [%s] failed. %s.' % (bucketName, ObsSftpUtil.makeErrorMessage(resp))
        self._logClient.log(logging.ERROR, respError)
        raise Exception(respError)

    def _buildSftpFolder(self):
        attr = SFTPAttributes()
        attr.st_size = 0
        attr.st_mode = S_IFDIR
        return attr

    @_errorCatch
    def stat(self, path):
        obsAdapter = None
        try:
            _path = self._realpath(path)
            if ObsSftpUtil.isRoot(_path):
                self._ar.update({'obs_key': _path})
                self._logClient.log(logging.DEBUG, 'Stat root path successfully, return a sftp folder SFTPAttributes.')
                return self._buildSftpFolder()
            if ObsSftpUtil.isBucket(_path):
                bucketName = ObsSftpUtil.getBucketName(_path)
                auditDict = {'obs_bucket': bucketName, 'obs_key': '-'}
                self._ar.update(auditDict)
                if self._bucketExists(bucketName):
                    self._logClient.log(logging.DEBUG,
                                        'Stat obs bucket [%s] successfully, return a sftp folder SFTPAttributes.',
                                        bucketName)
                    return self._buildSftpFolder()
                self._logClient.log(logging.ERROR, 'Stat obs bucket [%s] failed, error message [bucket is not exists].',
                                    bucketName)
                return SFTP_NO_SUCH_FILE

            bucketName, key = ObsSftpUtil.getBucketAndKey(_path)
            obsAdapter = obsadpter.ObsAdapter(client=self._client, bucketName=bucketName, key=key,
                                              logClient=self._logClient, ar=self._ar)
            pathInfo = obsAdapter.pathInfo()
            return SFTP_NO_SUCH_FILE if not pathInfo else pathInfo
        finally:
            del obsAdapter

    @_errorCatch
    def lstat(self, path):
        return self.stat(path)

    @_errorCatch
    def open(self, path, flags, attr):
        """ open just for file

        :param path: file path
        :param flags: SSH_FXF_READ|SSH_FXF_WRITE|SSH_FXF_APPEND|SSH_FXF_CREAT|SSH_FXF_TRUNC|SSH_FXF_EXCL
        :param attr: not useful for obs
        :return: file handler
        """
        perm = config.getConfig().auth.get(self._userName).get('perm')
        # put
        if flags & os.O_WRONLY:
            if 'put' not in perm.split(','):
                self._logClient.log(logging.ERROR,
                                          'user [%s] not supported put operation'  , self._userName)
                return SFTP_OP_UNSUPPORTED
        elif flags == os.O_RDONLY:
            if 'get' not in perm.split(','):
                self._logClient.log(logging.ERROR,
                                            'user [%s] not supported get operation'  , self._userName)
                return SFTP_OP_UNSUPPORTED
        else:
            # TODO read and write and append
            raise Exception('Read and Write| Append operation is not support. flags [%d]' % flags)

        _path = self._realpath(path)
        bucketName, key = ObsSftpUtil.getBucketAndKey(_path)
        auditDict = {'obs_start_time': ObsSftpUtil.utcFormater(time.time()), 'obs_bucket': bucketName, 'obs_key': key}
        obsAdapter = obsadpter.ObsAdapter(client=self._client, bucketName=bucketName, key=key,
                                          logClient=self._logClient, ar=self._ar, connHost=self._connHost)
        if not self._obsAdapters:
            self._obsAdapters = []
        self._obsAdapters.append(obsAdapter)
        self._ar.update(auditDict)
        _key = ObsSftpUtil.maybeAddTrailingSlash(key)
        # TODO just successfully for object bucket. file bucket need mode for jungle folder
        if obsAdapter.dirInfo(bucketName, _key):
            self._logClient.log(logging.WARNING,
                                'Open key [%s] of bucket [%s] failed. error message [Client want to open a dir].', _key,
                                bucketName)
            return SFTP_FAILURE
        try:
            fobj = ObsSftpFileHandle(flags=flags, obsAdapter=obsAdapter, bucketName=bucketName, key=key,
                                     logClient=self._logClient, sftpServer=self)
            return fobj
        except Exception as e:
            self._logClient.log(logging.ERROR, 'Open key [%s] of bucket [%s] failed. error message [%s] - %s.', key,
                                bucketName, str(e), traceback.format_exc())
            if 'not support' in str(e):
                return SFTP_OP_UNSUPPORTED
            return SFTP_FAILURE

    @_errorCatch
    def remove(self, path):
        obsAdapter = None
        try:
            _path = self._realpath(path)
            bucketName, key = ObsSftpUtil.getBucketAndKey(_path)
            obsAdapter = obsadpter.ObsAdapter(client=self._client, bucketName=bucketName, key=key,
                                              logClient=self._logClient, ar=self._ar)
            removeResult = obsAdapter.remove()
            if removeResult is None:
                return SFTP_NO_SUCH_FILE
            return SFTP_OK if removeResult else SFTP_FAILURE
        finally:
            del obsAdapter

    @_errorCatch
    def rename(self, oldPath, newPath):
        obsAdapter = None
        try:
            _oldPath = self._realpath(oldPath)
            _newPath = self._realpath(newPath)
            oldBucketName, oldKey = ObsSftpUtil.getBucketAndKey(_oldPath)
            newBucketName, newKey = ObsSftpUtil.getBucketAndKey(_newPath)
            obsAdapter = obsadpter.ObsAdapter(client=self._client, bucketName=newBucketName, key=newKey,
                                              logClient=self._logClient, ar=self._ar, connHost=self._connHost)
            renameSucceed = obsAdapter.rename(oldBucketName, oldKey)
            if renameSucceed is None:
                return SFTP_NO_SUCH_FILE
            return SFTP_OK if renameSucceed else SFTP_FAILURE
        finally:
            del obsAdapter

    @_errorCatch
    def mkdir(self, path, attr):
        obsAdapter = None
        try:
            _path = self._realpath(path)
            bucketName, key = ObsSftpUtil.getBucketAndKey(_path)
            obsAdapter = obsadpter.ObsAdapter(client=self._client, bucketName=bucketName, key=key,
                                              logClient=self._logClient, ar=self._ar, connHost=self._connHost)
            return SFTP_OK if obsAdapter.mkDir() else SFTP_FAILURE
        finally:
            del obsAdapter

    @_errorCatch
    def rmdir(self, path):
        obsAdapter = None
        try:
            _path = self._realpath(path)
            bucketName, key = ObsSftpUtil.getBucketAndKey(_path)
            _key = ObsSftpUtil.maybeAddTrailingSlash(key)
            obsAdapter = obsadpter.ObsAdapter(client=self._client, bucketName=bucketName, key=_key,
                                              logClient=self._logClient, ar=self._ar, connHost=self._connHost)
            if not obsAdapter.dirInfo(bucketName, _key):
                self._logClient.log(logging.DEBUG,
                                    'Rmdir key [%s] of bucket [%s] failed. error message [dir is not exists].',
                                    _key, bucketName)
                self._ar.update({'obs_key': _key, 'obs_bucket': bucketName})
                return SFTP_NO_SUCH_FILE
            return SFTP_OK if obsAdapter.rmDir() else SFTP_FAILURE
        finally:
            del obsAdapter

    @_errorCatch
    def chattr(self, path, attr):
        _path = self._realpath(path)
        self._ar.update({'obs_key': _path})
        self._logClient.log(logging.DEBUG, 'User want to change object [%s] attribute, return success, but invalid.', _path)
        return SFTP_OK

    @_errorCatch
    def symlink(self, target_path, path):
        _path = self._realpath(path)
        self._ar.update({'obs_key': _path})
        self._logClient.log(logging.WARNING, 'User want to create object [%s] soft-link to [%s], return not support.', target_path, path)
        return SFTP_OP_UNSUPPORTED

    @_errorCatch
    def readlink(self, path):
        _path = self._realpath(path)
        self._ar.update({'obs_key': _path})
        self._logClient.log(logging.WARNING, 'User want to read soft-link [%s] \'s object, return not support.', path)
        return SFTP_OP_UNSUPPORTED

    def session_ended(self):
        self._logClient.log(logging.INFO, 'Session of user [%s] has ended. must to close obs client relate resource.',
                            self._userName)
        self.close()

    def session_started(self):
        threading.current_thread().name = 'trans_%s' % (self._connHost)
        self._logClient.log(logging.INFO, 'Session of user [%s] has started.', self._userName)


class ObsSftpFileHandle(SFTPHandle):

    def __init__(self, flags=0, obsAdapter=None, bucketName=None, key=None, logClient=None, sftpServer=None):
        """
        Create a new file handle representing a local file being served over
        SFTP.  If ``flags`` is passed in, it's used to determine if the file
        is open in append mode.

        :param flags:
        :param obsAdapter: The obs adapter.
        :param bucketName: A string of obs bucket name.
        :param key: A string of obs key.
        :param logClient: A logging for record DEBUG/WARNING/INFO/ERROR log.
         """
        super(ObsSftpFileHandle, self).__init__(flags)
        self._obsAdapter = obsAdapter
        self._bucketName = bucketName
        self._key = key
        self._excepted = False
        self._logClient = logClient
        self._readFlag = False
        self._writeFlag = False
        self._appendFlag = False
        self._sftpServer = sftpServer
        # jungle flags, contains write only|append only|read only|write and read
        if flags & os.O_WRONLY:
            self._writeFlag = True
        elif flags == os.O_RDONLY:
            self._readFlag = True
        else:
            # TODO read and write and append
            raise Exception('Read and Write| Append operation is not support. flags [%d]' % flags)

    def _active(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            handler = args[0] if isinstance(args[0], ObsSftpFileHandle) else None
            try:
                handler._sftpServer.setActiveTime()
                return func(*args, **kwargs)
            finally:
                handler._sftpServer.setActiveTime()

        return wrapper

    @_active
    def close(self):
        """Close obs operator resource.

        close write resource if  _writeFlag is true.
        close read resource if _readFlag is true.

        :return: None
        """
        try:
            if self._writeFlag:
                if self._obsAdapter and not self._excepted:
                    if self._obsAdapter.close():
                        self._logClient.log(logging.INFO,
                                            'Close sftp write handle successfully. key [%s] of bucket [%s]' % (
                                                self._key, self._bucketName))
                        return
                self._obsAdapter._ar.update({'obs_ret_detail': 'failed'})
                self._logClient.log(logging.ERROR,
                                    'Close sftp write for key [%s] of bucket [%s] failed. error message [write has error] - writer is [%s] - excepted [%s]' % (
                                        self._key, self._bucketName, self._obsAdapter, self._excepted))
                self._obsAdapter.abortWriteUpload()
                return
            if self._readFlag:
                if not self._excepted:
                    self._obsAdapter.closeReader()
                    self._logClient.log(logging.INFO,
                                        'Close sftp read handle successfully. key [%s] of bucket [%s]' % (
                                            self._key, self._bucketName))
                    return
                self._logClient.log(logging.ERROR,
                                    'Close sftp read for key [%s] of bucket [%s] failed. error message [read has error] - excepted [%s]' % (
                                        self._key, self._bucketName, self._excepted))
                self._obsAdapter._ar.update({'obs_ret_detail': 'failed'})
        except Exception as e:
            self._logClient.log(logging.ERROR,
                                'Close sftp [%s] for key [%s] of bucket [%s] failed. excepted [%s] - error message [%s] - %s' % (
                                    'read' if self._readFlag else 'write', self._key, self._bucketName, self._excepted,
                                    str(e), traceback.format_exc()))
            raise Exception(e)
        finally:
            try:
                auditLogger.log(self._obsAdapter.ar)
            except Exception as e:
                self._logClient.log(logging.ERROR,
                                    'Log audit record for [%s] failed. - excepted [%s] - error message [%s] - %s' % (
                                        'Read' if self._readFlag else 'Write', self._excepted, str(e),
                                        traceback.format_exc()))
            del self._obsAdapter

    @_active
    def read(self, offset, length):
        self._readFlag = True
        self._logClient.log(logging.DEBUG, 'Start to read data from key [%s] of bucket [%s]- offset [%d] - length [%d]',
                            self._key, self._bucketName, offset, length)
        if self._obsAdapter:
            try:
                buffer = self._obsAdapter.readWithLongConnection(offset, length)
                if buffer is not None:
                    return buffer
            except Exception as e:
                self._excepted = True
                self._logClient.log(logging.ERROR,
                                    'Read data from key [%s] of bucket [%s] failed. error message [%s] - %s' % (
                                        self._key, self._bucketName, str(e), traceback.format_exc()))
        self._excepted = True
        return SFTP_FAILURE

    @_active
    def write(self, offset, data):
        length = 0
        try:
            length = len(data)
            self._writeFlag = True
            self._logClient.log(logging.DEBUG,
                                'Start to write data to key [%s] of bucket [%s]. - offset [%d] - length [%d]',
                                self._key, self._bucketName, offset, )
            if self._obsAdapter:
                try:
                    if self._obsAdapter.write(data):
                        return SFTP_OK
                except Exception as e:
                    self._excepted = True
                    self._logClient.log(logging.ERROR,
                                        'Write data to key [%s] of bucket [%s] failed, error message [%s] - %s' % (
                                            self._key, self._bucketName, str(e), traceback.format_exc()))
                    return SFTP_FAILURE
                self._logClient.log(logging.ERROR,
                                    'Write data to key [%s] of bucket [%s] failed, check upload part error message.',
                                    self._key, self._bucketName)
                self._excepted = True
                return SFTP_FAILURE
            self._excepted = True
            self._logClient.log(logging.ERROR,
                                'Write data to key [%s] of bucket [%s] failed. because of writer is None.',
                                self._key, self._bucketName)
            return SFTP_FAILURE
        finally:
            del data
            self._logClient.log(logging.DEBUG, 'Finished for data to key [%s] of bucket [%s]. - offset [%d] - length [%d]', self._key, self._bucketName, offset, length)

    @_active
    def chattr(self, attr):
        """
        Change the attributes of this file.  The ``attr`` object will contain
        only those fields provided by the client in its request, so you should
        check for the presence of fields before using them.

        :param .SFTPAttributes attr: the attributes to change on this file.
        :return: an `int` error code like `.SFTP_OK`.
        """
        return SFTP_OK
