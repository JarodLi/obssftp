# Copyright 2013 Lowell Alleman
#
#   Licensed under the Apache License, Version 2.0 (the "License"); you may not
#   use this file except in compliance with the License. You may obtain a copy
#   of the License at http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations
#   under the License.

#! /bin/python
# -*- coding: utf-8 -*-
import time
import logging
import logging.handlers
import os
import csv
import re
import sys
import traceback
from random import randint
from logging.handlers import BaseRotatingHandler
from logging import Handler, LogRecord
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import codecs
except ImportError:
    codecs = None
if sys.platform == 'linux':
    import fcntl

from obssftp import const, config
from obs import ObsClient
from obs import LogConf

from obssftp.const import OBS_SDK_LOG_CONFIG

# log日志打印流程：
# 1. logger.log-> logger.makeRecord -> logger.handle -> logger.callHandlers
# 2. handler.handle -> handler.filter, handler.emit() -> doRollover -> handler.close


class UTCFormatter(logging.Formatter):
    converter = time.gmtime


DEFAULT_LOGGER_NAME = 'obssftp'

VALID_LEVELS = {
    "DEBUG": logging.DEBUG, "INFO": logging.INFO, "WARNING": logging.WARNING,
    "ERROR": logging.ERROR, "CRITICAL": logging.CRITICAL
}


class Lock:
    def __init__(self, lock_file, lock_type):
        self._lock_file = lock_file
        self._lock_type = lock_type
        if not os.path.exists(os.path.dirname(self._lock_file)):
            os.makedirs(os.path.dirname(self._lock_file))
        self._handle = open(self._lock_file, 'w')

    def closed(self):
        return self._handle.closed

    def acquire(self):
        fcntl.lockf(self._handle, self._lock_type)

    def release(self):
        fcntl.lockf(self._handle, fcntl.LOCK_UN)

    def __del__(self):
        self._handle.close()
        # os.remove(self._lock_file)


class NullLogRecord(LogRecord):
    def __init__(self):
        pass

    def __getattr__(self, attr):
        return None


class LockException(Exception):
    # Error codes:
    LOCK_FAILED = 1


class ObsSftpFileRotatingFileHandler(BaseRotatingHandler):

    def __init__(self, filename, mode='a', maxBytes=0, backupCount=0,
                 encoding=None, delay=0):
        BaseRotatingHandler.__init__(self, filename, mode, encoding, delay)
        self.delay = delay
        self._exception = False
        self._open_lockfile()
        self._chmod(filename, mode=const.log_file_mode)
        self.maxBytes = maxBytes
        self.backupCount = backupCount


    def _open_lockfile(self):
        if self.baseFilename.endswith(".log"):
            lock_file = self.baseFilename[:-4]
        else:
            lock_file = self.baseFilename
        lock_file += ".lock"
        self.stream_lock = Lock(lock_file, fcntl.LOCK_EX)
        self.stream_lock.closed()

    def _open(self, mode=None):
        _mode = self.mode if not mode else mode
        return open(self.baseFilename, _mode) if not self.encoding else codecs.open(self.baseFilename, _mode,
                                                                                    self.encoding)

    def _close(self):
        if self.stream:
            try:
                if not self.stream.closed:
                    self.stream.flush()
                    self.stream.close()
            finally:
                self.stream = None

    def _chmod(self, file_path, mode):
        try:
            os.chmod(file_path, mode=mode)
        except Exception as e:
            # ignore chmod every error
            pass

    def acquire(self):
        Handler.acquire(self)
        if self.stream_lock:
            if self.stream_lock.closed():
                try:
                    self._open_lockfile()
                except Exception:
                    self.handleError(NullLogRecord())
                    self.stream_lock = None
                    return
            self.stream_lock.acquire()

    def release(self):
        try:
            if self._exception:
                self._close()
        except Exception:
            self.handleError(NullLogRecord())
        finally:
            try:
                if self.stream_lock and not self.stream_lock.closed():
                    self.stream_lock.release()
            except Exception:
                self.handleError(NullLogRecord())
            finally:
                Handler.release(self)

    def close(self):
        try:
            self._close()
            if not self.stream_lock.closed:
                self.stream_lock.close()
        finally:
            self.stream_lock = None
            Handler.close(self)

    def doRollover(self):
        self._close()
        if self.backupCount <= 0:
            self.stream = self._open("w")
            return
        try:
            tmpname = None
            while not tmpname or os.path.exists(tmpname):
                tmpname = "%s.rotate.%08d" % (self.baseFilename, randint(0, 99999999))
            try:
                # Do a rename test to determine if we can successfully rename the log file
                os.rename(self.baseFilename, tmpname)
            except (IOError, OSError):
                return

            for i in range(self.backupCount - 1, 0, -1):
                sfn = "%s.%d" % (self.baseFilename, i)
                dfn = "%s.%d" % (self.baseFilename, i + 1)
                if os.path.exists(sfn):
                    if os.path.exists(dfn):
                        os.remove(dfn)
                    os.rename(sfn, dfn)
                    self._chmod(dfn, mode=const.rotate_log_file_mode)
            dfn = self.baseFilename + ".1"
            if os.path.exists(dfn):
                os.remove(dfn)
            os.rename(tmpname, dfn)
            self._chmod(dfn, mode=const.rotate_log_file_mode)
        finally:
            if not self.delay:
                self.stream = self._open()
                self._chmod(self.baseFilename, mode=const.rotate_log_file_mode)

    def shouldRollover(self, record):
        del record
        if self.stream is None:
            return False
        if self._shouldRollover():
            self._close()
            self.stream = self._open()
            return self._shouldRollover()
        return False

    def _shouldRollover(self):
        if self.maxBytes > 0:
            self.stream.seek(0, 2)
            if self.stream.tell() >= self.maxBytes:
                return True
        return False


def getLogger(name: str = 'obssftp') -> logging.Logger:
    """ 运行日志初始化 """

    logger = logging.getLogger(name)
    if logger.hasHandlers():
        return logger
    try:
        cfg = config.getConfig()
        formatter = UTCFormatter(
            '%(asctime)s UTC ' \
            '[%(levelname)-8s] ' \
            '[%(filename)-11s:%(lineno)-3d] ' \
            '[Proc: %(process)d, %(processName)s] ' \
            '[Thread: %(thread)d, %(threadName)s] ' \
            '%(message)s')
        logger.setLevel(VALID_LEVELS[cfg.log['level']])
        logpath = cfg.log['logpath']
        logdir = os.path.dirname(logpath)
        logrotate_num = int(cfg.log['logrotate_num'])
        rotate_value, rotate_unit = re.search(
            r'^(\d+)\s*(?:([MmKk])?)?$', cfg.log['logrotate_size']).groups()
        logrotate_size = int(rotate_value) * const.UNIT_DICT[rotate_unit.upper()]
        if not os.path.exists(logdir):
            os.makedirs(logdir, mode=0o750)

        fh = ObsSftpFileRotatingFileHandler(
            logpath, maxBytes=logrotate_size, backupCount=logrotate_num)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        return logger
    except Exception as e:
        import traceback
        traceback.print_exc()


log = getLogger()
_CSV_HEAD = [
    'session_id',
    'client_ip_port',
    'server_ip_port',
    'user',
    'obs_endpoint',
    'obs_bucket',
    'sftp_opt',
    'result',
    'obs_ret_code',
    'obs_ret_detail',
    'md5',
    'sftp_start_time',
    'sftp_end_time',
    'sftp_total_time',
    'obs_start_time',
    'obs_end_time',
    'obs_total_time',
    'obs_key',
    'obs_opt',
    'object_size',
    'bps',
    'opt_size',
    'client_ak']


class AuditRecord(object):
    """定义audit日志字段"""

    def __init__(
            self,
            session_id='-',
            client_ip_port='-',
            server_ip_port='-',
            user='-',
            obs_endpoint='-',
            obs_bucket='-',
            sftp_opt='-',
            result='-',
            obs_ret_code='-',
            obs_ret_detail='-',
            md5='-',
            sftp_start_time='-',
            sftp_end_time='-',
            opt_size='-',
            sftp_total_time='-',
            obs_start_time='-',
            obs_end_time='-',
            obs_total_time='-',
            obs_key='-',
            obs_opt='-',
            bps='-',
            object_size='-',
            client_ak='-'):
        """
        Initialize a logging record with interesting information.
        """
        self.opt_size = opt_size
        self.object_size = object_size
        self.bps = bps
        self.obs_key = obs_key
        self.obs_opt = obs_opt
        self.session_id = session_id
        self.client_ip_port = client_ip_port
        self.server_ip_port = server_ip_port
        self.user = user
        self.obs_endpoint = obs_endpoint
        self.obs_bucket = obs_bucket
        self.sftp_opt = sftp_opt
        self.result = result
        self.obs_ret_code = obs_ret_code
        self.obs_ret_detail = obs_ret_detail
        self.md5 = md5
        self.sftp_start_time = sftp_start_time
        self.sftp_end_time = sftp_end_time
        self.sftp_total_time = sftp_total_time
        self.obs_start_time = obs_start_time
        self.obs_end_time = obs_end_time
        self.obs_total_time = obs_total_time
        self.client_ak = client_ak

    def update(self, dict):
        self.__dict__.update(dict)


class AuditLogger(logging.Logger):
    """audit记录器，继承自logging.logger进行日志记录"""

    def makeRecord(self, **kwargs):
        ar = AuditRecord(**kwargs)
        return ar

    def handle(self, auditRecord):
        self.callHandlers(auditRecord)

    def callHandlers(self, auditRecord):
        c = self
        found = 0
        while c:
            for hdlr in c.handlers:
                found = found + 1
                hdlr.handle(auditRecord)
            if not c.propagate:
                c = None  # break out
            else:
                c = c.parent

    def log(self, record_dict=None, **kwargs):
        if record_dict is not None:
            ar = self.makeRecord(**record_dict)
        else:
            ar = self.makeRecord(**kwargs)
        self.handle(ar)


class AuditFormatter(logging.Formatter):
    """audit日志格式处理"""

    def format(self, auditRecord):
        return auditRecord


class CSVHandler(logging.FileHandler):
    """参考FileHandler"""

    def __init__(self, filename, mode='a', encoding=None):
        filename = os.fspath(filename)
        self.has_head = self._has_head(filename)
        self.csv_stream = None
        # init会调用_open
        super(CSVHandler, self).__init__(filename)

    def _has_head(self, filename):
        filename = os.fspath(filename)
        baseFilename = os.path.abspath(filename)
        if not os.path.exists(self.baseFilename):
            return False
        with open(baseFilename, 'r') as f:
            return False if len([r for r in csv.reader(f)]) == 0 else True

    def filter(self, auditRecord):
        """
        Determine if the specified record is to be logged.

        Is the specified record to be logged? Returns 0 for no, nonzero for
        yes. If deemed appropriate, the record may be modified in-place.
        """
        return True

    def close(self):
        """
        Closes the stream.
        """
        # 直接调用父类close stream
        # return super(CSVHandler, self).close()
        self.acquire()
        try:
            if self.stream is not None and hasattr(self.stream, 'close'):
                stream = self.stream
                self.stream = None
                stream.close()

            if self.csv_stream is not None:
                self.csv_stream = None
        finally:
            self.release()

    def flush(self):
        self.acquire()
        try:
            if self.stream is not None and hasattr(self.stream, 'flush'):
                self.stream.flush()
        finally:
            self.release()

    def _open(self):
        f = open(self.baseFilename, self.mode, encoding=self.encoding)
        self.csv_stream = csv.DictWriter(f, _CSV_HEAD)
        # 无head,先写入head
        if not self.has_head:
            self.csv_stream.writeheader()
            self.has_head = True
        return f
        """
        Open the current base file with the (original) mode and encoding.
        Return the resulting stream.
        """

    def emit(self, auditRecord):
        """
        Emit a record.

        If the stream was not opened because 'delay' was specified in the
        constructor, open it before calling the superclass's emit.
        """
        try:
            if self.stream is None or self.csv_stream is None:
                self._open()

            row_dict = auditRecord.__dict__

            self.csv_stream.writerows([row_dict])
            self.stream.flush()
        except Exception as e:
            log.exception(e)


class TimeRotatingCSVHandler(logging.handlers.TimedRotatingFileHandler):
    def __init__(self, filename, mode='a', when='h', interval=1, backupCount=20, encoding=None):
        filename = os.fspath(filename)
        self.csv_stream = None
        self._rotateFailed = False
        # init will call _open
        self.baseFilename = filename
        self._open_lockfile()
        super(TimeRotatingCSVHandler, self).__init__(filename, when, interval, backupCount, utc=True)
        self._chmod(self.baseFilename, mode=const.log_file_mode)
        self._backupPool = ThreadPoolExecutor(max_workers=1)  # just a work
        try:
            cfg = config.getConfig()
            self._bucket = str(cfg.log.get('log_obs_bucket')).strip()
            self._endpoint = str(cfg.log.get('log_obs_endpoint')).strip()
            self._ak = cfg.log.get('log_ak')
            self._sk = cfg.log.get('log_sk')
            self._auditPath, self._auditFileName = os.path.split(filename)
        except Exception as e:
            log.log(logging.ERROR, 'Audit backup must disappeared. error message [%s] - %s.', str(e),
                    traceback.format_exc())
        self.backupLock = Lock('%s_backup.lock' % self.baseFilename, fcntl.LOCK_EX)

    def _has_head(self):
        if not os.path.exists(self.baseFilename):
            return False
        with open(self.baseFilename, 'r') as f:
            return False if len([r for r in csv.reader(f)]) == 0 else True

    def _open_lockfile(self):
        # Use 'file.lock' and not 'file.log.lock' (Only handles the normal "*.log" case.)
        if self.baseFilename.endswith('.log'):
            lock_file = self.baseFilename[:-4]
        else:
            lock_file = self.baseFilename
        lock_file += ".lock"
        self.stream_lock = Lock(lock_file, fcntl.LOCK_EX)

    def _open(self, mode=None):
        """
        Open the current base file with the (original) mode and encoding.
        Return the resulting stream.

        Note:  Copied from stdlib.  Added option to override 'mode'
        """
        self.acquireLock()
        try:
            _mode = self.mode if not mode else mode
            stream = open(self.baseFilename, _mode) if not self.encoding else codecs.open(self.baseFilename, _mode,
                                                                                          self.encoding)
            self.csv_stream = csv.DictWriter(stream, _CSV_HEAD)
            # write head first
            if not self._has_head():
                self.csv_stream.writeheader()
            return stream
        finally:
            self.releaseLock()

    def _closeStream(self):
        if self.stream:
            try:
                if not self.stream.closed:
                    self.stream.flush()
                    self.stream.close()
            finally:
                self.stream = None

    def _chmod(self, file_path, mode):
        try:
            os.chmod(file_path, mode=mode)
        except Exception as e:
            # ignore chmod every error
            pass

    def acquireLock(self):
        """ Acquire thread and file locks.  Re-opening log for 'degraded' mode.
        """
        # Issue a file lock.  (This is inefficient for multiple active threads
        # within a single process. But if you're worried about high-performance,
        # you probably aren't using this log handler.)
        if self.stream_lock:
            # If stream_lock=None, then assume close() was called or something
            # else weird and ignore all file-level locks.
            if self.stream_lock.closed():
                try:
                    self._open_lockfile()
                except Exception:
                    self.handleError(NullLogRecord())
                    # Don't try to open the stream lock again
                    self.stream_lock = None
                    return
            self.stream_lock.acquire()
        # Stream will be opened as part by FileHandler.emit()

    def releaseLock(self):
        """ Release file and thread locks. If in 'degraded' mode, close the
        stream to reduce contention until the log files can be rotated. """
        try:
            if self._rotateFailed:
                self._closeStream()
        except Exception:
            self.handleError(NullLogRecord())
        finally:
            try:
                if self.stream_lock and not self.stream_lock.closed():
                    self.stream_lock.release()
            except Exception:
                self.handleError(NullLogRecord())

    def close(self):
        self.acquireLock()
        try:
            if self.stream is not None and hasattr(self.stream, 'close'):
                stream = self.stream
                self.stream = None
                stream.close()

            if self.csv_stream is not None:
                self.csv_stream = None
        finally:
            self.releaseLock()

    def emit(self, auditRecord):
        if self.shouldRollover(auditRecord):
            self.acquireLock()
            try:
                self.doRollover()
            finally:
                self.releaseLock()

        if self.stream is None or self.csv_stream is None:
            self._open()

        row_dict = auditRecord.__dict__

        self.csv_stream.writerows([row_dict])
        self.stream.flush()

    def doRollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None
            self.csv_stream = None
        currTimestampS = int(time.time())
        dstTimestamp = time.localtime(currTimestampS)[-1]
        t = self.rolloverAt - self.interval
        if self.utc:
            timeTuple = time.gmtime(t)
        else:
            timeTuple = time.localtime(t)
            dstThen = timeTuple[-1]
            if dstTimestamp != dstThen:
                if dstTimestamp:
                    addend = 3600
                else:
                    addend = -3600
                timeTuple = time.localtime(t + addend)
        dfn = self.rotation_filename('%s.%s' % (self.baseFilename, time.strftime(self.suffix, timeTuple)))
        if not os.path.exists(dfn):
            if os.path.exists(self.baseFilename):
                os.rename(self.baseFilename, dfn)
        self._chmod(dfn, const.rotate_log_file_mode)
        if self.backupCount > 0:
            for deleteFilePath in self.getFilesToDelete():
                os.remove(deleteFilePath)
        if not self.delay:
            self.stream = self._open()
        nextRolloverTime = self.computeRollover(currTimestampS)
        while nextRolloverTime <= currTimestampS:
            nextRolloverTime = nextRolloverTime + self.interval
        if (self.when == 'MIDNIGHT' or self.when.startswith('W')) and not self.utc:
            dstAtRollover = time.localtime(nextRolloverTime)[-1]
            if dstTimestamp != dstAtRollover:
                if not dstTimestamp:
                    addend = -3600
                else:
                    addend = 3600
                    nextRolloverTime += addend
        self.rolloverAt = nextRolloverTime
        # Rollover audit log to obs
        self._backupPool.submit(self.backupAuditLogAction)

    def __del__(self):
        try:
            if self._backupPool:
                self._backupPool.shutdown()
        except Exception as e:
            pass

    def makeErrorMessage(self, resp):
        return 'error code [%s] - error message [%s] - request id [%s] - status [%d]' % (
            resp.errorCode, resp.errorMessage, resp.requestId, resp.status)

    def makeResponseMessage(self, resp):
        return 'request id [%s] - status [%d]' % (resp.requestId, resp.status)

    def backupAuditLogAction(self):
        """
         # STEP 1: initial a obs client
         # STEP 2: find local obssftp_audit.csv.xxx
         # STEP 3: upload obssftp_audit.csv.xxx to obs bucket
         # STEP 4： upload successfully, then remove local
        :return:
        """
        self.backupLock.acquire()
        _client = None
        start = time.time()
        walkCost = -1
        try:
            list_dirs = os.walk(self._auditPath)
            backFiles = []
            for root, dirs, files in list_dirs:
                for f in files:
                    if all(['%s.' % self._auditFileName in f, 'lock' not in f]):
                        backFiles.append(os.path.join(root, f))
            if len(backFiles) < 24:
                log.log(logging.DEBUG, 'Do not backup, because of backup files number [%d] little than 24.',
                        len(backFiles))
                return
            walkCost = int((time.time() - start) * 1000)
            _client = ObsClient(access_key_id=self._ak, secret_access_key=self._sk, server=self._endpoint,
                                path_style=True)
            _client.initLog(LogConf(OBS_SDK_LOG_CONFIG))
            for fileAbsPath in backFiles:
                log.info('Start to upload audit file [%s] to bucket [%s].', fileAbsPath, self._bucket)
                filename = fileAbsPath.split('/')[-1]
                dayPath = None
                try:
                    dayPath = str(filename[len(self._auditFileName) + 1:]).split('_')[0]
                except Exception as e:
                    log.log(logging.ERROR, 'Parse day path failed. error message [%s].', str(e))
                key = ('%s/%s' % (dayPath, filename)) if dayPath else filename
                try:
                    uploadResp = _client.putFile(self._bucket, key, file_path=fileAbsPath)
                    if uploadResp.status < 300:
                        cost = int((time.time() - start) * 1000)
                        log.info('Upload audit file [%s] successfully, upload cost [%d] - walk cost [%d] - %s',
                                 fileAbsPath, cost, walkCost, self.makeResponseMessage(uploadResp))
                        os.remove(fileAbsPath)
                        continue
                    cost = int((time.time() - start) * 1000)
                    log.info(
                        'Upload audit file [%s] to bucket [%s] of key [%s] failed. upload cost [%d] - walk cost [%d] - %s',
                        fileAbsPath, key, self._bucket, cost, walkCost, self.makeErrorMessage(uploadResp))
                except Exception as e:
                    cost = int((time.time() - start) * 1000)
                    log.log(
                        'Upload audit file [%s] to bucket [%s] of key [%s] failed. error message [%s] - upload cost [%d] - walk cost [%d]',
                        fileAbsPath, key, self._bucket, str(e), cost, walkCost, traceback.format_exc())
        except Exception as e:
            cost = int((time.time() - start) * 1000)
            log.log(logging.ERROR,
                    'Backup audit log to bucket [%s] failed. error message [%s] - upload cost [%d] - walk cost [%d] - %s.',
                    self._bucket, str(e), cost, walkCost, traceback.format_exc())
            pass
        finally:
            if _client:
                _client.close()
            self.backupLock.release()


def getAuditLogger():
    auditLogger = AuditLogger('obssftp_audit')
    auditFormatter = AuditFormatter()
    audditlog_path = config.getConfig().log['auditlogpath']
    auditlog_dir = os.path.dirname(audditlog_path)
    if not os.path.exists(auditlog_dir):
        os.makedirs(auditlog_dir)
    fh_audit = TimeRotatingCSVHandler(audditlog_path, when='H', interval=1, backupCount=48)
    fh_audit.setLevel(logging.DEBUG)
    fh_audit.setFormatter(auditFormatter)
    auditLogger.addHandler(fh_audit)
    return auditLogger
