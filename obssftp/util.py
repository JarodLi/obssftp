#! /bin/python
# -*- coding: utf-8 -*-
import time
import logging
import base64
import hashlib
import traceback
import re

from obssftp import const, logmgr, config

log = logmgr.getLogger(__name__)


def get_buffer_size():
    """receive client data larger than 10MB, will sync upload"""
    cfg = config.getConfig()
    buffer_value, buffer_unit = re.search(
        r'^(\d+)\s*(?:([gGMmKk])?)?$', cfg.auth.get('buffer_size')).groups()
    buffer_size = int(buffer_value.strip()) * const.UNIT_DICT.get(buffer_unit.upper())
    return buffer_size

class ObsSftpUtil:
    @staticmethod
    def strToTimestamp(str):
        return time.mktime(time.strptime(str, '%Y/%m/%d %H:%M:%S'))

    @staticmethod
    def isBucket(path):
        phy_path = path.rstrip('/')
        index = phy_path.rfind('/')
        return True if index == 0 and not ObsSftpUtil.isRoot(path) else False

    @staticmethod
    def isRoot(path):
        return path == '/'

    @staticmethod
    def getBucketName(path):
        if ObsSftpUtil.isRoot(path):
            return u'/'
        phy_path = path.rstrip('/')
        index = phy_path.find('/', 1)
        return phy_path[1:] if index <= 0 else phy_path[1:index]

    @staticmethod
    def getFileName(path):
        if ObsSftpUtil.isBucket(path):
            return ''
        if path == '/':
            return u'/'
        bucket = ObsSftpUtil.getBucketName(path)
        return path[len(bucket) + 2:]

    @staticmethod
    def getBucketAndKey(path):
        _path = ObsSftpUtil.normalizePath(path)
        bucket = ObsSftpUtil.getBucketName(_path)
        if ObsSftpUtil.isBucket(_path):
            return bucket, ''
        return bucket, path[len(bucket) + 2:]

    @staticmethod
    def normalizePath(path):
        return path.replace('\\', '/')

    @staticmethod
    def getKey(path):
        _path = ObsSftpUtil.normalizePath(path)
        return ObsSftpUtil.getFileName(_path)

    @staticmethod
    def calPartCount(objectSize):
        return int(
            objectSize / const.SEND_BUF_SIZE) if objectSize % const.SEND_BUF_SIZE == 0 else int(
            objectSize / const.SEND_BUF_SIZE) + 1

    @staticmethod
    def makeErrorMessage(resp):
        return 'error code [%s] - error message [%s] - request id [%s] - status [%d]' % (
            resp.errorCode, resp.errorMessage, resp.requestId, resp.status)

    @staticmethod
    def makeResponseMessage(resp):
        return 'request id [%s] - status [%d]' % (resp.requestId, resp.status)

    @staticmethod
    def isObsFolder(key):
        return key.endswith('/')

    @staticmethod
    def base64_encode(unencoded):
        unencoded = (unencoded.encode('UTF-8') if not isinstance(unencoded, bytes) else unencoded)
        encodeestr = base64.b64encode(unencoded, altchars=None)
        return encodeestr.decode('UTF-8')

    @staticmethod
    def md5(buf):
        return hashlib.md5(buf).digest()

    @staticmethod
    def utcFormater(ts):
        import datetime
        try:
            return datetime.datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S UTC')
        except Exception as e:
            log.log(logging.ERROR, 'Format date failed. error message [%s] - %s.', str(e), traceback.format_exc())
            return ts

    @staticmethod
    def normalizeBytes(size):
        if size <= 0:
            return '0B'
        if size < const.KB:
            return '%.2fB' % float(size)
        if size < const.MB:
            return '%.2fKB' % float(size/const.KB)
        if size < const.GB:
            return '%.2fMB' % float(size / const.MB)
        if size < const.TB:
            return '%.2fGB' % float(size / const.GB)
        return '%.2fTB' % float(size / const.TB)

    @staticmethod
    def maybeAddTrailingSlash(key):
        if key:
            return '%s/'%key.rstrip('/')
        return key

