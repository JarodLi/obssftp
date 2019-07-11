import traceback
import logging
import threading
import time
import functools
from stat import S_IFREG, S_IFDIR
from concurrent.futures import ThreadPoolExecutor, as_completed

from paramiko import SFTPAttributes
from obs import CompletePart, CompleteMultipartUploadRequest, GetObjectHeader

from obssftp import const
from obssftp.util import ObsSftpUtil


class WaitGroup(object):
    """ WaitGroup is like Go sync.WaitGroup. Provide a method for wait a increment condition by lock.

    Attributes:
        _cv: A lock for increase _count.
        _count: A increment thread condition.
    """

    def __init__(self):
        self._count = 0
        self._cv = threading.Condition()

    def add(self, n):
        """ Increase numbers of waiting

        :param n: waiting numbers
        :return: None
        """
        self._cv.acquire()
        self._count += n
        self._cv.release()

    def done(self):
        """Decrease a number of waiting

        :return: None
        """
        self._cv.acquire()
        self._count -= 1
        if self._count == 0:
            self._cv.notify_all()
        self._cv.release()

    def wait(self):
        """Waiting that the number of waiting great 0

        :return: None
        """
        self._cv.acquire()
        while self._count > 0:
            self._cv.wait()
        self._cv.release()


class MultiThreadScanBase(object):
    """A Multi-thread scan base class for obs 'folder'.

    Attributes:
        _lock: A lock for When change or get _exception value.
        _exception: A boolean indicating for record and fast-error when any exception occur.
        _scanPool: A ThreadPoolExecutor object for submit scan obs 'folder' task.
        _workPool: A ThreadPoolExecutor object for submit execute obs object task.
        _logClient: A logging object for record DEBUG/WARNING/INFO/ERROR log.
        _client: A ObsClient object for connection obs service. include listObject/delete et.
        _scanFutureList: A list object for append scan obs 'folder' future, and get the scan task result.
        _workFutureList: A list object for append execute obs object future, and get the execute result.
        _connHost: client host information for log
    """

    def __init__(self, obsClient, logClient, connHost=None):
        self._lock = threading.Lock()
        self._exception = []
        self._scanPool = ThreadPoolExecutor(max_workers=10)
        self._workPool = ThreadPoolExecutor(max_workers=const.DEFAULT_THREAD_NUMS)
        self._logClient = logClient
        self._client = obsClient
        self._scanFutureList = []
        self._workFutureList = []
        self._connHost = connHost

    def close(self):
        try:
            self._scanPool.shutdown()
            self._workPool.shutdown()
            self._scanFutureList = None
            self._workFutureList = None
            self._lock = None
        except Exception as e:
            self._logClient.log(logging.WARNING, 'Shutdown thread pool failed. error message [%s] - %s',
                                str(e), traceback.format_exc())


class MultiThreadScan(MultiThreadScanBase):
    """Multi-thread to remove obs 'folder'

    Attributes:
        _bucket: A string of obs bucket name.
        _key: A string of obs object key.
    """

    def __init__(self, bucket, key, obsClient, logClient, connHost=None):
        super(MultiThreadScan, self).__init__(obsClient, logClient, connHost)
        self._bucket = bucket
        self._key = key

    def _doScan(self, bucket, key, action, wg):
        """Scan the obs 'folder' and submit task use action.

        if has exception in other thread, the current scan must return.

        :param bucket: A string of obs bucket.
        :param key: A string of obs key.
        :param action: A functional
        :param wg: A WaitGroup for wait the folder sub-task finished.
        :return: None
        """
        threading.current_thread().name = 'trans_%s' % self._connHost
        try:
            _key = ObsSftpUtil.maybeAddTrailingSlash(key)
            subWg = WaitGroup()
            if self._lock.acquire():
                try:
                    if self._exception:
                        return
                finally:
                    self._lock.release()
                try:
                    objectsIter = ListObjects(client=self._client, bucketName=bucket, prefix=_key, delimiter='',
                                              logClient=self._logClient)
                    for i, obsObject in enumerate(objectsIter):
                        obsKey = obsObject.key
                        fileName = obsKey[len(_key):]
                        if not fileName:
                            continue
                        subWg.add(1)
                        self._workFutureList.append(self._workPool.submit(action, bucket, obsKey, subWg))
                    subWg.wait()
                    if not action(bucket, _key, None):
                        self._exception = True
                except Exception as e:
                    self._logClient.log(logging.ERROR, 'Scan key [%s] of bucket [%s] failed. error message [%s] - %s',
                                        _key, bucket,
                                        str(e), traceback.format_exc())
                    self._exception = True
                    return
        finally:
            wg.done()

    def recursiveAction(self, action):
        """Recursive scan obs 'folder' and execute the param action.

        :param action: A action for obs object.
        :return: return true if every sync task is successfully, otherwise, return false or raise a running exception.
        """
        try:
            wg = WaitGroup()
            wg.add(1)
            self._scanAndDoAction(self._bucket, key=self._key, action=action, wg=wg)
            wg.wait()
            # All tasks succeeded to be successful
            actionSucceed = not self._exception
            if not actionSucceed:
                return False
            for workFuture in as_completed(self._workFutureList):
                actionSucceed = actionSucceed and workFuture.result()
            return actionSucceed
        finally:
            self.close()

    def _scanAndDoAction(self, bucket, key, action, wg):
        self._scanFutureList.append(self._scanPool.submit(self._doScan, bucket, key, action, wg))


class MultiThreadScanAndRename(MultiThreadScanBase):
    """Multi-thread to rename obs 'folder'.

    Attributes:
        _srcBucket: A string of obs source bucket name.
        _srcKey: A string of obs source object key.
        _dstBucket: A string of obs destination bucket name.
        _dstKey: A string of obs destination object key.
    """

    def __init__(self, srcBucket, srcKey, dstBucket, dstKey, obsClient, logClient, connHost=None):
        super(MultiThreadScanAndRename, self).__init__(obsClient, logClient, connHost)
        self._srcBucket = srcBucket
        self._srcKey = srcKey
        self._dstBucket = dstBucket
        self._dstKey = dstKey

    def _doScan(self, srcBucket, srcKey, dstBucket, dstKey, preAction, action, afterAction, wg):
        """Scan obs 'folder' and rename source to destination.

        if has exception in other thread, the current scan must return.

        :param srcBucket: A string of source obs bucket.
        :param srcKey: A string of source obs key.
        :param dstBucket: A string of destination bukcet.
        :param dstKey: A string of destination key.
        :param preAction: A action of pre-rename.
        :param action: A action of rename.
        :param afterAction: A action of after-rename.
        :param wg: A WaitGroup for wait the folder sub-task finished.
        :return: None
        """
        threading.current_thread().name = 'trans_%s' % self._connHost
        try:
            _srcKey = srcKey
            _dstKey = dstKey
            subWg = WaitGroup()
            if self._lock.acquire():
                try:
                    # fast failed when any thread has a exception.
                    if self._exception:
                        return
                finally:
                    self._lock.release()
                try:
                    _srcKey = ObsSftpUtil.maybeAddTrailingSlash(srcKey)
                    _dstKey = ObsSftpUtil.maybeAddTrailingSlash(dstKey)
                    if not preAction(srcBucket, _srcKey, dstBucket, _dstKey):
                        # pre action failed, set _exception to true and return
                        self._exception = True
                        return
                    # list objects by prefix, maybe catch a exception.
                    objectsIter = ListObjects(self._client, srcBucket, prefix=_srcKey, delimiter='',
                                              logClient=self._logClient)
                    for i, obsObject in enumerate(objectsIter):
                        obsKey = obsObject.key
                        renameSrcKey = obsKey
                        fileName = renameSrcKey[len(_srcKey):]
                        # list objects is contains itself.
                        if not fileName:
                            continue
                        renameDstKey = _dstKey + renameSrcKey[len(_srcKey):]
                        subWg.add(1)
                        workFuture = self._workPool.submit(action, obsObject.size, srcBucket, renameSrcKey, dstBucket,
                                                           renameDstKey, subWg)
                        self._workFutureList.append(workFuture)
                    # waiting until sub object rename finished.
                    subWg.wait()
                    if self._lock.acquire():
                        try:
                            # fast failed when any thread has a exception.
                            if self._exception:
                                return
                            # rename success must delete source object key
                            if not afterAction(srcBucket, _srcKey):
                                self._exception = True
                        finally:
                            self._lock.release()
                except Exception as e:
                    self._logClient.log(logging.ERROR,
                                        'Scan and move source key [%s] of bucket [%s] to destination key [%s] of bucket [%s] failed. - error message [%s] - %s',
                                        _srcKey, srcBucket, _dstKey, dstBucket, str(e), traceback.format_exc())
                    self._exception = True
                    return
        finally:
            # Whether it is successful or not
            wg.done()

    def recursiveAction(self, preAction, action, afterAction):
        """Recursive scan obs 'folder', do rename action

        :param preAction: Jungle destination is exists, and craete destinationm objec key
        :param action: Rename object
        :param afterAction: Delete source object key
        :return: return True is all tasks succeeded, otherwise, return false or raise a running exception.
        """
        try:
            wg = WaitGroup()
            wg.add(1)
            self._scanAndDoAction(self._srcBucket, self._srcKey, self._dstBucket, self._dstKey, preAction, action,
                                  afterAction, wg)
            wg.wait()
            # All tasks succeeded to be successful
            actionSucceed = not self._exception
            if not actionSucceed:
                return False
            for workFuture in as_completed(self._workFutureList):
                actionSucceed = actionSucceed and workFuture.result()
            return actionSucceed
        finally:
            self.close()

    def _scanAndDoAction(self, srcBucket, srcKey, dstBucket, dstKey, preAction, action, afterAction, wg):
        # submit a scan task
        self._scanFutureList.append(
            self._scanPool.submit(self._doScan, srcBucket, srcKey, dstBucket, dstKey, preAction, action, afterAction,
                                  wg))


class ObsAdapter(object):

    """A obs adpter for operate obs when connection the sftp server.

    Attributes:
        _client: A ObsClient object for connection obs service. include listObject/delete et.
        _bucketName: A string of obs bucket name.
        _key: A string of obs object key.
        _buf: A bit list buffer for upload sftp client data to obs bucket.
        _bufLimit: A limit for buffer size, default is 10MB.
        _closed: A boolean indicating if the adapter is closed.
        _uploadId: A string of obs upload id, used in multi-part upload.
        _partNumber: A part number of multi-part upload.
        _partList: A list for record parts of multi-part upload.
        _partDict: A dict for record parts of multi-part upload, used in multi-thread upload.
        _logClient: A logging object for record DEBUG/WARNING/INFO/ERROR log.
        _reader: A reader for read a obs object, and response to sftp client.
        _workPool: A ThreadPoolExecutor for sync multi-part upload.
        _workFutureList: A list record has submitted task future for get the task result.
        _wg: A WaitGroup lock for submit multi-part upload task, and wait for every task finished.
        _lock: A lock for get or update _uploadException value.
        _uploadException: A boolean indicating for record and fast failed.
        _uploadExceptionMessage: when occur exception in upload, record exception message.
        _objectLength: operate obs object length for audit log
        _readerUseTimestamp: a timestamp for user reader idle time
        _ar: audit loh object
        _md5: file md5 value
        _md5Lock: a lock for calculate md5
        _openTime: a reader open timestamp
        _uploadFinishedNums: upload finished numbers
        _operateSize: operate size of object
        _connHost: client connection ip and port
    """

    def __init__(self, client, bucketName, key, logClient, ar, connHost=None):
        self._client = client
        self._bucketName = bucketName
        self._key = key.lstrip('/')
        self._buf = bytearray()
        self._bufLimit = const.SEND_BUF_SIZE
        self._closed = False
        self._uploadId = None
        self._partNumber = None
        self._partList = []
        self._partDict = {}
        self._logClient = logClient
        self._reader = None
        self._workPool = None
        self._workFutureList = []
        self._wg = WaitGroup()
        self._lock = threading.Lock()
        self._innerThreadLock = threading.Lock()
        self._uploadException = False
        self._uploadExceptionMessage = None
        self._objectLength = None
        self._readerUseTimestamp = time.time()
        self._ar = ar
        self._md5 = None
        self._md5Lock = threading.Lock()
        self._openTime = time.time()
        self._uploadFinishedNums = 0
        self._operateSize = 0
        self._connHost = connHost

    def __del__(self):
        """ release resource

        :return:
        """
        self.cleanThreadPool()
        self.closeReader()

    def cleanThreadPool(self):
        """ When user close sftp server connection call it.

        :return:
        """
        try:
            self._logClient.log(logging.INFO, 'Start to clean work pool.')
            if self._workPool:
                self._workPool.shutdown()
            self._closed = True
            self._md5Lock = None
            self._innerThreadLock = None
            self._lock = None
            self._wg = None
            self._workFutureList = None
            self.abortWriteUpload()
        except Exception as e:
            self._logClient.log(logging.WARNING, 'Close work pool failed, error message [%s] - %s.', str(e), traceback.format_exc())


    def _obsAuditRecord(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            adapter = args[0] if isinstance(args[0], ObsAdapter) else None
            result = None
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                end = time.time()
                totalTimeMs = int((end - start) * 1000)
                auditDict = {'obs_ret_detail': 'success' if result is not None else 'failed',
                             'obs_start_time': ObsSftpUtil.utcFormater(start),
                             'obs_end_time': ObsSftpUtil.utcFormater(end),
                             'obs_total_time': totalTimeMs, 'obs_bucket': adapter._bucketName, 'obs_key': adapter._key,
                             'obs_opt': func.__name__}
                adapter._ar.update(auditDict)

        return wrapper

    def _deleteAction(self, bucket, key, subWg):
        try:
            threading.current_thread().name = 'trans_%s' % self._connHost
            return self._deleteObjectBase(bucket=bucket, key=key, actionDescription='')
        except Exception as e:
            self._logClient.log(logging.ERROR,
                                'Delete object for key [%s] of bucket [%s] failed, error message [%S] %s', key, bucket,
                                str(e), traceback.format_exc())
            return False
        finally:
            if subWg:
                subWg.done()

    def _renameAction(self, objectSize, srcBucket, srcKey, dstBucket, dstKey, subWg):
        try:
            threading.current_thread().name = 'trans_%s' % self._connHost
            return self._renameObject(objectSize, srcBucket, srcKey, dstBucket, dstKey)
        except Exception as e:
            self._logClient.log(logging.ERROR,
                                'Rename object from source key [%s] of bucket [%s] to destination key [%s] of bucket [%s] failed, error messsage [%s] - %s',
                                srcKey, srcBucket, dstKey, dstBucket, str(e), traceback.format_exc())
            return False
        finally:
            if subWg:
                subWg.done()

    def _renameBeforeAction(self, srcBucket, srcKey, dstBucket, dstKey):
        # before rename, must destination key of bucket is not exists. then, create a fake folder.
        if self._renameExist(srcBucket, srcKey, dstBucket, dstKey):
            return False
        return self._putObjectBase(dstBucket, dstKey, b'')

    def _renameAfterAction(self, srcBucket, srcKey):
        # after folder delete, must delete source folder.
        return self._deleteObjectBase(srcBucket, srcKey, 'source')

    def _scanAndRename(self, srcBucket, srcKey, dstBucket, dstKey):
        scan = MultiThreadScanAndRename(srcBucket, srcKey, dstBucket, dstKey, self._client, self._logClient,
                                        connHost=self._connHost)
        return scan.recursiveAction(self._renameBeforeAction, self._renameAction, self._renameAfterAction)

    def _scanAndDelete(self):
        scan = MultiThreadScan(self._bucketName, self._key, self._client, self._logClient, connHost=self._connHost)
        return scan.recursiveAction(self._deleteAction)

    def _initiateMultiUpload(self):
        uploadId = self._initiateMultiPartUploadId(self._bucketName, self._key)
        self._uploadId = uploadId
        self._partNumber = 0
        self._partList = []
        return uploadId

    def _getUploadId(self):
        return self._uploadId if self._uploadId else self._initiateMultiUpload()

    def _uploadPartBase(self, bucket, key, partNumber, uploadId, data):
        calMd5Start = time.time()
        if self._md5Lock.acquire():
            try:
                if not self._md5:
                    import hashlib
                    self._md5 = hashlib.md5()
                self._md5.update(data)
            finally:
                self._md5Lock.release()
        partMd5 = ObsSftpUtil.base64_encode(ObsSftpUtil.md5(data))
        md5Cost = int((time.time() - calMd5Start) * 1000)
        start = time.time()
        resp = self._client.uploadPart(bucket, key, partNumber, uploadId, data, md5=partMd5)
        end = time.time()
        cost = int((end - start) * 1000)
        if not resp:
            self._logClient.log(logging.ERROR,
                                'Upload part for key [%s] of bucket [%s] failed, error message [response is None]. part number [%d] - upload id [%s] - calculate md5 cost [%d] - upload cost [%d].',
                                key, bucket, partNumber, uploadId, md5Cost, cost)
            return None
        if resp.status < 300:
            self._logClient.log(logging.DEBUG,
                                'Upload part for key [%s] of bucket [%s] successfully. part number [%d] - upload id [%s] - calculate md5 cost [%d] - cost [%d] - %s.',
                                key, bucket, partNumber, uploadId, md5Cost, cost,
                                ObsSftpUtil.makeResponseMessage(resp))
            etage = resp.body.etag
            if not etage:
                self._logClient.log(logging.ERROR,
                                    'Upload part for key [%s] of bucket [%s] failed. part number [%d] - error message [Etag is None] - calculate md5 cost [%d] - cost [%d] - %s.',
                                    key, bucket, partNumber, md5Cost, cost, ObsSftpUtil.makeResponseMessage(resp))
                return None
            return etage
        self._logClient.log(logging.ERROR,
                            'Upload part for key [%s] of bucket [%s] failed, part number [%d] - upload id [%s]- calculate md5 cost [%d] - cost [%d] - %s.',
                            key, bucket, partNumber, uploadId, md5Cost, cost,
                            ObsSftpUtil.makeErrorMessage(resp))
        return None

    def _uploadPart(self):
        return self._uploadPartBase(self._bucketName, self._key, self._partNumber, self._uploadId, bytes(self._buf))

    def _submitUploadPartAction(self, bucket, key, partNumber, uploadId, data, wg):
        try:
            threading.current_thread().name = 'trans_%s' % self._connHost
            eTag = self._uploadPartBase(bucket, key, partNumber, uploadId, data)
            if not eTag:
                if self._innerThreadLock.acquire():
                    try:
                        self._uploadException = True
                    finally:
                        self._innerThreadLock.release()
                self._uploadExceptionMessage = 'upload etag is None.'
                return False
            self._partDict[partNumber] = eTag
            return True
        except Exception as e:
            self._logClient.log(logging.ERROR,
                                'Sync upload part for key [%s] of bucket [%s] failed. error message [%s] - %s', key,
                                bucket,
                                str(e), traceback.format_exc())
            if self._innerThreadLock.acquire():
                try:
                    self._uploadException = True
                    self._uploadExceptionMessage = str(e)
                finally:
                    self._innerThreadLock.release()
            return False
        finally:
            if self._innerThreadLock.acquire():
                try:
                    self._uploadFinishedNums += 1
                finally:
                    self._innerThreadLock.release()
            if wg:
                wg.done()

    def _abortMultipartUploadBase(self, bucket, key, uploadId):
        try:
            self._logClient.log(logging.INFO,
                                'Start abort multi-part upload for key [%s] of bucket [%s]. upload id [%s].',
                                key, bucket, uploadId)
            resp = self._client.abortMultipartUpload(bucket, key, uploadId)
            if not resp:
                self._logClient.log(logging.ERROR,
                                    'Abort multi-part upload for key [%s] of bucket [%s] failed, error message [response is None]. upload id [%s].',
                                    key, bucket, uploadId)
                return
            if resp.status < 300:
                self._logClient.log(logging.DEBUG,
                                    'Abort multi-part upload for key [%s] of bucket [%s] successfully. upload id [%s] - %s.',
                                    key, bucket, uploadId, ObsSftpUtil.makeResponseMessage(resp))
                return
            self._logClient.log(logging.ERROR,
                                'Abort multi-part upload for key [%s] of bucket [%s] failed, upload id [%s] - %s.',
                                key, bucket, uploadId, ObsSftpUtil.makeErrorMessage(resp))
        except Exception as e:
            self._logClient.log(logging.ERROR,
                                'Abort multi-part upload for key [%s] of bucket [%s] failed. upload id [%s] - error message [%s] - %s.',
                                key, bucket, uploadId, str(e), traceback.format_exc())

    def _sendBuf(self):
        # first acquire a lock, then initiate a upload id or get has initiated upload id,
        # last sync submit a upload part task.
        if self._lock.acquire():
            try:
                if self._uploadException:
                    raise Exception('Sync Upload failed. error message [%s]' % self._uploadExceptionMessage)
                uploadId = self._getUploadId()
                if not uploadId:
                    self._uploadException = True
                    self._uploadExceptionMessage = 'not upload id'
                    raise Exception('Sync Upload failed. error message [%s].' % self._uploadExceptionMessage)
                if self._buf is None:
                    self._logClient.log(logging.WARNING,
                                        'Send buffer data for key [%s] of bucket [%s] failed. error message [No data to send].',
                                        self._key, self._bucketName)
                    self._uploadException = True
                    self._uploadExceptionMessage = 'send buffer is None'
                    raise Exception('Sync Upload failed. error message [%s].' % self._uploadExceptionMessage)
                self._partNumber += 1
                if not self._workPool:
                    self._workPool = ThreadPoolExecutor(max_workers=const.DEFAULT_THREAD_NUMS)
                while len(self._workFutureList) - self._uploadFinishedNums > const.DEFAULT_THREAD_NUMS:
                    self._logClient.log(logging.WARNING,
                                        'Worker length [%d] greater than max thread nums [%d]. wait a second.',
                                        len(self._workFutureList) - self._uploadFinishedNums, const.DEFAULT_THREAD_NUMS)
                    time.sleep(1)
                self._wg.add(1)
                self._workFutureList.append(
                    self._workPool.submit(self._submitUploadPartAction, self._bucketName, self._key, self._partNumber,
                                          self._uploadId, bytes(self._buf), self._wg))
                # record operation size
                self._operateSize += len(self._buf)
                self._buf = bytearray()
                return True
            finally:
                self._lock.release()

    def write(self, data):
        """Write a data to obs.

        Put data to buffer if buffer letter than _bufLimit. otherwise, sync upload to obs.

        :param data: the
        :return: return true if write successfully, otherwise, return false or raise a running exception.
        """
        self._logClient.log(logging.DEBUG, 'Start obsAdapter Write for key [%s] of bucket [%s]', self._key,
                            self._bucketName)
        while len(data) + len(self._buf) > self._bufLimit:
            needWriteLen = self._bufLimit - len(self._buf)
            self._buf += data[: needWriteLen]
            data = data[needWriteLen:]
            if not self._sendBuf():
                return False
        self._buf += data
        self._logClient.log(logging.DEBUG, 'Finished obsAdapter Write for key [%s] of bucket [%s]', self._key,
                            self._bucketName)
        return True

    def _headObject(self):
        resp = self._client.getObjectMetadata(self._bucketName, self._key)
        if not resp:
            self._logClient.log(logging.ERROR,
                                'Get object meta data for key [%s] of bucket [%s] failed, error message [response is None].',
                                self._key, self._bucketName)
            raise Exception('get object meta data failed. obs server response is None')
        if resp.status < 300:
            self._logClient.log(logging.INFO, 'Get object meta data for key [%s] of bucket [%s] successfully. %s.',
                                self._key,
                                self._bucketName, ObsSftpUtil.makeResponseMessage(resp))
            if any([not resp.body, resp.body.contentLength is None]):
                self._logClient.log(logging.ERROR,
                                    'Get object meta data for key [%s] of bucket [%s] successfully. but not response boy. %s.',
                                    self._key,
                                    self._bucketName, ObsSftpUtil.makeResponseMessage(resp))
                raise Exception('get object meta data failed. response body is None.')
            return resp.body
        self._logClient.log(logging.ERROR, 'Get object meta data for key [%s] of bucket [%s] failed, %s.', self._key,
                            self._bucketName,
                            ObsSftpUtil.makeErrorMessage(resp))
        raise Exception('get object meta data failed. %s' % ObsSftpUtil.makeErrorMessage(resp))

    def _getObjectRangeReader(self, offset, length):
        # first build a range read connection to get object,
        # then response a reader
        headers = GetObjectHeader()
        headers.range = '%d-%d' % (
            offset, offset + length if (offset + length < self._objectLength) else self._objectLength)
        resp = self._client.getObject(self._bucketName, self._key, headers=headers, loadStreamInMemory=False)
        if not resp:
            self._logClient.log(logging.ERROR,
                                'Get object range [%s] reader for key [%s] of bucket [%s] failed, error message [response is None].',
                                headers.range, self._key, self._bucketName)
            raise Exception('get object range [%s] reader failed. obs server response is None' % headers.range)
        if resp.status < 300:
            self._logClient.log(logging.DEBUG,
                                'Get object range [%s] reader for key [%s] of bucket [%s] successfully. %s.',
                                headers.range, self._key,
                                self._bucketName, ObsSftpUtil.makeResponseMessage(resp))
            return resp.body.response
        self._logClient.log(logging.ERROR, 'Get object range [%s] reader for key [%s] of bucket [%s] failed, %s.',
                            headers.range, self._key,
                            self._bucketName,
                            ObsSftpUtil.makeErrorMessage(resp))
        raise Exception('get object range [%s] reader failed. %s - content length [%d] - range [%s]' % (
            headers.range, ObsSftpUtil.makeErrorMessage(resp), self._objectLength, headers.range))

    def _clean(self):
        self._logClient.log(logging.DEBUG, 'Schedule clean reader. thread name [%s].', threading.current_thread().name)

        try:
            while time.time() - self._readerUseTimestamp < const.CONNECTION_IDLE_TIME_S:
                if self._closed:
                    self._logClient.log(logging.INFO, 'User initiative close reader. so do not close reader again.')
                    return
                time.sleep(1)
            self._logClient.log(logging.DEBUG, 'Idle time gt [%d]s. Start to clean reader.',
                                const.CONNECTION_IDLE_TIME_S)
            self.closeReader()
        except Exception as e:
            # ignore
            self._logClient.log(logging.WARNING, 'Ignore close reader connection failed. error message [%s] - %s.', str(e),
                                traceback.format_exc())
        finally:
            try:
                if self._cleanThread:
                    self._cleanThread.close()
            except Exception as e:
                self._logClient.log(logging.WARNING, 'Ignore close clean thread failed. error message [%s] - %s.', str(e),
                                    traceback.format_exc())

    def _getObject(self):
        # first build a connection to get object,
        # then initiate a _reader for client call SFTPHandler.read(offset, length)
        resp = self._client.getObject(self._bucketName, self._key, loadStreamInMemory=False)
        if not resp:
            self._logClient.log(logging.ERROR,
                                'Get object for key [%s] of bucket [%s] failed, error message [response is None].',
                                self._key, self._bucketName)
            return False
        if resp.status < 300:
            self._logClient.log(logging.INFO, 'Get object for key [%s] of bucket [%s] successfully. %s.', self._key,
                                self._bucketName, ObsSftpUtil.makeResponseMessage(resp))
            self._reader = resp.body.response
            self._cleanThread = threading.Thread(target=self._clean, name='trace-getobject-%s' % self._connHost).start()
            return True
        self._logClient.log(logging.ERROR, 'Get object for key [%s] of bucket [%s] failed, %s.', self._key,
                            self._bucketName,
                            ObsSftpUtil.makeErrorMessage(resp))
        return False

    def read(self, offset, length):
        """ Read a fixed length data from obs, range read

        :param offset: start offset
        :param length:  want to read data length
        :return:  1 bit list
        """
        buffer = b''
        readSize = 0
        reader = None
        try:
            # range read
            if self._objectLength is None:
                self._objectLength = self._headObject().contentLength
            if offset >= self._objectLength:
                return buffer
            length = length if offset + length < self._objectLength else self._objectLength - offset
            reader = self._getObjectRangeReader(offset, length)
            while readSize < length:
                lastLength = length - readSize
                readBufferSize = lastLength if lastLength < const.READ_BUF_SIZE else const.READ_BUF_SIZE
                buffer += reader.read(readBufferSize)
                readSize += readBufferSize
            return buffer
        except Exception as e:
            self._logClient.log(logging.ERROR,
                                'Read data from key [%s] of bucket [%s] failed, error message [read a middle data] - [%s] - %s',
                                self._key, self._bucketName, str(e), traceback.format_exc())
            return None
        finally:
            try:
                if reader:
                    reader.close()
            except Exception as e:
                self._logClient.log(logging.ERROR,
                                    'Close reader from key [%s] of bucket [%s] failed, error message [%s] - %s',
                                    self._key, self._bucketName, str(e), traceback.format_exc())

    def readWithLongConnection(self, offset, length):
        """Read a fixed length data from obs.

        if _reader is None, must to build a long connection with obs service.
        read a fixed length from _reader.

        :param offset:
        :param length: want to read data length
        :return: a bit list
        """
        realReadSize = 0
        try:
            if not self._reader:
                if not self._getObject():
                    return None
            buffer = b''
            readSize = 0
            # reader buffer size is 64KB that a tcp package data size
            if self._objectLength is None:
                self._objectLength = self._headObject().contentLength
            if offset >= self._objectLength:
                return buffer
            length = length if offset + length < self._objectLength else self._objectLength - offset
            while readSize < length:
                lastLength = length - readSize
                readBufferSize = lastLength if lastLength < const.READ_BUF_SIZE else const.READ_BUF_SIZE
                buffer += self._reader.read(readBufferSize)
                readSize += readBufferSize
            realReadSize = len(buffer)
            return buffer
        except Exception as e:
            self._logClient.log(logging.ERROR,
                                'Read data from key [%s] of bucket [%s] failed, error message [read a middle data] - [%s] - %s',
                                self._key, self._bucketName, str(e), traceback.format_exc())
            return None
        finally:
            self._operateSize += realReadSize
            self._readerUseTimestamp = time.time()

    def closeReader(self):
        """Close the reader of the connection with obs service.

        :return: None
        """
        if self._reader:
            try:
                if self._closed:
                    self._logClient.log(logging.WARNING,
                                        'Read operate for key [%s] of bucket [%s] has been closed.',
                                        self._key, self._bucketName)
                    return False
                self._reader.close()
                self._closed = True
            except Exception as e:
                self._logClient.log(logging.ERROR,
                                    'Close reader for key [%s] of bucket [%s] failed, error message [%s] - %s',
                                    self._key,
                                    self._bucketName, str(e), traceback.format_exc())
            finally:
                try:
                    end = time.time()
                    totalTimeS = int(end - self._openTime)
                    totalTimeMs = int(totalTimeS * 1000)
                    bps = '-'
                    if self._objectLength is None:
                        self._objectLength = self._operateSize
                    if totalTimeMs != 0:
                        bps = '%s/s' % str(ObsSftpUtil.normalizeBytes(float(self._operateSize / totalTimeMs) * 1000))
                    auditDict = {'obs_end_time': ObsSftpUtil.utcFormater(end), 'obs_ret_detail': 'success',
                                 'obs_opt': 'read', 'obs_total_time': totalTimeMs,
                                 'object_size': ObsSftpUtil.normalizeBytes(float(self._objectLength)),
                                 'opt_size': ObsSftpUtil.normalizeBytes(float(self._operateSize)), 'bps': bps}
                    self._ar.update(auditDict)
                except Exception as e:
                    self._logClient.log(logging.WARNING,
                                        'Calculate audit log for key [%s] of bucket [%s] failed. error message [%s] - %s',
                                        self._key, self._bucketName, str(e), traceback.format_exc())

    def _putObject(self, buf):
        self._operateSize += len(buf)
        return self._putObjectBase(self._bucketName, self._key, buf)

    def _putObjectBase(self, bucket, key, buf):
        # put object and build a upload data's md5 if data is not '' or None
        headers = {}
        if buf:
            if self._md5Lock.acquire():
                try:
                    if not self._md5:
                        import hashlib
                        self._md5 = hashlib.md5()
                    self._md5.update(buf)
                finally:
                    self._md5Lock.release()
            headers[const.CONTENT_MD5_HEADER] = ObsSftpUtil.base64_encode(ObsSftpUtil.base64_encode(self._md5.digest()))
        resp = self._client.putObject(bucket, key, buf, headers=headers)
        if not resp:
            self._logClient.log(logging.ERROR,
                                'Put object for key [%s] of bucket [%s] failed, error message [response is None].', key,
                                bucket)
            return False
        if resp.status < 300:
            self._logClient.log(logging.INFO, 'Put object for key [%s] of bucket [%s] successfully. %s.', key, bucket,
                                ObsSftpUtil.makeResponseMessage(resp))
            return True
        self._logClient.log(logging.ERROR, 'Put object for key [%s] of bucket [%s] failed, %s.', key, bucket,
                            ObsSftpUtil.makeErrorMessage(resp))
        return False

    def _completeMultipartUploadBase(self, bucketName, key, uploadId, partList):
        # build a complete merge part list request
        completeMultipartRequest = CompleteMultipartUploadRequest(partList)
        resp = self._client.completeMultipartUpload(bucketName, key, uploadId, completeMultipartRequest)
        if not resp:
            self._logClient.log(logging.ERROR,
                                'Complete multipart upload for key [%s] of bucket [%s] failed, error message [response is None], upload id [%s].',
                                key, bucketName, uploadId)
            return False
        if resp.status < 300:
            self._logClient.log(logging.INFO,
                                'Complete multipart upload for key [%s] of bucket [%s] successfully. upload id [%s] - %s.',
                                key, bucketName, uploadId, ObsSftpUtil.makeResponseMessage(resp))
            self._uploadId = None
            return True
        self._logClient.log(logging.ERROR,
                            'Complete multipart upload for key [%s] of bucket [%s] failed, upload id [%s] - %s.', key,
                            bucketName, uploadId, ObsSftpUtil.makeErrorMessage(resp))
        return False

    def abortWriteUpload(self):
        if self._uploadId:
            self._abortMultipartUploadBase(self._bucketName, self._key, self._uploadId)
            return
        self._logClient.log(logging.WARNING, 'User want to abort key [%s] of bucket [%s], but upload id is None.', self._key, self._bucketName)

    def _completeMultipartUpload(self):
        # complete multi-part upload if every upload part finished.
        # _wg must call done function in ever task finished or exception occur.
        self._wg.wait()
        actionSucceed = True
        # every task succeed just action succeed
        for workFuture in as_completed(self._workFutureList):
            actionSucceed = actionSucceed and workFuture.result()
        if not actionSucceed:
            self._abortMultipartUploadBase(self._bucketName, self._key, self._getUploadId())
            self._logClient.log(logging.ERROR,
                                'Complete merge upload part for key [%s] of bucket [%s] failed. error message [Not every task is succeed].',
                                self._key, self._bucketName)
            return False
        try:
            partDict = sorted(self._partDict.items(), key=lambda d: d[0])
            for partNumber, etag in partDict:
                self._partList.append(CompletePart(partNum=partNumber, etag=etag))
            return self._completeMultipartUploadBase(self._bucketName, self._key, self._uploadId, self._partList)
        except Exception as e:
            self._abortMultipartUploadBase(self._bucketName, self._key, self._getUploadId())
            raise e

    @property
    def ar(self):
        return self._ar

    def close(self):
        """Upload the last buffer or put a little than _bufLimit file, and close write channel with obs service.

        :return: return true if upload part and merge multi-part or put a lillte file successfully,
             return false if has closed or operate failed, otherwise, raise a running exception.
        """
        succeed = True
        try:

            if self._closed:
                self._logClient.log(logging.WARNING, 'Write operate for key [%s] of bucket [%s] has been closed.',
                                    self._key, self._bucketName)
                return False
            succeed = self._putObject(
                bytes(self._buf)) if self._uploadId is None else self._sendBuf() and self._completeMultipartUpload()
            self._closed = True
            return succeed
        finally:
            if self._workPool:
                try:
                    self._workPool.shutdown()
                except Exception as e:
                    self._logClient.log(logging.WARNING,
                                        'Shutdown multi-part upload thread pool failed. error message [%s] - %s',
                                        str(e), traceback.format_exc())
            try:
                md5Encode = '-'
                if self._md5:
                    md5Encode = ObsSftpUtil.base64_encode(self._md5.digest())
                end = time.time()
                totalTimeS = float(end - self._openTime)
                totalTimeMs = int(totalTimeS * 1000)
                if self._objectLength is None:
                    self._objectLength = self._operateSize
                bps = '-'
                if totalTimeMs != 0:
                    bps = '%s/s' % str(ObsSftpUtil.normalizeBytes(float(self._operateSize / totalTimeMs) * 1000))
                auditDict = {'obs_end_time': ObsSftpUtil.utcFormater(end),
                             'obs_ret_detail': 'success' if succeed else 'failed', 'md5': md5Encode, 'obs_opt': 'write',
                             'obs_total_time': totalTimeMs,
                             'object_size': ObsSftpUtil.normalizeBytes(float(self._objectLength)),
                             'opt_size': ObsSftpUtil.normalizeBytes(float(self._operateSize)), 'bps': bps}
                self._ar.update(auditDict)
            except Exception as e:
                self._logClient.log(logging.WARNING,
                                    'Calculate audit log failed. error message [%s] - %s',
                                    str(e), traceback.format_exc())
            del self._buf
            del self._md5

    @_obsAuditRecord
    def listDir(self):
        """List obs 'folder' and transform to SFTPAttributes objects.

        :return: A list of SFTPAttributes or None
        """
        _key = ObsSftpUtil.maybeAddTrailingSlash(self._key)
        contents = []
        objectsIter = ListObjects(self._client, self._bucketName, prefix=_key, delimiter='/', logClient=self._logClient)
        for i, obsObject in enumerate(objectsIter):
            attr = SFTPAttributes()
            obsKey = obsObject.key
            attr.filename = obsKey[len(_key):]
            if not attr.filename:
                continue
            if obsObject.lastModified is None:
                attr.filename = obsKey[len(_key): len(obsKey) - 1]
                attr.st_size = 0
                attr.st_mode = S_IFDIR
            else:
                attr.st_size = obsObject.size
                attr.st_mtime = ObsSftpUtil.strToTimestamp(obsObject.lastModified)
                attr.st_mode = S_IFREG
            contents.append(attr)
        return contents

    def _objectExists(self, bucketName, key):
        resp = self._client.getObjectMetadata(bucketName, key)
        if not resp:
            errorMessage = 'Jungle object exists for key [%s] of bucket [%s] failed, error message [response is None].' % (
                key, bucketName)
            raise Exception(errorMessage)
        if resp.status < 300:
            self._logClient.log(logging.DEBUG, 'Jungle object exists for key [%s] of bucket [%s] successfully. %s.',
                                key,
                                bucketName, ObsSftpUtil.makeResponseMessage(resp))
            return True
        if resp.status == 404:
            self._logClient.log(logging.DEBUG, 'Jungle object failed. error message [object is not exists] - %s.',
                                ObsSftpUtil.makeResponseMessage(resp))
            return False
        errorMessage = 'Jungle object exists for key [%s] of bucket [%s] failed, %s.' % (key,
                                                                                         bucketName,
                                                                                         ObsSftpUtil.makeErrorMessage(
                                                                                             resp))
        raise Exception(errorMessage)

    def objectInfo(self, bucketName, key):
        """ Get object information for outer

        :param bucketName: bucket name
        :param key: object key
        :return:
        """
        return self._objectInfo(bucketName, key)

    def _objectInfo(self, bucketName, key):
        resp = self._client.getObjectMetadata(bucketName, key)
        if not resp:
            self._logClient.log(logging.ERROR,
                                'Get object information for key [%s] of bucket [%s] failed, error message [response is None].',
                                key, bucketName)
            raise Exception('Get object information failed, obs response is None.')
        if resp.status < 300:
            self._logClient.log(logging.INFO, 'Get object information for key [%s] of bucket [%s] successfully. %s.',
                                key, bucketName, ObsSftpUtil.makeResponseMessage(resp))
            attr = SFTPAttributes()
            mtime = resp.body.lastModified
            if key and not ObsSftpUtil.isObsFolder(key):
                attr.st_size = resp.body.contentLength
                attr.st_mtime = mtime
                attr.st_mode = S_IFREG
            else:
                attr.st_size = 0
                attr.st_mode = S_IFDIR
            return attr
        if resp.status == 404:
            self._logClient.log(logging.DEBUG,
                                'Get object information for key [%s] of bucket [%s] failed, error message [object is not exists] - %s.',
                                key,
                                bucketName, ObsSftpUtil.makeErrorMessage(resp))
            return None
        self._logClient.log(logging.ERROR, 'Get object information for key [%s] of bucket [%s] failed, %s.', key,
                            bucketName, ObsSftpUtil.makeErrorMessage(resp))
        raise Exception('Get object information failed, %s' % ObsSftpUtil.makeErrorMessage(resp))

    def _prefixExists(self):
        return self._prefixExistsBase(self._bucketName, self._key)

    def _prefixExistsBase(self, bucket, key):
        _key = ObsSftpUtil.maybeAddTrailingSlash(key)
        objectsIter = ListObjects(self._client, bucket, prefix=_key, delimiter='/', logClient=self._logClient,
                                  maxKeys=2)
        subNums = 0
        for i, obsObject in enumerate(objectsIter):
            subNums += 1
        if subNums == 0:
            return None
        attr = SFTPAttributes()
        attr.st_mode = S_IFDIR
        attr.st_size = 0
        return attr

    def _pathInfoBase(self, bucket, key):
        """Jungle obs key is exists

        :return:
            if obs object or obs folder or obs prefix is exists return true,
            otherwise return false or raise a running exception.
        """
        if self._objectExists(bucket, key):
            return self._objectInfo(bucket, key)
        if all([not ObsSftpUtil.isObsFolder(key),
                self._objectExists(bucket, ObsSftpUtil.maybeAddTrailingSlash(key))]):
            return self._objectInfo(bucket, ObsSftpUtil.maybeAddTrailingSlash(key))
        return self._prefixExistsBase(bucket, key)

    def dirInfo(self, bucket, key):
        """Jungle obs dir or prefix is exists

        :return:
            if obs object or obs folder or obs prefix is exists return true,
            otherwise return false or raise a running exception.
        """
        if self._objectExists(bucket, ObsSftpUtil.maybeAddTrailingSlash(key)):
            return self._objectInfo(bucket, ObsSftpUtil.maybeAddTrailingSlash(key))
        return self._prefixExistsBase(bucket, key)

    @_obsAuditRecord
    def pathInfo(self):
        """Jungle obs key is exists

        :return:
            if obs object or obs folder or obs prefix is exists return true,
            otherwise return false or raise a running exception.
        """
        return self._pathInfoBase(self._bucketName, self._key)

    @_obsAuditRecord
    def mkDir(self):
        """Create a obs faker object.

        :return:
            if create successfully return true. otherwise, return false or raise a running exception
        """
        _key = self._key.rstrip('/')
        if self._pathInfoBase(self._bucketName, _key):
            self._logClient.log(logging.WARNING,
                                'Client create dir key [%s] of bucket [%s] failed. error message [same name file already exists].',
                                _key, self._bucketName)
            return False
        _key = ObsSftpUtil.maybeAddTrailingSlash(self._key)
        if self._pathInfoBase(self._bucketName, _key):
            self._logClient.log(logging.WARNING,
                                'Client create dir key [%s] of bucket [%s] failed. error message [key already exists].',
                                _key, self._bucketName)
            return False
        return self._putObjectBase(self._bucketName, _key, b'')

    def _deleteObjectBase(self, bucket, key, actionDescription):
        self._logClient.log(logging.WARNING, 'Start to Delete %s key [%s] of bucket [%s].',
                            ('[%s]' % actionDescription) if actionDescription else '', key,
                            bucket)
        resp = self._client.deleteObject(bucket, key)
        if not resp:
            self._logClient.log(logging.ERROR,
                                'Delete [%s] object for key [%s] of bucket [%s] failed, error message [response is None].',
                                actionDescription, key, bucket)
            return False
        if resp.status < 300:
            self._logClient.log(logging.INFO, 'Delete [%s] object for key [%s] of bucket [%s] successfully. %s.',
                                actionDescription, key, bucket, ObsSftpUtil.makeResponseMessage(resp))
            return True
        self._logClient.log(logging.ERROR, 'Delete [%s] object for key [%s] of bucket [%s] failed, %s.',
                            actionDescription, key, bucket, ObsSftpUtil.makeErrorMessage(resp))
        return False

    def _deleteObject(self):
        return self._deleteObjectBase(self._bucketName, self._key, '')

    @_obsAuditRecord
    def rmDir(self):
        """Recursive delete a obs 'folder' used multi-thread.

        :return:
            return true if recursive delete successfully. otherwise, return false or raise a running exception
        """
        return self._scanAndDelete()

    @_obsAuditRecord
    def remove(self):
        """Remove a obs object

        :return:
            return true if remove successfully. otherwise, return false or raise a running exception
        """
        # remove obs folder or
        if ObsSftpUtil.isObsFolder(self._key):
            self._logClient.log(logging.WARNING, 'Remove can not delete obs folder. bucket [%s] - key [%s]',
                                self._bucketName, self._key)
            return False
        if self._objectExists(self._bucketName, self._key):
            return self._deleteObject()
        self._logClient.log(logging.WARNING, 'Remove can not delete obs prefix. bucket [%s] - key [%s]',
                            self._bucketName, self._key)
        return None

    def _initiateMultiPartUploadId(self, bucket, key):
        resp = self._client.initiateMultipartUpload(bucket, key)
        if not resp:
            self._logClient.log(logging.ERROR,
                                'Initiate multi-part upload for key [%s] of bucket [%s] failed, error message [response is None].',
                                key, bucket)
            return None
        if resp.status < 300:
            uploadId = resp.body.uploadId
            if not uploadId:
                self._logClient.log(logging.INFO,
                                    'Initiate multi-part upload for key [%s] of bucket [%s] failed. %s - error message [response upload id is None]',
                                    key, bucket, ObsSftpUtil.makeResponseMessage(resp))
                return None
            self._logClient.log(logging.INFO,
                                'Initiate multi-part upload for key [%s] of bucket [%s] successfully. upload id [%s] - %s.',
                                key, bucket, uploadId, ObsSftpUtil.makeResponseMessage(resp))
            return uploadId
        self._logClient.log(logging.ERROR, 'Initiate multi-part upload for key [%s] of bucket [%s] failed, %s.', key,
                            bucket, ObsSftpUtil.makeErrorMessage(resp))
        return None

    def _submitCopyPartAction(self, srcBucket, srcKey, dstBucket, dstKey, uploadId, partNumber, rangeStart, rangeEnd,
                              wg):
        try:
            threading.current_thread().name = 'trans_%s' % self._connHost
            resp = self._client.copyPart(dstBucket, dstKey, partNumber, uploadId, '%s/%s' % (srcBucket, srcKey),
                                         '%d-%d' % (rangeStart, rangeEnd))
            if not resp:
                self._logClient.log(logging.ERROR,
                                    'Copy part from key [%s] of bucket [%s] to key [%s] of bucket [%s] failed. error message [response is None]. upload id [%s] - part number [%d].',
                                    srcKey, srcBucket, dstKey, dstBucket, uploadId, partNumber)
                return None
            if resp.status < 300:
                self._logClient.log(logging.DEBUG,
                                    'Copy part from key [%s] of bucket [%s] to key [%s] of bucket [%s] successfully. upload id [%s] - part number [%d] - %s.',
                                    srcKey, srcBucket, dstKey, dstBucket, uploadId, partNumber,
                                    ObsSftpUtil.makeResponseMessage(resp))
                eTag = resp.body.etag
                if not eTag:
                    self._logClient.log(logging.ERROR,
                                        'Copy part from key [%s] of bucket [%s] to key [%s] of bucket [%s] failed. error message [response etag is None]. upload id [%s] - part number [%d] - %s.',
                                        srcKey, srcBucket, dstKey, dstBucket, uploadId, partNumber,
                                        ObsSftpUtil.makeResponseMessage(resp))
                    return None
                return CompletePart(partNumber, eTag)
            self._logClient.log(logging.ERROR,
                                'Copy part from key [%s] of bucket [%s] to key [%s] of bucket [%s] failed. upload id [%s] - part number [%d] - %s.',
                                srcKey, srcBucket, dstKey, dstBucket, uploadId, partNumber,
                                ObsSftpUtil.makeErrorMessage(resp))
            return None
        except Exception as e:
            self._logClient.log(logging.ERROR,
                                'Copy part from key [%s] of bucket [%s] to key [%s] of bucket [%s] failed. upload id [%s] - part number [%d] - error message [%s] - %s.',
                                srcKey, srcBucket, dstKey, dstBucket, uploadId, partNumber, str(e),
                                traceback.format_exc())
            return None
        finally:
            if wg:
                wg.done()

    def _copyPartConcurrent(self, srcBucket, srcKey, objectSize, partCount, dstBucket, dstKey, uploadId):
        _workPool = ThreadPoolExecutor(max_workers=const.DEFAULT_THREAD_NUMS)
        try:
            partList = []
            wg = WaitGroup()
            copyPartFutures = []
            for partNumber in range(1, partCount + 1):
                rangeStart = (partNumber - 1) * const.SEND_BUF_SIZE
                rangeEnd = objectSize - 1 if partNumber == partCount else rangeStart + const.SEND_BUF_SIZE - 1
                wg.add(1)
                copyPartFutures.append(
                    _workPool.submit(self._submitCopyPartAction, srcBucket, srcKey, dstBucket, dstKey, uploadId,
                                     partNumber, rangeStart, rangeEnd, wg))
            wg.wait()
            if len(copyPartFutures) != partCount:
                self._logClient.log(logging.ERROR,
                                    'Submit Copy parts from key [%s] of bucket [%s] to key [%s] of bucket [%s] failed. upload id [%s] - error message [copy part future size [%d] is not equal part count [%d]].',
                                    srcKey, srcBucket, dstKey, dstBucket, uploadId, len(copyPartFutures), partCount)
                return None
            for copyPartFuture in as_completed(copyPartFutures):
                partEtag = copyPartFuture.result()
                # every tasks has log the error, so do not log again. just return None.
                if not partEtag:
                    return None
                partList.append(partEtag)
            return partList
        finally:
            try:
                _workPool.shutdown()
            except Exception as e:
                self._logClient.log(logging.WARNING,
                                    'Shutdown multi-part copy thread pool failed. error message [%s] - %s',
                                    str(e), traceback.format_exc())

    def _copyPart(self, srcBucket, srcKey, objectSize, partCount, dstBucket, dstKey, uploadId):
        partList = []
        for partNumber in range(1, partCount + 1):
            rangeStart = (partNumber - 1) * const.SEND_BUF_SIZE
            rangeEnd = objectSize - 1 if partNumber == partCount else rangeStart + const.SEND_BUF_SIZE - 1
            resp = self._client.copyPart(dstBucket, dstKey, partNumber, uploadId, '%s/%s' % (srcBucket, srcKey),
                                         '%d-%d' % (rangeStart, rangeEnd))
            if not resp:
                self._logClient.log(logging.ERROR,
                                    'Copy part from key [%s] of bucket [%s] to key [%s] of bucket [%s] failed. error message [response is None]. upload id [%s] - part number [%d].',
                                    srcKey, srcBucket, dstKey, dstBucket, uploadId, partNumber)
                return None
            if resp.status < 300:
                self._logClient.log(logging.DEBUG,
                                    'Copy part from key [%s] of bucket [%s] to key [%s] of bucket [%s] successfully. upload id [%s] - part number [%d] - %s.',
                                    srcKey, srcBucket, dstKey, dstBucket, uploadId, partNumber,
                                    ObsSftpUtil.makeResponseMessage(resp))
                eTag = resp.body.etag
                if not eTag:
                    self._logClient.log(logging.ERROR,
                                        'Copy part from key [%s] of bucket [%s] to key [%s] of bucket [%s] failed. error message [etag is None]. upload id [%s] - part number [%d] - %s.',
                                        srcKey, srcBucket, dstKey, dstBucket, uploadId, partNumber,
                                        ObsSftpUtil.makeResponseMessage(resp))
                    return None
                partList.append(CompletePart(partNumber, eTag))
                continue
            self._logClient.log(logging.ERROR,
                                'Copy part from key [%s] of bucket [%s] to key [%s] of bucket [%s] failed. upload id [%s] - part number [%d] - %s.',
                                srcKey, srcBucket, dstKey, dstBucket, uploadId, partNumber,
                                ObsSftpUtil.makeErrorMessage(resp))
            return None
        return partList

    def _copyObject(self, srcBucket, srcKey, dstBucket, dstKey):
        resp = self._client.copyObject(srcBucket, srcKey, dstBucket, dstKey)
        if not resp:
            self._logClient.log(logging.ERROR,
                                'Copy object from key [%s] of bucket [%s] to key [%s] of bucket [%s] failed. error message [response is None].',
                                srcKey, srcBucket, dstKey, dstBucket)
            return False
        if resp.status < 300:
            self._logClient.log(logging.INFO,
                                'Copy object from key [%s] of bucket [%s] to key [%s] of bucket [%s] successfully. %s.',
                                srcKey, srcBucket, dstKey, dstBucket, ObsSftpUtil.makeResponseMessage(resp))
            return True
        self._logClient.log(logging.ERROR,
                            'Copy object from key [%s] of bucket [%s] to key [%s] of bucket [%s] failed. %s.',
                            srcKey, srcBucket, dstKey, dstBucket, ObsSftpUtil.makeErrorMessage(resp))
        return False

    def _renameObject(self, objectSize, srcBucket, srcKey, dstBucket, dstKey):
        self._logClient.log(logging.INFO,
                            'Start to rename source key [%s] of bucket [%s] to destination key [%s] of bucket [%s].',
                            srcKey, srcBucket, dstKey, dstBucket)
        # rename succeed just copy and delete success.
        moveSucceed = True
        if objectSize > const.SEND_BUF_SIZE:
            uploadId = self._initiateMultiPartUploadId(dstBucket, dstKey)
            try:
                # initate upload id has log every error message, so just return None.
                if not uploadId:
                    return False
                partCount = ObsSftpUtil.calPartCount(objectSize)
                partList = self._copyPartConcurrent(srcBucket, srcKey, objectSize, partCount, dstBucket, dstKey,
                                                    uploadId)
                # copy parts has log every error message, so just return None.
                if not partList:
                    self._abortMultipartUploadBase(dstBucket, dstKey, uploadId)
                    return False
                moveSucceed = self._completeMultipartUploadBase(dstBucket, dstKey, uploadId, partList)
            except Exception as e:
                self._abortMultipartUploadBase(dstBucket, dstKey, uploadId)
                raise Exception(e)
        else:
            moveSucceed = self._copyObject(srcBucket, srcKey, dstBucket, dstKey)
        if all([moveSucceed, self._deleteObjectBase(srcBucket, srcKey, 'source')]):
            return True
        self._deleteObjectBase(dstBucket, dstKey, 'destination')
        return False

    def _renameFolder(self, srcBucket, srcKey, dstBucket, dstKey):
        _srcKey = ObsSftpUtil.maybeAddTrailingSlash(srcKey)
        _dstKey = ObsSftpUtil.maybeAddTrailingSlash(dstKey)
        if not self._putObjectBase(dstBucket, dstKey, b''):
            return False
        objectsIter = ListObjects(self._client, srcBucket, prefix=_srcKey, delimiter='/', logClient=self._logClient)
        for i, obsObject in enumerate(objectsIter):
            obsKey = obsObject.key
            renameSrcKey = obsKey
            _key = renameSrcKey[len(_srcKey):]
            if not _key:
                continue
            renameDstKey = dstKey + renameSrcKey[len(_srcKey):]
            if ObsSftpUtil.isObsFolder(obsKey):
                if not self._renameFolder(srcBucket, renameSrcKey, dstBucket, renameDstKey):
                    return False
            else:
                if not self._renameObject(obsObject.size, srcBucket, renameSrcKey, dstBucket, renameDstKey):
                    return False
        return self._deleteObjectBase(srcBucket, _srcKey, 'destination')

    def _renameExist(self, srcBucket, srcKey, dstBucket, dstKey):
        if self._objectExists(dstBucket, dstKey):
            self._logClient.log(logging.ERROR,
                                'Rename folder from source key [%s] of bucket [%s] to destination key [%s] of bucket [%s] failed. error message [The destination key is existed].',
                                srcKey, srcBucket, dstKey, dstBucket)
            return True
        return False

    @_obsAuditRecord
    def rename(self, srcBucket, srcKey):
        """Rename obs source key of bucket to destination key of bucket

        :param srcBucket: A string of source obs bucket name.
        :param srcKey: A string of source obs object key.
        :return:
            return None if source obs object or 'folder' not exists.
            return false if rename source obs object or 'folder' failed.
            return true if rename source obs object or 'folder' successfully.
        """
        dstBucket = self._bucketName
        dstKey = self._key
        # if user want to rename dir, but not put '/' suffix, it cause not found error.
        srcObject = self._pathInfoBase(srcBucket, srcKey)
        if srcObject:
            if srcObject.st_mode is S_IFDIR:
                # folder rename
                _srcKey = ObsSftpUtil.maybeAddTrailingSlash(srcKey)
                _dstKey = ObsSftpUtil.maybeAddTrailingSlash(dstKey)
                # fast failed that if jungle destination key is exists.
                if self._renameExist(srcBucket, _srcKey, dstBucket, _dstKey):
                    return False
                if all([srcBucket == dstBucket, any([_dstKey.startswith(_srcKey), _srcKey.startswith(_dstKey)])]):
                    self._logClient.log(logging.ERROR,
                                        'Rename folder from key [%s] of bucket [%s] to key [%s] of bucket [%s] failed. error message [The source cloud_url and the destination cloud_url are nested].',
                                        _srcKey, srcBucket, _dstKey, dstBucket)
                    return False
                return self._scanAndRename(srcBucket, _srcKey, self._bucketName, _dstKey)
            # object rename
            objectSize = srcObject.st_size
            _srcKey = srcKey.rstrip('/')
            _dstKey = dstKey.rstrip('/')
            if self._renameExist(srcBucket, _srcKey, dstBucket, _dstKey):
                return False
            return self._renameObject(objectSize, srcBucket, _srcKey, dstBucket, _dstKey)
        return None


class _List(object):
    """A Iterator base class for list obs objects

    Attributes:
        _marker: A string of obs list marker.
        _isTruncated: A boolean indicating for pagination list.
        _entity: A obs object keys of current pagination.
    """

    def __init__(self, marker=''):
        self._marker = marker
        self._isTruncated = True
        self._entity = []

    def _listResult(self):
        raise NotImplemented

    def __iter__(self):
        return self

    def __next__(self):
        """Iterator next object.

        :return:
            return a object if _entity is not None.
            return None if _entity is None and _isTruncated is True.
            raise a list object exception if list object has error.
        """
        while True:
            if self._entity:
                return self._entity.pop(0)
            if not self._isTruncated:
                raise StopIteration()
            self._listResult()


class ListObjects(_List):
    """Obs objects Iterator class.

    Attributes:
        _client: A ObsClient for connection service, and list objects.
        _bucketName: A string of obs bucket name.
        _prefix: A string of obs prefix.
        _marker: A string of obs marker.
        _delimiter: A string of obs delimiter.
        _marker: A maximum object keys of a pagination.
        _logClient: A logging of record DEBUG/WARNING/INFO/ERROR log.
    """

    def __init__(self, client, bucketName, prefix='', marker='', delimiter='', maxKeys=1000, logClient=None):
        super(ListObjects, self).__init__(marker)
        self._client = client
        self._bucketName = bucketName
        self._prefix = prefix
        self._marker = marker
        self._delimiter = delimiter
        self._maxKeys = maxKeys
        self._logClient = logClient

    def _listResult(self):
        resp = self._client.listObjects(self._bucketName, prefix=self._prefix, marker=self._marker,
                                        max_keys=self._maxKeys,
                                        delimiter=self._delimiter)
        if not resp:
            errorStr = 'List objects for prefix [%s] of bucket [%s] failed, error message [response is None].' % (
                self._prefix, self._bucketName)
            self._logClient.log(logging.ERROR, errorStr)

            self.isTruncated = False
            raise Exception(errorStr)
        if resp.status < 300:
            self._logClient.log(logging.INFO, 'List objects for prefix [%s] of bucket [%s] successfully. %s.',
                                self._prefix,
                                self._bucketName, ObsSftpUtil.makeResponseMessage(resp))
            self._logClient.log(logging.DEBUG, 'List result %r', resp.body)
            for content in resp.body.contents:
                self._entity.append(content)
            for dir in resp.body.commonPrefixs:
                self._entity.append(EnhanceObjectInfo(dir.prefix, None, None, None, None))
            self._entity.sort(key=lambda obj: obj.key)
            self._isTruncated = resp.body.is_truncated
            self._marker = resp.body.next_marker
            return
        errorStr = 'List objects for prefix [%s] of bucket [%s] failed, %s.' % (self._prefix,
                                                                                self._bucketName,
                                                                                ObsSftpUtil.makeErrorMessage(resp))
        self._logClient.log(logging.ERROR, errorStr)
        self._isTruncated = False
        raise Exception(errorStr)


class EnhanceObjectInfo(object):
    """A obs folder information class

    Attributes:
        _key: A string of obs object key.
        _lastModified: A long timestamp of obs object.
        _etag: A string of obs object md5.
        _size: A long of obs object size.
        _storageClass: A string of obs object storage type.
    """

    def __init__(self, key, lastModified, eTag, size, storageClass):
        self._key = key
        self._lastModified = lastModified
        self._etag = eTag
        self._size = size
        self._storageClass = storageClass

    @property
    def key(self):
        return self._key

    @property
    def lastModified(self):
        return self._lastModified

    @property
    def etag(self):
        return self._etag

    @property
    def size(self):
        return self._size

    @property
    def storageClass(self):
        return self._storageClass
