#! /bin/python
# -*- coding:utf-8 -*-
import sys
import socket
import traceback
import logging
import threading
import time
#from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool

import paramiko

from obssftp.server import ObsSftpServer
from obssftp import auth, const, config, logmgr

logClient = logmgr.getLogger()


def acceptClientConnection(conn, keyfile):
    transport = None
    try:
        host_key = paramiko.RSAKey.from_private_key_file(keyfile)
        transport = paramiko.Transport(conn)
        transport.add_server_key(host_key)
        transport.set_subsystem_handler(
            'sftp', paramiko.SFTPServer, ObsSftpServer)
        connIp, connPort = conn.getpeername()
        threading.current_thread().name = 'trans_%s_%d' % (connIp, connPort)
        server = auth.ObsServerAuth(connIp, connPort, transport)
        transport.start_server(server=server)
        channel = transport.accept(30)
        logClient.log(logging.INFO, 'channel [%s]', channel)
        while transport.is_active():
            time.sleep(1)
    except Exception as e:
        logClient.log(logging.ERROR, 'Accept client [%s] connection failed. error message [%s] - %s', conn, str(e), traceback.format_exc())
    finally:
        if transport:
            transport.close()
        if conn:
            conn.close()


def main():
    cfg = config.getConfig()
    listen_address = cfg.state.get('listen_address')
    listen_port = int(cfg.state.get('listen_port'))
    keypath = const.HOST_PRIVATE_KEY_PATH
    client_num = int(cfg.state.get('client_num'))
    startServer(listen_address, listen_port, keypath, 'INFO', client_num)


def startServer(listen_address, listen_port, keyFile, level='INFO', MaxTreadNums=64):
    paramikoLogLevel = getattr(paramiko.common, level)
    paramiko.common.logging.basicConfig(level=paramikoLogLevel)
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
    server_socket.bind((listen_address, listen_port))
    server_socket.listen(const.BACKLOG)
    #pool = ThreadPoolExecutor(max_workers=MaxTreadNums)
    pool = Pool(processes=MaxTreadNums)
    logClient.log(logging.INFO, 'SFTP Server bind in [%s:%s]' % (listen_address, listen_port))
    try:
        while True:
            conn, addr = server_socket.accept()
            #pool.submit(acceptClientConnection, conn, keyFile)
            pool.apply_async(acceptClientConnection, (conn, keyFile,))
    except Exception as e:
        logClient.log(logging.ERROR, 'Client connection failed. %s', traceback.format_exc())
    finally:
        try:
            #pool.shutdown()
            pool.close()
        except Exception as e:
            logClient.log(logging.ERROR, 'Shutdown connection pool failed. %s', traceback.format_exc())


if __name__ == '__main__':
    sys.exit(main())


