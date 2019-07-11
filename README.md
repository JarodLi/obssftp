# OBSSFTP: A sftp server on huawei OBS

目录
---------------

obssftp介绍
---------------

obssftp基于python语言开发，提供SFTP服务，允许用户通过SFTP方式将文件直接上传到OBS上。<br>
支持的SFTP客户端接口如下：

| 命令        | 描述  |  是否支持  |
| --------   | -----  | :----:  |
| cd path     | Change remote directory to 'path' |   ✔️     |
| chgrp grp path        |   Change group of file 'path' to 'grp'   |   ❌  |
| chmod mode path      |    Change permissions of file 'path' to 'mode'    |  ❌  |
| chown own path      |    Change owner of file 'path' to 'own'    |  ❌  |
| get [-r] remote [local]      |    Download file    |  ✔️  |
| ln [-s] oldpath newpath      |    Link remote file (-s for symlink)   |  ❌  |
| ls [-lafhrS] [path]      |    Display remote directory listing    |  ✔️  |
| mkdir path      |    Create remote directory   |  ✔️  |
| put [-r] local [remote]      |    Upload file    |  ✔️  |
| rename oldpath newpath      |    Rename remote file    |  ✔️  |
| rm path     |    Delete remote file    |  ✔️  |
| rmdir path      |    Remove remote directory    |  ✔️   |
| symlink oldpath newpath      |    Symlink remote file   |  ❌  |
| exit      |    Quit sftp    |  ✔️   |
| df [-hi] [path]      |   Display statistics for current directory or filesystem containing 'path'    |  ❌  |
| reget [-fPpRr] remote [local]      |   Resume download file    |  ❌  |
| reput [-fPpRr] [local] remote      |   Resume upload file    |  ❌  |   


安装指南
---------------

### 环境要求
- CPU: 8核
- 内存： 16GB
- 网卡带宽： 10Gbps
- 操作系统： centos 7.2及以上版本
- Python:  3.7.3
- openssl: 1.1.1c
- pip

### 安装

支持2种安装方式：      

1. 直接在linux上安装

1. 容器化安装

#### 在Linux上安装

1. 安装openssl
```
wget https://www.openssl.org/source/openssl-1.1.1c.tar.gz -P /opt   
tar xzvf /opt/openssl-1.1.1c.tar.gz -C /opt  
cd /opt/openssl-1.1.1c   
./config --prefix=/usr --openssldir=/usr/openssl shared zlib  
make  && make install
```

2. 安装python3.7.3
```
wget https://www.python.org/ftp/python/3.7.3/Python-3.7.3.tar.xz -P /opt  
tar xvJf /opt/Python-3.7.3.tar.xz -C /opt  
cd /opt/Python-3.7.3  
./configure --enable-shared --prefix=/usr --with-openssl=/usr/  
make  && make install   
rm -rf /usr/bin/python  && ln -s /usr/bin/python3 /usr/bin/python  
```
3. 安装obs python sdk 
```
wget https://raw.githubusercontent.com/huaweicloud/huaweicloud-sdk-python-obs/master/release/huaweicloud-obs-sdk-python_3.19.5.zip -P /opt   
unzip /opt/huaweicloud-obs-sdk-python_3.19.5.zip -d /opt  
unzip /opt/huaweicloud-obs-sdk-python.zip -d /opt/huaweicloud-obs-sdk-python   
cd /opt/huaweicloud-obs-sdk-python/src    
python setup.py install  
```
4. 安装obssftp
```
pip install obssftp-xxx.tar.gz
```
5. 启动
```
systemctl start obssftp
```

#### 容器化安装

1. 构建docker image
```
docker build -t obssftp:0.1 -f Dockerfile.dist .
```
**说明**Dockerfile.dist请从源码src/dockerfile目录获取  

2. 启动docker
```
docker run -dit --name obssftp --privileged -p 2222:2222 obssftp:0.1
```
**说明** -p指定的端口为obssftp监听端口，请以实际为准
3. 进入docker容器，用于初始化配置  
```
docker exec -ti obssftp /bin/bash
```

### 初始化配置

1. 在obssftp运行的节点上添加OS帐号
```
useradd obssftp
passwd obssftp
```
2. 配置登录信息，指定sftp客户端登录帐号，以及此帐号关联的OBS信息
```
obssftp add auth --user=obssftp --obs_endpoint=xxxx, --ak=xxxxx, --sk=xxxxx  
```

3. 配置操作日志归档桶信息
```
obssftp change log --log_obs_endpoint=xxx --log_ak=xxx --log_sk=xxx
```

**说明** 其他配置请通过obssftp --help查看

用户指南
---------------

1. 通过SFTP客户端上传
```
# sftp -P 2222 obssftp@127.0.0.1
obssftp@127.0.0.1's password:
Connected to 127.0.0.1.
sftp> ls
sftp> put hello.txt
Uploading hello.txt to /sftp-test/hello.txt
sftp> get hello.txt
Fetching /sftp-test/hello.txt to hello.txt
sftp> ls
hello.txt
```
2. 通过代码上传（以python举例）
```
ssh=paramiko.Transport(('127.0.0.1',2222))
ssh.connect(username='obssftp', password='ChangeMe@123')
sftp=paramiko.SFTPClient.from_transport(ssh)
sftp.listdir('/')
sftp.get('/sftp-test/hello.txt', './hello.txt')
sftp.put('./hello.txt', '/sftp-test/hello.txt')
scp.close()
```

约束
---------------
- 不支持对桶进行put、rename、rm、rmdir、mkdir操作
- 只支持password鉴权，暂不支持public key方式
- 采用分段上传方式，默认分段大小10MB，最多支持10000个分段， 能支持的最大文件约90GB。 
- get remote local操作，当local存在同名文件或文件夹，会报错，但下载不成功(同SFTP原生实现保持一致)
