#!/usr/bin/env python
import os
import shutil
from subprocess import run, DEVNULL, STDOUT
from distutils import sysconfig
from setuptools.command.install import install as _install

PROJECT = 'obssftp'

VERSION = '3.19.5'
CONFIG_DIR = '/usr/etc/obssftp'

from setuptools import setup, find_packages, Command, command

try:
    long_description = open('README.rst', 'rt').read()
except IOError:
    long_description = ''

def install_systemd():
    systemd_service_path = os.path.abspath(os.path.join(os.curdir, 'obssftp', 'config', 'obssftp.service'))
    if run('systemctl status obssftp.service', shell=True, stdout=DEVNULL, stderr=STDOUT).returncode == 0:
        run('systemctl stop obssftp.service', shell=True, stdout=DEVNULL, stderr=STDOUT)

    unit_path = '/usr/lib/systemd/system/obssftp.service'
    unit_link_path = '/etc/systemd/system/multi-user.target.wants/obssftp.service'
    if os.path.exists(unit_path):
        print('remove %s' % unit_path)
        os.remove(unit_path)

    if os.path.exists(unit_link_path):
        print('remove %s' % unit_link_path)
        os.remove(unit_link_path)

    print('copying %s -> %s' % (systemd_service_path, 'usr/lib/systemd/system'))
    shutil.copy(systemd_service_path, '/usr/lib/systemd/system/')

    print('runing script [systemctl daemon-reload]')
    run('systemctl daemon-reload', shell=True)

    print('runing script [systemctl enable obssftp.service]')
    run('systemctl enable obssftp.service', shell=True)

def install_config():
    obssftp_lib = 'obssftp-' + VERSION + '-py3.7.egg'
    config_src_dir = os.path.join(sysconfig.get_python_lib(), 'obssftp', 'config' )
    if os.path.exists(CONFIG_DIR):
        print('rmtree %s' % CONFIG_DIR)
        shutil.rmtree(CONFIG_DIR)
    print('move %s -> %s' % (config_src_dir, CONFIG_DIR))
    shutil.move(config_src_dir, CONFIG_DIR)

def install_keygen():
    host_key_dir = os.path.join(CONFIG_DIR, '.ssh')
    host_key_path = os.path.join(host_key_dir, 'id_rsa')
    if os.path.exists(host_key_dir):
        print('rmtree %s' % host_key_dir)
        shutil.rmtree(host_key_dir)
        print('mkdir %s' % host_key_dir)
    os.makedirs(host_key_dir)

    print('generate ssh key file to %s' % host_key_dir)
    if run('ssh-keygen -f %s -N ""' % host_key_path, shell=True).returncode != 0:
        print('ssh-keygen fail, cp /root/.ssh/id_rsa -> %s' % host_key_path)
        shutil.copy('/root/.ssh/id_rsa', host_key_path)

def install_complete():
    complete_path = os.path.join('/etc/bash_completion.d/', 'obssftp_cli')
    print('generate obssftp complete to %s' % complete_path)
    run('obssftp complete > %s' % complete_path, shell=True)
    run('source %s' % complete_path, shell=True)


class upgrade(_install):
    def run(self):
        _install.run(self)
        self.execute(install_systemd, (), msg='running install systemd')
        self.execute(install_complete, (), msg='running install complete')
        return True

class install(_install):
    def run(self):
        _install.run(self)
        self.execute(install_config, (), msg='running install config')
        self.execute(install_keygen, (), msg='running generate ssh key file')
        self.execute(install_systemd, (), msg='running install systemd')
        self.execute(install_complete, (), msg='running install complete')
        return True

if __name__ == '__main__':
    setup(
        name=PROJECT,
        version=VERSION,

        description='cli for obssftp',
        long_description=long_description,

        author='',
        author_email='',

        url='',
        download_url='',

        classifiers=['Development Status :: 3 - Alpha',
                     'License :: OSI Approved :: Apache Software License',
                     'Programming Language :: Python',
                     'Programming Language :: Python :: 3',
                     'Programming Language :: Python :: 3.7',
                     'Intended Audience :: Developers',
                     'Environment :: Console',
                     ],

        platforms=['Linux'],

        scripts=[],

        provides=[],
        install_requires=['paramiko>=2.4', 'cliff>=2.14.1', 'pycrypto>=2.6.1'],
        namespace_packages=[],
        packages=find_packages(),
        include_package_data=True,
        entry_points={
            'console_scripts': [
                'obssftp = obssftp.cli:main'
            ],
            'obssftp': [
                'add auth = obssftp.cli:AddAuth',
                'del auth = obssftp.cli:DelAuth',
                'change auth = obssftp.cli:ChangeAuth',
                'change log= obssftp.cli:ChangeLog',
                'change state= obssftp.cli:ChangeState',
                'show auth = obssftp.cli:ShowAuth',
                'show log = obssftp.cli:ShowLog',
                'show state = obssftp.cli:ShowState',
                'show = obssftp.cli:Show',
                'start = obssftp.cli:Run',
            ],
        },
        cmdclass={'upgrade': upgrade, 'install': install},

    zip_safe=False,
    )
