#  Licensed under the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License. You may obtain
#  a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#  License for the specific language governing permissions and limitations
#  under the License.

#! /bin/python
# -*- coding:utf-8 -*-
import sys
from cliff.show import ShowOne
from cliff.command import Command
from cliff.app import App
from cliff.commandmanager import CommandManager
import re

from obssftp import config, obs_sftp, logmgr

log = logmgr.getLogger(__name__)

class ShowAuth(ShowOne):
    """Show Auth"""

    def get_parser(self, prog_name):
        try:
            parser = super(ShowAuth, self).get_parser(prog_name)
            return parser
        except Exception as e:
            log.exception(e)

    def take_action(self, parsed_args):
        try:
            cfg = config.getConfig()
            return (cfg.auth.keys(), cfg.auth.values())
        except Exception as e:
            log.exception(e)


class ShowLog(ShowOne):
    """Show log"""

    def get_parser(self, prog_name):
        try:
            parser = super(ShowLog, self).get_parser(prog_name)
            return parser
        except Exception as e:
            log.exception(e)

    def take_action(self, parsed_args):
        try:
            cfg = config.getConfig()
            return (cfg.log.keys(), cfg.log.values())
        except Exception as e:
            log.exception(e)


class ShowState(ShowOne):
    """show state"""

    def get_parser(self, prog_name):
        try:
            parser = super(ShowState, self).get_parser(prog_name)
            return parser
        except Exception as e:
            log.exception(e)

    def take_action(self, parsed_args):
        try:
            cfg = config.getConfig()
            return (cfg.state.keys(), cfg.state.values())
        except Exception as e:
            log.exception(e)


class Show(ShowOne):
    """show all"""

    def get_parser(self, prog_name):
        try:
            parser = super(Show, self).get_parser(prog_name)
            parser.add_argument(
                '--type', '-t',
                choices=['all', 'auth', 'log', 'state'], dest='type',
                nargs='?', default='all')
            return parser
        except Exception as e:
            log.exception(e)

    def take_action(self, parsed_args):
        try:
            cfg = config.getConfig()
            if parsed_args.type == 'all':
                return (
                    tuple(cfg.auth.keys()) +
                    tuple(cfg.log.keys()) +
                    tuple(cfg.state.keys()),
                    tuple(cfg.auth.values()) +
                    tuple(cfg.log.values()) +
                    tuple(cfg.state.values()))
            if parsed_args.type == 'auth':
                return (cfg.auth.keys(), cfg.auth.values())
            if parsed_args.type == 'log':
                return (cfg.log.keys(), cfg.log.values())
            if parsed_args.type == 'state':
                return (cfg.state.keys(), cfg.state.values())
            return False
        except Exception as e:
            log.exception(e)


class AddAuth(Command):
    """add auth"""

    def get_parser(self, prog_name):
        try:
            parser = super(AddAuth, self).get_parser(prog_name)
            parser.add_argument('--user', dest='user', nargs='?', default=None)
            parser.add_argument('--auth_type', choices=['password'], dest='auth_type', nargs='?', default='')
            parser.add_argument('--obs_endpoint', dest='obs_endpoint', nargs='?', default=None)
            parser.add_argument('--ak',  dest='ak', nargs='?', default=None)
            parser.add_argument('--sk',  dest='sk', nargs='?', default=None)
            parser.add_argument('--perm', dest='perm', nargs='?', default='get,put')
            return parser
        except Exception as e:
            log.exception(e)

    def take_action(self, parsed_args):
        try:
            cfg = config.getConfig()
            if parsed_args.user in cfg.auth.keys():
                self.app.stdout.write(
                    'Add auth fail, user %s has exists!\n' %
                    parsed_args.user)
                log.error(
                    'Add auth fail, user %s has exists!' %
                    parsed_args.user)
                return 1
            ret, errMsg = cfg.AddAuth(
                user=parsed_args.user,
                auth_type=parsed_args.auth_type,
                obs_endpoint=parsed_args.obs_endpoint,
                ak=parsed_args.ak,
                sk=parsed_args.sk,
                perm=parsed_args.perm)
            if not ret:
                for msg in errMsg:
                    self.app.stderr.write(msg)
                    log.error(msg)
                return 1
            else:
                self.app.stdout.write('Add auth successfully!\n')
                log.info('Add auth successfully!')
                return 0
        except Exception as e:
            log.exception(e)


class DelAuth(Command):
    """del auth"""

    def get_parser(self, prog_name):
        try:
            parser = super(DelAuth, self).get_parser(prog_name)
            parser.add_argument(
                '--user',
                dest='user',
                nargs='?',
                default=None)
            return parser
        except Exception as e:
            log.exception(e)

    def take_action(self, parsed_args):
        try:
            cfg = config.getConfig()
            user = parsed_args.user
            if not user or user not in cfg.auth.keys():
                self.app.stderr.write('%s is invalid!\n' % user)
                log.error(('%s is invalid!' % user))
                return 1
            cfg.delAuth(user)
            return 0
        except Exception as e:
            log.exception(e)


class ChangeAuth(Command):
    """chang auth config"""

    def get_parser(self, prog_name):
        try:
            parser = super(ChangeAuth, self).get_parser(prog_name)
            cfg = config.getConfig()
            parser.add_argument('--user', dest='user', nargs='?', default=None)
            for key in list(cfg.auth.values())[0].keys():
                cmd_name = '--{}'.format(key)
                parser.add_argument(
                    cmd_name, dest=key, nargs='?', default=None)
            return parser
        except Exception as e:
            log.exception(e)

    def take_action(self, parsed_args):
        try:
            cfg = config.getConfig()
            if parsed_args.user not in cfg.auth.keys():
                self.app.stdout.write(
                    'user [%s] not exists! \n' %
                    parsed_args.user)
                log.error(('user [%s] not exists!' % parsed_args.user))
                return 1
            ret, errMsg = cfg.setAuth(
                parsed_args.__dict__.pop('user'),
                parsed_args.__dict__)
            if not ret:
                for msg in errMsg:
                    self.app.stderr.write(msg)
                    log.error(msg)
                return 1
            else:
                self.app.stdout.write('Change auth successfully!\n')
                log.info('Change auth successfully!')
                return 0
        except Exception as e:
            log.exception(e)


class ChangeLog(Command):
    """chang log config"""

    def get_parser(self, prog_name):
        try:
            parser = super(ChangeLog, self).get_parser(prog_name)
            cfg = config.getConfig()
            for key in cfg.log.keys():
                cmd_name = '--{}'.format(key)
                parser.add_argument(
                    cmd_name, dest=key, nargs='?', default=None)
            return parser
        except Exception as e:
            log.exception(e)

    def take_action(self, parsed_args):
        try:
            cfg = config.getConfig()
            ret, errMsg = cfg.setLog(parsed_args.__dict__)
            if not ret:
                for msg in errMsg:
                    self.app.stderr.write(msg)
                    log.error(msg)
                return 1
            else:
                self.app.stdout.write('Change log successfully!\n')
                log.info('Change log successfully!')
                return 0
        except Exception as e:
            log.exception(e)


class ChangeState(Command):
    """change state config"""

    def get_parser(self, prog_name):
        try:
            parser = super(ChangeState, self).get_parser(prog_name)
            cfg = config.getConfig()
            for key in cfg.state.keys():
                cmd_name = '--{}'.format(key)
                parser.add_argument(
                    cmd_name, dest=key, nargs='?', default=None)
            return parser
        except Exception as e:
            log.exception(e)

    def take_action(self, parsed_args):
        try:
            cfg = config.getConfig()
            ret, errMsg = cfg.setState(parsed_args.__dict__)
            if not ret:
                for msg in errMsg:
                    self.app.stderr.write(msg)
                    log.error(msg)
                return 1
            else:
                self.app.stdout.write('Change state successfully!\n')
                log.info('Change state successfully!')
                return 0
        except Exception as e:
            log.exception(e)


class Change(Command):
    """change all"""

    def get_parser(self, prog_name):
        parser = super(Change, self).get_parser(prog_name)
        #parser.add_argument('section', nargs='?', default='')
        parser.add_argument(
            '--type', '-t',
            choices=['auth', 'log', 'state'],
            dest='type',
            nargs='?',
            default='all')
        cfg = config.getConfig()
        for key in list(cfg.auth.values())[0].keys():
            cmd_name = '--{}'.format(key)
            parser.add_argument(cmd_name, dest=key, nargs='?', default='')
        for key in cfg.log.keys():
            cmd_name = '--{}'.format(key)
            parser.add_argument(cmd_name, dest=key, nargs='?', default='')
        for key in cfg.state.keys():
            cmd_name = '--{}'.format(key)
            parser.add_argument(cmd_name, dest=key, nargs='?', default='')
        return parser

    def take_action(self, parsed_args):
        log.info('sending greeting')
        log.debug('debugging')
        self.app.stdout.write('hi!\n')


class Run(Command):
    """run obssftp server"""

    def get_parser(self, prog_name):
        parser = super(Run, self).get_parser(prog_name)
        return parser

    def take_action(self, parsed_args):
        obs_sftp.main()


class CliDemoApp(App):

    def __init__(self):
        super(CliDemoApp, self).__init__(
            description='obssftp cli app',
            version='0.1',
            command_manager=CommandManager('obssftp'),
            deferred_help=True,
            )
        self.LOG = logmgr.getLogger()
        self.LOG.parent = None

    def initialize_app(self, argv):
        self.LOG.debug('initialize_app')
        cmd = ' '.join(argv)
        # 去除连续的空格
        cmd = re.sub(r'[ ]+', ' ', cmd).strip()
        # 去除=两边的空格
        cmd = re.sub(r'[  ]*=[  ]*', '=', cmd).strip()
        if not cmd.endswith(' '):
            cmd = '%s ' % cmd

        # 隐藏sk
        cmd = re.sub(r'--sk=.*? ', '--sk=****** ', cmd)
        cmd = re.sub(r'--sk .*? ', '--sk ****** ', cmd)
        self.LOG.info('cmd: %s' % cmd)

    def prepare_to_run_command(self, cmd):
        self.LOG.debug('prepare_to_run_command %s', cmd.__class__.__name__)

    def clean_up(self, cmd, result, err):
        self.LOG.debug('clean_up %s', cmd.__class__.__name__)
        if err:
            self.LOG.debug('got an error: %s', err)


def main(argv=sys.argv[1:]):
    myapp = CliDemoApp()
    return myapp.run(argv)
