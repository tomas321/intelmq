"""Processmanager implementations for IntelMQ
SPDX-FileCopyrightText: 2020 IntelMQ Team <intelmq-team@cert.at>
SPDX-License-Identifier: AGPL-3.0-or-later
"""
# pylint: disable=too-many-arguments
# pylint: disable=line-too-long
# pylint: disable=protected-access # This is due to access to members of  the controller; needs refactoring
# pylint: disable=unsubscriptable-object # 'Union' is unsubscriptable is a false negative
import abc
import distutils.version
import getpass
import http.client
import logging
import os
import signal
import socket
import subprocess
import sys
import time
import xmlrpc.client

from typing import Union, Iterable
from termstyle import green

from intelmq.lib.bot_debugger import BotDebugger
from intelmq.lib.exceptions import MissingDependencyError
from intelmq import VAR_RUN_PATH

try:
    import psutil
except ImportError:
    psutil = None

MESSAGES = {
    'enabled': 'Bot %s is enabled.',
    'disabled': 'Bot %s is disabled.',
    'starting': 'Starting %s...',
    'running': green('Bot %s is running.'),
    'stopped': 'Bot %s is stopped.',
    'stopping': 'Stopping bot %s...',
    'reloading': 'Reloading bot %s ...',
    'enabling': 'Enabling %s.',
    'disabling': 'Disabling %s.',
    'reloaded': 'Bot %s is reloaded.',
    'restarting': 'Restarting %s...',
}

ERROR_MESSAGES = {
    'starting': 'Bot %s failed to START.',
    'running': 'Bot %s is still running.',
    'stopped': 'Bot %s was NOT RUNNING.',
    'stopping': 'Bot %s failed to STOP.',
    'not found': ('Bot %s FAILED to start because the executable cannot be found. '
                  'Check your PATH variable and your the installation.'),
    'access denied': 'Bot %s failed to %s because of missing permissions.',
    'unknown': 'Status of Bot %s is unknown: %r.',
}


class ProcessManager(metaclass=abc.ABCMeta):
    """ An abstract class defining the interface for ProcessManager. """

    def __init__(self, runtime_configuration: dict, logger: logging.Logger, controller, return_type: str = "text", quiet=False) -> None:
        self.runtime_configuration = runtime_configuration
        self.logger = logger
        self.controller = controller
        self.return_type = return_type
        self.quiet = quiet

    @abc.abstractmethod
    def bot_run(self, bot_id: str, run_subcommand=None, console_type=None, message_action_kind=None, dryrun=None, msg=None,
                show_sent=None, loglevel=None):
        """Run the bot command."""
        raise NotImplementedError

    @abc.abstractmethod
    def bot_start(self, bot_id: str, getstatus=True):
        """Start a bot."""
        raise NotImplementedError

    @abc.abstractmethod
    def bot_stop(self, bot_id: str, getstatus=True):
        """Stop a bot."""
        raise NotImplementedError

    @abc.abstractmethod
    def bot_reload(self, bot_id: str, getstatus=True):
        """Reload a bot."""
        raise NotImplementedError

    @abc.abstractmethod
    def bot_status(self, bot_id: str, *args, **kwargs):
        """Get the status of a bot."""
        raise NotImplementedError

    def log_bot_error(self, status: str, *args):
        """Log an error."""
        if self.return_type == 'text':
            self.logger.error(ERROR_MESSAGES[status], *args)

    def log_bot_message(self, status: str, *args):
        """Log some information."""
        if self.quiet:
            return
        if self.return_type == 'text':
            self.logger.info(MESSAGES[status], *args)


class IntelMQProcessManager(ProcessManager):
    """ The IntelMQ Process Manager """
    PIDDIR = VAR_RUN_PATH
    PIDFILE = os.path.join(PIDDIR, "{}.pid")

    def __init__(self, runtime_configuration, logger, controller):
        if psutil is None:
            raise MissingDependencyError('psutil')

        if not os.path.exists(self.PIDDIR):
            try:
                os.makedirs(self.PIDDIR)
            except PermissionError as exc:  # pragma: no cover
                logger.error('Directory %s does not exist and cannot be '
                             'created: %s.', self.PIDDIR, exc)
        super().__init__(runtime_configuration, logger, controller)

    def bot_run(self, bot_id, run_subcommand=None, console_type=None, message_action_kind=None, dryrun=None, msg=None,
                show_sent=None, loglevel=None):
        """ Run a bot directly """
        pid = self.__check_pid(bot_id)
        module = self.runtime_configuration[bot_id]['module']
        status = self.__status_process(pid, module, bot_id) if pid else False
        if pid and status is True:
            self.logger.info("Main instance of the bot is running in the background and will be stopped; "
                             "when finished, we try to relaunch it again. "
                             "You may want to launch: 'intelmqctl stop {}' to prevent this message."
                             .format(bot_id))
            paused = True
            self.bot_stop(bot_id)
        elif status is False:
            paused = False
        else:
            self.logger.error(status)
            return 1

        self.log_bot_message('starting', bot_id)
        filename = self.PIDFILE.format(bot_id)
        with open(filename, 'w') as fio:
            fio.write(str(os.getpid()))

        output = ""
        try:
            botd = BotDebugger(self.runtime_configuration[bot_id], bot_id, run_subcommand,
                               console_type, message_action_kind, dryrun, msg, show_sent,
                               loglevel=loglevel)
            output = botd.run()
            retval = 0
        except KeyboardInterrupt:
            print('Keyboard interrupt.')
            retval = 0
        except SystemExit as exc:
            print('Bot exited with code %s.' % exc.code)
            retval = exc.code

        self.__remove_pidfile(bot_id)
        if paused:
            self.bot_start(bot_id)

        return retval, output

    def bot_start(self, bot_id, getstatus=True):
        """ Start the bot. """
        pid = self.__check_pid(bot_id)
        module = self.runtime_configuration[bot_id]['module']
        if pid:
            status = self.__status_process(pid, module, bot_id)
            if status is True:
                self.log_bot_message('running', bot_id)
                return 'running'
            if status is False:
                self.__remove_pidfile(bot_id)
            else:
                self.logger.error(status)
                return 'error'

        self.log_bot_message('starting', bot_id)
        module = self.runtime_configuration[bot_id]['module']
        cmdargs = [module, bot_id]
        try:
            proc = psutil.Popen(cmdargs, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except FileNotFoundError:
            self.log_bot_error("not found", bot_id)
            return 'stopped'
        else:
            filename = self.PIDFILE.format(bot_id)
            with open(filename, 'w') as fio:
                fio.write(str(proc.pid))

        if getstatus:
            time.sleep(0.5)
            return self.bot_status(bot_id, proc=proc)

    def bot_stop(self, bot_id, getstatus=True):
        """ Stop the bot process."""
        pid = self.__check_pid(bot_id)
        module = self.runtime_configuration[bot_id]['module']
        retval = ""
        if not pid:
            if self.controller._is_enabled(bot_id):
                self.log_bot_error('stopped', bot_id)
                retval = 'stopped'
            else:
                self.log_bot_message('disabled', bot_id)
                retval = 'disabled'
        else:
            status = self.__status_process(pid, module, bot_id)
            if status is False:
                self.__remove_pidfile(bot_id)
                self.log_bot_error('stopped', bot_id)
                retval = 'stopped'
            elif status is not True:
                self.log_bot_error('unknown', bot_id, status)
                retval = 'unknown'
            self.log_bot_message('stopping', bot_id)
            proc = psutil.Process(int(pid))
            try:
                proc.send_signal(signal.SIGTERM)
            except psutil.AccessDenied:
                self.log_bot_error('access denied', bot_id, 'STOP')
                retval = 'running'
            else:
                if getstatus:
                    # Wait for up to 2 seconds until the bot stops, #1434
                    starttime = time.time()
                    remaining = 2
                    status = self.__status_process(pid, module, bot_id)
                    while status is True and remaining > 0:
                        status = self.__status_process(pid, module, bot_id)
                        time.sleep(0.1)
                        remaining = 2 - (time.time() - starttime)

                    if status is True:
                        self.log_bot_error('running', bot_id)
                        retval = 'running'
                    elif status is not False:
                        self.log_bot_error('unknown', bot_id, status)
                        retval = 'unknown'
                    try:
                        self.__remove_pidfile(bot_id)
                    except FileNotFoundError:  # Bot was running interactively and file has been removed already
                        pass
                    self.log_bot_message('stopped', bot_id)
                    retval = 'stopped'
            return retval

    def bot_reload(self, bot_id, getstatus=True):
        """ Reload the bot process. """
        pid = self.__check_pid(bot_id)
        module = self.runtime_configuration[bot_id]['module']
        retval = ""
        if not pid:
            if self.controller._is_enabled(bot_id):
                self.log_bot_error('stopped', bot_id)
                retval = 'stopped'
            else:
                self.log_bot_message('disabled', bot_id)
                retval = 'disabled'
        else:
            status = self.__status_process(pid, module, bot_id)
            if status is False:
                self.__remove_pidfile(bot_id)
                self.log_bot_error('stopped', bot_id)
                retval = 'stopped'
            elif status is not True:
                self.log_bot_error('unknown', bot_id, status)
                retval = 'unknown'
            else:
                self.log_bot_message('reloading', bot_id)
                proc = psutil.Process(int(pid))
                try:
                    proc.send_signal(signal.SIGHUP)
                except psutil.AccessDenied:
                    self.log_bot_error('access denied', bot_id, 'RELOAD')
                    retval = 'running'
                else:
                    if getstatus:
                        time.sleep(0.5)
                        status = self.__status_process(pid, module, bot_id)
                        if status is True:
                            self.log_bot_message('running', bot_id)
                            retval = 'running'
                        elif status is False:
                            self.log_bot_error('stopped', bot_id)
                            retval = 'stopped'
                        else:
                            self.log_bot_error('unknown', bot_id, status)
                            retval = 'unknown'
        return retval

    def bot_status(self, bot_id: str, *args, **kwargs):
        proc = kwargs.get('proc', None)
        if proc:
            if proc.status() not in [psutil.STATUS_STOPPED, psutil.STATUS_DEAD, psutil.STATUS_ZOMBIE]:
                self.log_bot_message('running', bot_id)
                return 'running'
        else:
            pid = self.__check_pid(bot_id)
            module = self.runtime_configuration[bot_id]['module']
            status = self.__status_process(pid, module, bot_id) if pid else False
            if pid and status is True:
                self.log_bot_message('running', bot_id)
                return 'running'
            if status is not False:
                self.log_bot_error('unknown', bot_id, status)
                return 'unknown'

        if self.controller._is_enabled(bot_id):
            if not proc and pid:
                self.__remove_pidfile(bot_id)
            self.log_bot_message('stopped', bot_id)
            if proc and self.return_type == 'text':
                log = proc.stderr.read().decode()
                if not log:  # if nothing in stderr, print stdout
                    log = proc.stdout.read().decode()
                print(log.strip(), file=sys.stderr)
            return 'stopped'
        self.log_bot_message('disabled', bot_id)
        return 'disabled'

    def __check_pid(self, bot_id):
        filename = self.PIDFILE.format(bot_id)
        if os.path.isfile(filename):
            with open(filename, 'r') as fio:
                pid = fio.read()
            try:
                return int(pid.strip())
            except ValueError:
                return None
        return None

    def __remove_pidfile(self, bot_id):
        filename = self.PIDFILE.format(bot_id)
        os.remove(filename)

    @staticmethod
    def _interpret_commandline(pid: int, cmdline: Iterable[str],
                               module: str, bot_id: str) -> Union[bool, str]:
        """
        Separate function to allow easy testing

        Parameters
        ----------
        pid : int
            Process ID, used for return values (error messages) only.
        cmdline : Iterable[str]
            The command line of the process.
        module : str
            The module of the bot.
        bot_id : str
            The ID of the bot.

        Returns
        -------
        Union[bool, str]
            DESCRIPTION.
        """
        if len(cmdline) > 2 and cmdline[1].endswith('/%s' % module):
            return cmdline[2] == bot_id
        if (len(cmdline) > 3 and cmdline[1].endswith('/intelmqctl') and cmdline[2] == 'run'):
            return cmdline[3] == bot_id
        if len(cmdline) > 1:
            return 'Commandline of the process %d with commandline %r could not be interpreted.' % (pid, cmdline)
        return 'Unhandled error checking the process %d with commandline %r.' % (pid, cmdline)

    @classmethod
    def __status_process(cls, pid, module, bot_id):
        try:
            proc = psutil.Process(int(pid))
            cmdline = proc.cmdline()
            return IntelMQProcessManager._interpret_commandline(pid, cmdline, module, bot_id)
        except psutil.NoSuchProcess:
            return False
        except psutil.AccessDenied:
            return 'Could not get status of process: Access denied.'
        # let every other exception pass


class SupervisorProcessManager(ProcessManager):
    """ The Supervisor Process Manager """
    RpcFaults = {
        'UNKNOWN_METHOD': 1,
        'INCORRECT_PARAMETERS': 2,
        'BAD_ARGUMENTS': 3,
        'SIGNATURE_UNSUPPORTED': 4,
        'SHUTDOWN_STATE': 6,
        'BAD_NAME': 10,
        'BAD_SIGNAL': 11,
        'NO_FILE': 20,
        'NOT_EXECUTABLE': 21,
        'FAILED': 30,
        'ABNORMAL_TERMINATION': 40,
        'SPAWN_ERROR': 50,
        'ALREADY_STARTED': 60,
        'NOT_RUNNING': 70,
        'SUCCESS': 80,
        'ALREADY_ADDED': 90,
        'STILL_RUNNING': 91,
        'CANT_REREAD': 92}

    ProcessState = {
        'STOPPED': 0,
        'STARTING': 10,
        'RUNNING': 20,
        'BACKOFF': 30,
        'STOPPING': 40,
        'EXITED': 100,
        'FATAL': 200,
        'UNKNOWN': 1000}

    def is_running(self, state: int) -> bool:
        """ Check if the state is defined as 'running'. """
        return state in [
            self.ProcessState['STARTING'],
            self.ProcessState['RUNNING'],
            self.ProcessState['BACKOFF']]

    DEFAULT_SOCKET_PATH = "/var/run/supervisor.sock"
    SUPERVISOR_GROUP = "intelmq"
    __supervisor_xmlrpc = None

    def bot_run(self, bot_id, run_subcommand=None, console_type=None, message_action_kind=None, dryrun=None, msg=None,
                show_sent=None, loglevel=None):
        """ Run a bot directly """
        paused = False
        state = self._get_process_state(bot_id)
        if state in (self.ProcessState['STARTING'], self.ProcessState['RUNNING'], self.ProcessState['BACKOFF']):
            self.logger.warning("Main instance of the bot is running in the background and will be stopped; "
                                "when finished, we try to relaunch it again. "
                                "You may want to launch: 'intelmqctl stop {}' to prevent this message."
                                .format(bot_id))
            paused = True
            self.bot_stop(bot_id)

        self.log_bot_message("starting", bot_id)

        output = ""
        try:
            botd = BotDebugger(self.runtime_configuration[bot_id], bot_id, run_subcommand,
                               console_type, message_action_kind, dryrun, msg, show_sent,
                               loglevel=loglevel)
            output = botd.run()
            retval = 0
        except KeyboardInterrupt:
            print("Keyboard interrupt.")
            retval = 0
        except SystemExit as exc:
            print("Bot exited with code %s." % exc.code)
            retval = exc.code

        if paused:
            self.bot_start(bot_id)

        return retval, output

    def bot_start(self, bot_id: str, getstatus: bool = True):
        """ Start the bot. """
        state = self._get_process_state(bot_id)
        retval = ""
        if state is not None:
            if state == self.ProcessState['RUNNING']:
                self.log_bot_message("running", bot_id)
                retval = "running"

            elif not self.is_running(state):
                self._remove_bot(bot_id)

        self.log_bot_message("starting", bot_id)
        self._create_and_start_bot(bot_id)

        if getstatus:
            retval = self.bot_status(bot_id)
        return retval

    def bot_stop(self, bot_id: str, getstatus: bool = True):
        """ Stop the bot process."""
        state = self._get_process_state(bot_id)
        retval = ""
        if state is None:
            if not self.controller._is_enabled(bot_id):
                self.log_bot_message("disabled", bot_id)
                retval = "disabled"
            else:
                self.log_bot_error("stopped", bot_id)
                retval = "stopped"
        else:
            if not self.is_running(state):
                self._remove_bot(bot_id)
                self.log_bot_error("stopped", bot_id)
                retval = "stopped"

            self.log_bot_message("stopping", bot_id)

            self._get_supervisor().supervisor.stopProcess(self._process_name(bot_id))
            self._remove_bot(bot_id)

            if getstatus:
                retval = self.bot_status(bot_id)
        return retval

    def bot_reload(self, bot_id: str, getstatus: bool = True):
        """ Reload the bot process. """
        state = self._get_process_state(bot_id)
        retval = ""
        if state is None:
            if not self.controller._is_enabled(bot_id):
                self.log_bot_message("disabled", bot_id)
                retval = "disabled"
            else:
                self.log_bot_error("stopped", bot_id)
                retval = "stopped"
        else:
            if not self.is_running(state):
                self._remove_bot(bot_id)
                self.log_bot_error("stopped", bot_id)
                retval = "stopped"

            self.log_bot_message("reloading", bot_id)

            try:
                self._get_supervisor().supervisor.signalProcess(self._process_name(bot_id), "HUP")
            except xmlrpc.client.Fault as exc:
                if exc.faultCode == self.RpcFaults['UNKNOWN_METHOD']:
                    self._abort("Supervisor does not support signalProcess method, that was added in supervisor 3.2.0. "
                                "Reloading bots will not work.")
                else:
                    raise exc

            if getstatus:
                retval = self.bot_status(bot_id)
        return retval

    def bot_status(self, bot_id: str, *args, **kwargs) -> str:
        state = self._get_process_state(bot_id)
        retval = ""
        if state is None:
            if not self.controller._is_enabled(bot_id):
                self.log_bot_message("disabled", bot_id)
                retval = "disabled"
            else:
                self.log_bot_message("stopped", bot_id)
                retval = "stopped"
        else:
            if state == self.ProcessState['STARTING']:
                # If process is still starting, try check it later
                time.sleep(0.1)
                retval = self.bot_status(bot_id)

            elif state == self.ProcessState['RUNNING']:
                self.log_bot_message("running", bot_id)
                retval = "running"

            elif state == self.ProcessState['STOPPING']:
                self.log_bot_error("stopping", bot_id)
                retval = "stopping"

            else:
                self.log_bot_message("stopped", bot_id)
                retval = "stopped"
        return retval

    def _create_and_start_bot(self, bot_id: str) -> None:
        module = self.runtime_configuration[bot_id]["module"]
        cmdargs = (module, bot_id)

        self._get_supervisor().twiddler.addProgramToGroup(self.SUPERVISOR_GROUP, bot_id, {
            "command": " ".join(cmdargs),
            "stopsignal": "INT",
        })

    def _remove_bot(self, bot_id: str) -> None:
        self._get_supervisor().twiddler.removeProcessFromGroup(self.SUPERVISOR_GROUP, bot_id)

    def _get_process_state(self, bot_id: str):
        try:
            return self._get_supervisor().supervisor.getProcessInfo(self._process_name(bot_id))["state"]
        except xmlrpc.client.Fault as exc:
            if exc.faultCode == self.RpcFaults['BAD_NAME']:  # Process does not exists
                return None
            raise

    def _get_supervisor(self) -> xmlrpc.client.ServerProxy:
        class UnixStreamHTTPConnection(http.client.HTTPConnection):  # pylint: disable=C0115
            def connect(self):
                self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                self.sock.connect(self.host)

        class UnixStreamTransport(xmlrpc.client.Transport):  # pylint: disable=C0115
            def __init__(self, socket_path):
                self.socket_path = socket_path
                super().__init__()

            def make_connection(self, host):
                return UnixStreamHTTPConnection(self.socket_path)

        if not self.__supervisor_xmlrpc:
            socket_path = os.environ.get("SUPERVISOR_SOCKET", self.DEFAULT_SOCKET_PATH)

            if not os.path.exists(socket_path):
                self._abort("Socket '{}' does not exists. Is supervisor running?".format(socket_path))

            if not os.access(socket_path, os.W_OK):
                current_user = getpass.getuser()
                self._abort("Socket '{}' is not writable. "
                            "Has user '{}' write permission?".format(socket_path, current_user))

            self.__supervisor_xmlrpc = xmlrpc.client.ServerProxy(
                "http://none",
                transport=UnixStreamTransport(socket_path)
            )

            supervisor_version = self.__supervisor_xmlrpc.supervisor.getSupervisorVersion()
            self.logger.debug("Connected to supervisor {} named '{}' (API version {})".format(
                supervisor_version,
                self.__supervisor_xmlrpc.supervisor.getIdentification(),
                self.__supervisor_xmlrpc.supervisor.getAPIVersion()
            ))

            if distutils.version.StrictVersion(supervisor_version) < distutils.version.StrictVersion("3.2.0"):
                self.logger.warning("Current supervisor version is supported, but reloading bots will not work. "
                                    "Please upgrade supervisor to version 3.2.0 or higher.")

            supervisor_state = self.__supervisor_xmlrpc.supervisor.getState()["statename"]
            if supervisor_state != "RUNNING":
                raise Exception("Unexpected supervisor state {}".format(supervisor_state))

            try:
                self.__supervisor_xmlrpc.twiddler.getAPIVersion()
            except xmlrpc.client.Fault as exc:
                if exc.faultCode == self.RpcFaults['UNKNOWN_METHOD']:
                    self._abort("Twiddler is not supported. Is Twiddler for supervisor installed and enabled?")
                else:
                    raise exc

            if self.SUPERVISOR_GROUP not in self.__supervisor_xmlrpc.twiddler.getGroupNames():
                self._abort("Supervisor`s process group '{}' is not defined. "
                            "It must be created manually in supervisor config.".format(self.SUPERVISOR_GROUP))

        return self.__supervisor_xmlrpc

    def _process_name(self, bot_id: str) -> str:
        return "{}:{}".format(self.SUPERVISOR_GROUP, bot_id)

    def _abort(self, message: str):
        self.controller.abort(message)
