import asyncio
import json
import subprocess
import time

import annet.annlib.command

from annet.deploy import Fetcher, DeployDriver, DeployOptions, DeployResult, apply_deploy_rulebook
from annet.annlib.command import Command, CommandList
from annet.annlib.netdev.views.hardware import HardwareView
from annet.adapters.netbox.common.models import NetboxDevice
from annet.rulebook import common

from annet.connectors import AdapterWithConfig, AdapterWithName
from typing import Dict, List, Any, Optional, Tuple
from annet.storage import Device
from gnetclisdk.client import Credentials, Gnetcli, HostParams, QA, File
import gnetclisdk.proto.server_pb2 as pb
from pydantic import Field, field_validator, FieldValidationInfo
from pydantic_core import PydanticUndefined
from pydantic_settings import BaseSettings, SettingsConfigDict
import base64
import logging
import threading
import atexit
import shutil
import os.path

breed_to_device = {
    "routeros": "ros",
    "ios12": "cisco",
    "pc": "pc",
    "cuml2": "pc",
    "jun10": "juniper",
    "eos4": "arista",
    "h3c": "h3c",
    "vrp85": "huawei",
    "vrp55": "huawei",
}

DEFAULT_GNETCLI_SERVER_CONF = """
logging:
  level: debug
  json: true
dev_use_agent: true
port: 0

"""

_local_gnetcli: Optional[threading.Thread] = None
_local_gnetcli_p: Optional[subprocess.Popen] = None
_local_gnetcli_url: Optional[str] = None
LOG_FORMAT = "%(asctime)s - l:%(lineno)d - %(funcName)s() - %(levelname)s - %(message)s"
DATE_FMT = "%Y-%m-%d %H:%M:%S"
DEFAULT_GNETCLI_SERVER_PATH = "gnetcli_server"
_logger = logging.getLogger(__name__)


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="gnetcli_", validate_assignment=True)

    url: Optional[str] = None
    server_path: str = DEFAULT_GNETCLI_SERVER_PATH
    server_conf: str = DEFAULT_GNETCLI_SERVER_CONF
    insecure_grpc: bool = Field(default=True)
    login: Optional[str] = None
    password: Optional[str] = None
    dev_login: Optional[str] = None
    dev_password: Optional[str] = None
    ssh_agent_enabled: bool = True

    def make_dev_credentials(self) -> Optional[Credentials]:
        if not self.dev_login and not self.dev_password:
            return None
        return Credentials(self.dev_login, self.dev_password)

    def make_server_credentials(self) -> Optional[str]:
        if self.login and self.password:
            auth_token = base64.b64encode(b"%s:%s" % (self.login.encode(), self.password.encode())).strip().decode()
            return f"Basic {auth_token}"
        return None

    @field_validator("*", mode="before")
    @classmethod
    def not_none(cls, value: Any, info: FieldValidationInfo):
        # NOTE: All fields that are optional for values, will assume the value in
        # "default" (if defined in "Field") if "None" is informed as "value". That
        # is, "None" is never assumed if passed as a "value".
        if (
            cls.model_fields[info.field_name].get_default() is not PydanticUndefined
            and not cls.model_fields[info.field_name].is_required()
            and value is None
        ):
            return cls.model_fields[info.field_name].get_default()
        return value


async def get_config(breed: str) -> List[str]:
    if breed == "routeros":
        return ["/export verbose", "/user export verbose"]
    elif breed.startswith("ios"):
        return ["show running-config"]
    elif breed.startswith("jun"):
        return ["show configuration"]
    elif breed.startswith("eos4"):
        return ["show running-config | no-more"]
    elif breed.startswith(("h3c", "vrp")):
        return ["display current-configuration"]
    raise Exception("unknown breed %r" % breed)


def check_gnetcli_server(server_path: str, config: str = DEFAULT_GNETCLI_SERVER_CONF):
    global _local_gnetcli
    if not _local_gnetcli:
        t = threading.Thread(target=run_gnetcli_server,
                             kwargs={"server_path": server_path, "config": config})
        t.daemon = True
        t.start()
        time.sleep(1)
        _local_gnetcli = t
    if _local_gnetcli_p is None:
        raise Exception("server failed")


def cleanup():
    if _local_gnetcli_p is not None:
        _local_gnetcli_p.kill()


atexit.register(cleanup)


def get_device_ip(dev: Device) -> Optional[str]:
    if isinstance(dev, NetboxDevice):
        if dev.primary_ip:
            return dev.primary_ip.address.split("/")[0]
        for iface in dev.interfaces:
            for ip in iface.ip_addresses:
                return ip.address.split("/")[0]
    return None


def run_gnetcli_server(server_path: str, config: str = DEFAULT_GNETCLI_SERVER_CONF):
    global _local_gnetcli_p
    global _local_gnetcli_url
    abs_path: Optional[str] = shutil.which(server_path)
    if not abs_path and server_path == DEFAULT_GNETCLI_SERVER_PATH:
        abs_path = shutil.which(os.path.expanduser("~/go/bin/gnetcli_server"))
    gnetcli_abs_path: str = abs_path or server_path
    _logger.info("starting gnetcli server %s", gnetcli_abs_path)
    try:
        proc = subprocess.Popen(
            [gnetcli_abs_path, "--conf-file", "-"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
            bufsize=1,
            universal_newlines=True,
        )
    except Exception as e:
        logging.exception("server exec error %s", e)
        raise
    proc.stdin.write(config)
    proc.stdin.close()
    _local_gnetcli_p = proc
    while True:
        output = proc.stderr.readline()
        if output == "" and proc.poll() is not None:
            break
        if output:
            _logger.debug("gnetcli output: %s", output.strip())
            if _local_gnetcli_url is None:
                try:
                    data = json.loads(output)
                except Exception:
                    pass
                else:
                    if data.get("msg") == "init tcp socket":
                        _local_gnetcli_url = data.get("address")
                    if data.get("level") == "panic":
                        _logger.error("gnetcli error %s", data)
    _logger.debug("set gnetcli server exit code %s", proc.returncode)
    rc = proc.poll()
    return rc


class GnetcliFetcher(Fetcher, AdapterWithConfig, AdapterWithName):
    def __init__(
        self,
        url: Optional[str] = None,
        login: Optional[str] = None,
        password: Optional[str] = None,
        dev_login: Optional[str] = None,
        dev_password: Optional[str] = None,
        ssh_agent_enabled: bool = True,
        server_path: Optional[str] = None,
        server_conf: str = DEFAULT_GNETCLI_SERVER_CONF,
    ):
        conf_args = {
            "login": login,
            "password": password,
            "dev_login": dev_login,
            "dev_password": dev_password,
            "server_path": server_path,
            "url": url,
            "server_conf": server_conf,
            "ssh_agent_enabled": ssh_agent_enabled,
        }
        self.conf = AppSettings(**{k: v for k,v in conf_args.items() if v is not None})
        self.api = make_api(self.conf)

    @classmethod
    def name(cls) -> str:
        return "gnetcli"

    @classmethod
    def with_config(cls, **kwargs: Dict[str, Any]) -> Fetcher:
        return cls(**kwargs)

    def fetch_packages(self, devices: List[Device], processes: int = 1, max_slots: int = 0):
        if not devices:
            return {}, {}
        # TODO: implement fetch_packages
        return {}, {}

    def fetch(
        self,
        devices: List[Device],
        files_to_download: Optional[Dict[Device, List[str]]] = None,
        processes: int = 1,
        max_slots: int = 0,
    ) -> Tuple[Dict[Device, str], Dict[Device, Any]]:
        return asyncio.run(self.afetch(devices=devices, files_to_download=files_to_download))

    async def afetch(self, devices: List[Device], files_to_download: Optional[Dict[Device, List[str]]] = None):
        running = {}
        failed_running = {}
        for device in devices:
            try:
                if files_to_download or device.is_pc():
                    if files_to_download:
                        files =files_to_download.get(device, [])
                        dev_res = await self.adownload_dev(device=device, files=files)
                    else:
                        dev_res = {}
                else:
                    dev_res = await self.afetch_dev(device=device)
            except Exception as e:
                failed_running[device] = e
            else:
                running[device] = dev_res
        return running, failed_running

    async def afetch_dev(self, device: Device) -> str:
        cmds = await get_config(breed=device.breed)
        # map annet breed to gnetcli device type
        gnetcli_device = breed_to_device.get(device.breed, device.breed)
        dev_result = []
        ip = get_device_ip(device)
        for cmd in cmds:
            res = await self.api.cmd(
                hostname=device.fqdn,
                cmd=cmd,
                host_params=HostParams(
                    credentials=self.conf.make_dev_credentials(),
                    device=gnetcli_device,
                    ip=ip,
                ),
            )
            if res.status != 0:
                raise Exception("cmd error %s" % res)
            dev_result.append(res.out)
        return b"\n".join(dev_result).decode()

    async def adownload_dev(self, device: Device, files: List[str]) -> Dict[str, str]:
        ip = get_device_ip(device)
        downloaded = await self.api.download(
            hostname=device.fqdn,
            paths=files,
            host_params=HostParams(
                credentials=self.conf.make_dev_credentials(),
                device="pc",
                ip=ip,
            ),
        )
        res: Dict[str, str] = {}
        for file, file_data in downloaded.items():
            res[file] = file_data.content.decode()
        return res


def parse_annet_qa(qa: list[annet.annlib.command.Question]) -> list[QA]:
    res: list[QA] = []
    for annet_qa in qa:
        q = annet_qa.question
        if annet_qa.is_regexp:
            q = f"/{annet_qa.question}/"
        res.append(QA(question=q, answer=annet_qa.answer))
    return res


def make_api(conf: AppSettings) -> Gnetcli:
    if not conf.url:
        check_gnetcli_server(server_path=conf.server_path, config=conf.server_conf)
        if _local_gnetcli_url is None:
            _logger.info("waiting for _local_gnetcli_url appears")
            start = time.monotonic()
            while time.monotonic() - start < 5:
                if _local_gnetcli_p is not None and _local_gnetcli_p.returncode is not None:
                    raise Exception("gnetcli server died with code %s" % _local_gnetcli_p.returncode)

    auth_token = conf.make_server_credentials()
    api = Gnetcli(
        server=_local_gnetcli_url,
        auth_token=auth_token,
        insecure_grpc=conf.insecure_grpc,
        user_agent="annet",
    )
    return api


class GnetcliDeployer(DeployDriver, AdapterWithConfig, AdapterWithName):
    def __init__(
        self,
        url: Optional[str] = None,
        login: Optional[str] = None,
        password: Optional[str] = None,
        dev_login: Optional[str] = None,
        dev_password: Optional[str] = None,
        ssh_agent_enabled: bool = True,
        server_path: Optional[str] = None,
        server_conf: Optional[str] = None,
    ):
        conf_args = {
            "login": login,
            "password": password,
            "dev_login": dev_login,
            "dev_password": dev_password,
            "server_path": server_path,
            "url": url,
            "server_conf": server_conf,
            "ssh_agent_enabled": ssh_agent_enabled,
        }
        self.conf = AppSettings(**{k: v for k,v in conf_args.items() if v is not None})
        self.api = make_api(self.conf)

    @classmethod
    def name(cls) -> str:
        return "gnetcli"

    @classmethod
    def with_config(cls, **kwargs: Dict[str, Any]) -> DeployDriver:
        return GnetcliDeployer(**kwargs)

    async def bulk_deploy(self, deploy_cmds: Dict[Device, CommandList], args: DeployOptions) -> DeployResult:
        deploy_items = deploy_cmds.items()
        result = await asyncio.gather(*[asyncio.Task(self.deploy(device, cmds, args)) for device, cmds in deploy_items])
        res = DeployResult(hostnames=[], results={}, durations={}, original_states={})
        res.add_results(results={dev.fqdn: dev_res for (dev, _), dev_res in zip(deploy_items, result)})
        return res

    async def deploy(self, device: Device, cmds: CommandList, args: DeployOptions) -> List[pb.CMDResult]:
        gnetcli_device = breed_to_device.get(device.breed, device.breed)
        ip = get_device_ip(device)
        host_params = HostParams(
            credentials=self.conf.make_dev_credentials(),
            device=gnetcli_device,
            ip=ip,
        )
        if isinstance(cmds, dict):
            run_cmds = CommandList(Command(cmd) for cmd in cmds["cmds"].values())
            files: Dict[str, File] = {file: File(content=content, status=None) for file, content in cmds["files"].items()}
            await self.api.upload(hostname=device.fqdn, files=files, host_params=host_params)
        else:
            run_cmds = cmds
        async with self.api.cmd_session(hostname=device.fqdn) as sess:
            result: List[pb.CMDResult] = []
            for cmd in run_cmds:
                res = await sess.cmd(
                    cmd=cmd.cmd,
                    cmd_timeout=cmd.timeout,
                    host_params=host_params,
                    qa=parse_annet_qa(cmd.questions or []),
                )
                if res.status != 0:
                    raise Exception("cmd %s error %s status %s", cmd, res.error, res.status)
                result.append(res)
            return result

    def apply_deploy_rulebook(
        self,
        hw: HardwareView,
        cmd_paths: Dict[Tuple[str, ...], Dict[str, Any]],
        do_finalize: bool = True,
        do_commit: bool = True,
    ):
        res = apply_deploy_rulebook(hw=hw, cmd_paths=cmd_paths, do_finalize=do_finalize, do_commit=do_commit)
        return res

    def build_configuration_cmdlist(self, hw: HardwareView, do_finalize: bool = True, do_commit: bool = True):
        res = common.apply(hw, do_commit=do_commit, do_finalize=do_finalize)
        return res

    def build_exit_cmdlist(self, hw: HardwareView) -> CommandList:
        ret = CommandList()
        if hw.Huawei or hw.H3C:
            ret.add_cmd(Command("quit", suppress_eof=True))
        return ret
