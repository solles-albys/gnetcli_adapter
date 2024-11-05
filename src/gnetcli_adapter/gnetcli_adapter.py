import asyncio
import json
import subprocess
import time

import annet.annlib.command

from annet.deploy import DeployDriver, DeployOptions, DeployResult, apply_deploy_rulebook
from annet.annlib.command import Command, CommandList
from annet.annlib.netdev.views.hardware import HardwareView
from annet.adapters.netbox.common.models import NetboxDevice
from annet.rulebook import common

from annet.deploy import Fetcher
from annet.connectors import AdapterWithConfig, AdapterWithName
from typing import Dict, List, Any, Optional, Tuple
from annet.storage import Device
from gnetclisdk.client import Credentials, Gnetcli, HostParams, QA
import gnetclisdk.proto.server_pb2 as pb
from pydantic import Field, field_validator, FieldValidationInfo
from pydantic_core import PydanticUndefined
from pydantic_settings import BaseSettings, SettingsConfigDict
import base64
import logging
import threading
import atexit

breed_to_device = {
    "routeros": "ros",
    "ios12": "cisco",
}
_local_gnetcli: Optional[threading.Thread] = None
_local_gnetcli_p: Optional[subprocess.Popen] = None
_local_gnetcli_url: Optional[str] = None
LOG_FORMAT = "%(asctime)s - l:%(lineno)d - %(funcName)s() - %(levelname)s - %(message)s"
DATE_FMT = "%Y-%m-%d %H:%M:%S"
DEFAULT_GNETCLI_SERVER_PATH = "gnetcli_server"
_logger = logging.getLogger(__name__)


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="gnetcli_", validate_assignment=True)

    # url: Optional[str] = Field(default="localhost:50051")
    url: Optional[str] = None
    server_path: str = DEFAULT_GNETCLI_SERVER_PATH
    insecure_grpc: bool = Field(default=True)
    login: Optional[str] = None
    password: Optional[str] = None
    dev_login: Optional[str] = None
    dev_password: Optional[str] = None

    def make_dev_credentials(self) -> Credentials:
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
        return ["/export"]
    elif breed.startswith("ios"):
        return ["show running-config"]
    raise Exception("unknown breed")


def check_gnetcli_server(server_path: str):
    global _local_gnetcli
    if not _local_gnetcli:
        t = threading.Thread(target=run_gnetcli_server, args=(server_path,))
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
        for iface in dev.interfaces:
            for ip in iface.ip_addresses:
                return ip.address.split("/")[0]
    return None


def run_gnetcli_server(server_path: str):
    global _local_gnetcli_p
    global _local_gnetcli_url
    _logger.info("starting gnetcli server")
    try:
        proc = subprocess.Popen(
            [server_path, "--conf-file", "-"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
            bufsize=1,
            universal_newlines=True,
        )
    except Exception as e:
        logging.exception("server exec error %s", e)
        raise
    proc.stdin.write(
        """
logging:
  level: debug
  json: true
port: 0
    """
    )
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
        server_path: Optional[str] = None,
    ):
        self.conf = AppSettings(
            login=login,
            password=password,
            dev_login=dev_login,
            dev_password=dev_password,
            server_path=server_path,
            url=url,
        )
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
        raise NotImplementedError()

    def fetch(
        self,
        devices: List[Device],
        files_to_download: Dict[str, List[str]] = None,
        processes: int = 1,
        max_slots: int = 0,
    ) -> Tuple[Dict[Device, str], Dict[Device, Any]]:
        return asyncio.run(self.afetch(devices=devices))

    async def afetch(self, devices: List[Device]):
        running = {}
        failed_running = {}
        for device in devices:
            try:
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
        check_gnetcli_server(server_path=conf.server_path)
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
        server_path: Optional[str] = None,
    ):
        self.conf = AppSettings(
            login=login,
            password=password,
            dev_login=dev_login,
            dev_password=dev_password,
            url=url,
            server_path=server_path,
        )
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
        device_cls = breed_to_device[device.breed]
        ip = get_device_ip(device)
        async with self.api.cmd_session(hostname=device.fqdn) as sess:
            result: List[pb.CMDResult] = []
            for cmd in cmds:
                res = await sess.cmd(
                    cmd=cmd.cmd,
                    cmd_timeout=cmd.timeout,
                    host_params=HostParams(
                        credentials=self.conf.make_dev_credentials(),
                        device=device_cls,
                        ip=ip,
                    ),
                    qa=parse_annet_qa(cmd.questions),
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
        if hw.Huawei:
            ret.add_cmd(Command("quit", suppress_eof=True))
        return ret
