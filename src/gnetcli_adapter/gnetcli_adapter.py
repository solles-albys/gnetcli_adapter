import asyncio
import json
import subprocess
import time

from annet.deploy import DeployDriver, DeployOptions, DeployResult, apply_deploy_rulebook
from annet.annlib.command import Command, CommandList
from annet.annlib.netdev.views.hardware import HardwareView
from annet.rulebook import common

from annet.deploy import Fetcher, AdapterWithConfig, AdapterWithName
from typing import Dict, List, Any, Optional, Tuple
from annet.storage import Device
from gnetclisdk.client import Credentials, Gnetcli, HostParams
from pydantic import Field, field_validator, FieldValidationInfo
from pydantic_core import PydanticUndefined
from pydantic_settings import BaseSettings, SettingsConfigDict
import base64
import logging
import threading
import atexit

breed_to_device = {
    "routeros": "ros",
}
_local_gnetcli: Optional[threading.Thread] = None
_local_gnetcli_p: Optional[subprocess.Popen] = None
_local_gnetcli_url: Optional[str] = None
LOG_FORMAT = "%(asctime)s - l:%(lineno)d - %(funcName)s() - %(levelname)s - %(message)s"
DATE_FMT = "%Y-%m-%d %H:%M:%S"
_logger = logging.getLogger(__name__)
GNETCLI_SERVER = "/usr/bin/server"


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="gnetcli_", validate_assignment=True)

    server: str = Field(default="localhost:50051")
    insecure_grpc: bool = Field(default=True)
    login: str = Field(default="")
    password: str = Field(default="")
    dev_login: str = Field(default="")
    dev_password: str = Field(default="")

    def make_credentials(self) -> Credentials:
        return Credentials(self.dev_login, self.dev_password)

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
    raise Exception("unknown breed")


def check_gnetcli_server():
    global _local_gnetcli
    if not _local_gnetcli:
        t = threading.Thread(target=run_gnetcli_server, args=())
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


def run_gnetcli_server():
    global _local_gnetcli_p
    global _local_gnetcli_url
    _logger.info("starting gnetcli server")
    try:
        proc = subprocess.Popen(
            [GNETCLI_SERVER, "--conf-file", "-"],
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
    ):
        if not url:
            check_gnetcli_server()
            if _local_gnetcli_url is None:
                _logger.info("waiting for _local_gnetcli_url appears")
                start = time.monotonic()
                while time.monotonic() - start < 5:
                    if _local_gnetcli_p is not None and _local_gnetcli_p.returncode is not None:
                        raise Exception("gnetcli server died with code %s" % _local_gnetcli_p.returncode)
                    if _local_gnetcli_url is not None:
                        break

        self.conf = AppSettings(
            login=login, password=password, dev_login=dev_login, dev_password=dev_password, server=_local_gnetcli_url
        )
        auth_token = (
            base64.b64encode(b"%s:%s" % (self.conf.login.encode(), self.conf.password.encode())).strip().decode()
        )
        auth_token = f"Basic {auth_token}"
        self.api = Gnetcli(
            server=self.conf.server,
            auth_token=auth_token,
            insecure_grpc=self.conf.insecure_grpc,
            user_agent="annet",
        )

    def name(self) -> str:
        return "gnetcli"

    def with_config(self, **kwargs: Dict[str, Any]) -> Fetcher:
        return GnetcliFetcher(**kwargs)

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
        if device.breed not in breed_to_device:
            raise Exception("unknown breed for gnetcli")
        device_cls = breed_to_device[device.breed]

        cmds = await get_config(breed=device.breed)
        dev_result = []
        for cmd in cmds:
            res = await self.api.cmd(
                hostname=device.fqdn,
                cmd=cmd,
                host_params=HostParams(
                    credentials=self.conf.make_credentials(),
                    device=device_cls,
                ),
            )
            if res.status != 0:
                raise Exception("cmd error %s" % res)
            dev_result.append(res.out)
        return b"\n".join(dev_result).decode()


class GnetcliDeployer(DeployDriver, AdapterWithConfig, AdapterWithName):
    def __init__(
        self,
        login: Optional[str] = None,
        password: Optional[str] = None,
        dev_login: Optional[str] = None,
        dev_password: Optional[str] = None,
    ):
        self.conf = AppSettings(login=login, password=password, dev_login=dev_login, dev_password=dev_password)
        auth_token = (
            base64.b64encode(b"%s:%s" % (self.conf.login.encode(), self.conf.password.encode())).strip().decode()
        )
        auth_token = f"Basic {auth_token}"
        self.api = Gnetcli(
            server=self.conf.server,
            auth_token=auth_token,
            insecure_grpc=self.conf.insecure_grpc,
            user_agent="annet",
        )

    def name(self) -> str:
        return "gnetcli"

    def with_config(self, **kwargs: Dict[str, Any]) -> DeployDriver:
        return GnetcliDeployer(**kwargs)

    async def bulk_deploy(self, deploy_cmds: Dict[Device, CommandList], args: DeployOptions) -> DeployResult:
        deploy_items = deploy_cmds.items()
        result = await asyncio.gather(*[asyncio.Task(self.deploy(device, cmds, args)) for device, cmds in deploy_items])
        res = DeployResult(hostnames=[], results={}, durations={}, original_states={})
        res.add_results(results={dev.fqdn: dev_res for (dev, _), dev_res in zip(deploy_items, result)})
        return res

    async def deploy(self, device: Device, cmds: CommandList, args: DeployOptions) -> str:
        device_cls = breed_to_device[device.breed]
        async with self.api.cmd_session(hostname=device.fqdn) as sess:
            result = []
            for cmd in cmds:
                res = await sess.cmd(
                    cmd=cmd.cmd,
                    cmd_timeout=cmd.timeout,
                    host_params=HostParams(
                        credentials=self.conf.make_credentials(),
                        device=device_cls,
                    ),
                )
                if res.status != 0:
                    raise Exception("cmd %s error %s status %s", cmd, res.err, res.status)
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
