import os.path
from logging import getLogger

import os
from contextlib import ExitStack
from datetime import datetime
from typing import TextIO

import gnetclisdk.proto.server_pb2 as pb
from annet.deploy import ProgressBar
from annet.storage import Device

logger = getLogger(__name__)


class ProgressTracker:
    def __enter__(self) -> "ProgressTracker":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def start_group(self, group_name: str) -> None:
        pass

    def set_total(self, total: int) -> None:
        pass

    def upload_files(self, files: list[str]) -> None:
        pass

    def files_uploaded(self) -> None:
        pass

    def run_command(self, cmd: str) -> None:
        pass

    def run_reload_command(self, file: str, cmd: str):
        pass

    def command_done_ok(self, output: pb.CMDResult) -> None:
        pass

    def command_done_error(self, error: str) -> None:
        pass

    def command_done_error_suppressed(self, error: str) -> None:
        pass

    def finish_ok(self, notification: str) -> None:
        pass

    def finish_err(self, notification: str) -> None:
        pass


def format_trace(trace: list[pb.CMDTraceItem]) -> str:
    res: list[str] = []
    for t in trace:
        op = "unknown"  # TODO: get from pb
        if t.operation == 2:
            op = "write"
        elif t.operation == 3:
            op = "read"
        res.append(f"{op}={t.data}")
    return "\n".join(res)


def _render_cmd_res(res: pb.CMDResult) -> str:
    if res.trace:
        trace_str = "\n" + format_trace(res.trace)
    else:
        trace_str = ""
    try:
        error_str = res.error.decode("utf-8")
    except UnicodeDecodeError:
        error_str = str(res.error)
    if error_str:
        error_str = "\n" + error_str
    return f"out: {res.out_str}\nstatus: {res.status}{error_str}{trace_str}"


class ProgressBarTracker(ProgressTracker):
    def __init__(self, device: Device, progress_bar: ProgressBar) -> None:
        self.progress_bar = progress_bar
        self.tile = device.fqdn
        self.total_steps = 0
        self.done_steps = 0
        self.last_cmd = ""

    def set_total(self, total: int) -> None:
        self.total_steps = total

    def start_group(self, group_name: str) -> None:
        self.progress_bar.add_content(self.tile, f"=== {group_name} ===")

    def upload_files(self, files: list[str]) -> None:
        filelist = "".join(f"- {f}\n" for f in files)
        self.progress_bar.add_content(self.tile, f"=== Uploading files ===\n{filelist}")

    def files_uploaded(self) -> None:
        self.done_steps += 1
        self.progress_bar.add_content(self.tile, f"=== Files uploaded ===\n")
        self.progress_bar.set_progress(self.tile, self.done_steps, self.total_steps)

    def run_command(self, cmd: str):
        self.progress_bar.add_content(self.tile, f"cmd: {cmd}")
        self.progress_bar.set_progress(
            self.tile,
            self.done_steps,
            self.total_steps,
            cmd,
        )

    def run_reload_command(self, file: str, cmd: str):
        self.last_cmd = cmd
        self.progress_bar.add_content(self.tile, f"{file}: {cmd}")
        self.progress_bar.set_progress(self.tile, self.done_steps, self.total_steps, suffix=cmd)

    def command_done_ok(self, output: pb.CMDResult) -> None:
        self.done_steps += 1
        output_str = _render_cmd_res(output)
        self.progress_bar.add_content(self.tile, output_str)
        self.progress_bar.set_progress(
            self.tile,
            self.done_steps,
            self.total_steps,
            "Ok: " + self.last_cmd,
        )

    def command_done_error(self, error: str) -> None:
        self.done_steps += 1
        self.progress_bar.add_content(self.tile, error)
        self.progress_bar.set_progress(
            self.tile,
            self.done_steps,
            self.total_steps,
            "Error: " + self.last_cmd,
        )

    def command_done_error_suppressed(self, error: str) -> None:
        self.done_steps += 1
        self.progress_bar.add_content(self.tile, error)
        self.progress_bar.set_progress(
            self.tile,
            self.done_steps,
            self.total_steps,
            "Error (suppressed): " + self.last_cmd,
        )

    def finish_ok(self, notification: str) -> None:
        self.done_steps = self.total_steps
        self.progress_bar.set_progress(self.tile,self.done_steps, self.total_steps, suffix=notification)

    def finish_err(self, notification: str) -> None:
        self.done_steps = self.total_steps
        self.progress_bar.set_exception(self.tile, notification, "", self.total_steps)


class FileProgressTracker(ProgressTracker):
    def __init__(self, device: Device, dirname: str):
        self.dirname = dirname
        self.device = device
        self.file: TextIO | None = None

    def __enter__(self) -> ProgressTracker:
        filepath = self._make_file_path()
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        logger.info("Writing deploy logs to file: %s", filepath)
        self.file = open(filepath, "a")
        return self

    def _make_file_path(self) -> str:
        now = datetime.now()
        datedir = f"{now:%Y-%m-%d-%H-%M}"
        return os.path.join(
            self.dirname,
            datedir,
            f"{self.device.fqdn}_{now.timestamp():.0f}",
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()

    def _log(self, msg: str) -> None:
        self.file.write(f"{datetime.now()}: {msg}\n")

    def start_group(self, group_name: str) -> None:
        self._log(f"=== {group_name} ===")

    def set_total(self, total: int) -> None:
        pass

    def upload_files(self, files: list[str]) -> None:
        filelist = "\n".join(f"- {f}" for f in files)
        self._log(f"=== Uploading files ===\n{filelist}")

    def files_uploaded(self) -> None:
        self._log(f"=== Files uploaded ===")

    def run_command(self, cmd: str) -> None:
        self._log(f"cmd: {cmd}")

    def run_reload_command(self, file: str, cmd: str):
        self._log(f"file: {cmd}")

    def command_done_ok(self, output: pb.CMDResult) -> None:
        output_str = _render_cmd_res(output)
        self._log(output_str)

    def command_done_error(self, error: str) -> None:
        self._log(f"Error: {error}")

    def command_done_error_suppressed(self, error: str) -> None:
        self._log(f"Error (suppressed): {error}")

    def finish_ok(self, notification: str) -> None:
        self._log("Finished with success: " + notification)

    def finish_err(self, notification: str) -> None:
        self._log("Finished with failure: " + notification)



class LogProgressTracker(ProgressTracker):
    def __init__(self, device: Device):
        self.fqdn = device.fqdn

    def __enter__(self) -> ProgressTracker:
        return self

    def start_group(self, group_name: str) -> None:
        logger.info(f"{self.fqdn} - {group_name}")

    def upload_files(self, files: list[str]) -> None:
        logger.info(f"{self.fqdn} - upload %s files", len(files))

    def command_done_ok(self, output: pb.CMDResult) -> None:
        if output.error:
            try:
                error = output.error.decode("utf-8")
            except UnicodeDecodeError:
                error = str(output.error)
            logger.warning(f"{self.fqdn} - {error}")

    def command_done_error(self, error: str) -> None:
        logger.error(f"{self.fqdn} - {error}")

    def command_done_error_suppressed(self, error: str) -> None:
        logger.info(f"{self.fqdn} - {error}")

    def finish_ok(self, notification: str) -> None:
        logger.info(f"{self.fqdn} - finished with success - {notification}")

    def finish_err(self, notification: str) -> None:
        logger.info(f"{self.fqdn} - finished with failure - {notification}")



class CompositeTracker(ProgressTracker):
    def __init__(self, *trackers: ProgressTracker) -> None:
        self.trackers = list(trackers)
        self.stack = ExitStack()

    def __enter__(self):
        for tracker in self.trackers:
            self.stack.enter_context(tracker)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stack.close()

    def add_tracker(self, tracker: ProgressTracker) -> None:
        self.trackers.append(tracker)

    def start_group(self, group_name: str) -> None:
        for tracker in self.trackers:
            tracker.start_group(group_name)

    def set_total(self, total: int) -> None:
        for tracker in self.trackers:
            tracker.set_total(total)

    def upload_files(self, files: list[str]) -> None:
        for tracker in self.trackers:
            tracker.upload_files(files)

    def files_uploaded(self) -> None:
        for tracker in self.trackers:
            tracker.files_uploaded()

    def run_command(self, cmd: str) -> None:
        for tracker in self.trackers:
            tracker.run_command(cmd)

    def run_reload_command(self, file: str, cmd: str):
        for tracker in self.trackers:
            tracker.run_reload_command(file, cmd)

    def command_done_ok(self, output: pb.CMDResult) -> None:
        for tracker in self.trackers:
            tracker.command_done_ok(output)

    def command_done_error(self, error: str) -> None:
        for tracker in self.trackers:
            tracker.command_done_error(error)

    def command_done_error_suppressed(self, error: str) -> None:
        for tracker in self.trackers:
            tracker.command_done_error_suppressed(error)

    def finish_ok(self, notification: str) -> None:
        for tracker in self.trackers:
            tracker.finish_ok(notification)

    def finish_err(self, notification: str) -> None:
        for tracker in self.trackers:
            tracker.finish_err(notification)
