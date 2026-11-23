"""Read CC1 print status over the OpenCentauri SDCP WebSocket."""

import asyncio
import json
import re
from base64 import b64encode
from collections.abc import AsyncIterator
from dataclasses import dataclass
from struct import unpack_from
from time import time
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
from websockets.asyncio.client import connect


BOARD_ID = re.compile(r"[0-9a-f]{32}", re.IGNORECASE)
TOTAL_EXTRUSION_HEX = "54 6F 74 61 6C 45 78 74 72 75 73 69 6F 6E 00"
IDLE, STOPPING, STOPPED, COMPLETE, PRINTING = 0, 7, 8, 9, 13
CANVAS_STATUS = 324
CANVAS_POLL_SECONDS = 15
CANVAS_COLOR = re.compile(r"#?[0-9a-f]{6}", re.IGNORECASE)
THUMBNAIL_PATH = re.compile(r"/board-resource/history_image/[a-zA-Z0-9_-]+\.png")
MAX_THUMBNAIL_BYTES = 24 * 1024


@dataclass(frozen=True)
class Status:
    board_id: str
    code: int
    task_id: str | None
    extrusion_mm: float | None
    current_layer: int | None
    total_layers: int | None
    filename: str | None
    elapsed_seconds: float | None
    estimated_seconds: float | None
    canvas_connected: bool | None = None


@dataclass(frozen=True)
class Thumbnail:
    task_id: str
    url: str


@dataclass(frozen=True)
class CanvasSlot:
    ams_id: int
    tray_id: int
    present: bool
    active: bool
    filament_name: str
    brand: str
    tray_type: str
    tray_color: str


@dataclass(frozen=True)
class CanvasReport:
    slots: tuple[CanvasSlot, ...]


def nonnegative(value) -> float | None:
    return value if type(value) in (int, float) and 0 <= value < float("inf") else None


def _object(value) -> dict:
    return value if isinstance(value, dict) else {}


def _board_origins(frame: dict, envelope: dict, payload: dict) -> list[object]:
    origins = [part.get("MainboardID") for part in (frame, envelope, payload)]
    topic = frame.get("Topic")
    if isinstance(topic, str) and topic.startswith("sdcp/"):
        parts = topic.split("/")
        if len(parts) == 3:
            origins.append(parts[2])
    return [origin for origin in origins if origin is not None and origin != ""]


def parse_status(frame: dict) -> Status | None:
    envelope = _object(frame.get("Data"))
    payload = _object(envelope.get("Data"))
    status = _object(frame.get("Status")) or _object(payload.get("Status"))
    info = status.get("PrintInfo")
    origins = _board_origins(frame, envelope, payload)
    if not origins or not isinstance(info, dict):
        return None
    board_id = origins[0]
    if not isinstance(board_id, str) or not BOARD_ID.fullmatch(board_id) or any(
        not isinstance(origin, str) or origin.lower() != board_id.lower() for origin in origins[1:]
    ):
        return None
    code = info.get("Status")
    if type(code) is not int:
        return None
    task_id = info.get("TaskId")
    if task_id is not None:
        if not isinstance(task_id, str) or len(task_id) > 128:
            return None
        task_id = task_id.strip() or None
    extrusion = nonnegative(info.get("TotalExtrusion", info.get(TOTAL_EXTRUSION_HEX)))
    current_layer, total_layers = info.get("CurrentLayer"), info.get("TotalLayer")
    filename = info.get("Filename")
    if isinstance(filename, str):
        filename = filename.replace("\\", "/").rsplit("/", 1)[-1]
        filename = "".join(char for char in filename if char.isprintable()).strip()[:255] or None
    else:
        filename = None
    connected = status.get("AmsConnectStatus")
    return Status(
        board_id.lower(), code, task_id, extrusion,
        current_layer if type(current_layer) is int and current_layer >= 0 else None,
        total_layers if type(total_layers) is int and total_layers > 0 else None,
        filename, nonnegative(info.get("CurrentTicks")), nonnegative(info.get("TotalTicks")) or None,
        bool(connected) if type(connected) in (int, bool) and connected in (0, 1) else None,
    )


def _tray_label(value) -> str:
    if not isinstance(value, str):
        return ""
    label = "".join(char for char in value[:128] if char.isprintable()).strip()
    return "" if label == "?" or label.startswith("—") else label


def parse_canvas_report(frame: dict, board_id: str, request_id: str | None) -> CanvasReport | None:
    envelope = _object(frame.get("Data"))
    payload = _object(envelope.get("Data"))
    if any(not isinstance(origin, str) or origin.lower() != board_id for origin in _board_origins(frame, envelope, payload)):
        return None
    for part in (frame, envelope):
        command, response_id = part.get("Cmd"), part.get("RequestID")
        if command is not None and (type(command) is not int or command != CANVAS_STATUS):
            return None
        if response_id not in (None, "", request_id):
            return None
    report = payload if "canvas_list" in payload else frame
    units = report.get("canvas_list")
    ack = report.get("Ack", 0)
    if not isinstance(units, list) or type(ack) is not int or ack != 0:
        return None

    active_unit = report.get("active_canvas_id")
    active_tray = report.get("active_tray_id")
    active_key = (active_unit + 1, active_tray) if type(active_unit) is int and type(active_tray) is int else None
    slots: dict[tuple[int, int], CanvasSlot] = {}
    observed_unit = False
    for unit in units:
        if not isinstance(unit, dict):
            continue
        unit_id, connected = unit.get("canvas_id"), unit.get("connected")
        trays = unit.get("tray_list")
        if type(unit_id) is not int or not 0 <= unit_id <= 4 or type(connected) not in (int, bool):
            continue
        if connected == 0:
            observed_unit = True
            continue
        if connected != 1 or not isinstance(trays, list):
            continue
        for tray in trays:
            if not isinstance(tray, dict):
                continue
            tray_id, status = tray.get("tray_id"), tray.get("status")
            if type(tray_id) is not int or not 0 <= tray_id <= 3 or type(status) is not int or status not in (0, 1, 2):
                continue
            key = (unit_id + 1, tray_id)
            if key in slots:
                continue
            observed_unit = True
            present = status != 0
            color = tray.get("filament_color")
            slots[key] = CanvasSlot(
                *key, present, present and key == active_key,
                _tray_label(tray.get("filament_name")) if present else "",
                _tray_label(tray.get("brand")) if present else "",
                _tray_label(tray.get("filament_type")) if present else "",
                "#" + color.lstrip("#").upper() if present and isinstance(color, str) and CANVAS_COLOR.fullmatch(color) else "",
            )
    if units and not observed_unit:
        return None
    return CanvasReport(tuple(slots[key] for key in sorted(slots)))


def request(board_id: str, command: int, params: dict, request_id: str) -> str:
    return json.dumps({
        "Id": uuid4().hex,
        "Data": {
            "Cmd": command, "Data": params, "RequestID": request_id,
            "MainboardID": board_id, "TimeStamp": int(time()), "From": 0,
        },
        "Topic": f"sdcp/request/{board_id}",
    })


def history_thumbnail(host: str, frame: dict, board_id: str, request_id: str, task_id: str) -> Thumbnail | None:
    response = frame.get("Data")
    if not isinstance(response, dict) or response.get("Cmd") != 321 or response.get("RequestID") != request_id:
        return None
    for origin in (frame.get("MainboardID"), response.get("MainboardID")):
        if origin is not None and (not isinstance(origin, str) or origin.lower() != board_id):
            return None
    data = response.get("Data")
    details = data.get("HistoryDetailList") if isinstance(data, dict) else None
    if not isinstance(details, list):
        return None
    for detail in details:
        if not isinstance(detail, dict) or detail.get("TaskId") != task_id:
            continue
        url = detail.get("Thumbnail")
        if not isinstance(url, str) or len(url) > 256:
            continue
        try:
            parsed = urlsplit(url)
        except ValueError:
            continue
        if (parsed.scheme == "http" and parsed.netloc in (host, f"{host}:80", f"{host}:3030")
                and not parsed.query and not parsed.fragment and THUMBNAIL_PATH.fullmatch(parsed.path)):
            return Thumbnail(task_id, parsed.geturl())
    return None


async def fetch_thumbnail(url: str) -> str | None:
    pixels = bytearray()
    try:
        async with asyncio.timeout(4), httpx.AsyncClient(trust_env=False) as client:
            async with client.stream("GET", url, headers={"Accept-Encoding": "identity"}) as response:
                if response.status_code != 200 or response.headers.get("content-type", "").split(";", 1)[0] != "image/png":
                    return None
                async for chunk in response.aiter_raw(chunk_size=4096):
                    pixels.extend(chunk)
                    if len(pixels) > MAX_THUMBNAIL_BYTES:
                        return None
    except (TimeoutError, httpx.HTTPError):
        return None
    if len(pixels) < 45 or not pixels.startswith(b"\x89PNG\r\n\x1a\n\x00\x00\x00\x0dIHDR"):
        return None
    if not all(0 < size <= 512 for size in unpack_from(">II", pixels, 16)):
        return None
    return "data:image/png;base64," + b64encode(pixels).decode("ascii")


async def read_status(host: str, board_id: str | None) -> AsyncIterator[Status | Thumbnail | CanvasReport]:
    async with connect(
        f"ws://{host}:3030/websocket", open_timeout=5, close_timeout=2,
        ping_interval=None, proxy=None,
    ) as socket:
        loop = asyncio.get_running_loop()
        connected_at = loop.time()
        last_status = None
        next_heartbeat = connected_at
        preview_task = None
        preview_request = None
        preview_attempts = 0
        preview_retry_at = connected_at
        canvas_connected = False
        canvas_verified = False
        canvas_request = None
        next_canvas_poll = connected_at
        while loop.time() - (last_status if last_status is not None else connected_at) < 25:
            if loop.time() >= next_heartbeat:
                await socket.send("ping")
                if board_id and (last_status is None or loop.time() - last_status >= 8):
                    await socket.send(request(board_id, 0, {}, uuid4().hex))
                next_heartbeat = loop.time() + 8
            try:
                message = await asyncio.wait_for(socket.recv(), timeout=2)
            except TimeoutError:
                continue
            try:
                frame = json.loads(message)
            except ValueError:
                continue
            if not isinstance(frame, dict):
                continue
            if board_id and canvas_connected:
                report = parse_canvas_report(frame, board_id, canvas_request)
                if report is not None:
                    canvas_verified = canvas_request is not None
                    yield report
            if board_id and preview_task and preview_request:
                thumbnail = history_thumbnail(host, frame, board_id, preview_request, preview_task)
                if thumbnail:
                    preview_attempts = 2
                    preview_request = None
                    yield thumbnail
            status = parse_status(frame)
            if status is None:
                continue
            if board_id and status.board_id != board_id:
                raise ValueError("SDCP response came from a different mainboard")
            board_id = status.board_id
            last_status = loop.time()
            if status.canvas_connected is not None:
                canvas_connected = status.canvas_connected
            if not canvas_connected:
                canvas_verified = False
                canvas_request = None
                next_canvas_poll = last_status
            yield status
            if (canvas_connected and last_status >= next_canvas_poll
                    and (canvas_verified or status.code in (IDLE, COMPLETE, STOPPED))):
                canvas_request = uuid4().hex
                await socket.send(request(board_id, CANVAS_STATUS, {}, canvas_request))
                next_canvas_poll = last_status + CANVAS_POLL_SECONDS
            if status.code == PRINTING and status.task_id:
                if status.task_id != preview_task:
                    preview_task = status.task_id
                    preview_attempts = 0
                    preview_retry_at = last_status
                if preview_attempts < 2 and last_status >= preview_retry_at:
                    preview_request = uuid4().hex
                    await socket.send(request(board_id, 321, {"Id": [preview_task]}, preview_request))
                    preview_attempts += 1
                    preview_retry_at = last_status + 15
        raise TimeoutError("No CC1 print status received. Set the SDCP Mainboard ID if needed.")
