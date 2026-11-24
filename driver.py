"""FilaMan printer driver for single-spool OpenCentauri Patched CC1."""

import asyncio
import logging
from contextlib import aclosing
from datetime import UTC, datetime
from ipaddress import IPv4Address

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from websockets.exceptions import WebSocketException

from app.core.database import async_session_maker
from app.core.event_bus import event_bus
from app.models.location import Location
from app.models.printer import Printer, PrinterSlot, PrinterSlotAssignment
from app.models.spool import Spool, SpoolEvent
from app.plugins.base import BaseDriver

from .sdcp import (
    BOARD_ID, COMPLETE, IDLE, PRINTING, STOPPED, STOPPING, CanvasReport, CanvasSlot, Status, Thumbnail,
    fetch_thumbnail, read_status,
)
from .usage import Usage, booked_previews, initialize_usage, usage_printers


logger = logging.getLogger(__name__)
SLOT_INDEX = "0-0"
SLOT_NAME = "Spool holder"
DEFAULT_LOW_FILAMENT_THRESHOLD_G = 100


def _canvas_name(ams_id: int, tray_id: int) -> str:
    return f"CANVAS {ams_id} - Slot {tray_id + 1}"


class Driver(BaseDriver):
    driver_key = "centauri_cc1"

    def __init__(self, printer_id, config, emitter):
        super().__init__(printer_id, config, emitter)
        self._host = ""
        self._board_id = None
        self._slot_id = None
        self._spool_id = None
        self._slot_meta = {}
        self._canvas_connected: bool | None = None
        self._canvas_slots: tuple[CanvasSlot, ...] = ()
        self._canvas_assignments: dict[tuple[int, int], tuple[int, int | None]] = {}
        self._state_lock = asyncio.Lock()
        self._booking_lock = asyncio.Lock()
        self._usage = Usage(printer_id)
        self._wake_booking = asyncio.Event()
        self._tasks = []
        self._connected = False
        self._last_seen: str | None = None
        self._status: Status | None = None
        self._low_filament_threshold_g = DEFAULT_LOW_FILAMENT_THRESHOLD_G
        self._connection_error = None
        self._usage_issue = None
        self._armed = False
        self._observed_task = None
        self._active_run = None
        self._print_task = None
        self._print_data = {}
        self._preview_worker: asyncio.Task | None = None

    def validate_config(self) -> None:
        self._host = str(IPv4Address(str(self.config.get("host", "")).strip()))
        board_id = str(self.config.get("mainboard_id") or "").strip().lower()
        if board_id and not BOARD_ID.fullmatch(board_id):
            raise ValueError("SDCP Mainboard ID must contain 32 hexadecimal characters")
        self._board_id = board_id or None
        threshold = self.config.get("low_filament_threshold_g")
        if threshold is None:
            threshold = DEFAULT_LOW_FILAMENT_THRESHOLD_G
        if type(threshold) not in (int, float) or not 0 <= threshold < float("inf"):
            raise ValueError("Low filament threshold must be a finite, non-negative weight in grams")
        self._low_filament_threshold_g = threshold

    async def start(self) -> None:
        if self._running:
            return
        self.validate_config()
        await initialize_usage()
        await self._ensure_slot()
        self._running = True
        self._tasks = [
            asyncio.create_task(self._watch_printer()),
            asyncio.create_task(self._book_usage()),
        ]

    async def stop(self) -> None:
        self._running = False
        tasks = self._tasks + ([self._preview_worker] if self._preview_worker is not None else [])
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        self._preview_worker = None
        self._set_connected(False)

    async def _ensure_slot(self) -> None:
        async with async_session_maker.begin() as db:
            slot = await db.scalar(select(PrinterSlot).where(
                PrinterSlot.printer_id == self.printer_id,
                PrinterSlot.slot_no == 0,
            ))
            if slot is None:
                slot = PrinterSlot(printer_id=self.printer_id, slot_no=0, name=SLOT_NAME)
                db.add(slot)
                await db.flush()
            slot.is_active = True
            slot.custom_fields = {**(slot.custom_fields or {}), "slot_index": SLOT_INDEX}
            assignment = await db.get(PrinterSlotAssignment, slot.id)
            if assignment is None:
                assignment = PrinterSlotAssignment(slot_id=slot.id, present=False, meta={})
                db.add(assignment)
            spool = await db.get(Spool, assignment.spool_id) if assignment.spool_id is not None else None
            location_changed = await self._sync_slot_location(db, spool)
            self._slot_id = slot.id
            self._spool_id = assignment.spool_id
            self._slot_meta = assignment.meta or {}
        if location_changed:
            await self._publish_slot_locations()

    async def _sync_slot_location(
        self, db, spool: Spool | None, ams_id: int = 0, tray_id: int = 0,
    ) -> bool:
        identifier = f"centauri_cc1_{self.printer_id}_{ams_id}_{tray_id}"
        location = await db.scalar(select(Location).where(Location.identifier == identifier).with_for_update())
        if location is None and spool is None:
            return False

        printer = await db.get(Printer, self.printer_id)
        suffix = " - " + (_canvas_name(ams_id, tray_id) if ams_id else SLOT_NAME)
        name = printer.name[:255 - len(suffix)] + suffix
        changed = location is None or location.name != name
        if location is None:
            location = Location(name=name, identifier=identifier, custom_fields={
                "managed_by": "centauri_cc1_plugin", "printer_id": self.printer_id,
            })
            db.add(location)
            await db.flush()
        else:
            location.name = name

        occupants = await db.scalars(select(Spool).where(Spool.location_id == location.id).with_for_update())
        moves = [(previous, None) for previous in occupants if spool is None or previous.id != spool.id]
        if spool is not None and spool.location_id != location.id:
            moves.append((spool, location.id))
        now = datetime.now(UTC)
        for moved, target_id in moves:
            # SpoolService.move_location commits immediately. Record the same event
            # here so both moves and the slot assignment commit together.
            db.add(SpoolEvent(
                spool_id=moved.id, event_type="move_location", event_at=now,
                source="driver", from_location_id=moved.location_id, to_location_id=target_id,
            ))
            moved.location_id = target_id
        return changed or bool(moves)

    async def _publish_slot_locations(self) -> None:
        await event_bus.publish({"event": "locations_changed"})
        await event_bus.publish({"event": "spools_changed"})

    async def _sync_canvas_slots(self, slots: tuple[CanvasSlot, ...]) -> None:
        assignments = {}
        locations_changed = False
        async with async_session_maker.begin() as db:
            stored = await db.scalars(select(PrinterSlot).where(PrinterSlot.printer_id == self.printer_id))
            rows = {row.slot_no: row for row in stored}
            active_numbers = {slot.ams_id * 4 + slot.tray_id for slot in slots}
            for row in rows.values():
                if (row.custom_fields or {}).get("cc1_canvas") and row.slot_no not in active_numbers:
                    row.is_active = False
            for slot in slots:
                ams_id, tray_id = slot.ams_id, slot.tray_id
                number = ams_id * 4 + tray_id
                row = rows.get(number)
                if row is None:
                    row = PrinterSlot(printer_id=self.printer_id, slot_no=number)
                    db.add(row)
                row.is_active = True
                row.name = _canvas_name(ams_id, tray_id)
                row.custom_fields = {**(row.custom_fields or {}), "slot_index": f"{ams_id}-{tray_id}", "cc1_canvas": True}
                if row.id is None:
                    await db.flush()
                assignment = await db.get(PrinterSlotAssignment, row.id, with_for_update=True)
                if assignment is None:
                    assignment = PrinterSlotAssignment(slot_id=row.id)
                    db.add(assignment)
                if not slot.present and assignment.spool_id is not None:
                    locations_changed |= await self._sync_slot_location(db, None, ams_id, tray_id)
                    assignment.spool_id = None
                assignment.present = slot.present
                assignment.meta = {"tray_color": slot.tray_color, "tray_type": slot.tray_type}
                assignment.updated_at = datetime.now(UTC)
                assignments[ams_id, tray_id] = (row.id, assignment.spool_id)
        self._canvas_slots = slots
        self._canvas_assignments = assignments
        await event_bus.publish({"event": "slots_update", "printer_id": self.printer_id})
        if locations_changed:
            await self._publish_slot_locations()

    async def send_filament_to_tray(
        self, ams_id: int, tray_id: int, filament_data: dict, spool_id: int | None = None,
    ) -> None:
        if type(spool_id) is not int or spool_id <= 0:
            raise ValueError("Select an existing FilaMan spool")
        meta = {
            "tray_color": filament_data.get("color") or "",
            "tray_type": filament_data.get("material_type") or "",
            "active_spool_id": spool_id,
        }
        await self._set_slot_spool(ams_id, tray_id, spool_id, meta)

    async def remove_spool(self, spool_id: int, ams_id: int = 0, tray_id: int = 0) -> None:
        if type(spool_id) is not int or spool_id <= 0:
            raise ValueError("Select the spool currently in the holder")
        await self._set_slot_spool(ams_id, tray_id, None, {}, removing=spool_id)

    async def _set_slot_spool(
        self, ams_id: int, tray_id: int, spool_id: int | None, meta: dict, *, removing: int | None = None,
    ) -> None:
        if type(ams_id) is not int or type(tray_id) is not int:
            raise ValueError("Select a detected slot.")
        key = (ams_id, tray_id)
        async with self._state_lock:
            idle = self._connected and self._status is not None and self._status.code in (IDLE, STOPPED, COMPLETE)
            if key == (0, 0):
                slot_id = self._slot_id
                canvas_slot = None
            else:
                canvas_slot = next((slot for slot in self._canvas_slots if (slot.ams_id, slot.tray_id) == key), None)
                if not self._canvas_connected or canvas_slot is None or not canvas_slot.present:
                    raise ValueError("Select a detected CANVAS slot with filament loaded.")
                if not idle:
                    raise ValueError("Wait until the printer is online and the print has ended to change a CANVAS link.")
                slot_id, _ = self._canvas_assignments[key]
                meta = {"tray_color": canvas_slot.tray_color, "tray_type": canvas_slot.tray_type}
            async with async_session_maker.begin() as db:
                assignment = await db.get(PrinterSlotAssignment, slot_id, with_for_update=True)
                if removing is not None:
                    if assignment.spool_id is None:
                        return
                    if assignment.spool_id != removing:
                        raise ValueError("The spool in the holder changed. Refresh and try again.")
                    if not idle:
                        raise ValueError("Wait until the printer is online and the print has ended to remove the spool.")
                spool = await db.get(Spool, spool_id, with_for_update=True) if spool_id is not None else None
                if spool_id is not None and spool is None:
                    raise ValueError("Selected spool no longer exists")
                if spool is not None:
                    linked = await db.scalar(select(PrinterSlotAssignment.slot_id).join(PrinterSlot).where(
                        PrinterSlot.printer_id == self.printer_id, PrinterSlotAssignment.spool_id == spool.id,
                        PrinterSlotAssignment.slot_id != slot_id,
                    ).limit(1))
                    if linked is not None:
                        raise ValueError("This spool is already linked to another slot on this printer.")
                location_changed = await self._sync_slot_location(db, spool, ams_id, tray_id)
                assignment.spool_id = spool_id
                assignment.present = canvas_slot is not None or spool_id is not None
                assignment.updated_at = datetime.now(UTC)
                assignment.meta = meta
            if canvas_slot is None:
                self._spool_id = spool_id
                self._slot_meta = meta
            else:
                self._canvas_assignments[key] = (slot_id, spool_id)
        await event_bus.publish({"event": "slots_update", "printer_id": self.printer_id})
        if location_changed:
            await self._publish_slot_locations()

    async def get_unbooked_prints(self, state: str, page: int = 1) -> dict:
        return await self._usage.unbooked_prints(state, page)

    async def get_review_count(self) -> dict:
        return {"count": await self._usage.review_count()}

    async def get_usage_printers(self, event_keys: list[str]) -> dict:
        return {"printers": await usage_printers(event_keys)}

    async def get_usage_previews(self, event_ids: list[int]) -> dict:
        return {"previews": await booked_previews(event_ids)}

    async def get_latest_usage_receipt(self) -> dict:
        return {"receipt": await self._usage.latest_receipt()}

    async def resolve_print_review(self, run_id: str, spool_id: int | None = None) -> dict:
        if not isinstance(run_id, str):
            raise ValueError("Select a print to review.")
        board, _, task = run_id.partition(":")
        if not BOARD_ID.fullmatch(board) or not 1 <= len(task) <= 128:
            raise ValueError("Select a valid print to review.")
        if spool_id is not None and (type(spool_id) is not int or spool_id <= 0):
            raise ValueError("Select the spool used for this print.")
        async with self._state_lock, self._booking_lock:
            result = await self._usage.resolve_review(run_id, spool_id)
        if result["status"] == "pending":
            self._wake_booking.set()
        return result

    def health(self) -> dict:
        status = self._status
        slots = [{
            "slot_index": SLOT_INDEX,
            "slot_name": SLOT_NAME,
            "slot_kind": "toolhead",
            "present": self._spool_id is not None,
            "spool_id": self._spool_id,
            "tray_color": self._slot_meta.get("tray_color", ""),
            "tray_type": self._slot_meta.get("tray_type", ""),
        }]
        for slot in self._canvas_slots:
            _, spool_id = self._canvas_assignments[slot.ams_id, slot.tray_id]
            slots.append({
                "slot_index": f"{slot.ams_id}-{slot.tray_id}",
                "slot_name": _canvas_name(slot.ams_id, slot.tray_id),
                "slot_kind": "ams",
                "ams_id": slot.ams_id,
                "tray_id": slot.tray_id,
                "present": slot.present,
                "spool_id": spool_id,
                "tray_color": slot.tray_color,
                "tray_type": slot.tray_type,
                "filament_name": slot.filament_name,
                "brand": slot.brand,
                "active": slot.active,
            })
        return {
            **super().health(),
            "connected": self._connected,
            "last_seen": self._last_seen,
            "low_filament_threshold_g": self._low_filament_threshold_g,
            "print_status": status.code if status else None,
            "current_layer": status.current_layer if status else None,
            "total_layers": status.total_layers if status else None,
            "extrusion_mm": status.extrusion_mm if status else None,
            "print_details": self._print_data,
            "last_error": self._connection_error,
            "last_usage_issue": self._usage_issue,
            "active_spool_id": self._spool_id,
            "canvas_connected": self._canvas_connected is True,
            "slots": slots,
        }

    def _set_connected(self, connected: bool) -> None:
        if connected == self._connected:
            return
        self._connected = connected
        self.emit({
            "event_type": "printer_status",
            "connected": connected,
            "timestamp": datetime.now(UTC).isoformat(),
        })

    def _review(self, issue: str) -> None:
        self._usage_issue = issue
        logger.warning("CC1 printer %s: %s", self.printer_id, issue)

    async def _watch_printer(self) -> None:
        while True:
            try:
                async with aclosing(read_status(self._host, self._board_id)) as statuses:
                    async for status in statuses:
                        if isinstance(status, Thumbnail):
                            if status.task_id == self._print_task and not self._print_data.get("thumbnail"):
                                self._preview_worker = asyncio.create_task(self._load_thumbnail(status))
                            continue
                        if isinstance(status, CanvasReport):
                            try:
                                async with self._state_lock:
                                    if self._canvas_connected and status.slots != self._canvas_slots:
                                        await self._sync_canvas_slots(status.slots)
                            except SQLAlchemyError:
                                logger.exception("CC1 printer %s could not save CANVAS status", self.printer_id)
                            continue
                        self._board_id = status.board_id
                        self._connection_error = None
                        self._last_seen = datetime.now(UTC).isoformat()
                        self._set_connected(True)
                        try:
                            async with self._state_lock:
                                if status.canvas_connected is not None and status.canvas_connected != self._canvas_connected:
                                    if not status.canvas_connected:
                                        await self._sync_canvas_slots(())
                                    self._canvas_connected = status.canvas_connected
                                self._update_print_details(status)
                                await self._handle_status(status)
                                self._status = status
                        except SQLAlchemyError:
                            self._review("Could not save status. Retrying when the printer reports again.")
                            logger.exception("CC1 printer %s could not save a status frame", self.printer_id)
            except (OSError, WebSocketException, ValueError) as exc:
                self._connection_error = str(exc)
            except Exception:
                self._connection_error = "Could not save CC1 telemetry."
                logger.exception("CC1 printer %s could not process a status frame", self.printer_id)
            finally:
                self._set_connected(False)
                if self._active_run is not None and self._running:
                    self._review("Connection lost during a print. Check its usage if the end was missed.")
                self._active_run = None
                self._observed_task = None
                self._armed = False
                self._status = None
                self._set_print_task(None)
            await asyncio.sleep(5)

    def _set_print_task(self, task_id: str | None) -> None:
        if task_id != self._print_task:
            if self._preview_worker is not None:
                self._preview_worker.cancel()
                self._preview_worker = None
            self._print_data = {}
            self._print_task = task_id

    def _update_print_details(self, status: Status) -> None:
        ended = status.code in (STOPPED, COMPLETE)
        task_id = status.task_id or self._active_run
        if ended and self._status is not None and self._status.code == status.code:
            task_id = task_id or self._print_task
        self._set_print_task(task_id if status.code != IDLE else None)
        if self._print_task is None:
            return
        if self._canvas_connected:
            self._print_data["uses_canvas"] = True
        for key in ("filename", "elapsed_seconds", "estimated_seconds"):
            value = getattr(status, key)
            if value is not None and (value != 0 or key not in self._print_data):
                self._print_data[key] = value

    async def _load_thumbnail(self, preview: Thumbnail) -> None:
        thumbnail = await fetch_thumbnail(preview.url)
        if thumbnail is None:
            return
        async with self._state_lock:
            if self._print_task == preview.task_id:
                self._print_data["thumbnail"] = thumbnail

    async def _handle_status(self, status: Status) -> None:
        new_task = (
            status.code in (PRINTING, STOPPING) and status.task_id is not None
            and status.task_id != self._observed_task
        )
        changed_task = self._active_run is not None and status.task_id not in (None, self._active_run)
        if changed_task or new_task:
            issue = "A different print appeared before completion was observed."
            if await self._usage.mark_interrupted(status.board_id, keep_task=status.task_id, issue=issue):
                self._review(issue)
            if changed_task:
                self._active_run = None

        if status.code == STOPPING:
            self._armed = False
        if new_task:
            if await self._usage.track(
                status.board_id, status.task_id, self._slot_id, allow_new=self._armed, details=self._print_data,
            ):
                self._active_run = status.task_id
                self._usage_issue = None
            elif status.code == PRINTING:
                self._review("Print was already running when monitoring began or was already handled.")
            self._observed_task = status.task_id
            self._armed = False
            return

        if status.code not in (IDLE, STOPPED, COMPLETE):
            return
        if status.code in (COMPLETE, STOPPED):
            # The tested firmware clears TaskId in the first terminal frame.
            # An anonymous frame can only finish a run seen on this connection.
            new_terminal = self._status is None or self._status.code != status.code
            task_id = self._active_run or (status.task_id if new_terminal else None)
            if task_id:
                queued, issue = await self._usage.finish(
                    status.board_id, task_id, status.extrusion_mm, stopped=status.code == STOPPED,
                    details=self._print_data if self._print_task == task_id else {},
                )
                if queued:
                    self._wake_booking.set()
                if issue:
                    self._review(issue)
            elif self._status is None:
                issue = "Connection resumed after the print ended. Its final extrusion could not be verified."
                # The terminal frame has no TaskId. Keep the binding available
                # if the printer sends the same running TaskId after reconnecting.
                if await self._usage.mark_interrupted(status.board_id, issue=issue, provisional=True):
                    self._review(issue)
        elif self._status is None or self._status.code != IDLE:
            issue = "Printer returned to idle before a Complete or Stopped frame."
            if await self._usage.mark_interrupted(status.board_id, issue=issue, provisional=self._status is None):
                self._review(issue)
        self._active_run = None
        self._observed_task = None
        self._armed = True

    async def _book_usage(self) -> None:
        while True:
            self._wake_booking.clear()
            try:
                async with self._booking_lock:
                    issue = await self._usage.book_pending()
                if issue:
                    self._review(issue)
            except Exception:
                self._review("Usage could not be saved yet. FilaMan will retry.")
                logger.exception("CC1 printer %s could not book usage", self.printer_id)
            try:
                await asyncio.wait_for(self._wake_booking.wait(), timeout=15)
            except TimeoutError:
                pass
