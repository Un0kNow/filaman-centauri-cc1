"""Store run-to-spool bindings and retry completed usage safely."""

from asyncio import Lock
from datetime import UTC, datetime
from math import isfinite, pi

from sqlalchemy import JSON, Column, Float, ForeignKey, Integer, MetaData, String, Table, Text, and_, func, or_, select, tuple_, update
from sqlalchemy.exc import IntegrityError

from app.core.database import async_session_maker, engine
from app.core.event_bus import event_bus
from app.models.printer import PrinterSlotAssignment
from app.models.spool import Spool, SpoolEvent
from app.services.spool_service import SpoolService

from .sdcp import BOARD_ID, MAX_THUMBNAIL_BYTES


metadata = MetaData()
runs = Table(
    "centauri_cc1_runs", metadata,
    Column("board_id", String(32), primary_key=True),
    Column("task_id", String(128), primary_key=True),
    Column("printer_id", Integer, nullable=False),
    Column("spool_id", Integer, ForeignKey(Spool.id, ondelete="SET NULL")),
    Column("grams_per_mm", Float),
    Column("state", String(16), nullable=False),
    Column("grams", Float),
    Column("completed_at", String(40)),
    Column("issue", String(200)),
)
# Keep the table name from 0.1.3 so pending bookings survive an upgrade.
run_details = Table(
    "centauri_cc1_pending_details", metadata,
    Column("board_id", String(32), primary_key=True),
    Column("task_id", String(128), primary_key=True),
    Column("details", JSON, nullable=False),
)
usage_previews = Table(
    "centauri_cc1_usage_previews", metadata,
    Column("event_id", Integer, ForeignKey(SpoolEvent.id, ondelete="CASCADE"), primary_key=True),
    Column("thumbnail", Text, nullable=False),
)
DETAILS_KEY = and_(runs.c.board_id == run_details.c.board_id, runs.c.task_id == run_details.c.task_id)
UNBOOKED_PAGE_SIZE = 30
ACKNOWLEDGED_AT = run_details.c.details["review_acknowledged_at"].as_string()
NEEDS_REVIEW = or_(runs.c.state == "review", and_(runs.c.state == "tracking", runs.c.issue.is_not(None)))
OPEN_REVIEW = and_(NEEDS_REVIEW, ACKNOWLEDGED_AT.is_(None))
EVENT_KEY_PREFIX = "cc1-sdcp:"
PREVIEW_PREFIX = "data:image/png;base64,"
MAX_PREVIEW_LENGTH = len(PREVIEW_PREFIX) + 4 * ((MAX_THUMBNAIL_BYTES + 2) // 3)
_initialize_lock = Lock()


async def initialize_usage() -> None:
    async with _initialize_lock:
        async with engine.begin() as conn:
            await conn.run_sync(metadata.create_all)
        await _move_legacy_previews()


async def _store_event_details(db, event: SpoolEvent, details: dict | None = None, *, printer_id: int | None = None) -> None:
    meta = dict(event.meta or {})
    print_details = {**(meta.get("cc1_print") or {}), **(details or {})}
    thumbnail = print_details.pop("thumbnail", None)
    print_details.pop("review_acknowledged_at", None)
    if isinstance(thumbnail, str) and thumbnail.startswith(PREVIEW_PREFIX) and len(thumbnail) <= MAX_PREVIEW_LENGTH:
        event_id = event.id
        saved = await db.scalar(select(usage_previews.c.event_id).where(usage_previews.c.event_id == event_id))
        if saved is None:
            await db.execute(usage_previews.insert().values(event_id=event_id, thumbnail=thumbnail))
        meta["cc1_has_preview"] = True
    if print_details or "cc1_print" in meta:
        meta["cc1_print"] = print_details
    if printer_id is not None:
        meta["cc1_printer_id"] = printer_id
    event.meta = meta


async def _move_legacy_previews() -> None:
    last_id = 0
    while True:
        async with async_session_maker.begin() as db:
            events = (await db.scalars(select(SpoolEvent).where(
                SpoolEvent.id > last_id, SpoolEvent.source == "centauri_cc1",
                SpoolEvent.event_type == "print_consumption",
                SpoolEvent.meta["cc1_print"]["thumbnail"].as_string().is_not(None),
            ).order_by(SpoolEvent.id).limit(30).with_for_update())).all()
            if not events:
                return
            for event in events:
                await _store_event_details(db, event)
            last_id = events[-1].id


async def booked_previews(event_ids: list[int]) -> dict[int, str]:
    if (not isinstance(event_ids, list) or len(event_ids) > 30
            or any(type(id_) is not int or not 1 <= id_ <= 2**63 - 1 for id_ in event_ids)):
        raise ValueError("Select up to 30 valid usage events.")
    if not event_ids:
        return {}
    async with async_session_maker() as db:
        rows = await db.execute(select(usage_previews.c.event_id, usage_previews.c.thumbnail).where(
            usage_previews.c.event_id.in_(event_ids),
        ))
        return dict(rows.tuples().all())


def _grams_per_mm(spool: Spool | None) -> float | None:
    if spool is None or spool.filament is None:
        return None
    diameter = spool.filament.diameter_mm
    density = spool.filament.density_g_cm3
    if any(value is None or not isfinite(value) or value <= 0 for value in (diameter, density)):
        return None
    factor = pi * (diameter / 2) * (diameter / 2) * density / 1000
    return factor if isfinite(factor) and factor > 0 else None


def _measured_extrusion(details: dict) -> float | None:
    mm = details.get("extrusion_mm")
    if (not details.get("uses_canvas") and details.get("outcome") in ("completed", "canceled")
            and type(mm) in (int, float) and 0 < mm < float("inf")):
        return mm
    return None


def _event_key(board_id: str, task_id: str) -> str:
    return f"{EVENT_KEY_PREFIX}{board_id}:{task_id}"


async def usage_printers(event_keys: list[str]) -> dict[str, int]:
    if not isinstance(event_keys, list) or len(event_keys) > 30:
        raise ValueError("Select up to 30 usage events.")
    jobs = set()
    for key in event_keys:
        if not isinstance(key, str) or not key.startswith(EVENT_KEY_PREFIX):
            raise ValueError("Invalid CC1 usage event key.")
        board_id, _, task_id = key[len(EVENT_KEY_PREFIX):].partition(":")
        if not BOARD_ID.fullmatch(board_id) or not 1 <= len(task_id) <= 128:
            raise ValueError("Invalid CC1 usage event key.")
        jobs.add((board_id, task_id))
    if not jobs:
        return {}
    async with async_session_maker() as db:
        rows = await db.execute(select(
            runs.c.board_id, runs.c.task_id, runs.c.printer_id,
        ).where(tuple_(runs.c.board_id, runs.c.task_id).in_(jobs)))
        return {_event_key(board, task): printer for board, task, printer in rows}


class Usage:
    def __init__(self, printer_id: int):
        self.printer_id = printer_id

    def _run(self, board_id: str, task_id: str):
        return (
            runs.c.board_id == board_id,
            runs.c.task_id == task_id,
            runs.c.printer_id == self.printer_id,
        )

    async def track(
        self, board_id: str, task_id: str, slot_id: int, *, allow_new: bool, details: dict,
    ) -> bool:
        # The board/task key also protects against configuring the physical CC1 twice.
        key = (runs.c.board_id == board_id, runs.c.task_id == task_id)
        query = select(runs.c.printer_id, runs.c.state).where(*key)
        async with async_session_maker() as db:
            existing = (await db.execute(query.add_columns(runs.c.issue))).one_or_none()
            if existing is not None:
                tracking = (existing.printer_id, existing.state) == (self.printer_id, "tracking")
                if tracking and (details or existing.issue):
                    await self._save_details(db, board_id, task_id, {**details, "review_acknowledged_at": None})
                    if existing.issue:
                        await db.execute(update(runs).where(*key).values(issue=None, completed_at=None))
                    await db.commit()
                return tracking
            if not allow_new:
                return False

            assignment = await db.get(PrinterSlotAssignment, slot_id)
            spool = None
            if assignment is not None and assignment.spool_id is not None:
                spool = await SpoolService(db).get_spool(assignment.spool_id)
            try:
                await db.execute(runs.insert().values(
                    board_id=board_id,
                    task_id=task_id,
                    printer_id=self.printer_id,
                    spool_id=spool.id if spool is not None else None,
                    grams_per_mm=_grams_per_mm(spool),
                    state="tracking",
                ))
                if details:
                    await self._save_details(db, board_id, task_id, details)
                await db.commit()
                return True
            except IntegrityError:
                await db.rollback()
                existing = (await db.execute(query)).one_or_none()
                if existing is None:
                    raise
                return existing == (self.printer_id, "tracking")

    async def finish(
        self, board_id: str, task_id: str, extrusion_mm: float | None, *, stopped: bool, details: dict,
    ) -> tuple[bool, str | None]:
        key = self._run(board_id, task_id)
        async with async_session_maker() as db:
            run = (await db.execute(
                select(runs, run_details.c.details).outerjoin(run_details, DETAILS_KEY)
                .where(*key, runs.c.state == "tracking")
            )).mappings().one_or_none()
            if run is None:
                return False, None

            issue = None
            grams = None
            state = "pending"
            uses_canvas = bool(details.get("uses_canvas") or (run["details"] or {}).get("uses_canvas"))
            if stopped and extrusion_mm == 0:
                state = "stopped"
            elif uses_canvas:
                issue = "CANVAS reported total extrusion. Usage for individual spools could not be verified."
            elif run["spool_id"] is None:
                issue = "No spool was assigned at the start of the print."
            elif run["grams_per_mm"] is None:
                issue = "The assigned spool needs a filament diameter and density."
            elif extrusion_mm is None or extrusion_mm <= 0:
                issue = "Terminal frame has no positive total extrusion."
            else:
                grams = extrusion_mm * run["grams_per_mm"]
                if not isfinite(grams) or grams <= 0:
                    issue = "The reported extrusion could not be converted to grams."
                    grams = None
            result = await db.execute(
                update(runs).where(*key, runs.c.state == "tracking").values(
                    state="review" if issue else state,
                    grams=grams,
                    spool_id=None if uses_canvas else run["spool_id"],
                    completed_at=datetime.now(UTC).isoformat(),
                    issue=issue,
                )
            )
            if result.rowcount != 1:
                return False, None
            if uses_canvas:
                details = {**details, "uses_canvas": True}
            await self._save_details(db, board_id, task_id, {
                **details, "outcome": "canceled" if stopped else "completed", "extrusion_mm": extrusion_mm,
                "review_acknowledged_at": None,
            })
            await db.commit()
            return state == "pending" and issue is None, issue

    @staticmethod
    async def _save_details(db, board_id: str, task_id: str, details: dict) -> None:
        key = (run_details.c.board_id == board_id, run_details.c.task_id == task_id)
        existing = await db.scalar(select(run_details.c.details).where(*key))
        if existing is None:
            await db.execute(run_details.insert().values(board_id=board_id, task_id=task_id, details=details))
        else:
            await db.execute(update(run_details).where(*key).values(details={**existing, **details}))

    async def mark_interrupted(
        self, board_id: str, *, keep_task: str | None = None, issue: str, provisional: bool = False,
    ) -> bool:
        query = update(runs).where(
            runs.c.board_id == board_id, runs.c.printer_id == self.printer_id, runs.c.state == "tracking",
        )
        if keep_task is not None:
            query = query.where(runs.c.task_id != keep_task)
        async with async_session_maker.begin() as db:
            result = await db.execute(query.values(
                state="tracking" if provisional else "review", issue=issue, completed_at=datetime.now(UTC).isoformat(),
            ))
            return result.rowcount > 0

    async def review_count(self) -> int:
        async with async_session_maker() as db:
            result = await db.execute(
                select(func.count())
                .select_from(runs.outerjoin(run_details, DETAILS_KEY))
                .where(runs.c.printer_id == self.printer_id, OPEN_REVIEW)
            )
            return result.scalar_one()

    async def latest_receipt(self) -> dict | None:
        latest = (
            select(runs, run_details.c.details).outerjoin(run_details, DETAILS_KEY)
            .where(
                runs.c.printer_id == self.printer_id,
                or_(runs.c.state.in_(("pending", "booked", "reviewed", "stopped")), NEEDS_REVIEW),
            )
            .order_by(runs.c.completed_at.is_(None), runs.c.completed_at.desc(), runs.c.board_id, runs.c.task_id)
            .limit(1)
            .subquery()
        )
        event_key = EVENT_KEY_PREFIX + latest.c.board_id + ":" + latest.c.task_id
        async with async_session_maker() as db:
            matches = (await db.execute(
                select(latest, SpoolEvent).outerjoin(SpoolEvent, and_(
                    SpoolEvent.source == "centauri_cc1", SpoolEvent.event_type == "print_consumption",
                    SpoolEvent.meta["source_event_key"].as_string() == event_key,
                )).limit(2)
            )).mappings().all()
        if not matches:
            return None

        run = matches[0]
        event = run[SpoolEvent]
        details = run["details"] or {}
        receipt = {
            "state": run["state"], "filename": details.get("filename"),
            "print_at": run["completed_at"], "issue": run["issue"],
        }
        if event is None:
            if run["state"] == "booked":
                return {**receipt, "state": "unavailable", "issue": "FilaMan has no matching deduction for this print."}
            if details.get("review_acknowledged_at"):
                receipt["state"] = "reviewed"
            elif run["state"] == "tracking":
                receipt["state"] = "review"
            return receipt
        if len(matches) > 1:
            return {**receipt, "state": "unavailable", "issue": "Multiple FilaMan deductions match this print."}
        delta = event.delta_weight_g
        if delta is None or not isfinite(delta) or delta >= 0:
            return {**receipt, "state": "unavailable", "issue": "The FilaMan deduction has no valid weight."}
        event_details = (event.meta or {}).get("cc1_print") or {}
        return {
            "state": "booked", "filename": event_details.get("filename") or details.get("filename"),
            "print_at": event.event_at.isoformat(), "booked_at": event.created_at.isoformat(),
            "spool_id": event.spool_id, "grams": -delta, "event_id": event.id,
            "manual": event_details.get("resolution") == "manual",
        }

    async def unbooked_prints(self, state: str, page: int) -> dict:
        if state not in ("review", "reviewed", "stopped") or type(page) is not int or not 1 <= page <= 100_000:
            raise ValueError("Select a valid history view and page")
        condition = runs.c.state == state
        if state == "review":
            condition = OPEN_REVIEW
        elif state == "reviewed":
            condition = or_(condition, and_(NEEDS_REVIEW, ACKNOWLEDGED_AT.is_not(None)))
        async with async_session_maker() as db:
            rows = (await db.execute(
                select(runs, run_details.c.details)
                .outerjoin(run_details, DETAILS_KEY)
                .where(runs.c.printer_id == self.printer_id, condition)
                .order_by(runs.c.completed_at.is_(None), runs.c.completed_at.desc(), runs.c.board_id, runs.c.task_id)
                .offset((page - 1) * UNBOOKED_PAGE_SIZE).limit(UNBOOKED_PAGE_SIZE + 1)
            )).mappings().all()
        return {
            "items": [{
                "id": f"{row['board_id']}:{row['task_id']}", "spool_id": row["spool_id"],
                "event_at": row["completed_at"], "state": state, "issue": row["issue"],
                "can_deduct": row["state"] == "review" and _measured_extrusion(row["details"] or {}) is not None,
                "details": row["details"] or ({"extrusion_mm": 0} if state == "stopped" else {}),
            } for row in rows[:UNBOOKED_PAGE_SIZE]],
            "has_more": len(rows) > UNBOOKED_PAGE_SIZE,
        }

    async def resolve_review(self, run_id: str, spool_id: int | None) -> dict:
        board, _, task = run_id.partition(":")
        key = self._run(board, task)
        async with async_session_maker.begin() as db:
            run = (await db.execute(
                select(runs, run_details.c.details).outerjoin(run_details, DETAILS_KEY).where(*key)
            )).mappings().one_or_none()
            if run is None:
                raise ValueError("Print record not found for this printer.")
            state = run["state"]
            details = run["details"] or {}
            if state in ("pending", "booked"):
                return {"status": state}
            if state == "reviewed" or details.get("review_acknowledged_at"):
                return {"status": "reviewed"}
            if state != "review" and not (state == "tracking" and run["issue"]):
                raise ValueError("This print does not need review.")

            # Recover the booking if FilaMan saved it before the driver could
            # update the run. Check across spools before changing the binding.
            existing = await db.scalar(select(SpoolEvent).where(
                SpoolEvent.source == "centauri_cc1", SpoolEvent.event_type == "print_consumption",
                SpoolEvent.meta["source_event_key"].as_string() == _event_key(board, task),
            ).limit(1))
            now = datetime.now(UTC).isoformat()
            if existing is not None:
                await _store_event_details(db, existing, details, printer_id=self.printer_id)
                await db.execute(update(runs).where(*key).values(state="booked", spool_id=existing.spool_id, issue=None))
                await db.execute(run_details.delete().where(run_details.c.board_id == board, run_details.c.task_id == task))
                return {"status": "booked"}
            if spool_id is None:
                await self._save_details(db, board, task, {"review_acknowledged_at": now})
                if state == "review":
                    await db.execute(update(runs).where(*key).values(state="reviewed"))
                return {"status": "reviewed"}

            if details.get("uses_canvas"):
                raise ValueError("A confirmed extrusion reading for each CANVAS spool is required to deduct filament.")
            mm = _measured_extrusion(details)
            if state != "review" or mm is None:
                raise ValueError("A confirmed final extrusion reading is required to deduct filament.")
            spool = await SpoolService(db).get_spool(spool_id)
            if spool is None or spool.remaining_weight_g is None or not isfinite(spool.remaining_weight_g):
                raise ValueError("The selected spool needs a recorded remaining weight.")
            factor = run["grams_per_mm"] if spool.id == run["spool_id"] else None
            factor = factor or _grams_per_mm(spool)
            if factor is None or not 0 < mm * factor < float("inf"):
                raise ValueError("The selected spool needs a valid filament diameter and density.")
            await self._save_details(db, board, task, {"resolution": "manual", "resolved_at": now})
            await db.execute(update(runs).where(*key).values(
                state="pending", spool_id=spool.id, grams=mm * factor, grams_per_mm=factor, issue=None,
            ))
            return {"status": "pending"}

    async def book_pending(self) -> str | None:
        async with async_session_maker() as db:
            pending = (await db.execute(select(runs).where(
                runs.c.printer_id == self.printer_id, runs.c.state == "pending",
            ))).mappings().all()
        last_issue = None
        for run in pending:
            async with async_session_maker() as db:
                board_id, task_id = run["board_id"], run["task_id"]
                detail_key = (run_details.c.board_id == board_id, run_details.c.task_id == task_id)
                details = await db.scalar(select(run_details.c.details).where(*detail_key))
                service = SpoolService(db)
                spool_id = run["spool_id"]
                spool = await service.get_spool(spool_id) if spool_id is not None else None
                if spool is None or spool.remaining_weight_g is None:
                    issue = "The assigned spool was removed or has no remaining weight."
                    last_issue = issue
                else:
                    issue = None
                    # The binding and grams were committed before booking. FilaMan
                    # deduplicates retries on the same spool, even after a restart.
                    outcome = details.get("outcome") if details else None
                    kind = outcome if outcome in ("completed", "canceled") else "print"
                    note = f"CC1 {kind} | {details.get('filename') or 'Unknown print'}" if details else None
                    event, _ = await service.record_consumption(
                        spool=spool,
                        delta_weight_g=-run["grams"],
                        event_at=datetime.fromisoformat(run["completed_at"]),
                        source="centauri_cc1",
                        source_event_key=_event_key(board_id, task_id),
                        note=note,
                    )
                    await _store_event_details(db, event, details, printer_id=self.printer_id)
                await db.execute(update(runs).where(
                    *self._run(board_id, task_id), runs.c.state == "pending",
                ).values(state="review" if issue else "booked", issue=issue))
                if details and issue is None:
                    await db.execute(run_details.delete().where(*detail_key))
                await db.commit()
            if issue is None:
                await event_bus.publish({"event": "spools_changed"})
        return last_issue
