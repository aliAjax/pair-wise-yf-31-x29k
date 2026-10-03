#!/usr/bin/env python3
"""Airline disruption recovery engine using standard-library SQLite and HTTP."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, time, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

PORT = 8202
ROLES = {"viewer", "scheduler", "ops_manager", "auditor"}


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, details: Any = None):
        super().__init__(message)
        self.status, self.code, self.message, self.details = status, code, message, details


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime | None = None) -> str:
    return (value or utcnow()).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_time(value: str | None) -> datetime:
    if not value:
        raise ApiError(400, "time_required", "必须提供 ISO 8601 时间")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ApiError(400, "invalid_time", f"时间格式错误: {value}") from exc
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def parse_clock(value: str) -> time:
    try:
        return time.fromisoformat(value)
    except ValueError as exc:
        raise ApiError(400, "invalid_clock", f"时刻格式应为 HH:MM: {value}") from exc


def overlaps(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:
    return a_start < b_end and b_start < a_end


class Repository:
    def __init__(self, db_path: str | Path):
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._init()

    @contextmanager
    def tx(self):
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

class Repository:
    def __init__(self, db_path: str | Path):
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._init()

    @contextmanager
    def tx(self):
        # 进程内串行化写事务；跨进程/跨实例时由 active 延期上的部分唯一索引兜底
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield self.conn
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    @staticmethod
    def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
        return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}

    def _init(self) -> None:
        with self._lock:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS airports(code TEXT PRIMARY KEY, country TEXT NOT NULL, curfew_start TEXT NOT NULL, curfew_end TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS aircraft(id TEXT PRIMARY KEY, model TEXT NOT NULL, maintenance_due TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active');
                CREATE TABLE IF NOT EXISTS crew(id TEXT PRIMARY KEY, name TEXT NOT NULL, base TEXT NOT NULL, duty_start TEXT NOT NULL, max_duty_minutes INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'active');
                CREATE TABLE IF NOT EXISTS permits(id INTEGER PRIMARY KEY AUTOINCREMENT, origin TEXT NOT NULL, destination TEXT NOT NULL, valid_from TEXT NOT NULL, valid_to TEXT NOT NULL, curfew_exempt INTEGER NOT NULL DEFAULT 0, UNIQUE(origin,destination,valid_from,valid_to));
                CREATE TABLE IF NOT EXISTS flights(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, flight_no TEXT NOT NULL UNIQUE, origin TEXT NOT NULL, destination TEXT NOT NULL,
                    std TEXT NOT NULL, sta TEXT NOT NULL, aircraft_id TEXT NOT NULL REFERENCES aircraft(id), crew_id TEXT NOT NULL REFERENCES crew(id),
                    passenger_count INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'scheduled', delay_minutes INTEGER NOT NULL DEFAULT 0,
                    revision INTEGER NOT NULL DEFAULT 1, cancel_reason TEXT, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS disruptions(id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, resource_id TEXT NOT NULL, starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active', created_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS recovery_plans(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, disruption_id INTEGER NOT NULL REFERENCES disruptions(id), name TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'draft', revision INTEGER NOT NULL DEFAULT 1, score_json TEXT, metrics_json TEXT,
                    created_by TEXT NOT NULL, created_at TEXT NOT NULL, locked_at TEXT, locked_by TEXT
                );
                CREATE TABLE IF NOT EXISTS assignments(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id INTEGER NOT NULL REFERENCES recovery_plans(id) ON DELETE CASCADE,
                    flight_id INTEGER NOT NULL REFERENCES flights(id), aircraft_id TEXT NOT NULL REFERENCES aircraft(id), crew_id TEXT NOT NULL REFERENCES crew(id),
                    new_std TEXT NOT NULL, new_sta TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'planned', delay_minutes INTEGER NOT NULL DEFAULT 0,
                    missed_connections INTEGER NOT NULL DEFAULT 0, UNIQUE(plan_id,flight_id)
                );
                CREATE TABLE IF NOT EXISTS audit_log(id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id INTEGER, actor TEXT NOT NULL, role TEXT NOT NULL, action TEXT NOT NULL, detail_json TEXT NOT NULL, created_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS maintenance_deferrals(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, aircraft_id TEXT NOT NULL REFERENCES aircraft(id),
                    valid_from TEXT NOT NULL, valid_to TEXT NOT NULL, reason TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'active', revision INTEGER NOT NULL DEFAULT 1,
                    created_by TEXT NOT NULL, created_at TEXT NOT NULL,
                    expired_at TEXT, expired_by TEXT
                );
                CREATE TABLE IF NOT EXISTS flight_receipts(
                    flight_id INTEGER PRIMARY KEY REFERENCES flights(id), flight_no TEXT NOT NULL,
                    aircraft_id TEXT NOT NULL, crew_id TEXT NOT NULL, std TEXT NOT NULL, sta TEXT NOT NULL,
                    landed_at TEXT NOT NULL, deferral_id INTEGER REFERENCES maintenance_deferrals(id),
                    detail_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS reschedule_queue(
                    aircraft_id TEXT PRIMARY KEY REFERENCES aircraft(id), flight_ids_json TEXT NOT NULL DEFAULT '[]',
                    updated_at TEXT NOT NULL
                );
                """
            )
            # 同一架飞机至多一个 active 延期放行——两人并发申请的最终防线
            self.conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_deferral_active ON maintenance_deferrals(aircraft_id) WHERE status='active'")
            # 旧库补列
            flight_cols = self._columns(self.conn, "flights")
            if "deferral_id" not in flight_cols:
                self.conn.execute("ALTER TABLE flights ADD COLUMN deferral_id INTEGER REFERENCES maintenance_deferrals(id)")
            assignment_cols = self._columns(self.conn, "assignments")
            if "invalid_reason" not in assignment_cols:
                self.conn.execute("ALTER TABLE assignments ADD COLUMN invalid_reason TEXT")

    @staticmethod
    def audit(conn: sqlite3.Connection, plan_id: int | None, actor: str, role: str, action: str, detail: dict[str, Any]) -> None:
        conn.execute("INSERT INTO audit_log(plan_id,actor,role,action,detail_json,created_at) VALUES(?,?,?,?,?,?)",
                     (plan_id, actor, role, action, json.dumps(detail, ensure_ascii=False, sort_keys=True), iso()))


class AirlineRecoveryService:
    def __init__(self, db_path: str | Path):
        self.repo = Repository(db_path)

    @staticmethod
    def identity(headers: Any) -> tuple[str, str]:
        actor, role = headers.get("X-User-Id", "").strip(), headers.get("X-Role", "").strip()
        if not actor or role not in ROLES:
            raise ApiError(401, "unauthorized", "需要 X-User-Id 和有效 X-Role")
        return actor, role

    @staticmethod
    def _dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
        return dict(row) if row else None

    def seed_airport(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager":
            raise ApiError(403, "seed_forbidden", "只有运行经理可以维护机场数据")
        code = str(body.get("code", "")).upper().strip()
        country = str(body.get("country", "")).upper().strip()
        if not code or not country:
            raise ApiError(400, "missing_fields", "code 和 country 必填")
        start = str(body.get("curfew_start", "23:00")); end = str(body.get("curfew_end", "06:00"))
        parse_clock(start); parse_clock(end)
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO airports(code,country,curfew_start,curfew_end) VALUES(?,?,?,?)", (code, country, start, end))
            return dict(conn.execute("SELECT * FROM airports WHERE code=?", (code,)).fetchone())

    def seed_aircraft(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager": raise ApiError(403, "seed_forbidden", "只有运行经理可以维护飞机")
        ident, model = str(body.get("id", "")).strip(), str(body.get("model", "")).strip()
        if not ident or not model: raise ApiError(400, "missing_fields", "id 和 model 必填")
        due = iso(parse_time(body.get("maintenance_due")))
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO aircraft(id,model,maintenance_due,status) VALUES(?,?,?,?)", (ident, model, due, body.get("status", "active")))
            return dict(conn.execute("SELECT * FROM aircraft WHERE id=?", (ident,)).fetchone())

    def seed_crew(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager": raise ApiError(403, "seed_forbidden", "只有运行经理可以维护机组")
        ident, name, base = str(body.get("id", "")).strip(), str(body.get("name", "")).strip(), str(body.get("base", "")).upper().strip()
        duty = parse_time(body.get("duty_start")); maximum = body.get("max_duty_minutes")
        if not ident or not name or not base or not isinstance(maximum, int) or maximum <= 0:
            raise ApiError(400, "invalid_crew", "id、name、base 和正整数 max_duty_minutes 必填")
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO crew(id,name,base,duty_start,max_duty_minutes,status) VALUES(?,?,?,?,?,?)", (ident, name, base, iso(duty), maximum, body.get("status", "active")))
            return dict(conn.execute("SELECT * FROM crew WHERE id=?", (ident,)).fetchone())

    def create_permit(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager": raise ApiError(403, "permit_forbidden", "只有运行经理可以维护航线许可")
        origin, destination = str(body.get("origin", "")).upper(), str(body.get("destination", "")).upper()
        if not origin or not destination: raise ApiError(400, "missing_fields", "origin 和 destination 必填")
        valid_from, valid_to = parse_time(body.get("valid_from")), parse_time(body.get("valid_to"))
        if valid_to <= valid_from: raise ApiError(400, "invalid_permit", "许可结束时间必须晚于开始时间")
        with self.repo.tx() as conn:
            try:
                cur = conn.execute("INSERT INTO permits(origin,destination,valid_from,valid_to,curfew_exempt) VALUES(?,?,?,?,?)",
                                   (origin, destination, iso(valid_from), iso(valid_to), int(bool(body.get("curfew_exempt")))))
            except sqlite3.IntegrityError as exc:
                raise ApiError(409, "permit_exists", "相同航线与有效期的许可已存在") from exc
            return dict(conn.execute("SELECT * FROM permits WHERE id=?", (cur.lastrowid,)).fetchone())

    def create_flight(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "flight_forbidden", "当前角色不能创建航班")
        required = ("flight_no", "origin", "destination", "std", "sta", "aircraft_id", "crew_id")
        if any(not body.get(k) for k in required): raise ApiError(400, "missing_fields", f"缺少字段: {', '.join(k for k in required if not body.get(k))}")
        std, sta = parse_time(body["std"]), parse_time(body["sta"])
        if sta <= std: raise ApiError(400, "invalid_times", "到达时间必须晚于起飞时间")
        passengers = body.get("passenger_count", 0)
        if not isinstance(passengers, int) or passengers < 0: raise ApiError(400, "invalid_passengers", "passenger_count 必须是非负整数")
        with self.repo.tx() as conn:
            for table, ident in (("aircraft", body["aircraft_id"]), ("crew", body["crew_id"])):
                row = conn.execute(f"SELECT status FROM {table} WHERE id=?", (ident,)).fetchone()
                if not row or row["status"] != "active": raise ApiError(409, "resource_unavailable", f"{table} {ident} 不可用")
            # 维护间隔：到达时间不得晚于维护到期点，除非有生效延期放行覆盖该航段
            aircraft_row = conn.execute("SELECT maintenance_due FROM aircraft WHERE id=?", (body["aircraft_id"],)).fetchone()
            covering = None
            if parse_time(aircraft_row["maintenance_due"]) < sta:
                covering = self._active_deferral(conn, body["aircraft_id"], iso(sta))
                if not covering:
                    raise ApiError(409, "maintenance_due", f"飞机 {body['aircraft_id']} 维护间隔不足且没有延期放行覆盖该航段")
            try:
                cur = conn.execute("""INSERT INTO flights(flight_no,origin,destination,std,sta,aircraft_id,crew_id,passenger_count,deferral_id,updated_at)
                                      VALUES(?,?,?,?,?,?,?,?,?,?)""",
                                   (body["flight_no"].upper(), body["origin"].upper(), body["destination"].upper(), iso(std), iso(sta),
                                    body["aircraft_id"], body["crew_id"], passengers, covering["id"] if covering else None, iso()))
            except sqlite3.IntegrityError as exc:
                raise ApiError(409, "flight_exists", "航班号已存在") from exc
            return dict(conn.execute("SELECT * FROM flights WHERE id=?", (cur.lastrowid,)).fetchone())

    def create_disruption(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "disruption_forbidden", "当前角色不能登记中断")
        kind, resource = str(body.get("kind", "")).strip(), str(body.get("resource_id", "")).strip()
        if kind not in {"airport_closure", "aircraft_fault", "crew_timeout"} or not resource:
            raise ApiError(400, "invalid_disruption", "kind 或 resource_id 无效")
        start, end = parse_time(body.get("starts_at")), parse_time(body.get("ends_at"))
        if end <= start: raise ApiError(400, "invalid_times", "中断结束时间必须晚于开始时间")
        with self.repo.tx() as conn:
            cur = conn.execute("INSERT INTO disruptions(kind,resource_id,starts_at,ends_at,created_at) VALUES(?,?,?,?,?)",
                               (kind, resource.upper() if kind == "airport_closure" else resource, iso(start), iso(end), iso()))
            return dict(conn.execute("SELECT * FROM disruptions WHERE id=?", (cur.lastrowid,)).fetchone())

    def create_plan(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "plan_forbidden", "当前角色不能创建恢复方案")
        disruption_id, name = body.get("disruption_id"), str(body.get("name", "")).strip()
        assignments = body.get("assignments", [])
        if not isinstance(disruption_id, int) or not name or not isinstance(assignments, list):
            raise ApiError(400, "invalid_plan", "disruption_id、name 和 assignments 必填")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM disruptions WHERE id=?", (disruption_id,)).fetchone():
                raise ApiError(404, "disruption_not_found", "中断事件不存在")
            cur = conn.execute("INSERT INTO recovery_plans(disruption_id,name,created_by,created_at) VALUES(?,?,?,?)", (disruption_id, name, actor, iso()))
            plan_id = cur.lastrowid
            for item in assignments:
                self._insert_assignment(conn, plan_id, item, replace=False)
            Repository.audit(conn, plan_id, actor, role, "plan_created", {"disruption_id": disruption_id, "assignment_count": len(assignments)})
            return self.get_plan(plan_id)

    def _insert_assignment(self, conn: sqlite3.Connection, plan_id: int, item: dict[str, Any], replace: bool) -> None:
        required = ("flight_id", "aircraft_id", "crew_id", "new_std", "new_sta")
        if any(item.get(k) in (None, "") for k in required): raise ApiError(400, "invalid_assignment", f"飞行调整缺少字段: {', '.join(k for k in required if item.get(k) in (None, ''))}")
        plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
        if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
        if plan["status"] != "draft": raise ApiError(409, "plan_locked", "已锁定方案不能修改")
        std, sta = parse_time(item["new_std"]), parse_time(item["new_sta"])
        if sta <= std: raise ApiError(400, "invalid_times", "新到达时间必须晚于新起飞时间")
        flight = conn.execute("SELECT * FROM flights WHERE id=?", (item["flight_id"],)).fetchone()
        if not flight: raise ApiError(404, "flight_not_found", "航班不存在")
        if flight["status"] == "canceled" and item.get("status", "planned") != "canceled":
            raise ApiError(409, "canceled_flight", "已取消航班不能安排执行")
        delay = int((std - parse_time(flight["std"])).total_seconds() // 60)
        missed = int(item.get("missed_connections", 0))
        if missed < 0: raise ApiError(400, "invalid_connections", "missed_connections 不能为负")
        try:
            if replace:
                conn.execute("""UPDATE assignments SET aircraft_id=?,crew_id=?,new_std=?,new_sta=?,status=?,delay_minutes=?,missed_connections=?,invalid_reason=NULL
                                WHERE plan_id=? AND flight_id=?""",
                             (item["aircraft_id"], item["crew_id"], iso(std), iso(sta), item.get("status", "planned"), delay, missed, plan_id, item["flight_id"]))
                if conn.execute("SELECT changes()").fetchone()[0] == 0:
                    raise KeyError
            else:
                conn.execute("""INSERT INTO assignments(plan_id,flight_id,aircraft_id,crew_id,new_std,new_sta,status,delay_minutes,missed_connections)
                                VALUES(?,?,?,?,?,?,?,?,?)""",
                             (plan_id, item["flight_id"], item["aircraft_id"], item["crew_id"], iso(std), iso(sta), item.get("status", "planned"), delay, missed))
        except sqlite3.IntegrityError as exc:
            raise ApiError(409, "assignment_conflict", "方案中该航班已存在或资源无效") from exc
        except KeyError as exc:
            raise ApiError(404, "assignment_not_found", "待替换的航班调整不存在") from exc

    def add_assignment(self, plan_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "assignment_forbidden", "当前角色不能修改方案")
        expected = body.get("expected_revision")
        if not isinstance(expected, int): raise ApiError(400, "revision_required", "expected_revision 必须是整数")
        with self.repo.tx() as conn:
            plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
            if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
            if plan["status"] != "draft": raise ApiError(409, "plan_locked", "已锁定方案不能修改")
            if plan["revision"] != expected: raise ApiError(409, "revision_conflict", "方案已被其他人更新")
            self._insert_assignment(conn, plan_id, body, replace=True)
            conn.execute("UPDATE recovery_plans SET revision=revision+1 WHERE id=?", (plan_id,))
            Repository.audit(conn, plan_id, actor, role, "assignment_reassigned", {"flight_id": body.get("flight_id")})
            return self.get_plan(plan_id)

    def _validate_plan(self, conn: sqlite3.Connection, plan_id: int) -> list[dict[str, Any]]:
        plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
        if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
        rows = [dict(r) for r in conn.execute("""SELECT a.*, f.flight_no, f.origin, f.destination, f.passenger_count, f.status flight_status
                                                  FROM assignments a JOIN flights f ON f.id=a.flight_id WHERE a.plan_id=? ORDER BY a.new_std""", (plan_id,))]
        if not rows: raise ApiError(409, "empty_plan", "方案没有飞行调整")
        problems: list[dict[str, Any]] = []
        by_aircraft: dict[str, list[dict[str, Any]]] = {}
        by_crew: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            if row["status"] == "canceled": continue
            if row["invalid_reason"]:
                problems.append({"assignment_id": row["id"], "code": row["invalid_reason"]})
                continue
            std, sta = parse_time(row["new_std"]), parse_time(row["new_sta"])
            aircraft = conn.execute("SELECT * FROM aircraft WHERE id=?", (row["aircraft_id"],)).fetchone()
            crew = conn.execute("SELECT * FROM crew WHERE id=?", (row["crew_id"],)).fetchone()
            origin = conn.execute("SELECT * FROM airports WHERE code=?", (row["origin"],)).fetchone()
            destination = conn.execute("SELECT * FROM airports WHERE code=?", (row["destination"],)).fetchone()
            if not aircraft or aircraft["status"] != "active": problems.append({"assignment_id": row["id"], "code": "aircraft_unavailable"})
            elif parse_time(aircraft["maintenance_due"]) < sta and not self._active_deferral(conn, aircraft["id"], row["new_sta"]):
                problems.append({"assignment_id": row["id"], "code": "maintenance_due", "resource": aircraft["id"]})
            if not crew or crew["status"] != "active": problems.append({"assignment_id": row["id"], "code": "crew_unavailable"})
            if not origin or not destination: problems.append({"assignment_id": row["id"], "code": "airport_unknown"})
            if crew:
                duty_start, max_duty = parse_time(crew["duty_start"]), crew["max_duty_minutes"]
                if (sta - duty_start).total_seconds() / 60 > max_duty: problems.append({"assignment_id": row["id"], "code": "duty_limit", "resource": crew["id"]})
            if destination:
                curfew_start, curfew_end = parse_clock(destination["curfew_start"]), parse_clock(destination["curfew_end"])
                permit = conn.execute("""SELECT * FROM permits WHERE origin=? AND destination=? AND valid_from<=? AND valid_to>=?""",
                                      (row["origin"], row["destination"], row["new_sta"], row["new_sta"])).fetchone()
                arrival_clock = sta.timetz().replace(tzinfo=None)
                inside = arrival_clock >= curfew_start or arrival_clock < curfew_end if curfew_start > curfew_end else curfew_start <= arrival_clock < curfew_end
                if inside and not (permit and permit["curfew_exempt"]): problems.append({"assignment_id": row["id"], "code": "airport_curfew"})
                if row["origin"] != row["destination"] and not permit: problems.append({"assignment_id": row["id"], "code": "route_permit_missing"})
                elif row["origin"] != row["destination"] and not (parse_time(permit["valid_from"]) <= std <= parse_time(permit["valid_to"])):
                    problems.append({"assignment_id": row["id"], "code": "route_permit_window"})
            by_aircraft.setdefault(row["aircraft_id"], []).append(row)
            by_crew.setdefault(row["crew_id"], []).append(row)
        for bucket_name, buckets in (("aircraft", by_aircraft), ("crew", by_crew)):
            for resource, items in buckets.items():
                for i, left in enumerate(items):
                    for right in items[i + 1:]:
                        if overlaps(parse_time(left["new_std"]), parse_time(left["new_sta"]), parse_time(right["new_std"]), parse_time(right["new_sta"])):
                            problems.append({"code": f"{bucket_name}_overlap", "resource": resource, "assignments": [left["id"], right["id"]]})
        return problems

    def validate_plan(self, plan_id: int, actor: str, role: str) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager", "auditor"}: raise ApiError(403, "validate_forbidden", "当前角色不能校验方案")
        with self.repo.tx() as conn:
            problems = self._validate_plan(conn, plan_id)
            if not problems:
                metrics = self._metrics(conn, plan_id)
                conn.execute("UPDATE recovery_plans SET metrics_json=?,score_json=? WHERE id=?", (json.dumps(metrics, ensure_ascii=False), json.dumps(self._score(metrics)), plan_id))
            return {"valid": not problems, "problems": problems, "plan": self.get_plan(plan_id)}

    def _metrics(self, conn: sqlite3.Connection, plan_id: int) -> dict[str, Any]:
        rows = conn.execute("""SELECT a.*,f.passenger_count FROM assignments a JOIN flights f ON f.id=a.flight_id WHERE a.plan_id=?""", (plan_id,)).fetchall()
        canceled = sum(1 for row in rows if row["status"] == "canceled")
        return {"flight_count": len(rows), "canceled": canceled, "total_delay_minutes": sum(max(0, row["delay_minutes"]) for row in rows),
                "affected_passengers": sum(row["passenger_count"] for row in rows), "missed_connections": sum(row["missed_connections"] for row in rows)}

    @staticmethod
    def _score(metrics: dict[str, Any]) -> dict[str, int]:
        score = metrics["canceled"] * 100000 + metrics["missed_connections"] * 5000 + metrics["total_delay_minutes"] * 100 + metrics["affected_passengers"]
        return {"cost_score": score, "lower_is_better": 1}

    def lock_plan(self, plan_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager": raise ApiError(403, "lock_forbidden", "只有运行经理可以锁定恢复方案")
        expected = body.get("expected_revision")
        if not isinstance(expected, int): raise ApiError(400, "revision_required", "expected_revision 必须是整数")
        with self.repo.tx() as conn:
            plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
            if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
            if plan["status"] == "locked": return self.get_plan(plan_id)
            if plan["revision"] != expected: raise ApiError(409, "revision_conflict", "方案版本已变化")
            problems = self._validate_plan(conn, plan_id)
            if problems: raise ApiError(409, "plan_invalid", "方案未通过约束校验", problems)
            conflicts = []
            for row in conn.execute("SELECT * FROM assignments WHERE plan_id=? AND status!='canceled'", (plan_id,)).fetchall():
                conflicting = conn.execute("""SELECT a.*,p.name plan_name FROM assignments a JOIN recovery_plans p ON p.id=a.plan_id
                    WHERE p.id!=? AND p.status='locked' AND a.status!='canceled' AND (a.aircraft_id=? OR a.crew_id=?)
                    AND a.new_std<? AND a.new_sta>?""",
                    (plan_id, row["aircraft_id"], row["crew_id"], row["new_sta"], row["new_std"])).fetchall()
                conflicts.extend({"assignment_id": row["id"], "conflict_plan_id": item["plan_id"], "conflict_plan": item["plan_name"], "resource": item["aircraft_id"] if item["aircraft_id"] == row["aircraft_id"] else item["crew_id"]} for item in conflicting)
            if conflicts: raise ApiError(409, "locked_resource_conflict", "与已锁定方案存在飞机或机组冲突", conflicts)
            metrics = self._metrics(conn, plan_id)
            conn.execute("""UPDATE recovery_plans SET status='locked',metrics_json=?,score_json=?,locked_at=?,locked_by=? WHERE id=?""",
                         (json.dumps(metrics, ensure_ascii=False), json.dumps(self._score(metrics)), iso(), actor, plan_id))
            for row in conn.execute("""SELECT a.*,f.flight_no FROM assignments a JOIN flights f ON f.id=a.flight_id WHERE a.plan_id=? AND a.status!='canceled'""", (plan_id,)):
                conn.execute("UPDATE flights SET std=?,sta=?,aircraft_id=?,crew_id=?,delay_minutes=?,revision=revision+1,updated_at=? WHERE id=?",
                             (row["new_std"], row["new_sta"], row["aircraft_id"], row["crew_id"], max(0, row["delay_minutes"]), iso(), row["flight_id"]))
                conn.execute("UPDATE assignments SET status='active' WHERE id=?", (row["id"],))
            Repository.audit(conn, plan_id, actor, role, "plan_locked", {"metrics": metrics})
            return self.get_plan(plan_id)

    def cancel_flight(self, flight_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "cancel_forbidden", "当前角色不能取消航班")
        reason = str(body.get("reason", "")).strip()
        if not reason: raise ApiError(400, "reason_required", "取消原因必填")
        with self.repo.tx() as conn:
            flight = conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone()
            if not flight: raise ApiError(404, "flight_not_found", "航班不存在")
            if flight["status"] == "canceled": return {"flight": dict(flight), "idempotent": True}
            conn.execute("UPDATE flights SET status='canceled',cancel_reason=?,revision=revision+1,updated_at=? WHERE id=?", (reason, iso(), flight_id))
            if flight["status"] == "needs_reschedule":
                self._dequeue_flight(conn, flight["aircraft_id"], flight_id)
            Repository.audit(conn, None, actor, role, "flight_canceled", {"flight_id": flight_id, "reason": reason})
            return {"flight": dict(conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone()), "idempotent": False}

    def recover_flight(self, flight_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "recover_forbidden", "当前角色不能恢复航班")
        expected = body.get("expected_revision")
        if not isinstance(expected, int): raise ApiError(400, "revision_required", "expected_revision 必须是整数")
        with self.repo.tx() as conn:
            flight = conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone()
            if not flight: raise ApiError(404, "flight_not_found", "航班不存在")
            if flight["revision"] != expected: raise ApiError(409, "revision_conflict", "航班版本已变化")
            if flight["status"] != "canceled": raise ApiError(409, "not_canceled", "只有取消航班可以恢复")
            std, sta = parse_time(body.get("new_std")), parse_time(body.get("new_sta"))
            if sta <= std: raise ApiError(400, "invalid_times", "到达时间必须晚于起飞时间")
            aircraft_id, crew_id = body.get("aircraft_id", flight["aircraft_id"]), body.get("crew_id", flight["crew_id"])
            conn.execute("""UPDATE flights SET status='scheduled',std=?,sta=?,aircraft_id=?,crew_id=?,cancel_reason=NULL,
                            delay_minutes=0,revision=revision+1,updated_at=? WHERE id=?""",
                         (iso(std), iso(sta), aircraft_id, crew_id, iso(), flight_id))
            Repository.audit(conn, None, actor, role, "flight_recovered", {"flight_id": flight_id})
            return {"flight": dict(conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone())}

    # ---- 机务延期放行 / 航段重排 ----

    @staticmethod
    def _active_deferral(conn: sqlite3.Connection, aircraft_id: str, at: str) -> sqlite3.Row | None:
        return conn.execute("""SELECT * FROM maintenance_deferrals
                               WHERE aircraft_id=? AND status='active' AND valid_from<=? AND valid_to>=?""",
                            (aircraft_id, at, at)).fetchone()

    def _segment_problems(self, conn: sqlite3.Connection, aircraft_id: str, crew_id: str,
                          origin_code: str, destination_code: str, std: datetime, sta: datetime,
                          exclude_flight_id: int | None = None) -> list[dict[str, Any]]:
        """重排/新建航段时统一核对：维护间隔（延期可覆盖）、执勤、宵禁与航线许可、飞机/机体重叠。"""
        problems: list[dict[str, Any]] = []
        aircraft = conn.execute("SELECT * FROM aircraft WHERE id=?", (aircraft_id,)).fetchone()
        crew = conn.execute("SELECT * FROM crew WHERE id=?", (crew_id,)).fetchone()
        origin = conn.execute("SELECT * FROM airports WHERE code=?", (origin_code,)).fetchone()
        destination = conn.execute("SELECT * FROM airports WHERE code=?", (destination_code,)).fetchone()
        sta_iso = iso(sta)
        if not aircraft or aircraft["status"] != "active":
            problems.append({"code": "aircraft_unavailable", "resource": aircraft_id})
        elif parse_time(aircraft["maintenance_due"]) < sta and not self._active_deferral(conn, aircraft_id, sta_iso):
            problems.append({"code": "maintenance_due", "resource": aircraft_id})
        if not crew or crew["status"] != "active":
            problems.append({"code": "crew_unavailable", "resource": crew_id})
        else:
            duty_start, max_duty = parse_time(crew["duty_start"]), crew["max_duty_minutes"]
            if (sta - duty_start).total_seconds() / 60 > max_duty:
                problems.append({"code": "duty_limit", "resource": crew_id})
        if not origin or not destination:
            problems.append({"code": "airport_unknown"})
        else:
            permit = conn.execute("""SELECT * FROM permits WHERE origin=? AND destination=? AND valid_from<=? AND valid_to>=?""",
                                  (origin_code, destination_code, sta_iso, sta_iso)).fetchone()
            curfew_start, curfew_end = parse_clock(destination["curfew_start"]), parse_clock(destination["curfew_end"])
            arrival_clock = sta.timetz().replace(tzinfo=None)
            inside = arrival_clock >= curfew_start or arrival_clock < curfew_end if curfew_start > curfew_end else curfew_start <= arrival_clock < curfew_end
            if inside and not (permit and permit["curfew_exempt"]):
                problems.append({"code": "airport_curfew"})
            if origin_code != destination_code and not permit:
                problems.append({"code": "route_permit_missing"})
            elif origin_code != destination_code and not (parse_time(permit["valid_from"]) <= std <= parse_time(permit["valid_to"])):
                problems.append({"code": "route_permit_window"})
        if aircraft or crew:
            sql = "SELECT id,flight_no,aircraft_id,crew_id FROM flights WHERE status='scheduled' AND std<? AND sta>? AND (aircraft_id=? OR crew_id=?)"
            params: list[Any] = [sta_iso, iso(std), aircraft_id, crew_id]
            if exclude_flight_id is not None:
                sql += " AND id!=?"; params.append(exclude_flight_id)
            for other in conn.execute(sql, params).fetchall():
                if other["aircraft_id"] == aircraft_id:
                    problems.append({"code": "aircraft_overlap", "resource": aircraft_id,
                                     "conflict_flight_id": other["id"], "conflict_flight_no": other["flight_no"]})
                if other["crew_id"] == crew_id:
                    problems.append({"code": "crew_overlap", "resource": crew_id,
                                     "conflict_flight_id": other["id"], "conflict_flight_no": other["flight_no"]})
        return problems

    @staticmethod
    def _queue_get(conn: sqlite3.Connection, aircraft_id: str) -> list[int]:
        row = conn.execute("SELECT flight_ids_json FROM reschedule_queue WHERE aircraft_id=?", (aircraft_id,)).fetchone()
        return json.loads(row["flight_ids_json"]) if row else []

    def _enqueue_reschedule(self, conn: sqlite3.Connection, aircraft_id: str, flight_ids: list[int]) -> None:
        ids = set(self._queue_get(conn, aircraft_id)) | {int(i) for i in flight_ids}
        if ids:
            placeholders = ",".join("?" for _ in ids)
            valid = [r["id"] for r in conn.execute(
                f"SELECT id FROM flights WHERE id IN ({placeholders}) AND status='needs_reschedule'", tuple(ids)).fetchall()]
        else:
            valid = []
        if valid:
            conn.execute("""INSERT INTO reschedule_queue(aircraft_id,flight_ids_json,updated_at) VALUES(?,?,?)
                            ON CONFLICT(aircraft_id) DO UPDATE SET flight_ids_json=excluded.flight_ids_json,updated_at=excluded.updated_at""",
                         (aircraft_id, json.dumps(valid), iso()))
        else:
            conn.execute("DELETE FROM reschedule_queue WHERE aircraft_id=?", (aircraft_id,))

    def _dequeue_flight(self, conn: sqlite3.Connection, aircraft_id: str, flight_id: int) -> None:
        ids = [i for i in self._queue_get(conn, aircraft_id) if i != flight_id]
        if ids:
            conn.execute("UPDATE reschedule_queue SET flight_ids_json=?,updated_at=? WHERE aircraft_id=?",
                         (json.dumps(ids), iso(), aircraft_id))
        else:
            conn.execute("DELETE FROM reschedule_queue WHERE aircraft_id=?", (aircraft_id,))

    def _deactivate_deferral(self, conn: sqlite3.Connection, deferral: sqlite3.Row, actor: str, role: str, action: str,
                             extra: dict[str, Any] | None = None) -> None:
        """延期状态一变：未执行航段立即失效退回重排；草稿方案中依赖它的航段标记失效；已落地航段保留回执。"""
        deferral_id, aircraft_id = deferral["id"], deferral["aircraft_id"]
        conn.execute("UPDATE maintenance_deferrals SET status='expired',revision=revision+1,expired_at=?,expired_by=? WHERE id=?",
                     (iso(), actor, deferral_id))
        pending = [r["id"] for r in conn.execute(
            "SELECT id FROM flights WHERE deferral_id=? AND status='scheduled'", (deferral_id,)).fetchall()]
        for flight_id in pending:
            conn.execute("UPDATE flights SET status='needs_reschedule',revision=revision+1,updated_at=? WHERE id=?", (iso(), flight_id))
        if pending:
            self._enqueue_reschedule(conn, aircraft_id, pending)
        # 依赖该延期的草稿航段（该飞机且超出维护间隔）立即失效，逼迫重排时重新核对
        draft_rows = conn.execute("""SELECT a.id,a.plan_id FROM assignments a
                                     JOIN recovery_plans p ON p.id=a.plan_id
                                     JOIN aircraft ac ON ac.id=a.aircraft_id
                                     WHERE p.status='draft' AND a.invalid_reason IS NULL
                                       AND a.aircraft_id=? AND a.new_sta>ac.maintenance_due""", (aircraft_id,)).fetchall()
        for row in draft_rows:
            conn.execute("UPDATE assignments SET invalid_reason='deferral_expired' WHERE id=?", (row["id"],))
        for plan_id in {row["plan_id"] for row in draft_rows}:
            conn.execute("UPDATE recovery_plans SET revision=revision+1 WHERE id=?", (plan_id,))
        kept = [r["flight_id"] for r in conn.execute(
            "SELECT flight_id FROM flight_receipts WHERE deferral_id=?", (deferral_id,)).fetchall()]
        detail: dict[str, Any] = {"deferral_id": deferral_id, "aircraft_id": aircraft_id,
                                  "invalidated_flight_ids": pending, "invalidated_assignment_ids": [r["id"] for r in draft_rows],
                                  "kept_landed_flight_ids": kept}
        if extra:
            detail.update(extra)
        Repository.audit(conn, None, actor, role, action, detail)

    def _sweep_expired_deferrals(self, conn: sqlite3.Connection) -> int:
        rows = conn.execute("SELECT * FROM maintenance_deferrals WHERE status='active' AND valid_to<=?", (iso(),)).fetchall()
        for row in rows:
            self._deactivate_deferral(conn, row, "system", "scheduler", "deferral_expired_auto")
        return len(rows)

    def grant_deferral(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}:
            raise ApiError(403, "deferral_forbidden", "当前角色不能申请延期放行")
        aircraft_id, reason = str(body.get("aircraft_id", "")).strip(), str(body.get("reason", "")).strip()
        if not aircraft_id or not reason:
            raise ApiError(400, "missing_fields", "aircraft_id 和 reason 必填")
        valid_from, valid_to = parse_time(body.get("valid_from")), parse_time(body.get("valid_to"))
        if valid_to <= valid_from:
            raise ApiError(400, "invalid_times", "延期结束时间必须晚于开始时间")
        with self.repo.tx() as conn:
            self._sweep_expired_deferrals(conn)
            if not conn.execute("SELECT 1 FROM aircraft WHERE id=?", (aircraft_id,)).fetchone():
                raise ApiError(404, "aircraft_not_found", "飞机不存在")
            try:
                cur = conn.execute("""INSERT INTO maintenance_deferrals(aircraft_id,valid_from,valid_to,reason,created_by,created_at)
                                      VALUES(?,?,?,?,?,?)""",
                                   (aircraft_id, iso(valid_from), iso(valid_to), reason, actor, iso()))
            except sqlite3.IntegrityError as exc:
                existing = conn.execute(
                    "SELECT * FROM maintenance_deferrals WHERE aircraft_id=? AND status='active' ORDER BY id",
                    (aircraft_id,)).fetchone()
                queue = self._queue_get(conn, aircraft_id)
                raise ApiError(409, "deferral_conflict", "该飞机已有生效中的延期放行，只有一份申请能写入",
                               {"active_deferral_id": existing["id"] if existing else None,
                                "aircraft_id": aircraft_id, "pending_reschedule_flight_ids": queue}) from exc
            deferral_id = cur.lastrowid
            # 把窗口内依赖该延期才能执行（到达时间已超出维护到期点）的在飞航段挂上依赖
            bound = []
            for row in conn.execute("""SELECT f.id FROM flights f JOIN aircraft ac ON ac.id=f.aircraft_id
                                      WHERE f.aircraft_id=? AND f.status='scheduled'
                                        AND f.sta>ac.maintenance_due AND f.sta>=? AND f.sta<=?""",
                                    (aircraft_id, iso(valid_from), iso(valid_to))).fetchall():
                conn.execute("UPDATE flights SET deferral_id=?,revision=revision+1,updated_at=? WHERE id=?",
                             (deferral_id, iso(), row["id"]))
                bound.append(row["id"])
            Repository.audit(conn, None, actor, role, "deferral_granted",
                             {"deferral_id": deferral_id, "aircraft_id": aircraft_id, "bound_flight_ids": bound})
            return dict(conn.execute("SELECT * FROM maintenance_deferrals WHERE id=?", (deferral_id,)).fetchone())

    def extend_deferral(self, deferral_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}:
            raise ApiError(403, "deferral_forbidden", "当前角色不能延长延期放行")
        expected, new_valid_to = body.get("expected_revision"), parse_time(body.get("valid_to"))
        if not isinstance(expected, int):
            raise ApiError(400, "revision_required", "expected_revision 必须是整数")
        with self.repo.tx() as conn:
            self._sweep_expired_deferrals(conn)
            deferral = conn.execute("SELECT * FROM maintenance_deferrals WHERE id=?", (deferral_id,)).fetchone()
            if not deferral:
                raise ApiError(404, "deferral_not_found", "延期放行不存在")
            if deferral["status"] != "active":
                raise ApiError(409, "deferral_not_active", "延期放行已失效，不能延长")
            if deferral["revision"] != expected:
                raise ApiError(409, "revision_conflict", "延期版本已变化")
            if new_valid_to <= parse_time(deferral["valid_to"]):
                raise ApiError(400, "invalid_times", "新到期时间必须晚于当前到期时间")
            conn.execute("UPDATE maintenance_deferrals SET valid_to=?,revision=revision+1 WHERE id=?",
                         (iso(new_valid_to), deferral_id))
            Repository.audit(conn, None, actor, role, "deferral_extended",
                             {"deferral_id": deferral_id, "valid_to": iso(new_valid_to)})
            return dict(conn.execute("SELECT * FROM maintenance_deferrals WHERE id=?", (deferral_id,)).fetchone())

    def revoke_deferral(self, deferral_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager":
            raise ApiError(403, "deferral_forbidden", "只有运行经理可以撤销延期放行")
        reason = str(body.get("reason", "")).strip()
        if not reason:
            raise ApiError(400, "reason_required", "撤销原因必填")
        with self.repo.tx() as conn:
            deferral = conn.execute("SELECT * FROM maintenance_deferrals WHERE id=?", (deferral_id,)).fetchone()
            if not deferral:
                raise ApiError(404, "deferral_not_found", "延期放行不存在")
            if deferral["status"] != "active":
                return {"deferral": dict(deferral), "idempotent": True}
            self._deactivate_deferral(conn, deferral, actor, role, "deferral_revoked", {"reason": reason})
            return {"deferral": dict(conn.execute("SELECT * FROM maintenance_deferrals WHERE id=?", (deferral_id,)).fetchone()),
                    "pending_reschedule_flight_ids": self._queue_get(conn, deferral["aircraft_id"]), "idempotent": False}

    def expire_deferral(self, deferral_id: int, actor: str, role: str) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}:
            raise ApiError(403, "deferral_forbidden", "当前角色不能处理延期到期")
        with self.repo.tx() as conn:
            deferral = conn.execute("SELECT * FROM maintenance_deferrals WHERE id=?", (deferral_id,)).fetchone()
            if not deferral:
                raise ApiError(404, "deferral_not_found", "延期放行不存在")
            if deferral["status"] != "active":
                return {"deferral": dict(deferral), "idempotent": True}
            self._deactivate_deferral(conn, deferral, actor, role, "deferral_expired_manual")
            return {"deferral": dict(conn.execute("SELECT * FROM maintenance_deferrals WHERE id=?", (deferral_id,)).fetchone()),
                    "pending_reschedule_flight_ids": self._queue_get(conn, deferral["aircraft_id"]), "idempotent": False}

    def land_flight(self, flight_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}:
            raise ApiError(403, "land_forbidden", "当前角色不能登记落地")
        landed_at = parse_time(body.get("landed_at")) if body.get("landed_at") else utcnow()
        with self.repo.tx() as conn:
            flight = conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone()
            if not flight:
                raise ApiError(404, "flight_not_found", "航班不存在")
            existing = conn.execute("SELECT * FROM flight_receipts WHERE flight_id=?", (flight_id,)).fetchone()
            if existing:
                return {"receipt": dict(existing), "idempotent": True}
            if flight["status"] == "canceled":
                raise ApiError(409, "flight_canceled", "已取消航班不能登记落地")
            receipt = {"flight_no": flight["flight_no"], "origin": flight["origin"], "destination": flight["destination"],
                       "passenger_count": flight["passenger_count"], "delay_minutes": flight["delay_minutes"]}
            conn.execute("""INSERT INTO flight_receipts(flight_id,flight_no,aircraft_id,crew_id,std,sta,landed_at,deferral_id,detail_json)
                            VALUES(?,?,?,?,?,?,?,?,?)""",
                         (flight["id"], flight["flight_no"], flight["aircraft_id"], flight["crew_id"],
                          flight["std"], flight["sta"], iso(landed_at), flight["deferral_id"],
                          json.dumps(receipt, ensure_ascii=False)))
            conn.execute("UPDATE flights SET status='landed',revision=revision+1,updated_at=? WHERE id=?", (iso(), flight_id))
            self._dequeue_flight(conn, flight["aircraft_id"], flight_id)
            Repository.audit(conn, None, actor, role, "flight_landed",
                             {"flight_id": flight_id, "deferral_id": flight["deferral_id"]})
            return {"receipt": dict(conn.execute("SELECT * FROM flight_receipts WHERE flight_id=?", (flight_id,)).fetchone()),
                    "idempotent": False}

    def reschedule_flights(self, actor: str, role: str, body: dict[str, Any], use_stored_slots: bool = False) -> dict[str, Any]:
        """按飞机编号取出待重排航段，逐条重新核对维护间隔、执勤、宵禁与重叠；成功才重新占用资源。"""
        if role not in {"scheduler", "ops_manager"}:
            raise ApiError(403, "reschedule_forbidden", "当前角色不能重排航段")
        aircraft_id = str(body.get("aircraft_id", "")).strip()
        if not aircraft_id:
            raise ApiError(400, "missing_fields", "aircraft_id 必填")
        items = body.get("items", [])
        if not use_stored_slots and (not isinstance(items, list) or not items):
            raise ApiError(400, "missing_fields", "items 必填")
        with self.repo.tx() as conn:
            self._sweep_expired_deferrals(conn)
            queued_ids = self._queue_get(conn, aircraft_id)
            if not queued_ids:
                return {"aircraft_id": aircraft_id, "processed": [], "remaining_flight_ids": [], "queue_empty": True}
            proposals: dict[int, dict[str, Any]] = {}
            if use_stored_slots:
                for row in conn.execute(f"SELECT * FROM flights WHERE id IN ({','.join('?' for _ in queued_ids)})", tuple(queued_ids)):
                    proposals[row["id"]] = {"flight_id": row["id"], "aircraft_id": row["aircraft_id"], "crew_id": row["crew_id"],
                                            "new_std": row["std"], "new_sta": row["sta"]}
            else:
                for item in items:
                    fid = item.get("flight_id")
                    if isinstance(fid, int):
                        proposals[fid] = item
            processed: list[dict[str, Any]] = []
            for fid in list(queued_ids):
                if fid not in proposals:
                    continue
                item, flight = proposals[fid], conn.execute("SELECT * FROM flights WHERE id=?", (fid,)).fetchone()
                if not flight:
                    self._dequeue_flight(conn, aircraft_id, fid)
                    processed.append({"flight_id": fid, "result": "dropped", "problems": [{"code": "flight_not_found"}]})
                    continue
                if flight["status"] == "landed":
                    self._dequeue_flight(conn, aircraft_id, fid)
                    processed.append({"flight_id": fid, "flight_no": flight["flight_no"], "result": "landed_receipt_kept"})
                    continue
                if flight["status"] != "needs_reschedule":
                    self._dequeue_flight(conn, aircraft_id, fid)
                    processed.append({"flight_id": fid, "flight_no": flight["flight_no"], "result": "no_longer_pending"})
                    continue
                new_aircraft = str(item.get("aircraft_id") or flight["aircraft_id"]).strip()
                new_crew = str(item.get("crew_id") or flight["crew_id"]).strip()
                sp = f"sp_{fid}"
                conn.execute(f"SAVEPOINT {sp}")
                try:
                    std, sta = parse_time(item["new_std"]), parse_time(item["new_sta"])
                    if sta <= std:
                        raise ApiError(400, "invalid_times", "到达时间必须晚于起飞时间")
                    problems = self._segment_problems(conn, new_aircraft, new_crew, flight["origin"], flight["destination"],
                                                      std, sta, exclude_flight_id=fid)
                    if problems:
                        conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
                        processed.append({"flight_id": fid, "flight_no": flight["flight_no"], "result": "failed", "problems": problems})
                        continue
                    due_row = conn.execute("SELECT maintenance_due FROM aircraft WHERE id=?", (new_aircraft,)).fetchone()
                    covering = self._active_deferral(conn, new_aircraft, iso(sta)) if due_row and parse_time(due_row["maintenance_due"]) < sta else None
                    delay = int((std - parse_time(flight["std"])).total_seconds() // 60)
                    conn.execute("""UPDATE flights SET status='scheduled',std=?,sta=?,aircraft_id=?,crew_id=?,
                                    delay_minutes=?,deferral_id=?,revision=revision+1,updated_at=? WHERE id=?""",
                                 (iso(std), iso(sta), new_aircraft, new_crew, max(0, delay), covering["id"] if covering else None, iso(), fid))
                    conn.execute(f"RELEASE SAVEPOINT {sp}")
                except ApiError as exc:
                    conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
                    processed.append({"flight_id": fid, "flight_no": flight["flight_no"], "result": "failed",
                                      "problems": [{"code": exc.code, "message": exc.message}]})
                    continue
                self._dequeue_flight(conn, aircraft_id, fid)
                processed.append({"flight_id": fid, "flight_no": flight["flight_no"], "result": "rescheduled",
                                  "aircraft_id": new_aircraft, "crew_id": new_crew,
                                  "new_std": iso(std), "new_sta": iso(sta), "deferral_id": covering["id"] if covering else None})
            remaining = self._queue_get(conn, aircraft_id)
            Repository.audit(conn, None, actor, role, "flights_rescheduled",
                             {"aircraft_id": aircraft_id, "processed": processed, "remaining_flight_ids": remaining})
            return {"aircraft_id": aircraft_id, "processed": processed, "remaining_flight_ids": remaining, "queue_empty": not remaining}

    def list_deferrals(self, aircraft_id: str | None = None) -> dict[str, Any]:
        with self.repo.tx() as conn:
            swept = self._sweep_expired_deferrals(conn)
            if aircraft_id:
                rows = conn.execute("SELECT * FROM maintenance_deferrals WHERE aircraft_id=? ORDER BY id DESC", (aircraft_id,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM maintenance_deferrals ORDER BY id DESC").fetchall()
            return {"deferrals": [dict(r) for r in rows], "swept": swept}

    def get_plan(self, plan_id: int) -> dict[str, Any]:
        conn = self.repo.conn
        plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
        if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
        assignments = [dict(r) for r in conn.execute("""SELECT a.*,f.flight_no,f.origin,f.destination,f.passenger_count FROM assignments a JOIN flights f ON f.id=a.flight_id WHERE a.plan_id=? ORDER BY a.new_std""", (plan_id,))]
        result = dict(plan)
        result["metrics"] = json.loads(plan["metrics_json"]) if plan["metrics_json"] else self._metrics(conn, plan_id)
        result["score"] = json.loads(plan["score_json"]) if plan["score_json"] else None
        result["assignments"] = assignments
        return result

    def compare_plans(self, disruption_id: int) -> dict[str, Any]:
        plans = []
        for row in self.repo.conn.execute("SELECT id FROM recovery_plans WHERE disruption_id=? ORDER BY id", (disruption_id,)):
            plan = self.get_plan(row["id"])
            if not plan["score"]:
                problems = self._validate_plan(self.repo.conn, row["id"])
                plan["valid"] = not problems
            else:
                plan["valid"] = True
            plans.append(plan)
        plans.sort(key=lambda item: item["score"]["cost_score"] if item["score"] else 10**18)
        return {"disruption_id": disruption_id, "recommended_plan_id": plans[0]["id"] if plans else None, "plans": plans}

    def state(self) -> dict[str, Any]:
        conn = self.repo.conn
        with self.repo.tx() as write_conn:
            self._sweep_expired_deferrals(write_conn)
        flights = [dict(r) for r in conn.execute("SELECT * FROM flights ORDER BY std")]
        plans = [self.get_plan(r["id"]) for r in conn.execute("SELECT id FROM recovery_plans ORDER BY id DESC LIMIT 20")]
        return {"flights": flights,
                "disruptions": [dict(r) for r in conn.execute("SELECT * FROM disruptions ORDER BY id DESC")],
                "deferrals": [dict(r) for r in conn.execute("SELECT * FROM maintenance_deferrals ORDER BY id DESC")],
                "receipts": [dict(r) for r in conn.execute("SELECT * FROM flight_receipts ORDER BY landed_at DESC")],
                "reschedule_queue": [dict(r) for r in conn.execute("SELECT * FROM reschedule_queue ORDER BY aircraft_id")],
                "plans": plans, "server_time": iso()}


def respond(handler: BaseHTTPRequestHandler, status: int, payload: Any) -> None:
    raw = json.dumps(payload, ensure_ascii=False, default=str).encode()
    handler.send_response(status); handler.send_header("Content-Type", "application/json; charset=utf-8"); handler.send_header("Content-Length", str(len(raw))); handler.end_headers(); handler.wfile.write(raw)


class Handler(BaseHTTPRequestHandler):
    service: AirlineRecoveryService
    web_root: Path
    def log_message(self, fmt: str, *args: Any) -> None: print(f"{self.address_string()} - {fmt % args}")
    def body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if not length: return {}
        try: value = json.loads(self.rfile.read(length))
        except json.JSONDecodeError as exc: raise ApiError(400, "invalid_json", "请求体不是有效 JSON") from exc
        if not isinstance(value, dict): raise ApiError(400, "invalid_json", "请求体必须是对象")
        return value
    def get_api(self, path: str) -> tuple[int, Any]:
        if path == "/health": return 200, {"status": "ok", "service": "airline-recovery"}
        actor, role = self.service.identity(self.headers)
        if path == "/api/state": return 200, self.service.state()
        parts = [p for p in path.split("/") if p]
        if path == "/api/state": return 200, self.service.state()
        if path == "/api/deferrals": return 200, self.service.list_deferrals()
        if len(parts) == 4 and parts[:2] == ["api", "deferrals", "aircraft"]:
            return 200, self.service.list_deferrals(parts[3].upper())
        if len(parts) == 3 and parts[:2] == ["api", "plans"] and parts[2].isdigit(): return 200, self.service.get_plan(int(parts[2]))
        if len(parts) == 4 and parts[:2] == ["api", "disruptions"] and parts[2].isdigit() and parts[3] == "compare": return 200, self.service.compare_plans(int(parts[2]))
        raise ApiError(404, "not_found", "接口不存在")
    def post_api(self, path: str) -> tuple[int, Any]:
        actor, role = self.service.identity(self.headers); body = self.body(); parts = [p for p in path.split("/") if p]
        table = {
            "/api/airports": lambda: (201, self.service.seed_airport(actor, role, body)),
            "/api/aircraft": lambda: (201, self.service.seed_aircraft(actor, role, body)),
            "/api/crew": lambda: (201, self.service.seed_crew(actor, role, body)),
            "/api/permits": lambda: (201, self.service.create_permit(actor, role, body)),
            "/api/flights": lambda: (201, self.service.create_flight(actor, role, body)),
            "/api/disruptions": lambda: (201, self.service.create_disruption(actor, role, body)),
            "/api/recovery-plans": lambda: (201, self.service.create_plan(actor, role, body)),
            "/api/deferrals": lambda: (201, self.service.grant_deferral(actor, role, body)),
            "/api/reschedule": lambda: (200, self.service.reschedule_flights(actor, role, body)),
            "/api/reschedule/retry": lambda: (200, self.service.reschedule_flights(actor, role, body, use_stored_slots=True)),
        }
        if path in table: return table[path]()
        if len(parts) == 4 and parts[:2] == ["api", "deferrals"] and parts[2].isdigit():
            deferral_id, action = int(parts[2]), parts[3]
            if action == "extend": return 200, self.service.extend_deferral(deferral_id, actor, role, body)
            if action == "revoke": return 200, self.service.revoke_deferral(deferral_id, actor, role, body)
            if action == "expire": return 200, self.service.expire_deferral(deferral_id, actor, role)
        if len(parts) == 4 and parts[:2] == ["api", "plans"] and parts[2].isdigit():
            plan_id, action = int(parts[2]), parts[3]
            if action == "assignments": return 200, self.service.add_assignment(plan_id, actor, role, body)
            if action == "validate": return 200, self.service.validate_plan(plan_id, actor, role)
            if action == "lock": return 200, self.service.lock_plan(plan_id, actor, role, body)
        if len(parts) == 4 and parts[:2] == ["api", "flights"] and parts[2].isdigit():
            flight_id, action = int(parts[2]), parts[3]
            if action == "cancel": return 200, self.service.cancel_flight(flight_id, actor, role, body)
            if action == "recover": return 200, self.service.recover_flight(flight_id, actor, role, body)
            if action == "land": return 200, self.service.land_flight(flight_id, actor, role, body)
        raise ApiError(404, "not_found", "接口不存在")
    def handle_request(self, method: str) -> None:
        parsed = urlparse(self.path)
        try:
            if method == "GET" and parsed.path == "/":
                raw = (self.web_root / "index.html").read_bytes(); self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(raw))); self.end_headers(); self.wfile.write(raw); return
            status, payload = self.get_api(parsed.path) if method == "GET" else self.post_api(parsed.path)
            respond(self, status, payload)
        except ApiError as exc:
            payload = {"error": exc.code, "message": exc.message}
            if exc.details is not None: payload["details"] = exc.details
            respond(self, exc.status, payload)
        except Exception as exc:
            print(f"unhandled error: {exc!r}"); respond(self, 500, {"error": "internal_error", "message": str(exc)})
    def do_GET(self) -> None: self.handle_request("GET")
    def do_POST(self) -> None: self.handle_request("POST")


def create_server(db_path: str | Path, host: str = "127.0.0.1", port: int = PORT) -> ThreadingHTTPServer:
    service = AirlineRecoveryService(db_path)
    handler = type("AirlineHandler", (Handler,), {"service": service, "web_root": Path(__file__).resolve().parent / "static"})
    return ThreadingHTTPServer((host, port), handler)


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--host", default="127.0.0.1"); parser.add_argument("--port", type=int, default=PORT); parser.add_argument("--db", default=os.environ.get("AIRLINE_DB", "airline_recovery.db")); args = parser.parse_args()
    server = create_server(args.db, args.host, args.port); print(f"airline-recovery listening on http://{args.host}:{args.port}")
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally: server.server_close()

if __name__ == "__main__": main()
