import sys, tempfile, threading, unittest
from datetime import timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import AirlineRecoveryService, ApiError, iso, utcnow


class DeferralFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.svc = AirlineRecoveryService(Path(self.tmp.name) / "test.db")
        base = utcnow() + timedelta(days=1)
        self.svc.seed_airport("ops", "ops_manager", {"code": "AAA", "country": "CN", "curfew_start": "23:00", "curfew_end": "05:00"})
        self.svc.seed_airport("ops", "ops_manager", {"code": "BBB", "country": "CN", "curfew_start": "23:00", "curfew_end": "05:00"})
        self.svc.seed_aircraft("ops", "ops_manager", {"id": "AC1", "model": "A320", "maintenance_due": iso(base + timedelta(days=5))})
        self.svc.seed_aircraft("ops", "ops_manager", {"id": "AC2", "model": "A320", "maintenance_due": iso(base + timedelta(days=5))})
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR1", "name": "甲组", "base": "AAA", "duty_start": iso(base - timedelta(hours=2)), "max_duty_minutes": 720})
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR2", "name": "乙组", "base": "AAA", "duty_start": iso(base - timedelta(hours=2)), "max_duty_minutes": 720})
        self.svc.create_permit("ops", "ops_manager", {"origin": "AAA", "destination": "BBB", "valid_from": iso(base - timedelta(days=1)), "valid_to": iso(base + timedelta(days=2))})
        self.base = base

    def tearDown(self):
        self.tmp.cleanup()

    def make_flight(self, number, aircraft, crew, std=None, sta=None):
        std = std or self.base
        sta = sta or (std + timedelta(hours=2))
        return self.svc.create_flight("sched", "scheduler", {"flight_no": number, "origin": "AAA", "destination": "BBB", "std": iso(std), "sta": iso(sta), "aircraft_id": aircraft, "crew_id": crew, "passenger_count": 150})

    def flight(self, flight_id):
        return dict(self.svc.repo.conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone())

    def pending_flight(self, number, aircraft, crew, std=None, sta=None, valid_until=None):
        """建一个航班，用延期放行覆盖，再把延期结束，使航班退回待重排。"""
        flight = self.make_flight(number, aircraft, crew, std, sta)
        deferral = self.svc.apply_deferral("sched", "scheduler", {"aircraft_id": aircraft, "reason": "故障保留", "granted_at": iso(self.base), "valid_until": iso(valid_until or (self.base + timedelta(days=2)))})
        self.svc.resolve_deferral(deferral["id"], "ops", "ops_manager", {"action": "expire"})
        return self.flight(flight["id"])

    def test_deferral_resolution_invalidates_unexecuted_and_keeps_landed_receipts(self):
        self.svc.seed_aircraft("ops", "ops_manager", {"id": "AC3", "model": "A320", "maintenance_due": iso(self.base + timedelta(hours=12))})
        f1 = self.make_flight("AB200", "AC3", "CR1", self.base + timedelta(days=1), self.base + timedelta(days=1, hours=2))
        f2 = self.make_flight("AB201", "AC3", "CR2", self.base + timedelta(days=1, hours=6), self.base + timedelta(days=1, hours=8))
        f3 = self.make_flight("AB202", "AC3", "CR1", self.base + timedelta(days=3), self.base + timedelta(days=3, hours=2))
        deferral = self.svc.apply_deferral("sched", "scheduler", {"aircraft_id": "AC3", "reason": "故障保留", "granted_at": iso(self.base), "valid_until": iso(self.base + timedelta(days=2))})
        landed = self.svc.mark_landed(f2["id"], "sched", "scheduler", {"expected_revision": f2["revision"]})["flight"]
        self.assertEqual(landed["status"], "landed")
        result = self.svc.resolve_deferral(deferral["id"], "ops", "ops_manager", {"action": "expire"})
        self.assertEqual(result["invalidated"], 1)
        self.assertEqual(result["retained_receipts"], 1)
        self.assertEqual(self.flight(f1["id"])["status"], "pending_reschedule")
        self.assertEqual(self.flight(f2["id"])["status"], "landed")
        self.assertIsNotNone(self.flight(f2["id"])["landed_at"])
        self.assertEqual(self.flight(f3["id"])["status"], "scheduled")
        again = self.svc.resolve_deferral(deferral["id"], "ops", "ops_manager", {"action": "expire"})
        self.assertTrue(again["idempotent"])

    def test_reschedule_rechecks_maintenance_duty_and_curfew(self):
        self.svc.seed_aircraft("ops", "ops_manager", {"id": "AC3", "model": "A320", "maintenance_due": iso(self.base + timedelta(hours=12))})
        f1 = self.pending_flight("AB210", "AC3", "CR1", self.base + timedelta(days=1), self.base + timedelta(days=1, hours=2))
        self.assertEqual(f1["status"], "pending_reschedule")
        with self.assertRaises(ApiError) as ctx:
            self.svc.reschedule_flight(f1["id"], "sched", "scheduler", {"expected_revision": f1["revision"], "new_std": iso(self.base + timedelta(days=1)), "new_sta": iso(self.base + timedelta(days=1, hours=2))})
        self.assertEqual(ctx.exception.code, "segment_invalid")
        codes = {p["code"] for p in ctx.exception.details}
        self.assertIn("maintenance_due", codes)
        self.assertIn("duty_limit", codes)
        self.svc.apply_deferral("sched", "scheduler", {"aircraft_id": "AC3", "reason": "再次保留", "granted_at": iso(self.base), "valid_until": iso(self.base + timedelta(days=7))})
        with self.assertRaises(ApiError) as ctx:
            self.svc.reschedule_flight(f1["id"], "sched", "scheduler", {"expected_revision": f1["revision"], "new_std": iso(self.base + timedelta(days=1)), "new_sta": iso(self.base + timedelta(days=1, hours=2))})
        codes = {p["code"] for p in ctx.exception.details}
        self.assertNotIn("maintenance_due", codes)
        self.assertIn("duty_limit", codes)
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR3", "name": "丙组", "base": "AAA", "duty_start": iso(self.base + timedelta(days=1) - timedelta(hours=2)), "max_duty_minutes": 720})
        ok = self.svc.reschedule_flight(f1["id"], "sched", "scheduler", {"expected_revision": f1["revision"], "new_std": iso(self.base + timedelta(days=1)), "new_sta": iso(self.base + timedelta(days=1, hours=2)), "crew_id": "CR3"})["flight"]
        self.assertEqual(ok["status"], "scheduled")
        self.assertEqual(ok["crew_id"], "CR3")
        self.svc.seed_aircraft("ops", "ops_manager", {"id": "AC5", "model": "A320", "maintenance_due": iso(self.base + timedelta(days=5))})
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR4", "name": "戊组", "base": "AAA", "duty_start": iso(self.base + timedelta(days=1) - timedelta(hours=2)), "max_duty_minutes": 720})
        curfew_std = (self.base + timedelta(days=1)).replace(hour=0, minute=0)
        curfew_sta = (self.base + timedelta(days=1)).replace(hour=2, minute=0)
        f2 = self.pending_flight("AB211", "AC5", "CR4", curfew_std, curfew_sta)
        with self.assertRaises(ApiError) as ctx:
            self.svc.reschedule_flight(f2["id"], "sched", "scheduler", {"expected_revision": f2["revision"], "new_std": iso(curfew_std), "new_sta": iso(curfew_sta)})
        self.assertEqual(ctx.exception.code, "segment_invalid")
        self.assertIn("airport_curfew", {p["code"] for p in ctx.exception.details})
        day_std = (self.base + timedelta(days=1)).replace(hour=14, minute=0)
        day_sta = (self.base + timedelta(days=1)).replace(hour=16, minute=0)
        ok2 = self.svc.reschedule_flight(f2["id"], "sched", "scheduler", {"expected_revision": f2["revision"], "new_std": iso(day_std), "new_sta": iso(day_sta)})["flight"]
        self.assertEqual(ok2["status"], "scheduled")

    def test_batch_reschedule_retries_and_never_double_books(self):
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR3", "name": "丙组", "base": "AAA", "duty_start": iso(self.base + timedelta(days=1) - timedelta(hours=2)), "max_duty_minutes": 1440})
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR4", "name": "丁组", "base": "AAA", "duty_start": iso(self.base + timedelta(days=1) - timedelta(hours=2)), "max_duty_minutes": 1440})
        t10 = (self.base + timedelta(days=1)).replace(hour=10, minute=0)
        t12 = (self.base + timedelta(days=1)).replace(hour=12, minute=0)
        t11 = (self.base + timedelta(days=1)).replace(hour=11, minute=0)
        t13 = (self.base + timedelta(days=1)).replace(hour=13, minute=0)
        t15 = (self.base + timedelta(days=1)).replace(hour=15, minute=0)
        f1 = self.pending_flight("AB220", "AC1", "CR3", t10, t12)
        f2 = self.pending_flight("AB221", "AC1", "CR4", t10, t12)
        result = self.svc.reschedule_pending("AC1", "sched", "scheduler", {"items": [
            {"flight_id": f1["id"], "expected_revision": 999, "new_std": iso(t10), "new_sta": iso(t12)},
            {"flight_id": f2["id"], "expected_revision": f2["revision"], "new_std": iso(t11), "new_sta": iso(t13)},
            {"flight_id": f2["id"], "expected_revision": f2["revision"], "new_std": iso(t13), "new_sta": iso(t15)},
        ]})
        self.assertEqual(len(result["rescheduled"]), 2)
        self.assertEqual(len(result["failed"]), 1)
        self.assertIn("resource_overlap", {p["code"] for p in result["failed"][0]["problems"]})
        self.assertEqual(result["rescheduled"][0]["attempts"], 2)
        self.assertEqual(self.flight(f1["id"])["std"], iso(t10))
        self.assertEqual(self.flight(f2["id"])["std"], iso(t13))
        again = self.svc.reschedule_pending("AC1", "sched", "scheduler", {"items": [{"flight_id": f2["id"], "expected_revision": f2["revision"], "new_std": iso(t10), "new_sta": iso(t12)}]})
        self.assertEqual(len(again["rescheduled"]), 0)
        self.assertEqual(again["failed"][0]["error"], "not_pending_reschedule")

    def test_concurrent_deferral_for_same_aircraft_only_one_succeeds(self):
        barrier = threading.Barrier(2)
        outcomes = []

        def apply():
            barrier.wait()
            try:
                deferral = self.svc.apply_deferral("sched", "scheduler", {"aircraft_id": "AC1", "reason": "故障保留", "valid_until": iso(self.base + timedelta(days=7))})
                outcomes.append(("ok", deferral["id"]))
            except ApiError as exc:
                outcomes.append(("err", exc.code))

        t1 = threading.Thread(target=apply)
        t2 = threading.Thread(target=apply)
        t1.start(); t2.start(); t1.join(); t2.join()
        oks = [item for item in outcomes if item[0] == "ok"]
        errs = [item for item in outcomes if item[0] == "err"]
        self.assertEqual(len(oks), 1)
        self.assertEqual(len(errs), 1)
        self.assertEqual(errs[0][1], "deferral_active_exists")

    def test_single_reschedule_rejects_stale_revision_without_silent_overwrite(self):
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR3", "name": "丙组", "base": "AAA", "duty_start": iso(self.base + timedelta(days=1) - timedelta(hours=2)), "max_duty_minutes": 1440})
        t10 = (self.base + timedelta(days=1)).replace(hour=10, minute=0)
        t12 = (self.base + timedelta(days=1)).replace(hour=12, minute=0)
        f1 = self.pending_flight("AB230", "AC1", "CR3", t10, t12)
        with self.assertRaises(ApiError) as ctx:
            self.svc.reschedule_flight(f1["id"], "sched", "scheduler", {"expected_revision": 999, "new_std": iso(t10), "new_sta": iso(t12)})
        self.assertEqual(ctx.exception.code, "revision_conflict")
        self.assertEqual(self.flight(f1["id"])["status"], "pending_reschedule")

    def test_plan_validation_accepts_deferral_covered_flight(self):
        self.svc.seed_aircraft("ops", "ops_manager", {"id": "AC4", "model": "A320", "maintenance_due": iso(self.base + timedelta(hours=12))})
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR3", "name": "丙组", "base": "AAA", "duty_start": iso(self.base + timedelta(days=1) - timedelta(hours=2)), "max_duty_minutes": 720})
        f1 = self.make_flight("AB240", "AC4", "CR3", self.base + timedelta(days=1), self.base + timedelta(days=1, hours=2))
        plan = self.svc.create_plan("sched", "scheduler", {"disruption_id": self.svc.create_disruption("sched", "scheduler", {"kind": "aircraft_fault", "resource_id": "AC4", "starts_at": iso(self.base), "ends_at": iso(self.base + timedelta(days=1))})["id"], "name": "保留放行方案", "assignments": [{"flight_id": f1["id"], "aircraft_id": "AC4", "crew_id": "CR3", "new_std": iso(self.base + timedelta(days=1)), "new_sta": iso(self.base + timedelta(days=1, hours=2))}]})
        check = self.svc.validate_plan(plan["id"], "auditor", "auditor")
        self.assertFalse(check["valid"])
        self.assertIn("maintenance_due", {p["code"] for p in check["problems"]})
        self.svc.apply_deferral("sched", "scheduler", {"aircraft_id": "AC4", "reason": "故障保留", "granted_at": iso(self.base), "valid_until": iso(self.base + timedelta(days=2))})
        check = self.svc.validate_plan(plan["id"], "auditor", "auditor")
        self.assertTrue(check["valid"], check["problems"])


if __name__ == "__main__":
    unittest.main()
