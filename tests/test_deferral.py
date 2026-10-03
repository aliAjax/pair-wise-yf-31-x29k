import sys, tempfile, threading, unittest
from datetime import timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import AirlineRecoveryService, ApiError, iso, utcnow


class MaintenanceDeferralTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.svc = AirlineRecoveryService(Path(self.tmp.name) / "test.db")
        # 用固定基准时刻，保证宵禁等判断不依赖测试运行时间
        now = utcnow().replace(minute=0, second=0, microsecond=0)
        self.base = now + timedelta(days=1)
        while self.base.hour != 8:
            self.base += timedelta(hours=1)
        b = self.base
        self.svc.seed_airport("ops", "ops_manager", {"code": "AAA", "country": "CN", "curfew_start": "23:00", "curfew_end": "05:00"})
        self.svc.seed_airport("ops", "ops_manager", {"code": "BBB", "country": "CN", "curfew_start": "23:00", "curfew_end": "05:00"})
        # AC1 维护已到期（基准前 1 小时），AC2 状态良好
        self.svc.seed_aircraft("ops", "ops_manager", {"id": "AC1", "model": "A320", "maintenance_due": iso(b - timedelta(hours=1))})
        self.svc.seed_aircraft("ops", "ops_manager", {"id": "AC2", "model": "A320", "maintenance_due": iso(b + timedelta(days=5))})
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR1", "name": "甲组", "base": "AAA", "duty_start": iso(b - timedelta(hours=2)), "max_duty_minutes": 720})
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR2", "name": "乙组", "base": "AAA", "duty_start": iso(b - timedelta(hours=2)), "max_duty_minutes": 720})
        # 备用机组：值勤窗口更晚，用于白天重排
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR3", "name": "丙组", "base": "AAA", "duty_start": iso(b + timedelta(hours=8)), "max_duty_minutes": 720})
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR4", "name": "丁组", "base": "AAA", "duty_start": iso(b + timedelta(hours=8)), "max_duty_minutes": 720})
        self.svc.create_permit("ops", "ops_manager", {"origin": "AAA", "destination": "BBB", "valid_from": iso(b - timedelta(days=1)), "valid_to": iso(b + timedelta(days=2))})
        # 延期窗口覆盖 base..base+6h
        self.deferral = self.svc.grant_deferral("sched", "scheduler", {
            "aircraft_id": "AC1", "reason": "机务检查中，先延期放行",
            "valid_from": iso(b - timedelta(hours=1)), "valid_to": iso(b + timedelta(hours=6))})

    def tearDown(self): self.tmp.cleanup()

    def flight(self, number, aircraft="AC1", crew="CR1", std=None, sta=None, passengers=150):
        std = std or self.base
        sta = sta or self.base + timedelta(hours=2)
        return self.svc.create_flight("sched", "scheduler", {"flight_no": number, "origin": "AAA", "destination": "BBB",
                                                              "std": iso(std), "sta": iso(sta), "aircraft_id": aircraft,
                                                              "crew_id": crew, "passenger_count": passengers})

    def test_flight_needs_deferral_when_maintenance_due(self):
        with self.assertRaises(ApiError) as ctx:
            self.flight("ZZ1", aircraft="AC1", crew="CR1", std=self.base + timedelta(days=2), sta=self.base + timedelta(days=2, hours=2))
        self.assertEqual(ctx.exception.code, "maintenance_due")
        held = self.flight("AB100")
        self.assertEqual(held["deferral_id"], self.deferral["id"])

    def test_concurrent_grant_only_one_wins(self):
        # 先让初始延期失效，随后两人并发申请同一架飞机的新延期
        self.svc.expire_deferral(self.deferral["id"], "sched", "scheduler")
        barrier = threading.Barrier(2)
        outcomes = []

        def apply(name):
            try:
                barrier.wait()
                d = self.svc.grant_deferral(name, "scheduler", {
                    "aircraft_id": "AC1", "reason": f"{name} 的延期申请",
                    "valid_from": iso(self.base + timedelta(hours=10)), "valid_to": iso(self.base + timedelta(hours=20))})
                outcomes.append(("ok", d["id"]))
            except ApiError as exc:
                outcomes.append(("err", exc.code, exc.details))

        t1 = threading.Thread(target=apply, args=("alice",)); t2 = threading.Thread(target=apply, args=("bob",))
        t1.start(); t2.start(); t1.join(); t2.join()
        oks = [o for o in outcomes if o[0] == "ok"]
        errs = [o for o in outcomes if o[0] == "err"]
        self.assertEqual(len(oks), 1, outcomes)
        self.assertEqual(len(errs), 1)
        self.assertEqual(errs[0][1], "deferral_conflict")
        # 冲突响应指向已经生效的那份延期，输家据此按飞机编号处理未完成航段
        self.assertEqual(errs[0][2]["active_deferral_id"], oks[0][1])

    def test_expiry_invalidates_pending_but_keeps_landed_receipt(self):
        f1 = self.flight("AB201")
        f2 = self.flight("AB202", std=self.base + timedelta(hours=1), sta=self.base + timedelta(hours=3))
        # F1 在延期到期前落地，保留回执
        landed = self.svc.land_flight(f1["id"], "sched", "scheduler", {"landed_at": iso(self.base + timedelta(hours=2))})
        self.assertTrue(landed["receipt"]["deferral_id"], self.deferral["id"])
        # 延期到期：未执行的 F2 立即失效退回待重排，F1 不动
        result = self.svc.expire_deferral(self.deferral["id"], "sched", "scheduler")
        self.assertIn(f2["id"], result["pending_reschedule_flight_ids"])
        state = self.svc.state()
        statuses = {f["id"]: f["status"] for f in state["flights"]}
        self.assertEqual(statuses[f1["id"]], "landed")
        self.assertEqual(statuses[f2["id"]], "needs_reschedule")
        kept = [r for r in state["receipts"] if r["flight_id"] == f1["id"]]
        self.assertEqual(len(kept), 1)
        self.assertEqual(kept[0]["aircraft_id"], "AC1")

    def test_sweep_auto_expires_based_on_window(self):
        f = self.flight("AB301")
        # 不调 expire，直接查列表时自动扫描到期（窗口相对当前时间）
        short = self.svc.grant_deferral("sched", "scheduler", {"aircraft_id": "AC2", "reason": "短时延期",
                                                                "valid_from": iso(utcnow() - timedelta(minutes=10)),
                                                                "valid_to": iso(utcnow() - timedelta(minutes=1))})
        out = self.svc.list_deferrals("AC2")
        self.assertEqual(out["swept"], 1)
        self.assertEqual([d for d in out["deferrals"] if d["id"] == short["id"]][0]["status"], "expired")

    def test_reschedule_rechecks_all_constraints_and_idempotent(self):
        f = self.flight("AB401")
        self.svc.expire_deferral(self.deferral["id"], "sched", "scheduler")
        f = {x["id"]: x for x in self.svc.state()["flights"]}[f["id"]]
        self.assertEqual(f["status"], "needs_reschedule")

        # 1) 仍用 AC1 且延期已失效 -> 维护间隔不过
        r = self.svc.reschedule_flights("sched", "scheduler", {"aircraft_id": "AC1", "items": [{
            "flight_id": f["id"], "new_std": iso(self.base + timedelta(hours=1)),
            "new_sta": iso(self.base + timedelta(hours=3))}]})
        self.assertEqual(r["processed"][0]["result"], "failed")
        self.assertIn("maintenance_due", [p["code"] for p in r["processed"][0]["problems"]])
        self.assertEqual(r["remaining_flight_ids"], [f["id"]])

        # 2) 宵禁不通过（02:00-04:00 到达 BBB 且非豁免）
        r = self.svc.reschedule_flights("sched", "scheduler", {"aircraft_id": "AC1", "items": [{
            "flight_id": f["id"], "aircraft_id": "AC2", "crew_id": "CR2",
            "new_std": iso(self.base - timedelta(hours=6)), "new_sta": iso(self.base - timedelta(hours=4))}]})
        self.assertIn("airport_curfew", [p["code"] for p in r["processed"][0]["problems"]])

        # 3) 换 AC2/CR3、白天时刻 -> 成功，飞机和机组重新占用
        new_std, new_sta = self.base + timedelta(hours=10), self.base + timedelta(hours=12)
        r = self.svc.reschedule_flights("sched", "scheduler", {"aircraft_id": "AC1", "items": [{
            "flight_id": f["id"], "aircraft_id": "AC2", "crew_id": "CR3",
            "new_std": iso(new_std), "new_sta": iso(new_sta)}]})
        self.assertEqual(r["processed"][0]["result"], "rescheduled")
        self.assertTrue(r["queue_empty"])
        row = {x["id"]: x for x in self.svc.state()["flights"]}[f["id"]]
        self.assertEqual(row["status"], "scheduled")
        self.assertEqual(row["aircraft_id"], "AC2")
        self.assertEqual(row["crew_id"], "CR3")
        self.assertIsNone(row["deferral_id"])

        # 4) 队列已空，重复重试幂等
        r = self.svc.reschedule_flights("sched", "scheduler", {"aircraft_id": "AC1", "items": [{
            "flight_id": f["id"], "new_std": iso(new_std), "new_sta": iso(new_sta)}]})
        self.assertTrue(r["queue_empty"])
        self.assertEqual(r["processed"], [])

    def test_reschedule_blocks_double_booking_aircraft_and_crew(self):
        f = self.flight("AB501")
        # 另一班已经占住 AC2/CR4
        other = self.flight("AB502", aircraft="AC2", crew="CR4",
                            std=self.base + timedelta(hours=10), sta=self.base + timedelta(hours=12))
        self.svc.expire_deferral(self.deferral["id"], "sched", "scheduler")
        r = self.svc.reschedule_flights("sched", "scheduler", {"aircraft_id": "AC1", "items": [{
            "flight_id": f["id"], "aircraft_id": "AC2", "crew_id": "CR4",
            "new_std": iso(self.base + timedelta(hours=10, minutes=30)),
            "new_sta": iso(self.base + timedelta(hours=12, minutes=30))}]})
        codes = {p["code"] for p in r["processed"][0]["problems"]}
        self.assertIn("aircraft_overlap", codes)
        self.assertIn("crew_overlap", codes)
        self.assertEqual(r["processed"][0]["problems"][0]["conflict_flight_id"], other["id"])
        # 失败后航段仍在队列，飞机/机组没有被重复占用
        self.assertEqual(r["remaining_flight_ids"], [f["id"]])
        row = {x["id"]: x for x in self.svc.state()["flights"]}[f["id"]]
        self.assertEqual(row["status"], "needs_reschedule")

    def test_conflict_loser_retries_by_aircraft_and_rebooks(self):
        # 模拟：输家写入失败后拿到冲突信息，按飞机编号把未完成航段重排
        f = self.flight("AB601")
        self.svc.expire_deferral(self.deferral["id"], "sched", "scheduler")
        row = {x["id"]: x for x in self.svc.state()["flights"]}[f["id"]]
        self.assertEqual(row["status"], "needs_reschedule")

        barrier = threading.Barrier(2)
        outcomes = []

        def apply(name):
            try:
                barrier.wait()
                d = self.svc.grant_deferral(name, "scheduler", {
                    "aircraft_id": "AC1", "reason": f"{name} 的申请",
                    "valid_from": iso(self.base - timedelta(hours=1)),
                    "valid_to": iso(self.base + timedelta(hours=30))})
                outcomes.append(("ok", d))
            except ApiError as exc:
                outcomes.append(("err", exc))

        t1 = threading.Thread(target=apply, args=("carol",)); t2 = threading.Thread(target=apply, args=("dave",))
        t1.start(); t2.start(); t1.join(); t2.join()
        winner = next(o[1] for o in outcomes if o[0] == "ok")
        loser = next(o[1] for o in outcomes if o[0] == "err")
        self.assertEqual(loser.code, "deferral_conflict")
        self.assertEqual(loser.details["active_deferral_id"], winner["id"])
        self.assertEqual(loser.details["pending_reschedule_flight_ids"], [f["id"]])

        # 输家不重复占用，而是按飞机编号重试未完成航段：新延期覆盖原时刻 -> 重新占用成功
        r = self.svc.reschedule_flights("dave", "scheduler", {"aircraft_id": "AC1", "items": [{
            "flight_id": f["id"], "new_std": iso(self.base + timedelta(hours=1)),
            "new_sta": iso(self.base + timedelta(hours=3))}]})
        self.assertEqual(r["processed"][0]["result"], "rescheduled")
        self.assertEqual(r["processed"][0]["deferral_id"], winner["id"])
        row = {x["id"]: x for x in self.svc.state()["flights"]}[f["id"]]
        self.assertEqual(row["status"], "scheduled")
        self.assertEqual(row["aircraft_id"], "AC1")
        self.assertEqual(row["deferral_id"], winner["id"])

    def test_revoke_cascades_and_draft_assignment_invalidates(self):
        f = self.flight("AB701")
        disruption = self.svc.create_disruption("sched", "scheduler", {
            "kind": "aircraft_fault", "resource_id": "AC1",
            "starts_at": iso(self.base - timedelta(hours=1)), "ends_at": iso(self.base + timedelta(hours=3))})
        plan = self.svc.create_plan("sched", "scheduler", {
            "disruption_id": disruption["id"], "name": "依赖延期的草稿",
            "assignments": [{"flight_id": f["id"], "aircraft_id": "AC1", "crew_id": "CR1",
                             "new_std": iso(self.base + timedelta(hours=1)), "new_sta": iso(self.base + timedelta(hours=3))}]})
        out = self.svc.revoke_deferral(self.deferral["id"], "ops", "ops_manager", {"reason": "检查不合格"})
        self.assertIn(f["id"], out["pending_reschedule_flight_ids"])
        check = self.svc.validate_plan(plan["id"], "auditor", "auditor")
        self.assertFalse(check["valid"])
        self.assertIn("deferral_expired", [p["code"] for p in check["problems"]])
        with self.assertRaises(ApiError) as ctx:
            self.svc.lock_plan(plan["id"], "ops", "ops_manager", {"expected_revision": 2})
        self.assertEqual(ctx.exception.code, "plan_invalid")
        # 重新提交航段后失效标记清除，可再次校验（此时维护间隔仍不过 -> maintenance_due）
        self.svc.add_assignment(plan["id"], "sched", "scheduler", {
            "expected_revision": 2, "flight_id": f["id"], "aircraft_id": "AC1", "crew_id": "CR1",
            "new_std": iso(self.base + timedelta(hours=1)), "new_sta": iso(self.base + timedelta(hours=3))})
        check = self.svc.validate_plan(plan["id"], "auditor", "auditor")
        codes = {p["code"] for p in check["problems"]}
        self.assertNotIn("deferral_expired", codes)
        self.assertIn("maintenance_due", codes)

    def test_extend_deferral_revision_guards_window(self):
        extended = self.svc.extend_deferral(self.deferral["id"], "sched", "scheduler",
                                            {"expected_revision": 1, "valid_to": iso(self.base + timedelta(hours=24))})
        self.assertEqual(extended["revision"], 2)
        with self.assertRaises(ApiError) as ctx:
            self.svc.extend_deferral(self.deferral["id"], "sched", "scheduler",
                                     {"expected_revision": 1, "valid_to": iso(self.base + timedelta(hours=30))})
        self.assertEqual(ctx.exception.code, "revision_conflict")
        # 延长后覆盖窗口内的新航段可以挂延期
        f = self.flight("AB901", std=self.base + timedelta(hours=8), sta=self.base + timedelta(hours=10))
        self.assertEqual(f["deferral_id"], self.deferral["id"])

    def test_landed_flight_skips_reschedule_retry(self):
        f1 = self.flight("AB801")
        self.svc.land_flight(f1["id"], "sched", "scheduler", {})
        self.svc.expire_deferral(self.deferral["id"], "sched", "scheduler")
        # 已落地的航段不在队列里，按飞机编号重试是空操作，回执保留
        r = self.svc.reschedule_flights("sched", "scheduler", {"aircraft_id": "AC1", "items": [{
            "flight_id": f1["id"], "new_std": iso(self.base), "new_sta": iso(self.base + timedelta(hours=2))}]})
        self.assertTrue(r["queue_empty"])
        receipts = [x for x in self.svc.state()["receipts"] if x["flight_id"] == f1["id"]]
        self.assertEqual(len(receipts), 1)


if __name__ == "__main__": unittest.main()
