"""Phase 6.1: deterministic real SQLite scheduler and Outbox fencing tests."""
import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from patrol.components.persistence.index import Database, StaleLease
from patrol.components.scheduling.index import Scheduler
from patrol.components.outbox.index import Outbox
from patrol.components.queue.index import Queue
from patrol.components.config.index import Config
from patrol.components.runtime.index import Tick, main
from patrol.components.wakeup.index import Wakeups
from patrol.components.promises.index import Promises
from patrol.components.scheduling.policy.scoring import priority, preemption


class PreemptiveSchedulerTests(unittest.TestCase):
    def setUp(self):
        directory=tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.db=Database(Path(directory.name)/"schedule.sqlite3")
        self.scheduler=Scheduler(self.db,lease_seconds=120)
        with self.db.transaction() as c:
            c.execute("INSERT INTO meta(key,value) VALUES('actor','muse')")

    def add(self,key,kind="social",now=1000,score=50,weight=50,deadline=None):
        return self.scheduler.register(key,kind,now,priority_score=score,
                                       weight=weight,deadline=deadline)

    def test_issue31_schedule_next_imports_fresh_owner_work_before_selection(self):
        low=Queue(self.db).enqueue("work:old-lower","human_action",
                                   {"reason":"background review"},1000,priority=45)
        self.scheduler.sync_work(1000)
        owner_work=Queue(self.db).enqueue("work:owner-dm","human_action",
                                          {"reason":"Owner requests reply"},1001,
                                          priority=85)
        # No tick reconcile or explicit schedule-sync after this new DM.
        # schedule-next itself must make the Owner's work runnable.
        result=self.scheduler.dispatch("muse-worker",1002)
        self.assertTrue(result["granted"],result)
        self.assertEqual(result["activity"]["work_id"],owner_work)
        self.assertGreaterEqual(result["activities_registered"],1)
        self.assertNotEqual(result["activity"]["work_id"],low)
        claim=Queue(self.db).claim_specific(owner_work,"muse-worker",1003,
                                             {"approved_by":"Owner"})
        self.assertEqual(claim["scheduler_activity_id"],result["activity"]["id"])
        ident=Outbox(self.db).stage("owner-ack","muse","send","owner-chat",
                                   {"content":"收到，1"},1004,
                                   schedule_context={
                                       "id":result["activity"]["id"],
                                       "owner":"muse-worker",
                                       "generation":result["activity"]["lease_version"]})
        self.assertTrue(ident)
        self.assertEqual(self.db.read(
            "SELECT status FROM outbox WHERE id=?",(ident,))[0]["status"],"prepared")

    def test_issue31_owner_recovery_after_live_lower_priority_slice_yields(self):
        lower=Queue(self.db).enqueue("work:low-running","human_action",
                                      {"reason":"lower priority"},1000,priority=45)
        first=self.scheduler.dispatch("worker-low",1001)["activity"]
        self.assertEqual(first["work_id"],lower)
        claim_low=Queue(self.db).claim_specific(lower,"worker-low",1002,
                                              {"approved_by":"Owner"})
        owner=Queue(self.db).enqueue("work:urgent-owner","human_action",
                                     {"reason":"owner DM"},1003,priority=85)
        blocked=self.scheduler.dispatch("worker-owner",1004,for_work_id=owner)
        self.assertFalse(blocked["granted"])
        self.assertEqual(blocked["requested_work_id"],owner)
        self.assertEqual(blocked["hint_status"],"eligible")
        self.assertEqual(blocked["advice"],"checkpoint_at_safe_point")
        with self.assertRaises(StaleLease):
            Queue(self.db).claim_specific(owner,"worker-owner",1004,
                                         {"approved_by":"Owner"})
        yielded=self.scheduler.checkpoint(first["id"],"worker-low",
                                         first["lease_version"],1005,
                                         {"safe_point":"handoff urgent DM"},voluntary=True)
        self.assertTrue(yielded["suspended"])
        self.assertEqual(self.db.read(
            "SELECT status FROM work_items WHERE id=?",(lower,))[0]["status"],"pending")
        resumed=self.scheduler.dispatch("worker-owner",1006,for_work_id=owner)
        self.assertTrue(resumed["granted"],resumed)
        self.assertEqual(resumed["activity"]["work_id"],owner)
        Queue(self.db).claim_specific(owner,"worker-owner",1007,
                                     {"approved_by":"Owner"})
        guard={"id":resumed["activity"]["id"],"owner":"worker-owner",
               "generation":resumed["activity"]["lease_version"]}
        oid=Outbox(self.db).stage("unique-owner-reply","muse","send","owner-chat",
                                  {"content":"收到，1"},1008,schedule_context=guard)
        self.assertTrue(oid)
        with self.assertRaises(StaleLease):
            Outbox(self.db).stage("stale-old-worker","muse","send","owner-chat",
                {"content":"duplicate"},1009,
                schedule_context={"id":first["id"],"owner":"worker-low",
                                  "generation":first["lease_version"]})
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM outbox")[0]["n"],1)

    def test_issue31_expired_linked_leases_recover_then_owner_dispatches(self):
        low=Queue(self.db).enqueue("work:stale-low","human_action",
                                    {"reason":"old"},1000,priority=45)
        first=self.scheduler.dispatch("worker-vanished",1000)["activity"]
        claimed=Queue(self.db).claim_specific(low,"worker-vanished",1001,
                                              {"approved_by":"Owner"})
        urgent=Queue(self.db).enqueue("work:fresh-owner","human_action",
                                       {"reason":"Owner DM"},1010,priority=85)
        # Dispatch must fence the scheduler AND its linked Queue lease.
        result=self.scheduler.dispatch("worker-new",1120,for_work_id=urgent)
        self.assertTrue(result["granted"],result)
        self.assertEqual(result["lease_recovered"],1)
        self.assertEqual(result["activity"]["work_id"],urgent)
        old_work=self.db.read("SELECT status,lease_version FROM work_items WHERE id=?",
                              (low,))[0]
        self.assertEqual(old_work["status"],"pending")
        self.assertGreater(old_work["lease_version"],claimed["lease_version"])
        with self.assertRaises(StaleLease):
            Outbox(self.db).stage("crashed-worker-late","muse","send","owner-chat",
                {"content":"late"},1121,
                schedule_context={"id":first["id"],"owner":"worker-vanished",
                                  "generation":first["lease_version"]})
        Queue(self.db).claim_specific(urgent,"worker-new",1121,
                                     {"approved_by":"Owner"})
        oid=Outbox(self.db).stage("owner-after-recovery","muse","send","owner-chat",
                                 {"content":"收到，1"},1122,
                                 schedule_context={"id":result["activity"]["id"],
                                     "owner":"worker-new",
                                     "generation":result["activity"]["lease_version"]})
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM outbox")[0]["n"],1)
        self.assertTrue(oid)

    def test_issue31_orphaned_queue_lease_recovered_only_after_real_expiry(self):
        wid=Queue(self.db).enqueue("work:orphan","human_action",
                                   {"reason":"legacy inconsistent state"},1000,priority=85)
        current=self.scheduler.dispatch("worker-old",1000)["activity"]
        queue_old=Queue(self.db).claim_specific(wid,"worker-old",1001,
                                                {"approved_by":"Owner"})
        # Reproduce a damaged/old handoff: Scheduler suspended while the
        # underlying Queue row still claims a worker. Do not assume this
        # partial state can be created by the current atomic checkpoint.
        with self.db.transaction() as c:
            c.execute("""UPDATE schedule_activities SET status='suspended',
                lease_owner=NULL,lease_until=NULL,slice_until=NULL
                WHERE id=?""",(current["id"],))
        premature=self.scheduler.dispatch("worker-new",1100,for_work_id=wid)
        self.assertFalse(premature["granted"],premature)
        self.assertEqual(premature["lease_recovered"],0)
        self.assertEqual(premature["hint_status"],"work_running")
        still=self.db.read("SELECT status,lease_version FROM work_items WHERE id=?",
                           (wid,))[0]
        self.assertEqual(still["status"],"running")
        self.assertEqual(still["lease_version"],queue_old["lease_version"])
        recovered=self.scheduler.dispatch("worker-new",1121,for_work_id=wid)
        self.assertTrue(recovered["granted"],recovered)
        self.assertEqual(recovered["lease_recovered"],1)
        self.assertEqual(recovered["activity"]["work_id"],wid)
        new_claim=Queue(self.db).claim_specific(wid,"worker-new",1122,
                                               {"approved_by":"Owner"})
        self.assertGreater(new_claim["lease_version"],queue_old["lease_version"])
        with self.assertRaises(StaleLease):
            Outbox(self.db).stage("orphan-old-writer","muse","send","owner",
                {"content":"old"},1123,schedule_context={
                    "id":current["id"],"owner":"worker-old",
                    "generation":current["lease_version"]})
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM outbox")[0]["n"],0)

    def test_issue31_operator_hint_does_not_override_ineligible_or_higher_work(self):
        low=Queue(self.db).enqueue("work:hint-low","human_action",
                                    {"reason":"lower priority"},1000,priority=45)
        high=Queue(self.db).enqueue("work:hint-high","human_action",
                                     {"reason":"more urgent"},1001,priority=85)
        result=self.scheduler.dispatch("operator",1002,for_work_id=low)
        self.assertFalse(result["granted"],result)
        self.assertEqual(result["hint_status"],"outscored_by_other_activity")
        self.assertEqual(result["candidate"]["work_id"],high)
        self.assertIsNone(self.scheduler.assess(1002)["running"])
        with self.db.transaction() as c:
            c.execute("UPDATE work_items SET status='needs_approval' WHERE id=?",(high,))
        rejected=self.scheduler.dispatch("operator",1003,for_work_id=high)
        self.assertFalse(rejected["granted"],rejected)
        self.assertEqual(rejected["hint_status"],"work_needs_approval")
        self.assertIsNone(self.scheduler.assess(1003)["running"])
        unknown=self.scheduler.dispatch("operator",1004,for_work_id="not-found")
        self.assertEqual(unknown["hint_status"],"unknown_or_nonhuman_work")
        self.assertFalse(unknown["granted"])
        correct=self.scheduler.dispatch("operator",1005,for_work_id=low)
        self.assertTrue(correct["granted"],correct)
        self.assertEqual(correct["activity"]["work_id"],low)

    def test_issue31_sync_does_not_duplicate_custom_work_activity(self):
        work=Queue(self.db).enqueue("work:custom-key","human_action",
                                     {"reason":"existing handoff"},1000,priority=85)
        registered=self.scheduler.register("operator:existing","work",1000,
                        priority_score=85,work_id=work)
        result=self.scheduler.dispatch("muse",1001)
        self.assertTrue(result["granted"])
        self.assertEqual(result["activity"]["id"],registered)
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM schedule_activities "
                                     "WHERE work_id=?",(work,))[0]["n"],1)

    def test_issue31_cli_for_work_id_guard_reports_candidate_without_stealing(self):
        other=Queue(self.db).enqueue("work:cli-high","human_action",
                                     {"reason":"urgent"},1000,priority=90)
        focused=Queue(self.db).enqueue("work:cli-low","human_action",
                                       {"reason":"operator target"},1001,priority=40)
        out=io.StringIO()
        with patch("patrol.components.runtime.composition.cli.time.time",
                   return_value=1002),contextlib.redirect_stdout(out):
            code=main(["--db",str(self.db.path),"schedule-next",
                       "--owner","operator","--for-work-id",focused])
        self.assertEqual(code,0,out.getvalue())
        report=json.loads(out.getvalue())
        self.assertFalse(report["granted"],report)
        self.assertEqual(report["hint_status"],"outscored_by_other_activity")
        self.assertEqual(report["requested_work_id"],focused)
        self.assertEqual(report["candidate"]["work_id"],other)

    def test_issue31_focused_sync_not_starved_by_bounded_priority_scan(self):
        queue=Queue(self.db)
        for n in range(501):
            queue.enqueue("work:bulk-"+str(n),"human_action",
                          {"reason":"existing urgent backlog"},1000,
                          priority=90)
        target=queue.enqueue("work:focused-low","human_action",
                             {"reason":"diagnose only"},1001,priority=10)
        result=self.scheduler.dispatch("operator",1002,for_work_id=target)
        self.assertFalse(result["granted"],result)
        self.assertEqual(result["hint_status"],"outscored_by_other_activity")
        self.assertEqual(result["requested_activity"]["work_id"],target)
        self.assertEqual(self.db.read(
            "SELECT count(*) AS n FROM schedule_activities WHERE work_id=?",
            (target,))[0]["n"],1)

    def test_deadline_beats_high_social_score_without_fixed_priority_lanes(self):
        self.add("social","social",score=75)
        task=self.add("urgent","work",score=45,deadline=1015)
        d=self.scheduler.dispatch("worker-1",1000)
        self.assertTrue(d["granted"])
        self.assertEqual(d["activity"]["id"],task)
        self.assertEqual(d["activity"]["lease_version"],1)
        self.assertLessEqual(d["activity"]["slice_until"],d["activity"]["lease_until"])

    def test_preemption_requires_cooperative_checkpoint_and_fences_old_generation(self):
        social=self.add("chat","social",score=60)
        first=self.scheduler.dispatch("muse-a",1000)
        self.assertEqual(first["activity"]["id"],social)
        generation=first["activity"]["lease_version"]
        self.add("task","work",now=1001,score=50,deadline=1020)
        decision=self.scheduler.dispatch("muse-b",1001)
        self.assertFalse(decision["granted"])
        self.assertEqual(decision["advice"],"checkpoint_at_safe_point")
        self.assertIn(decision["preemption"],("urgent_safe_point","medium_safe_point"))
        state=self.scheduler.snapshot(1001)["activities"]
        self.assertTrue(next(x for x in state if x["id"]==social)["preempt_requested"])
        with self.assertRaises(StaleLease):
            Outbox(self.db).stage("reply-too-late","muse","send","chat",
                {"content":"must not send"},1001,
                schedule_context={"id":social,"owner":"muse-a",
                                  "generation":generation})
        paused=self.scheduler.checkpoint(social,"muse-a",generation,1002,
                                          {"phase":"chat","safe_point":"model_finished"})
        self.assertTrue(paused["suspended"])
        self.assertEqual(paused["activity"]["checkpoint"]["phase"],"chat")
        with self.assertRaises(StaleLease):
            self.scheduler.checkpoint(social,"muse-a",generation,1003,{"phase":"stale"})
        next_task=self.scheduler.dispatch("muse-b",1003)
        self.assertTrue(next_task["granted"])
        self.assertEqual(next_task["activity"]["kind"],"work")
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM outbox")[0]["n"],0)

    def test_staged_action_remains_durable_after_safe_point_yield(self):
        social=self.add("chat","social",score=60)
        claimed=self.scheduler.dispatch("muse-a",1000)["activity"]
        guard={"id":social,"owner":"muse-a","generation":claimed["lease_version"]}
        ident=Outbox(self.db).stage("a-first","muse","send","chat",
                                    {"content":"authorized payload"},1001,
                                    producer_id="muse-a",schedule_context=guard)
        self.scheduler.checkpoint(social,"muse-a",claimed["lease_version"],1002,
                                   {"sent_intent":ident},voluntary=True)
        row=self.db.read("SELECT status FROM outbox WHERE id=?",(ident,))[0]
        self.assertEqual(row["status"],"prepared")
        # An immutable, already-staged action may be retrieved even after its
        # original scheduler generation has been fenced by a safe-point yield.
        duplicate=Outbox(self.db).stage("a-first","muse","send","chat",
                                       {"content":"authorized payload"},1003,
                                       schedule_context=guard)
        self.assertEqual(duplicate,ident)
        audit=self.db.read("SELECT * FROM scheduled_outbox WHERE outbox_id=?",(ident,))
        self.assertEqual(audit[0]["lease_generation"],claimed["lease_version"])
        with self.assertRaises(StaleLease):
            Outbox(self.db).stage("another","muse","send","chat",
                                    {"content":"late"},1003,schedule_context=guard)

    def test_resume_preserves_checkpoint_and_increments_fence(self):
        ident=self.add("research","research",score=50)
        first=self.scheduler.dispatch("a",1000)["activity"]
        self.scheduler.checkpoint(ident,"a",first["lease_version"],1015,
                                  {"step":2,"summary":"partial"},voluntary=True)
        next_run=self.scheduler.dispatch("b",1016)["activity"]
        self.assertEqual(next_run["id"],ident)
        self.assertGreater(next_run["lease_version"],first["lease_version"])
        self.assertEqual(next_run["checkpoint"]["step"],2)
        with self.assertRaises(StaleLease):
            self.scheduler.complete(ident,"a",first["lease_version"],1017,{"proof":"stale"})
        done=self.scheduler.complete(ident,"b",next_run["lease_version"],1017,
                                      {"summary":"finished"})
        self.assertEqual(done["completed"],ident)
        self.assertFalse(done["remote_action_issued"])

    def test_recovered_expired_lease_never_silently_reuses_old_generation(self):
        ident=self.add("long-research","research")
        old=self.scheduler.dispatch("old",1000)["activity"]
        replacement=self.scheduler.dispatch("new",1120)
        self.assertTrue(replacement["granted"],replacement)
        self.assertEqual(replacement["lease_recovered"],1)
        self.assertEqual(replacement["activity"]["id"],ident)
        self.assertGreater(replacement["activity"]["lease_version"],old["lease_version"]+1)
        with self.assertRaises(StaleLease):
            self.scheduler.checkpoint(ident,"old",old["lease_version"],1121,
                                      {"state":"old"})
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM outbox")[0]["n"],0)

    def test_weighted_fairness_charges_real_slice_and_stays_bounded(self):
        low=self.add("small-weight",weight=10)
        r=self.scheduler.dispatch("a",1000)["activity"]
        self.assertEqual(r["id"],low)
        after=self.scheduler.checkpoint(low,"a",r["lease_version"],1030,
                                         {"state":"slice"})
        self.assertGreater(after["activity"]["virtual_runtime"],150)
        self.assertAlmostEqual(after["activity"]["cpu_seconds"],30)
        self.assertEqual(after["activity"]["slice_until"],r["slice_until"])

    def test_repeated_checkpoints_do_not_extend_a_competitive_slice(self):
        first_id=self.add("session-a",score=52,weight=50)
        second_id=self.add("session-b",score=52,weight=50)
        active=self.scheduler.dispatch("worker-a",1000)["activity"]
        original_end=active["slice_until"]
        for at in (1010,1020,1030,1040):
            saved=self.scheduler.checkpoint(active["id"],"worker-a",
                         active["lease_version"],at,{"progress":at})
            self.assertFalse(saved["suspended"])
            self.assertEqual(saved["activity"]["slice_until"],original_end)
        decision=self.scheduler.dispatch("worker-b",original_end)
        self.assertEqual(decision["advice"],"checkpoint_at_safe_point")
        yielded=self.scheduler.checkpoint(active["id"],"worker-a",
                     active["lease_version"],original_end+0.01,
                     {"progress":"yield"})
        self.assertTrue(yielded["suspended"])
        next_activity=self.scheduler.dispatch("worker-b",original_end+0.02)["activity"]
        self.assertIn(next_activity["id"],(first_id,second_id))
        self.assertNotEqual(next_activity["id"],active["id"])

    def test_time_slice_fits_a_short_configured_lease(self):
        short=Scheduler(self.db,lease_seconds=30)
        self.add("short-lease",weight=100)
        active=short.dispatch("worker",1000)["activity"]
        self.assertLess(active["slice_until"],active["lease_until"])

    def test_retry_wait_and_blocked_work_cannot_dispatch_early(self):
        work=Queue(self.db).enqueue("human:delayed","human_action",
                                    {"reason":"retry"},1000,priority=95)
        with self.db.transaction() as c:
            c.execute("UPDATE work_items SET status='retry_wait',next_action_at=2000 "
                      "WHERE id=?",(work,))
        self.scheduler.sync_work(1000)
        self.assertIsNone(self.scheduler.assess(1001)["candidate"])
        self.assertFalse(self.scheduler.dispatch("worker",1001)["granted"])
        with self.db.transaction() as c:
            c.execute("UPDATE work_items SET status='blocked' WHERE id=?",(work,))
        self.assertIsNone(self.scheduler.assess(2100)["candidate"])
        with self.db.transaction() as c:
            c.execute("UPDATE work_items SET status='retry_wait' WHERE id=?",(work,))
        self.assertTrue(self.scheduler.dispatch("worker",2100)["granted"])

    def test_aging_can_raise_old_low_priority_activity(self):
        older=self.add("old",now=0,score=47)
        newer=self.add("new",now=2800,score=60)
        picked=self.scheduler.dispatch("a",2800)["activity"]
        self.assertEqual(picked["id"],older)
        self.assertEqual(picked["kind"],"social")

    def test_quantum_expiry_without_competitor_does_not_pause(self):
        ident=self.add("solo",score=40)
        first=self.scheduler.dispatch("muse",1000)["activity"]
        safe=self.scheduler.checkpoint(ident,"muse",first["lease_version"],1080,
                                        {"phase":"next turn"})
        self.assertFalse(safe["suspended"])
        self.assertEqual(safe["activity"]["status"],"running")
        self.assertEqual(safe["activity"]["lease_version"],first["lease_version"])

    def test_equal_priority_activities_rotate_after_slice(self):
        a=self.add("social-a",score=52,weight=50)
        b=self.add("social-b",score=52,weight=50)
        first=self.scheduler.dispatch("worker-a",1000)["activity"]
        self.assertIn(first["id"],(a,b))
        # Competing equal-value work cannot force an immediate context switch.
        early=self.scheduler.dispatch("worker-b",1001)
        self.assertFalse(early["granted"])
        self.assertEqual(early["advice"],"continue")
        end=first["slice_until"]
        gate=self.scheduler.dispatch("worker-b",end)
        self.assertFalse(gate["granted"])
        self.assertEqual(gate["advice"],"checkpoint_at_safe_point")
        result=self.scheduler.checkpoint(first["id"],"worker-a",first["lease_version"],
                                         end+0.01,{"safe_point":"slice boundary"})
        self.assertTrue(result["suspended"])
        second=self.scheduler.dispatch("worker-b",end+0.02)["activity"]
        self.assertNotEqual(second["id"],first["id"])

    def test_external_work_completion_fences_ongoing_activity(self):
        work=Queue(self.db).enqueue("human:finished-elsewhere","human_action",
                                    {"reason":"work"},1000,priority=75)
        self.scheduler.sync_work(1000)
        active=self.scheduler.dispatch("owner",1001)["activity"]
        with self.db.transaction() as c:
            c.execute("UPDATE work_items SET status='done' WHERE id=?",(work,))
        self.scheduler.sync_work(1002)
        state=self.db.read("SELECT status,lease_version FROM schedule_activities WHERE id=?",
                           (active["id"],))[0]
        self.assertEqual(state["status"],"completed")
        self.assertGreater(state["lease_version"],active["lease_version"])
        with self.assertRaises(StaleLease):
            Outbox(self.db).stage("late","muse","send","chat",{"content":"stale"},1002,
                                 schedule_context={"id":active["id"],
                                   "owner":"owner","generation":active["lease_version"]})

    def test_idempotent_register_and_immutable_keys(self):
        ident=self.add("key",score=42)
        self.assertEqual(self.add("key",score=42),ident)
        with self.assertRaises(ValueError):
            self.add("key",score=43)
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM schedule_activities")[0]["n"],1)

    def test_linked_local_work_requires_independent_completion_evidence(self):
        work=Queue(self.db).enqueue("human:task","human_action",
                                    {"reason":"task needed"},1000,priority=82,start_by=1050)
        self.assertEqual(self.scheduler.sync_work(1001),1)
        self.assertEqual(self.scheduler.sync_work(1002),0)
        selected=self.scheduler.dispatch("worker",1003)["activity"]
        self.assertEqual(selected["work_id"],work)
        with self.assertRaises(ValueError):
            self.scheduler.complete(selected["id"],"worker",
                                    selected["lease_version"],1004,{"done":True})
        # The scheduler does not claim or finish the original work itself.
        self.assertEqual(self.db.read("SELECT status FROM work_items WHERE id=?",(work,))[0]["status"],"pending")
        with self.db.transaction() as c:
            c.execute("UPDATE work_items SET status='done' WHERE id=?",(work,))
        self.assertEqual(self.scheduler.complete(selected["id"],"worker",
                         selected["lease_version"],1004,{"work_proof":"separate"} )["completed"],selected["id"])

    def test_cancel_only_nonrunning_and_assess_does_not_change_owner(self):
        ident=self.add("unused")
        before=self.scheduler.assess(1000)
        self.assertEqual(before["advice"],"ready_to_dispatch")
        self.scheduler.cancel(ident,1001,"no longer relevant")
        self.assertEqual(self.scheduler.assess(1001)["advice"],"idle")
        active=self.add("active",now=1002)
        claimed=self.scheduler.dispatch("worker",1002)["activity"]
        with self.assertRaises(ValueError):
            self.scheduler.cancel(active,1003,"cannot kill remote work")
        self.assertEqual(claimed["status"],"running")

    def test_reconciliation_uses_latest_muse_score_not_old_work_priority(self):
        with self.db.transaction() as c:
            c.execute("""INSERT INTO events(source,scope,stream,external_key,
                         payload,observed_at) VALUES
                         ('messages','owner','','msg1','{}',1000)""")
            event_id=c.execute("SELECT last_insert_rowid()").fetchone()[0]
            c.execute("""INSERT INTO message_scores(event_id,base_score,baseline_at)
                         VALUES(?,?,?)""",(event_id,25,1000))
        ident=Queue(self.db).enqueue("human:score-updated","human_action",
                                     {"event_id":event_id},1000,
                                     priority=25,event_id=event_id)
        self.assertEqual(self.scheduler.sync_work(1001),1)
        a=self.db.read("SELECT base_priority FROM schedule_activities WHERE work_id=?",
                       (ident,))[0]
        self.assertEqual(a["base_priority"],25)
        with self.db.transaction() as c:
            c.execute("UPDATE message_scores SET muse_score=92 WHERE event_id=?",
                      (event_id,))
        self.assertEqual(self.scheduler.sync_work(1002),0)
        b=self.db.read("SELECT base_priority FROM schedule_activities WHERE work_id=?",
                       (ident,))[0]
        self.assertEqual(b["base_priority"],92)

    def test_bounded_work_import_advances_past_old_already_registered_rows(self):
        for i in range(5):
            Queue(self.db).enqueue("human:backlog-"+str(i),"human_action",
                                   {"index":i},1000+i,priority=70)
        self.assertEqual(self.scheduler.sync_work(1100,limit=2),2)
        self.assertEqual(self.scheduler.sync_work(1101,limit=2),2)
        self.assertEqual(self.scheduler.sync_work(1102,limit=2),1)
        self.assertEqual(self.scheduler.sync_work(1103,limit=2),0)
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM schedule_activities")[0]["n"],5)

    def test_tick_imports_work_but_does_not_invoke_external_model(self):
        Queue(self.db).enqueue("human:auto","human_action",
                              {"reason":"owner work"},1000,priority=82)
        report=Tick(self.db,Config(database=str(self.db.path)),
                    clock=lambda:1000,monotonic=lambda:0).run(light=True)
        self.assertEqual(report["scheduler"]["work_registered"],1)
        self.assertFalse(report["scheduler"]["model_invoked"])
        self.assertFalse(report["scheduler"]["remote_action_issued"])
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM schedule_activities")[0]["n"],1)
        # Light Tick leaves human decisions pending for the host; it never
        # takes and blocks a work lease merely because it lacks a model.
        self.assertEqual(self.db.read("SELECT status FROM work_items WHERE logical_key='human:auto'")[0]["status"],"pending")


    def test_linked_work_yield_releases_queue_lease_and_reclaims_on_resume(self):
        work=Queue(self.db).enqueue("human:handoff","human_action",
                                    {"reason":"formal work"},1000,priority=50)
        self.scheduler.sync_work(1000)
        active=self.scheduler.dispatch("worker-a",1001)["activity"]
        original=Queue(self.db).claim_specific(work,"worker-a",1002,
                                               {"operator":"approved"})
        self.assertEqual(original["status"],"running")
        self.add("deadline-task","research",now=1003,score=70,deadline=1010)
        self.assertEqual(self.scheduler.assess(1003)["advice"],"checkpoint_at_safe_point")
        saved=self.scheduler.checkpoint(active["id"],"worker-a",
                        active["lease_version"],1004,
                        {"step":2,"summary":"partial"},voluntary=True)
        self.assertTrue(saved["suspended"])
        queue_state=self.db.read("SELECT status,lease_owner,lease_version FROM work_items WHERE id=?",(work,))[0]
        self.assertEqual(queue_state["status"],"pending")
        self.assertIsNone(queue_state["lease_owner"])
        self.assertGreater(queue_state["lease_version"],original["lease_version"])
        with self.assertRaises(StaleLease):
            Queue(self.db).claim_specific(work,"worker-a",1004.1,{"operator":"stale"})
        urgent=self.scheduler.dispatch("worker-b",1005)["activity"]
        self.assertEqual(urgent["logical_key"],"deadline-task")
        self.scheduler.complete(urgent["id"],"worker-b",urgent["lease_version"],
                                1006,{"finished":"urgent"})
        resumed=self.scheduler.dispatch("worker-a",1007)["activity"]
        self.assertEqual(resumed["id"],active["id"])
        self.assertEqual(resumed["checkpoint"]["step"],2)
        again=Queue(self.db).claim_specific(work,"worker-a",1008,{"operator":"approved"})
        self.assertGreater(again["lease_version"],original["lease_version"])
        with self.assertRaises(StaleLease):
            Queue(self.db).finish(original,1009,evidence={"stale":True})

    def test_recovery_fences_both_scheduler_and_queue_leases(self):
        work=Queue(self.db).enqueue("human:crash-recovery","human_action",
                                    {"reason":"task"},1000,priority=75)
        self.scheduler.sync_work(1000)
        active=self.scheduler.dispatch("worker-a",1001)["activity"]
        old=Queue(self.db).claim_specific(work,"worker-a",1002,{"started":"real"})
        # Scheduler expires before the matching Queue lease. Dispatch must
        # release the old Queue owner, not wait for the next patrol tick.
        resumed=self.scheduler.dispatch("worker-b",1121)
        self.assertEqual(resumed["lease_recovered"],1)
        self.assertEqual(resumed["activity"]["id"],active["id"])
        row=self.db.read("SELECT status,lease_version FROM work_items WHERE id=?",(work,))[0]
        self.assertEqual(row["status"],"pending")
        self.assertGreater(row["lease_version"],old["lease_version"])
        fresh=Queue(self.db).claim_specific(work,"worker-b",1122,{"reconciled":"outbox"})
        self.assertEqual(fresh["lease_owner"],"worker-b")

    def test_scheduler_work_requires_matching_queue_claim_before_effect(self):
        work=Queue(self.db).enqueue("human:fenced","human_action",
                                    {"reason":"requires claim"},1000,priority=75)
        self.scheduler.sync_work(1000)
        active=self.scheduler.dispatch("worker-a",1001)["activity"]
        context={"id":active["id"],"owner":"worker-a",
                 "generation":active["lease_version"]}
        with self.assertRaises(StaleLease):
            Outbox(self.db).stage("premature","muse","send","chat",
                                  {"content":"not claimed"},1001,schedule_context=context)
        Queue(self.db).claim_specific(work,"worker-a",1002,{"operator":"approved"})
        ident=Outbox(self.db).stage("claimed","muse","send","chat",
                                   {"content":"safe"},1003,schedule_context=context)
        self.assertTrue(ident)

    def test_unscheduled_business_staging_cannot_bypass_active_generation(self):
        self.add("social-work","social")
        with self.assertRaises(StaleLease):
            Outbox(self.db).stage("unfenced-ready","muse","send","chat",
                                  {"content":"must be scheduled"},1000)
        running=self.scheduler.dispatch("worker",1001)["activity"]
        with self.assertRaises(StaleLease):
            Outbox(self.db).stage("unfenced-running","muse","send","chat",
                                  {"content":"must be scheduled"},1002)
        self.scheduler.checkpoint(running["id"],"worker",running["lease_version"],
                                  1003,{"phase":"paused"},voluntary=True)
        with self.assertRaises(StaleLease):
            Outbox(self.db).stage("unfenced-suspended","muse","send","chat",
                                  {"content":"must be scheduled"},1004)
        # System receipt actions are intentionally independent of the
        # Scheduler, so the existing HATCH read/ack lane still works.
        ident=Outbox(self.db).stage("system-ack","muse","ack","inbox",
                                   {"ids":[1]},1005)
        self.assertTrue(ident)

    def test_disappearing_challenger_clears_preemption_and_avoids_yield(self):
        running_id=self.add("current","research",score=50)
        first=self.scheduler.dispatch("worker",1000)["activity"]
        contender=self.add("cancelled-urgent","research",now=1001,
                           score=90,deadline=1010)
        decision=self.scheduler.dispatch("other",1002)
        self.assertEqual(decision["advice"],"checkpoint_at_safe_point")
        self.scheduler.cancel(contender,1003,"no longer needed")
        continued=self.scheduler.checkpoint(running_id,"worker",
                           first["lease_version"],1004,{"progress":"ongoing"})
        self.assertFalse(continued["suspended"])
        self.assertFalse(continued["activity"]["preempt_requested"])
        self.assertEqual(self.scheduler.reconcile(1005)["decision"]["advice"],"continue")


    def test_deadline_escalation_triggers_host_wakeup_for_low_base_work(self):
        work=Queue(self.db).enqueue("human:deadline-wakeup","human_action",
                                    {"reason":"due soon"},1000,priority=15,
                                    due_at=1010)
        self.scheduler.sync_work(1000)
        count=Wakeups(self.db).stage(1000,threshold=70)
        self.assertEqual(count,1)
        result=self.db.read("""SELECT priority,source_id FROM wakeup_requests
            WHERE source_type='work'""")[0]
        self.assertEqual(result["source_id"],work)
        self.assertGreaterEqual(result["priority"],70)

    def test_distant_low_priority_work_does_not_wake_host_early(self):
        Queue(self.db).enqueue("human:distant","human_action",
                               {"reason":"not due"},1000,priority=15,
                               due_at=1000+7200)
        self.scheduler.sync_work(1000)
        self.assertEqual(Wakeups(self.db).stage(1000,threshold=70),0)


    def test_evidenced_blocked_work_handoff_reserves_scheduler_atomically(self):
        work=Queue(self.db).enqueue("human:manual-unblock","human_action",
                                    {"reason":"operator decision"},1000,priority=85)
        self.scheduler.sync_work(1000)
        with self.db.transaction() as cur:
            cur.execute("UPDATE work_items SET status='blocked' WHERE id=?",(work,))
        self.assertIsNone(self.scheduler.assess(1001)["candidate"])
        claimed=Queue(self.db).claim_specific(work,"muse-a",1002,
                                               {"approved_by":"operator"})
        self.assertEqual(claimed["status"],"running")
        active=self.scheduler.snapshot(1002)["decision"]["running"]
        self.assertEqual(claimed["scheduler_activity_id"],active["id"])
        self.assertEqual(claimed["scheduler_generation"],active["lease_version"])
        context={"id":active["id"],"owner":"muse-a",
                 "generation":active["lease_version"]}
        sent=Outbox(self.db).stage("manual-safe","muse","send","chat",
                                    {"content":"approved"},1003,
                                    schedule_context=context)
        self.assertTrue(sent)

    def test_scheduler_fencing_survives_completion_of_last_activity(self):
        activity=self.add("short-maintenance","maintenance")
        running=self.scheduler.dispatch("worker",1000)["activity"]
        self.scheduler.complete(activity,"worker",running["lease_version"],
                                1001,{"result":"done"})
        with self.assertRaises(StaleLease):
            Outbox(self.db).stage("post-completion-bypass","muse",
                                  "send","chat",{"content":"no fence"},1002)


    def test_bounded_waiting_credit_raises_old_activity_past_moderate_gap(self):
        self.add("urgent-looking-background","maintenance",now=1000,score=35)
        low=self.add("patient-background","maintenance",now=1000,score=5)
        picked=self.scheduler.dispatch("worker",1000)["activity"]
        # Use the pure heuristic to avoid expiring the simulated worker lease.
        running=self.db.read("SELECT * FROM schedule_activities WHERE id=?",(picked["id"],))[0]
        waiting=self.db.read("SELECT * FROM schedule_activities WHERE id=?",(low,))[0]
        self.assertLess(priority(waiting,1001),priority(running,1001,running=True))
        self.assertGreater(priority(waiting,5000),priority(running,5000,running=True))
        self.assertIsNotNone(preemption(running,waiting,5000))

    def test_aging_second_term_is_capped_for_extreme_wait(self):
        # Issue #45 (a): total aging saturates at 60 (30 + 30) no matter
        # how long an activity has waited. Before the cap, a 6-day wait
        # yielded aging ~17280 and a 1e9-second wait ~33 million.
        row={"base_priority":0,"deadline":None,"ready_since":0,
             "virtual_runtime":0,"switch_cost":0}
        self.assertEqual(priority(row,518400),60.0)
        self.assertEqual(priority(row,10**9),60.0)

    def test_extreme_wait_alone_cannot_preempt_high_base_running_slice(self):
        # Issue #45 (b): a challenger whose only advantage is an extreme
        # wait must not preempt a running slice with a larger base
        # advantage once aging is bounded. Before the cap, the challenger
        # below scored ~17281 and preempted unconditionally.
        now=10**7
        running={"base_priority":100,"deadline":None,"ready_since":now,
                 "virtual_runtime":0,"switch_cost":4,"slice_until":now+10000}
        challenger={"base_priority":5,"deadline":None,
                    "ready_since":now-518400,
                    "virtual_runtime":0,"switch_cost":4}
        self.assertEqual(priority(challenger,now),61.0)
        self.assertLess(priority(challenger,now),
                        priority(running,now,running=True))
        self.assertIsNone(preemption(running,challenger,now))

    def test_bounded_waiting_credit_cannot_beat_large_base_gap(self):
        # Issue #45: aging must saturate, so a merely old challenger cannot
        # preempt a running slice whose base advantage exceeds the bounded
        # credit. Before the cap, this challenger scored ~17281 and
        # preempted everything, livelocking grant -> preempt -> StaleLease.
        self.add("high-base-running","maintenance",now=1000,score=100)
        low=self.add("old-low-challenger","maintenance",now=1000,score=5)
        picked=self.scheduler.dispatch("worker",1000)["activity"]
        running=self.db.read("SELECT * FROM schedule_activities WHERE id=?",(picked["id"],))[0]
        waiting=self.db.read("SELECT * FROM schedule_activities WHERE id=?",(low,))[0]
        self.assertEqual(running["id"],picked["id"])
        self.assertLess(priority(waiting,1000+518400),priority(running,1000+518400,running=True))
        self.assertIsNone(preemption(running,waiting,1000+518400))

    def test_issue45_six_day_backlog_does_not_fence_new_owner_claim(self):
        # End-to-end regression for the production grant -> immediate
        # preempt_requested -> work-claim StaleLease loop.
        now=1000+6*86400
        stale=Queue(self.db).enqueue("issue45:old-social","human_action",
            {"reason":"stale low-value event"},1000,priority=5)
        owner=Queue(self.db).enqueue("issue45:new-owner","human_action",
            {"reason":"new Owner message"},now,priority=85)
        first=self.scheduler.dispatch("worker-owner",now,for_work_id=owner)
        self.assertTrue(first["granted"],first)
        self.assertEqual(first["activity"]["work_id"],owner)
        self.scheduler.reconcile(now+1)
        running=self.scheduler.assess(now+1)["running"]
        self.assertEqual(running["work_id"],owner)
        self.assertFalse(running["preempt_requested"])
        claim=Queue(self.db).claim_specific(owner,"worker-owner",now+2,
                                            {"approved_by":"Owner"})
        self.assertEqual(claim["scheduler_activity_id"],running["id"])
        self.assertEqual(self.db.read(
            "SELECT status FROM work_items WHERE id=?",(stale,))[0]["status"],
            "pending")

    def test_issue57_voluntary_maintenance_yield_allows_real_work(self):
        # A high-priority maintenance placeholder previously won every
        # dispatch after --yield, even with valid human work waiting.
        maint=self.add("authmanifest-cutover","maintenance",now=1000,score=100)
        work=Queue(self.db).enqueue("issue57:owner-work","human_action",
                                   {"reason":"real work"},1000,priority=45)
        first=self.scheduler.dispatch("worker",1000)["activity"]
        self.assertEqual(first["id"],maint)
        parked=self.scheduler.checkpoint(maint,"worker",
            first["lease_version"],1001,{"phase":"no actual work to perform"},
            voluntary=True)
        self.assertTrue(parked["suspended"])
        self.assertEqual(parked["activity"]["voluntary_yield_streak"],1)
        self.assertEqual(parked["activity"]["resume_after"],1061)
        resumed=self.scheduler.dispatch("worker",1002)
        self.assertTrue(resumed["granted"],resumed)
        self.assertEqual(resumed["activity"]["work_id"],work)
        item=Queue(self.db).claim_specific(work,"worker",1003,
                                          {"approved_by":"Owner"})
        self.assertEqual(item["scheduler_activity_id"],resumed["activity"]["id"])

    def test_issue57_repeated_voluntary_maintenance_yield_has_durable_backoff(self):
        maint=self.add("idle-maintenance","maintenance",now=1000,score=100)
        first=self.scheduler.dispatch("worker",1000)["activity"]
        for i,(at,delay) in enumerate(((1001,60),(1062,120),(1183,240),(1424,480)),1):
            paused=self.scheduler.checkpoint(maint,"worker",
                first["lease_version"],at,{"phase":"still idle"},voluntary=True)
            self.assertEqual(paused["activity"]["voluntary_yield_streak"],i)
            self.assertEqual(paused["activity"]["resume_after"],at+delay)
            self.assertIsNone(self.scheduler.assess(at+1)["candidate"])
            # Reopening the same SQLite DB must retain the deferral.
            reopened=Scheduler(Database(self.db.path),lease_seconds=120)
            self.assertIsNone(reopened.assess(at+1)["candidate"])
            granted=reopened.dispatch("worker",at+delay)
            self.assertTrue(granted["granted"],granted)
            first=granted["activity"]
            self.assertEqual(first["id"],maint)

    def test_issue57_preempted_maintenance_resumes_without_cooldown(self):
        maint=self.add("real-maintenance","maintenance",now=1000,score=45)
        first=self.scheduler.dispatch("worker",1000)["activity"]
        urgent=self.add("urgent-owner-research","research",now=1001,score=95)
        advice=self.scheduler.reconcile(1001)["decision"]
        self.assertEqual(advice["candidate"]["id"],urgent)
        self.assertTrue(advice["running"]["preempt_requested"])
        paused=self.scheduler.checkpoint(maint,"worker",
            first["lease_version"],1002,{"phase":"resume after interrupt"})
        self.assertTrue(paused["suspended"])
        self.assertEqual(paused["activity"]["voluntary_yield_streak"],0)
        self.assertEqual(paused["activity"]["resume_after"],0)
        critical=self.scheduler.dispatch("worker",1003)["activity"]
        self.assertEqual(critical["id"],urgent)
        self.scheduler.complete(critical["id"],"worker",
                                critical["lease_version"],1004,
                                {"decision":"read_only","reason":"verified"})
        after=self.scheduler.dispatch("worker",1005)
        self.assertTrue(after["granted"],after)
        self.assertEqual(after["activity"]["id"],maint)
        self.assertIsNotNone(after["activity"]["checkpoint"])

    def test_issue45_same_priority_aged_backlog_claim_survives_reconcile(self):
        # Three equal-priority six-day-old tasks were repeatedly granted then
        # instantly preempted by another equally old task before work-claim.
        queue=Queue(self.db)
        work_ids=[queue.enqueue("issue45:peer-"+str(i),"human_action",
                    {"reason":"legitimate backlog"},1000,priority=45)
                  for i in range(3)]
        self.scheduler.sync_work(1000)
        now=1000+6*86400
        granted=self.scheduler.dispatch("worker",now)
        self.assertTrue(granted["granted"],granted)
        self.assertIn(granted["activity"]["work_id"],work_ids)
        self.scheduler.reconcile(now+1)
        active=self.scheduler.assess(now+1)["running"]
        self.assertEqual(active["id"],granted["activity"]["id"])
        self.assertFalse(active["preempt_requested"],active)
        claim=queue.claim_specific(granted["activity"]["work_id"],
                                   "worker",now+2,{"approved_by":"Owner"})
        self.assertEqual(claim["scheduler_activity_id"],active["id"])
        self.assertEqual(sum(x["status"]=="pending" for x in
            self.db.read("SELECT status FROM work_items")),2)

    def test_issue45_aged_running_credit_does_not_block_fresh_owner(self):
        queue=Queue(self.db)
        for i in range(3):
            queue.enqueue("issue45:old-peer-"+str(i),"human_action",
                          {"reason":"old backlog"},1000,priority=45)
        self.scheduler.sync_work(1000)
        now=1000+6*86400
        active=self.scheduler.dispatch("worker-old",now)["activity"]
        urgent=queue.enqueue("issue45:new-owner-dm","human_action",
                             {"reason":"fresh Owner request"},now+1,
                             priority=85)
        decision=self.scheduler.reconcile(now+2)["decision"]
        self.assertEqual(decision["candidate"]["work_id"],urgent)
        self.assertTrue(decision["running"]["preempt_requested"],decision)
        self.assertEqual(decision["preemption"],"medium_safe_point")
        paused=self.scheduler.checkpoint(active["id"],"worker-old",
                           active["lease_version"],now+3,
                           {"phase":"yield to fresh Owner"})
        self.assertTrue(paused["suspended"])
        next_up=self.scheduler.dispatch("worker-owner",now+4,for_work_id=urgent)
        self.assertTrue(next_up["granted"],next_up)
        self.assertEqual(next_up["activity"]["work_id"],urgent)
        queue.claim_specific(urgent,"worker-owner",now+5,
                             {"approved_by":"Owner"})

    def test_imminent_deadline_protected_from_aged_nonurgent_activity(self):
        imminent=self.add("hard-due","work",now=1000,score=70,deadline=1015)
        waiting=self.add("old-background","maintenance",now=0,score=5)
        active=self.scheduler.dispatch("worker",1000)["activity"]
        self.assertEqual(active["id"],imminent)
        running=self.db.read("SELECT * FROM schedule_activities WHERE id=?",(imminent,))[0]
        challenger=self.db.read("SELECT * FROM schedule_activities WHERE id=?",(waiting,))[0]
        # A giant waiting credit must not force a task due in 15 seconds
        # out of its running safe point.
        self.assertIsNone(preemption(running,challenger,1000))


    def test_versioned_checkpoint_restores_progress_and_inspects_outbox(self):
        task=self.add("versioned-context","research",now=1000,score=55)
        running=self.scheduler.dispatch("muse",1001)["activity"]
        guard={"id":task,"owner":"muse","generation":running["lease_version"]}
        outbox_id=Outbox(self.db).stage("restore-intent","muse","send","chat",
                                      {"content":"already drafted"},1002,
                                      schedule_context=guard)
        checkpoint={"schema_version":1,"goal":"finish research",
                    "phase":"draft","completed_steps":["verified topic"],
                    "next_actions":["review source"],"decisions":["avoid duplicates"],
                    "resource_refs":[{"type":"task","id":"tsk_123"}],
                    "pending_outbox_ids":[outbox_id],
                    "source_task_version":2,"saved_at":1003}
        self.scheduler.checkpoint(task,"muse",running["lease_version"],1003,
                                  checkpoint,voluntary=True)
        pack=self.scheduler.restore(task,1004)
        self.assertEqual(pack["checkpoint_format"],"v1")
        self.assertEqual(pack["checkpoint"]["completed_steps"],["verified topic"])
        self.assertTrue(pack["requires_outbox_reconciliation"])
        self.assertEqual(pack["outbox_receipts"][0]["status"],"prepared")
        self.assertFalse(pack["side_effects_executed"])
        self.assertFalse(pack["model_state_restored"])
        with self.db.transaction() as cur:
            cur.execute("UPDATE outbox SET status='confirmed' WHERE id=?",(outbox_id,))
        self.assertFalse(self.scheduler.restore(task,1005)["requires_outbox_reconciliation"])

    def test_versioned_checkpoint_rejects_invalid_ids_and_untrusted_structure(self):
        task=self.add("invalid-context","maintenance",now=1000)
        running=self.scheduler.dispatch("muse",1000)["activity"]
        base={"schema_version":1,"goal":"continue","phase":"analysis",
              "completed_steps":[],"next_actions":[],"decisions":[],
              "resource_refs":[],"pending_outbox_ids":[],
              "source_task_version":None,"saved_at":1001}
        for bad in (
            dict(base, schema_version=99),
            dict(base, resource_refs=[{"type":"task","id":"injected instructions with spaces"}]),
            dict(base, pending_outbox_ids=["duplicate","duplicate"]),
            dict(base, saved_at=float("nan")),
            dict(base, source_task_version=-1),
            dict(base, unexpected="must fail"),
        ):
            with self.assertRaises(ValueError):
                self.scheduler.checkpoint(task,"muse",running["lease_version"],1001,bad)
        self.assertIsNone(self.scheduler.restore(task,1002)["checkpoint"])
        self.scheduler.checkpoint(task,"muse",running["lease_version"],1003,
                                  {"legacy_step":"still valid"})
        self.assertEqual(self.scheduler.restore(task,1004)["checkpoint_format"],"legacy")


    def test_due_nonhuman_promise_is_runnable_and_requires_progress(self):
        pid=Promises(self.db).create("muse","owner","source:promise-nonhuman",
            "deliver verifiable result",1000,1001,1002,due_at=1300,kind="complex")
        self.assertEqual(self.scheduler.sync_work(1000),1)
        self.assertEqual(self.scheduler.sync_work(1000),0)
        self.assertEqual(Wakeups(self.db).stage(1000,threshold=70),1)
        wake=Wakeups(self.db).claim_next("hatch",1000)
        self.assertEqual(wake["source_type"],"promise")
        Wakeups(self.db).acknowledge(wake["id"],"hatch",wake["lease_version"],
                                     1001,"accepted",{"host":"verified"})
        running=self.scheduler.dispatch("muse",1002)["activity"]
        self.assertEqual(running["kind"],"promise")
        with self.assertRaises(ValueError):
            self.scheduler.complete(running["id"],"muse",running["lease_version"],
                                     1003,{"claimed":"no real progress"})
        Wakeups(self.db).record_execution(wake["id"],wake["lease_version"],
            running["id"],running["lease_version"],"muse","started",1003,
            {"session":"actual"})
        Promises(self.db).progress(pid,1004,{"artifact":"first concrete step"},1150)
        self.scheduler.complete(running["id"],"muse",running["lease_version"],
                                1005,{"recorded_progress":"promise"})
        Wakeups(self.db).record_execution(wake["id"],wake["lease_version"],
            running["id"],running["lease_version"],"muse","finished",1006,
            {"progress":"verified"})
        self.assertEqual(self.scheduler.sync_work(1006),0)
        self.assertEqual(self.scheduler.sync_work(1050),1)
        candidate=self.scheduler.assess(1050)["candidate"]
        self.assertEqual(candidate["kind"],"promise")
        self.assertNotEqual(candidate["id"],running["id"])


if __name__=="__main__":
    unittest.main()
