"""#26/#31: single-transaction Queue+Scheduler renew, no business progress forgery."""
import tempfile
import unittest
from pathlib import Path

from patrol.components.host.index import HostHandoff, MuseConsumer
from patrol.components.persistence.index import Database, StaleLease
from patrol.components.queue.index import Queue
from patrol.components.outbox.index import Outbox
from patrol.components.wakeup.index import Wakeups


class AgentLeaseRecoveryTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.db=Database(Path(tmp.name)/"renew.sqlite3")
        with self.db.transaction() as c:
            c.execute("INSERT INTO meta(key,value) VALUES('actor','agent_1')")
        self.queue=Queue(self.db)
        self.host=HostHandoff(self.db)
        self.wake=Wakeups(self.db)
        self.muse=MuseConsumer(self.db)
        self.id=self.queue.enqueue("owner:long-task","human_action",
            {"reason":"long running authorized Agent work"},1000,priority=95)
        self.wake.stage(1000)
        self.ticket=self.host.prepare("hatch",1000,capability="best_effort")
        self.started=self.muse.consume(self.ticket["wakeup_id"],
            self.ticket["notification_generation"],"real-agent",1001)

    def renew(self,at,seq,worker="real-agent",wake_gen=None,work_gen=None):
        pack=self.started
        return self.muse.renew_liveness(self.ticket["wakeup_id"],
            self.ticket["notification_generation"] if wake_gen is None else wake_gen,
            worker,pack["scheduler_activity_id"],pack["scheduler_generation"],
            pack["work_lease_version"] if work_gen is None else work_gen,
            at,{"observation_seq":seq,"host_session":"hatch-session-1"})

    def test_issue47_recovery_report_distinguishes_started_from_sent(self):
        work_before=dict(self.db.read("SELECT status,lease_version FROM work_items "
                                      "WHERE id=?",(self.id,))[0])
        report=self.muse.recovery_report(self.id,1002)
        self.assertEqual(report["diagnosis"],
                         "worker_started_without_verified_remote_effect")
        self.assertEqual(report["activity"]["id"],
                         self.started["scheduler_activity_id"])
        self.assertEqual(report["wakes"][0]["execution_status"],"started")
        self.assertEqual(report["scheduled_outbox_receipts"],[])
        self.assertFalse(report["safe_to_force_complete"])
        self.assertFalse(report["side_effects_executed"])
        self.assertEqual(dict(self.db.read(
            "SELECT status,lease_version FROM work_items WHERE id=?",
            (self.id,))[0]),work_before)

    def test_issue47_recovery_report_uses_only_exact_scheduled_receipts(self):
        pack=self.started
        out=Outbox(self.db).stage("issue47:original-message","agent_1",
                                 "send","chat_owner",
                                 {"content":"test-only original message"},1002,
                                 schedule_context={"id":pack["scheduler_activity_id"],
                                     "owner":"real-agent",
                                     "generation":pack["scheduler_generation"]})
        # Test fixture models already verified remote readback, NOT an Agent
        # shortcut that claims a real platform receipt.
        with self.db.transaction() as c:
            c.execute("""UPDATE outbox SET status='confirmed',
                remote_id='message_fixture_1' WHERE id=?""",(out,))
        report=self.muse.recovery_report(self.id,1003)
        self.assertEqual(report["diagnosis"],
                         "confirmed_remote_effects_local_finish_pending")
        self.assertEqual(report["scheduled_outbox_receipts"][0]["id"],out)
        self.assertEqual(report["scheduled_outbox_receipts"][0]["remote_id"],
                         "message_fixture_1")
        self.assertFalse(report["safe_to_force_complete"])
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM outbox")[0]["n"],1)
        with self.db.transaction() as c:
            c.execute("UPDATE outbox SET status='unknown' WHERE id=?",(out,))
        unknown=self.muse.recovery_report(self.id,1004)
        self.assertEqual(unknown["diagnosis"],
                         "unknown_remote_effect_reconcile_exact_request")
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM outbox")[0]["n"],1)

    def test_issue47_exact_confirmed_resume_finishes_without_reposting(self):
        pack=self.started
        oid=Outbox(self.db).stage("issue47:reply-exact-once","agent_1",
                                 "send","chat_owner",{"content":"already replied"},
                                 1002,schedule_context={
                                     "id":pack["scheduler_activity_id"],
                                     "owner":"real-agent",
                                     "generation":pack["scheduler_generation"]})
        # Synthetic independent readback in isolated SQLite, not a live
        # platform statement. Real runtime requires ActionSender confirmation.
        with self.db.transaction() as c:
            c.execute("""UPDATE outbox SET status='confirmed',
                remote_id='message_test_exact' WHERE id=?""",(oid,))
        yielded=self.muse.scheduler.checkpoint(
            pack["scheduler_activity_id"],"real-agent",
            pack["scheduler_generation"],1003,
            {"phase":"resume original completion only"},voluntary=True)
        self.assertTrue(yielded["suspended"])
        self.assertEqual(self.db.read(
            "SELECT status FROM work_items WHERE id=?",(self.id,))[0]["status"],
            "pending")
        result=self.muse.recover_exact_confirmed(
            self.ticket["wakeup_id"],self.ticket["notification_generation"],
            "real-agent",1004)
        self.assertEqual(result["status"],"finished")
        self.assertTrue(result["recovered_original_confirmed"])
        self.assertEqual(result["confirmed_outbox_ids"],[oid])
        self.assertEqual(self.db.read(
            "SELECT status FROM work_items WHERE id=?",(self.id,))[0]["status"],
            "done")
        self.assertEqual(self.db.read(
            "SELECT status FROM schedule_activities WHERE id=?",
            (pack["scheduler_activity_id"],))[0]["status"],"completed")
        self.assertEqual(self.db.read(
            "SELECT status FROM wakeup_execution_receipts WHERE wakeup_id=?",
            (self.ticket["wakeup_id"],))[0]["status"],"finished")
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM outbox")[0]["n"],1)
        again=self.muse.recover_exact_confirmed(
            self.ticket["wakeup_id"],self.ticket["notification_generation"],
            "real-agent",1005)
        self.assertEqual(again["status"],"already_finished")
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM outbox")[0]["n"],1)

    def test_issue47_recovery_rejects_missing_remote_proof_and_wrong_worker(self):
        pack=self.started
        oid=Outbox(self.db).stage("issue47:unverified","agent_1",
                                 "send","chat_owner",{"content":"unknown"},
                                 1002,schedule_context={
                                     "id":pack["scheduler_activity_id"],
                                     "owner":"real-agent",
                                     "generation":pack["scheduler_generation"]})
        self.muse.scheduler.checkpoint(
            pack["scheduler_activity_id"],"real-agent",
            pack["scheduler_generation"],1003,{"phase":"inspect"},voluntary=True)
        for worker in ("unknown-agent","real-agent"):
            result=self.muse.recover_exact_confirmed(
                self.ticket["wakeup_id"],self.ticket["notification_generation"],
                worker,1004)
            self.assertEqual(result["status"],
                "original_worker_evidence_missing" if worker!="real-agent"
                else "exact_confirmed_effects_missing")
            self.assertEqual(self.db.read(
                "SELECT status FROM work_items WHERE id=?",(self.id,))[0]["status"],
                "pending")
        self.assertEqual(self.db.read(
            "SELECT status FROM outbox WHERE id=?",(oid,))[0]["status"],"prepared")
        self.assertEqual(self.db.read(
            "SELECT status FROM schedule_activities WHERE id=?",
            (pack["scheduler_activity_id"],))[0]["status"],"suspended")
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM outbox")[0]["n"],1)

    def test_long_worker_survives_both_120_second_lease_boundaries(self):
        first=self.renew(1090,1)
        self.assertTrue(first["worker_liveness_observed"])
        self.assertFalse(first["business_progress_recorded"])
        self.assertFalse(first["host_accepted_inferred"])
        self.assertEqual(first["lease_until"],1210)
        second=self.renew(1180,2)
        self.assertEqual(second["lease_until"],1300)
        work=self.db.read("SELECT * FROM work_items WHERE id=?",(self.id,))[0]
        activity=self.db.read("SELECT * FROM schedule_activities WHERE id=?",
                              (self.started["scheduler_activity_id"],))[0]
        receipt=self.db.read("SELECT * FROM wakeup_execution_receipts WHERE wakeup_id=?",
                             (self.ticket["wakeup_id"],))[0]
        self.assertEqual(work["lease_until"],activity["lease_until"])
        self.assertEqual(work["lease_until"],1300)
        self.assertIsNone(work["last_progress_at"])
        self.assertEqual(receipt["status"],"started")
        self.assertEqual(receipt["updated_at"],1001)
        self.assertLess(activity["slice_until"],1090)
        finished=self.muse.finish(self.ticket["wakeup_id"],
            self.ticket["notification_generation"],"real-agent",
            self.started["scheduler_activity_id"],self.started["scheduler_generation"],
            self.started["work_lease_version"],1250,
            {"decision":"read_only","reason":"Observed no remote write necessary"})
        self.assertEqual(finished["status"],"finished")

    def test_wrong_worker_generation_replays_and_expired_lease_fail_closed(self):
        with self.assertRaises(StaleLease):
            self.renew(1002,1,worker="other-agent")
        with self.assertRaises(StaleLease):
            self.renew(1002,1,work_gen=self.started["work_lease_version"]+1)
        with self.assertRaises(StaleLease):
            self.renew(1002,1,wake_gen=2)
        self.renew(1003,1)
        with self.assertRaises(StaleLease):
            self.renew(1004,1)
        with self.assertRaises(StaleLease):
            self.renew(1124,2)
        self.assertEqual(self.db.read(
            "SELECT observation_seq FROM worker_lease_heartbeats WHERE wakeup_id=?",
            (self.ticket["wakeup_id"],))[0]["observation_seq"],1)

    def test_preempt_requested_is_not_cleared_by_liveness(self):
        activity=self.started["scheduler_activity_id"]
        with self.db.transaction() as c:
            c.execute("UPDATE schedule_activities SET preempt_requested=1 WHERE id=?",
                      (activity,))
        returned=self.renew(1010,1)
        self.assertTrue(returned["safe_point_yield_recommended"])
        state=self.db.read("SELECT preempt_requested FROM schedule_activities WHERE id=?",
                           (activity,))[0]
        self.assertEqual(state["preempt_requested"],1)

    def test_liveness_does_not_replay_or_approve_remote_effect(self):
        self.renew(1090,1)
        self.assertEqual(self.db.read("SELECT count(*) n FROM outbox")[0]["n"],0)
        self.assertEqual(self.db.read(
            "SELECT status FROM wakeup_requests WHERE id=?",
            (self.ticket["wakeup_id"],))[0]["status"],"pending")


if __name__=="__main__":
    unittest.main()
