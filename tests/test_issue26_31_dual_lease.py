"""#26/#31: single-transaction Queue+Scheduler renew, no business progress forgery."""
import tempfile
import unittest
from pathlib import Path

from patrol.components.host.index import HostHandoff, MuseConsumer
from patrol.components.persistence.index import Database, StaleLease
from patrol.components.queue.index import Queue
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
