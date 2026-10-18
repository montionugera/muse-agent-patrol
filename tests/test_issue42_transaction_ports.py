"""#42: cross-component write ownership remains ONE SQLite transaction."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from patrol.components.host.index import HostHandoff, MuseConsumer
from patrol.components.persistence.index import Database, StaleLease
from patrol.components.queue.index import Queue
from patrol.components.wakeup.index import Wakeups


class ComponentTransactionTests(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory()
        self.addCleanup(t.cleanup)
        self.db=Database(Path(t.name)/"tx-ports.sqlite3")
        with self.db.transaction() as c:
            c.execute("INSERT INTO meta(key,value) VALUES('actor','muse')")
        q=Queue(self.db)
        self.work=q.enqueue("owner:test-atomic","human_action",
                            {"reason":"verify atomic work"},1000,priority=95)
        Wakeups(self.db).stage(1000)
        self.handoff=HostHandoff(self.db).prepare(
            "host",1000,capability="best_effort")
        self.muse=MuseConsumer(self.db)
        self.ticket=self.muse.consume(self.handoff["wakeup_id"],
                                      self.handoff["notification_generation"],
                                      "agent",1001)
        self.assertEqual(self.ticket["status"],"started")

    def state(self):
        return {
            "work":self.db.read("SELECT status,lease_until,lease_version FROM work_items WHERE id=?",
                                (self.work,))[0],
            "schedule":self.db.read("SELECT status,lease_until,lease_version FROM schedule_activities WHERE id=?",
                                    (self.ticket["scheduler_activity_id"],))[0],
            "receipt":self.db.read("SELECT status FROM wakeup_execution_receipts WHERE wakeup_id=?",
                                   (self.handoff["wakeup_id"],))[0]["status"],
            "wake":self.db.read("SELECT status FROM wakeup_requests WHERE id=?",
                                (self.handoff["wakeup_id"],))[0]["status"],
            "heartbeat_count":self.db.read("SELECT count(*) n FROM worker_lease_heartbeats")[0]["n"]}

    def finish(self,now=1003):
        return self.muse.finish(self.handoff["wakeup_id"],
                    self.handoff["notification_generation"],"agent",
                    self.ticket["scheduler_activity_id"],
                    self.ticket["scheduler_generation"],
                    self.ticket["work_lease_version"],now,
                    {"decision":"read_only","reason":"no remote write required"})

    def renew(self,now=1050):
        return self.muse.renew_liveness(
            self.handoff["wakeup_id"],self.handoff["notification_generation"],
            "agent",self.ticket["scheduler_activity_id"],
            self.ticket["scheduler_generation"],self.ticket["work_lease_version"],
            now,{"observation_seq":1,"host_session":"host-real-1"})

    def test_finish_fault_after_queue_and_scheduler_write_rolls_everything_back(self):
        before=self.state()
        with patch.object(self.muse.wakeups,"agent_finish_tx",
                          side_effect=RuntimeError("injected storage crash")):
            with self.assertRaisesRegex(RuntimeError,"injected"):
                self.finish()
        self.assertEqual(self.state(),before)
        self.assertEqual(self.finish()["status"],"finished")
        after=self.state()
        self.assertEqual(after["work"]["status"],"done")
        self.assertEqual(after["schedule"]["status"],"completed")
        self.assertEqual(after["receipt"],"finished")
        self.assertEqual(after["wake"],"obsolete")

    def test_renew_fault_after_heartbeat_and_scheduler_write_rolls_back(self):
        before=self.state()
        with patch.object(self.muse.queue,"agent_renew_tx",
                          side_effect=RuntimeError("injected Queue failure")):
            with self.assertRaises(RuntimeError):
                self.renew()
        self.assertEqual(self.state(),before)
        self.assertEqual(self.renew()["status"],"lease_renewed")
        after=self.state()
        self.assertEqual(after["heartbeat_count"],1)
        self.assertEqual(after["work"]["lease_until"],after["schedule"]["lease_until"])

    def test_wrong_generation_fails_before_any_mutation(self):
        before=self.state()
        with self.assertRaises(StaleLease):
            self.muse.finish(self.handoff["wakeup_id"],
                self.handoff["notification_generation"],"agent",
                self.ticket["scheduler_activity_id"],
                self.ticket["scheduler_generation"],
                self.ticket["work_lease_version"]+1,1003,
                {"decision":"read_only","reason":"valid reason"})
        self.assertEqual(self.state(),before)


if __name__=="__main__":
    unittest.main()
