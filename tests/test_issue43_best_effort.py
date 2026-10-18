"""#43 regression: no fake Host ACK; no live HATCH/network is used."""
import tempfile
import unittest
from pathlib import Path

from patrol.components.host.index import HostHandoff, MuseConsumer
from patrol.components.outbox.index import Outbox
from patrol.components.persistence.index import Database, StaleLease
from patrol.components.queue.index import Queue
from patrol.components.wakeup.index import Wakeups


class BestEffortWakeTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.db=Database(Path(temp.name)/"best-effort.sqlite3")
        self.work=Queue(self.db)
        self.wakeup=Wakeups(self.db)
        self.host=HostHandoff(self.db)
        self.muse=MuseConsumer(self.db)
        with self.db.transaction() as c:
            c.execute("INSERT INTO meta(key,value) VALUES('actor','actor_1')")

    def enqueue(self, key, priority=90):
        ident=self.work.enqueue(key,"human_action",{"verified_input":key},
                                1000,priority=priority)
        return ident

    def prepare(self, at=1000):
        return self.host.prepare("hatch",at,capability="best_effort")

    def test_wake_is_durable_cooldown_not_host_accepted(self):
        wid=self.enqueue("first")
        self.wakeup.stage(1000)
        advice=self.prepare()
        self.assertTrue(advice["should_wake"])
        self.assertEqual(advice["host_capability"],"best_effort")
        self.assertFalse(advice["requires_host_receipt"])
        self.assertFalse(advice["host_acceptance_verified"])
        self.assertEqual(advice["source_id"],wid)
        self.assertEqual(advice["notification_outcome"],"unknown")
        wake=self.db.read("SELECT * FROM wakeup_requests WHERE id=?",
                          (advice["wakeup_id"],))[0]
        self.assertEqual(wake["status"],"pending")
        self.assertIsNone(wake["confirmed_at"])
        self.assertEqual(wake["attempts"],0)
        self.assertEqual(self.prepare(1001)["status"],"notification_cooled_down")
        self.assertEqual(self.prepare(1059)["status"],"notification_cooled_down")
        next_advice=self.prepare(1060)
        self.assertEqual(next_advice["wakeup_id"],advice["wakeup_id"])
        self.assertEqual(next_advice["notification_generation"],2)
        self.assertEqual(next_advice["notification_attempt"],2)
        self.assertEqual(self.db.read(
            "SELECT attempts FROM wakeup_requests WHERE id=?",
            (advice["wakeup_id"],))[0]["attempts"],0)
        with self.assertRaises(StaleLease):
            self.muse.consume(advice["wakeup_id"],1,"worker",1061)

    def test_three_worker_decisions_complete_without_host_ack(self):
        ids=[self.enqueue("owner:"+str(i),90-i) for i in range(3)]
        self.wakeup.stage(1000)
        for i,wid in enumerate(ids):
            at=1000+i*10
            advice=self.prepare(at)
            self.assertEqual(advice["source_id"],wid)
            ticket=self.muse.consume(advice["wakeup_id"],
                                     advice["notification_generation"],"muse",at+1)
            self.assertEqual(ticket["status"],"started")
            self.assertEqual(ticket["host_capability"],"best_effort")
            self.assertFalse(ticket["host_accepted_verified"])
            with self.assertRaises((StaleLease,ValueError)):
                self.muse.finish(advice["wakeup_id"],advice["notification_generation"],
                                 "impostor",ticket["scheduler_activity_id"],
                                 ticket["scheduler_generation"],
                                 ticket["work_lease_version"],at+2,
                                 {"decision":"read_only","reason":"test"})
            out=Outbox(self.db).stage("reply:"+str(i),"actor_1","send","owner-chat",
                {"content":"response"},at+2,schedule_context={
                    "id":ticket["scheduler_activity_id"],"owner":"muse",
                    "generation":ticket["scheduler_generation"]})
            with self.assertRaisesRegex(ValueError,"confirmed"):
                self.muse.finish(advice["wakeup_id"],advice["notification_generation"],
                    "muse",ticket["scheduler_activity_id"],
                    ticket["scheduler_generation"],ticket["work_lease_version"],
                    at+3,{"decision":"sent","outbox":out})
            # Unit fixture only: production ActionSender must obtain exact
            # platform requestId readback before persisting confirmed.
            with self.db.transaction() as c:
                c.execute("UPDATE outbox SET status='confirmed' WHERE id=?",(out,))
            done=self.muse.finish(advice["wakeup_id"],advice["notification_generation"],
                "muse",ticket["scheduler_activity_id"],
                ticket["scheduler_generation"],ticket["work_lease_version"],
                at+3,{"decision":"sent","outbox":out})
            self.assertEqual(done["remote_effects_confirmed"],1)
            self.assertEqual(self.db.read(
                "SELECT status FROM wakeup_requests WHERE id=?",
                (advice["wakeup_id"],))[0]["status"],"obsolete")
            self.assertIsNone(self.db.read(
                "SELECT confirmed_at FROM wakeup_requests WHERE id=?",
                (advice["wakeup_id"],))[0]["confirmed_at"])
            self.wakeup.stage(at+4)
        self.assertEqual(self.db.read(
            "SELECT count(*) n FROM outbox WHERE status='confirmed'")[0]["n"],3)

    def test_legacy_unknown_can_be_consumed_without_rewriting_history(self):
        wid=self.enqueue("legacy-unknown")
        self.wakeup.stage(1000)
        old=self.host.prepare("legacy-host",1000)
        self.assertEqual(old["status"],"reserved")
        self.assertEqual(self.wakeup.recover(1121,detail=True)["unknown"],1)
        advice=self.prepare(1122)
        self.assertEqual(advice["wakeup_id"],old["wakeup_id"])
        self.assertEqual(advice["notification_generation"],1)
        self.assertEqual(self.db.read(
            "SELECT status FROM wakeup_requests WHERE id=?",
            (old["wakeup_id"],))[0]["status"],"unknown")
        ticket=self.muse.consume(old["wakeup_id"],1,"worker",1123)
        self.assertEqual(ticket["status"],"started")
        self.assertEqual(ticket["work_id"],wid)
        self.assertEqual(self.prepare(1130)["status"],"scheduler_busy")

    def test_worker_competition_and_host_absence_are_not_grants(self):
        self.enqueue("race")
        self.wakeup.stage(1000)
        self.assertEqual(self.host.prepare("hatch",1000,capability="unavailable")["status"],
                         "host_unavailable")
        ticket=self.prepare(1000)
        first=self.muse.consume(ticket["wakeup_id"],1,"worker-a",1001)
        self.assertEqual(first["status"],"started")
        second=self.muse.consume(ticket["wakeup_id"],1,"worker-b",1002)
        self.assertIn(second["status"],("work_not_ready","scheduler_deferred"))
        self.assertEqual(self.db.read(
            "SELECT count(*) n FROM work_items WHERE status='running'")[0]["n"],1)
        self.assertIsNone(self.db.read(
            "SELECT confirmed_at FROM wakeup_requests WHERE id=?",
            (ticket["wakeup_id"],))[0]["confirmed_at"])


if __name__=="__main__":
    unittest.main()
