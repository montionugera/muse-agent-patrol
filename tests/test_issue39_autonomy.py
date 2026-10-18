"""Issue #39: deterministic SQLite evidence; real HATCH/LLM E2E remains external."""
import tempfile
import unittest
from pathlib import Path

from patrol.components.persistence.index import Database, StaleLease
from patrol.components.queue.index import Queue
from patrol.components.runtime.index import Tick
from patrol.components.config.index import Config
from patrol.components.host.index import HostHandoff
from patrol.components.host.components.consumer import MuseConsumer
from patrol.components.wakeup.index import Wakeups
from patrol.components.outbox.index import Outbox


class AutonomyIssue39Tests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.db=Database(Path(tmp.name)/"autonomy.sqlite3")
        self.queue=Queue(self.db)
        self.wakes=Wakeups(self.db)
        self.host=HostHandoff(self.db)
        self.muse=MuseConsumer(self.db)
        with self.db.transaction() as c:
            c.execute("INSERT INTO meta(key,value) VALUES('actor','agent_muse')")

    def _new(self, key, now=1000, priority=85):
        return self.queue.enqueue(key,"human_action",{"reason":"verified model decision"},now,
                                  priority=priority,start_by=now+180)

    def _accepted(self, ident, now=1000):
        self.wakes.stage(now)
        ticket=self.host.prepare("hatch",now)
        self.assertEqual(ticket["source_id"],ident)
        self.wakes.acknowledge(ticket["wakeup_id"],"hatch",
                               ticket["lease_version"],now+1,"accepted",
                               {"real_host_acceptance_contract":"fixture"})
        return ticket

    def test_tick_does_not_claim_model_actions_and_unknown_types_are_diagnosed(self):
        human=self._new("human")
        unknown=self.queue.enqueue("unknown","unimplemented_kind",{"reason":"no executor"},
                                   1000,priority=100)
        tick=Tick(self.db,Config(),clock=lambda:1000,monotonic=lambda:0)
        tick.run_id="tick-unit"
        tick.request_deadline=100
        self.assertEqual(tick.execute(5),0)
        self.assertEqual(self.db.read("SELECT status FROM work_items WHERE id=?",
                                      (human,))[0]["status"],"pending")
        self.assertEqual(self.queue.quarantine_unknown(1000),1)
        self.assertEqual(self.db.read("SELECT status FROM work_items WHERE id=?",
                                      (unknown,))[0]["status"],"blocked")
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM alerts WHERE kind='blocked'")[0]["n"],1)

    def test_exact_legacy_block_reclassification_is_bounded_and_safe(self):
        old=self._new("legacy")
        denied=self._new("denied")
        with self.db.transaction() as c:
            c.execute("""UPDATE work_items SET status='blocked',
                block_reason='no authorized executor for human_action' WHERE id=?""",(old,))
            c.execute("""UPDATE work_items SET status='blocked',
                block_reason='Owner denied grant' WHERE id=?""",(denied,))
        self.queue.recover(1002)
        self.queue.recover(1003)
        self.assertEqual(self.db.read("SELECT status FROM work_items WHERE id=?",
                                      (old,))[0]["status"],"pending")
        self.assertEqual(self.db.read("SELECT status FROM work_items WHERE id=?",
                                      (denied,))[0]["status"],"blocked")
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM alerts WHERE kind='work_reclassified'")[0]["n"],1)

    def test_three_host_wakes_are_consumed_without_per_work_owner_evidence(self):
        ids=[self._new("action:"+str(i),priority=90-i) for i in range(3)]
        self.wakes.stage(1000)
        for n,ident in enumerate(ids):
            at=1000+n*10
            ticket=self.host.prepare("hatch",at)
            self.assertTrue(ticket["should_wake"])
            self.assertEqual(ticket["source_id"],ident)
            with self.assertRaises(StaleLease):
                self.muse.consume(ticket["wakeup_id"],ticket["lease_version"],"muse-worker",at+1)
            self.wakes.acknowledge(ticket["wakeup_id"],"hatch",ticket["lease_version"],
                                   at+1,"accepted",{"host_receipt":"fixture-"+str(n)})
            pack=self.muse.consume(ticket["wakeup_id"],ticket["lease_version"],
                                    "muse-worker",at+2)
            self.assertEqual(pack["status"],"started")
            out=Outbox(self.db).stage("once:"+str(n),"agent_muse","send","owner-chat",
                                     {"content":"ack "+str(n)},at+3,
                                     schedule_context={"id":pack["scheduler_activity_id"],
                                                       "owner":"muse-worker",
                                                       "generation":pack["scheduler_generation"]})
            evidence={"decision":"sent","outbox_id":out}
            with self.assertRaisesRegex(ValueError,"confirmed"):
                self.muse.finish(ticket["wakeup_id"],ticket["lease_version"],"muse-worker",
                                 pack["scheduler_activity_id"],pack["scheduler_generation"],
                                 pack["work_lease_version"],at+4,evidence)
            # Fake confirmed receipt only in the unit test. Production requires
            # ActionSender/exact remote readback to make this transition.
            with self.db.transaction() as c:
                c.execute("UPDATE outbox SET status='confirmed' WHERE id=?",(out,))
            finished=self.muse.finish(ticket["wakeup_id"],ticket["lease_version"],"muse-worker",
                                      pack["scheduler_activity_id"],pack["scheduler_generation"],
                                      pack["work_lease_version"],at+4,evidence)
            self.assertEqual(finished["remote_effects_confirmed"],1)
            self.assertEqual(finished["confirmed_outbox_ids"],[out])
            self.assertEqual(self.db.read("SELECT status FROM work_items WHERE id=?",
                                          (ident,))[0]["status"],"done")
            self.assertEqual(self.db.read("SELECT status FROM wakeup_execution_receipts WHERE wakeup_id=?",
                                          (ticket["wakeup_id"],))[0]["status"],"finished")
            self.wakes.stage(at+5)
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM outbox")[0]["n"],3)

    def test_no_remote_effect_requires_explicit_reason_and_stale_is_rejected(self):
        wid=self._new("read-only")
        ticket=self._accepted(wid)
        pack=self.muse.consume(ticket["wakeup_id"],ticket["lease_version"],"worker",1002)
        with self.assertRaisesRegex(ValueError,"no-outbox"):
            self.muse.finish(ticket["wakeup_id"],ticket["lease_version"],"worker",
                             pack["scheduler_activity_id"],pack["scheduler_generation"],
                             pack["work_lease_version"],1003,{"done":True})
        result=self.muse.finish(ticket["wakeup_id"],ticket["lease_version"],"worker",
                                pack["scheduler_activity_id"],pack["scheduler_generation"],
                                pack["work_lease_version"],1003,
                                {"decision":"read_only","reason":"No reply required"})
        self.assertEqual(result["status"],"finished")
        self.assertEqual(
            self.muse.consume(ticket["wakeup_id"],ticket["lease_version"],"worker",1004)["status"],
            "work_not_ready")


if __name__=="__main__":
    unittest.main()
