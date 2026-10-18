"""#26: exact transactional Muse startup for work, social and due promises.

Uses synthetic SQLite only; no simulated Host acceptance presented as live proof.
"""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from patrol.components.host.index import HostHandoff, MuseConsumer
from patrol.components.persistence.index import Database, StaleLease
from patrol.components.promises.index import Promises
from patrol.components.queue.index import Queue
from patrol.components.social.index import SocialOpportunities
from patrol.components.wakeup.index import Wakeups
from patrol.components.scheduling.index import Scheduler
from patrol.components.executor.index import Authorization


def opportunity():
    return {"protocolVersion":"1","mode":"read_only_social_discovery",
            "actorId":"muse","modelInvoked":False,"actionExecuted":False,
            "items":[{"kind":"post","resource":{"type":"post","id":"one"},
                "suggestedAction":"community.post.comment","contentTrust":"untrusted",
                "matchedInterests":[],"title":"untrusted","preview":"untrusted"}]}


class AtomicConsumerStartTests(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory()
        self.addCleanup(t.cleanup)
        self.db=Database(Path(t.name)/"atomic-start.sqlite")
        with self.db.transaction() as c:
            c.execute("INSERT INTO meta(key,value) VALUES('actor','muse')")
        self.queue=Queue(self.db)
        self.wake=Wakeups(self.db)
        self.host=HostHandoff(self.db)
        self.muse=MuseConsumer(self.db)
        self.scheduler=Scheduler(self.db)

    def work(self,mode="best_effort"):
        work_id=self.queue.enqueue("atomic-owner","human_action",
            {"reason":"real Agent decision fixture"},1000,priority=95)
        self.wake.stage(1000)
        ticket=self.host.prepare("hatch",1000,capability=mode)
        if mode=="reliable_ack":
            self.wake.acknowledge(ticket["wakeup_id"],"hatch",
                ticket["lease_version"],1001,"accepted",{"real_ack_fixture":True})
        return ticket,work_id,(ticket["notification_generation"] if mode=="best_effort"
                               else ticket["lease_version"])

    def counts(self,work_id=None):
        result={"running":self.db.read(
            "SELECT count(*) n FROM schedule_activities WHERE status='running'")[0]["n"],
            "started":self.db.read(
            "SELECT count(*) n FROM wakeup_execution_receipts")[0]["n"],
            "outbox":self.db.read("SELECT count(*) n FROM outbox")[0]["n"]}
        if work_id:
            w=self.db.read("SELECT status,attempts,lease_version FROM work_items WHERE id=?",
                           (work_id,))[0]
            result.update(work_status=w["status"],work_attempts=w["attempts"],
                          work_generation=w["lease_version"])
        return result

    def test_queue_fault_rolls_back_scheduler_grant_and_claim(self):
        ticket,work_id,gen=self.work()
        with patch.object(self.muse.queue,"agent_claim_specific_tx",
                          side_effect=RuntimeError("fixture queue crash")):
            with self.assertRaisesRegex(RuntimeError,"queue crash"):
                self.muse.consume(ticket["wakeup_id"],gen,"worker",1002)
        state=self.counts(work_id)
        self.assertEqual(state,dict(running=0,started=0,outbox=0,
                                    work_status="pending",work_attempts=0,
                                    work_generation=0))
        self.assertEqual(self.muse.consume(ticket["wakeup_id"],gen,"worker",1003)["status"],
                         "started")
        self.assertEqual(self.counts(work_id)["started"],1)

    def test_receipt_fault_rolls_back_queue_and_scheduler_and_alert(self):
        ticket,work_id,gen=self.work("reliable_ack")
        original=self.muse.wakeups.agent_record_execution_tx
        def after_write(*args,**kwargs):
            original(*args,**kwargs)
            raise RuntimeError("crash after durable worker write but before commit")
        with patch.object(self.muse.wakeups,"agent_record_execution_tx",
                          side_effect=after_write):
            with self.assertRaisesRegex(RuntimeError,"crash after"):
                self.muse.consume(ticket["wakeup_id"],gen,"worker",1002)
        self.assertEqual(self.counts(work_id),dict(
            running=0,started=0,outbox=0,work_status="pending",
            work_attempts=0,work_generation=0))
        self.assertEqual(self.db.read(
            "SELECT count(*) n FROM alerts WHERE kind='local_handoff'")[0]["n"],0)
        self.assertEqual(self.muse.consume(ticket["wakeup_id"],gen,"worker",1003)["status"],
                         "started")

    def test_lost_response_readback_is_idempotent_and_generation_fenced(self):
        ticket,work_id,gen=self.work()
        with patch.object(Authorization,"cached",
                          side_effect=RuntimeError("response connection lost")):
            with self.assertRaisesRegex(RuntimeError,"response connection lost"):
                self.muse.consume(ticket["wakeup_id"],gen,"worker",1002)
        before=self.counts(work_id)
        pack=self.muse.consume(ticket["wakeup_id"],gen,"worker",1003)
        self.assertTrue(pack["already_started"])
        self.assertEqual(pack["status"],"started")
        self.assertEqual(self.counts(work_id),before)
        with self.assertRaises(StaleLease):
            self.muse.consume(ticket["wakeup_id"],gen,"different-worker",1004)
        self.assertEqual(self.counts(work_id),before)

    def test_social_receipt_fault_rolls_back_social_grant(self):
        social=SocialOpportunities(self.db)
        aid=social.admit(opportunity(),1000)["activities"][0]["activity_id"]
        ticket=self.host.prepare("hatch",1000,capability="best_effort")
        self.assertEqual(ticket["source_type"],"social")
        with patch.object(self.muse.wakeups,"agent_record_execution_tx",
                          side_effect=RuntimeError("worker journal unavailable")):
            with self.assertRaisesRegex(RuntimeError,"journal unavailable"):
                self.muse.consume(ticket["wakeup_id"],1,"worker",1002)
        self.assertEqual(self.counts()["running"],0)
        self.assertEqual(self.counts()["started"],0)
        pack=self.muse.consume(ticket["wakeup_id"],1,"worker",1003)
        self.assertEqual(pack["scheduler_activity_id"],aid)
        self.assertFalse(pack["already_started"])
        self.assertTrue(self.muse.consume(ticket["wakeup_id"],1,"worker",1004)["already_started"])

    def test_both_promise_types_rollback_and_recover_same_intent(self):
        for kind in ("human_action","research"):
            with self.subTest(kind=kind):
                # Every promise uses a unique source key and standalone SQLite.
                if kind=="research":
                    tmp=tempfile.TemporaryDirectory()
                    self.addCleanup(tmp.cleanup)
                    self.db=Database(Path(tmp.name)/"research.sqlite")
                    with self.db.transaction() as c:
                        c.execute("INSERT INTO meta(key,value) VALUES('actor','muse')")
                    self.muse=MuseConsumer(self.db)
                    self.host=HostHandoff(self.db)
                    self.wake=Wakeups(self.db)
                    self.scheduler=Scheduler(self.db)
                promise=Promises(self.db).create("muse","owner","atomic-"+kind,
                    "Promise made",1000,1000,1000,due_at=1500,kind=kind)
                self.scheduler.sync_work(1000)
                self.wake.stage(1000)
                ticket=self.host.prepare("hatch",1000,capability="best_effort")
                self.assertEqual(ticket["source_id"],promise)
                with patch.object(self.muse.wakeups,"agent_record_execution_tx",
                                  side_effect=RuntimeError("before startup commit")):
                    with self.assertRaisesRegex(RuntimeError,"before startup commit"):
                        self.muse.consume(ticket["wakeup_id"],1,"worker",1002)
                self.assertEqual(self.counts()["running"],0)
                self.assertEqual(self.counts()["started"],0)
                pack=self.muse.consume(ticket["wakeup_id"],1,"worker",1003)
                self.assertEqual(pack["promise"]["id"],promise)
                self.assertTrue(self.muse.consume(
                    ticket["wakeup_id"],1,"worker",1004)["already_started"])


if __name__=="__main__":
    unittest.main()
