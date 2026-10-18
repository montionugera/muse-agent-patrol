"""#58: real local Host -> Muse promise consumption, proof and fenced follow-up."""
import tempfile
import unittest
from pathlib import Path

from patrol.components.host.index import HostHandoff, MuseConsumer
from patrol.components.outbox.index import Outbox
from patrol.components.persistence.index import Database, StaleLease
from patrol.components.promises.index import Promises
from patrol.components.queue.index import Queue
from patrol.components.scheduling.index import Scheduler
from patrol.components.wakeup.index import Wakeups


class PromiseHostConsumerTests(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory()
        self.addCleanup(t.cleanup)
        self.db=Database(Path(t.name)/"promise.sqlite")
        with self.db.transaction() as c:
            c.execute("INSERT INTO meta(key,value) VALUES('actor','muse')")
        self.promises=Promises(self.db)
        self.wakes=Wakeups(self.db)
        self.host=HostHandoff(self.db)
        self.consumer=MuseConsumer(self.db)
        self.scheduler=Scheduler(self.db)

    def create(self,kind="human_action",name="x",now=1000):
        p=self.promises.create("muse","owner","msg-"+name,
            "Do the promised work",now,now,now,due_at=now+500,kind=kind)
        self.scheduler.sync_work(now)
        self.wakes.stage(now)
        return p

    def start(self,kind="human_action",capability="best_effort",name="x"):
        p=self.create(kind,name)
        notice=self.host.prepare("hatch",1000,capability=capability)
        self.assertEqual(notice["source_type"],"promise")
        self.assertEqual(notice["source_id"],p)
        if capability=="reliable_ack":
            with self.assertRaises(StaleLease):
                self.consumer.consume(notice["wakeup_id"],notice["lease_version"],
                                      "worker",1001)
            self.wakes.acknowledge(notice["wakeup_id"],"hatch",
                notice["lease_version"],1001,"accepted",
                {"host_acceptance":"real-fixture"})
            gen=notice["lease_version"]
        else:
            gen=notice["notification_generation"]
        pack=self.consumer.consume(notice["wakeup_id"],gen,"worker",1002)
        self.assertEqual(pack["status"],"started")
        self.assertEqual(pack["source_type"],"promise")
        self.assertEqual(pack["promise"]["id"],p)
        self.assertEqual(pack["host_accepted_verified"],
                         capability=="reliable_ack")
        return p,notice,gen,pack

    @staticmethod
    def proof(decision):
        return {"decision":decision,
                "proof":{"kind":"artifact","reference":"artifact://test-proof"}}

    def finish(self,notice,gen,pack,now=1003,evidence=None):
        return self.consumer.finish(notice["wakeup_id"],gen,"worker",
            pack["scheduler_activity_id"],pack["scheduler_generation"],
            pack["work_lease_version"],now,
            evidence if evidence is not None else self.proof("promise_done"))

    def test_linked_human_promise_best_effort_progress_then_due_again(self):
        p,notice,gen,pack=self.start()
        self.assertGreater(pack["work_lease_version"],0)
        with self.assertRaisesRegex(ValueError,"proof|artifact"):
            self.finish(notice,gen,pack,evidence={"decision":"promise_done"})
        progress=self.consumer.promise_progress(
            notice["wakeup_id"],gen,"worker",pack["scheduler_activity_id"],
            pack["scheduler_generation"],pack["work_lease_version"],1003,
            self.proof("promise_progress"),1060)
        self.assertFalse(progress["business_completed"])
        self.assertEqual(self.db.read(
            "SELECT status,next_action_at FROM promises WHERE id=?",(p,))[0]["next_action_at"],1060)
        self.assertEqual(self.db.read(
            "SELECT status FROM work_items WHERE id=?",(pack["work_id"],))[0]["status"],"pending")
        self.assertEqual(self.db.read(
            "SELECT status FROM schedule_activities WHERE id=?",
            (pack["scheduler_activity_id"],))[0]["status"],"suspended")
        with self.assertRaises(StaleLease):
            self.finish(notice,gen,pack,1004)
        self.scheduler.sync_work(1060)
        self.wakes.stage(1060)
        again=self.host.prepare("hatch",1060,capability="best_effort")
        self.assertEqual(again["source_type"],"promise")
        self.assertNotEqual(again["wakeup_id"],notice["wakeup_id"])
        started=self.consumer.consume(again["wakeup_id"],
            again["notification_generation"],"worker",1062)
        self.assertEqual(started["status"],"started")
        done=self.finish(again,again["notification_generation"],started,1063)
        self.assertEqual(done["status"],"finished")
        self.assertTrue(done["business_promise_completed"])
        self.assertEqual(self.db.read(
            "SELECT status FROM promises WHERE id=?",(p,))[0]["status"],"done")
        self.assertEqual(self.db.read(
            "SELECT status FROM work_items WHERE id=?",(pack["work_id"],))[0]["status"],"done")

    def test_linked_human_promise_reliable_requires_real_ack(self):
        p,notice,gen,pack=self.start("human_action","reliable_ack","human-ack")
        self.assertGreater(pack["work_lease_version"],0)
        self.assertEqual(self.finish(notice,gen,pack)["status"],"finished")
        self.assertEqual(self.db.read(
            "SELECT status FROM promises WHERE id=?",(p,))[0]["status"],"done")
        self.assertEqual(self.db.read(
            "SELECT status FROM work_items WHERE id=?",
            (pack["work_id"],))[0]["status"],"done")
        with self.assertRaises(StaleLease):
            self.finish(notice,gen,pack)

    def test_standalone_research_promise_reliable_requires_real_ack(self):
        p,notice,gen,pack=self.start("research","reliable_ack","research")
        self.assertEqual(pack["work_lease_version"],0)
        self.assertEqual(self.finish(notice,gen,pack)["status"],"finished")
        self.assertEqual(self.db.read(
            "SELECT status FROM promises WHERE id=?",(p,))[0]["status"],"done")
        with self.assertRaises(StaleLease):
            self.finish(notice,gen,pack)

    def test_standalone_research_promise_best_effort_progress(self):
        p,notice,gen,pack=self.start("research","best_effort","study")
        progress=self.consumer.promise_progress(
            notice["wakeup_id"],gen,"worker",pack["scheduler_activity_id"],
            pack["scheduler_generation"],0,1003,
            self.proof("promise_progress"),1120)
        self.assertEqual(progress["status"],"progress_recorded")
        self.assertEqual(self.db.read(
            "SELECT status FROM promises WHERE id=?",(p,))[0]["status"],"open")
        self.scheduler.sync_work(1120)
        self.wakes.stage(1120)
        again=self.host.prepare("hatch",1120,capability="best_effort")
        self.assertEqual(again["source_type"],"promise")
        new=self.consumer.consume(again["wakeup_id"],again["notification_generation"],
                                  "worker",1121)
        self.assertEqual(new["status"],"started")
        self.assertEqual(self.finish(again,again["notification_generation"],new,1122)["status"],"finished")

    def test_unconfirmed_effects_block_promise_completion_and_progress(self):
        p,notice,gen,pack=self.start(name="remote")
        out=Outbox(self.db).stage("promise-reply-remote","muse","send",
            "chat-owner",{"content":"ack"},1003,schedule_context={
                "id":pack["scheduler_activity_id"],"owner":"worker",
                "generation":pack["scheduler_generation"]})
        with self.assertRaisesRegex(ValueError,"confirmed"):
            self.finish(notice,gen,pack,1004)
        with self.assertRaisesRegex(ValueError,"confirmed"):
            self.consumer.promise_progress(
                notice["wakeup_id"],gen,"worker",pack["scheduler_activity_id"],
                pack["scheduler_generation"],pack["work_lease_version"],1004,
                self.proof("promise_progress"),1100)
        with self.db.transaction() as c:
            c.execute("UPDATE outbox SET status='confirmed',remote_id='remote-fixture' WHERE id=?",
                      (out,))
        done=self.finish(notice,gen,pack,1005)
        self.assertEqual(done["confirmed_outbox_ids"],[out])
        self.assertEqual(self.db.read(
            "SELECT status FROM promises WHERE id=?",(p,))[0]["status"],"done")

    def test_no_false_cooldown_when_next_step_not_due(self):
        self.create("research","future")
        with self.db.transaction() as c:
            c.execute("""UPDATE promises SET next_action_at=1050, start_by=1100
                WHERE source_message_id='msg-future'""")
            c.execute("""UPDATE schedule_activities SET logical_key=(
                'promise_due:'||(SELECT id FROM promises WHERE source_message_id='msg-future')||':1050.0'
                ) WHERE kind='promise'""")
        # No old due signal may be presented as a valid Host notification.
        answer=self.host.prepare("hatch",1000,capability="best_effort")
        self.assertFalse(answer["should_wake"])
        self.assertIn(answer["status"],("promise_not_due","promise_source_missing"))


if __name__=="__main__":
    unittest.main()
