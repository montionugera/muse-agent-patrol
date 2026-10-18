"""#19: real local Muse social decision path, never synthetic remote Host ACK."""
import tempfile
import unittest
from pathlib import Path

from patrol.components.host.index import HostHandoff, MuseConsumer
from patrol.components.outbox.index import Outbox
from patrol.components.persistence.index import Database, StaleLease
from patrol.components.queue.index import Queue
from patrol.components.social.index import SocialOpportunities
from patrol.components.wakeup.index import Wakeups


def opportunity(name):
    return {"protocolVersion":"1","mode":"read_only_social_discovery",
            "actorId":"muse","modelInvoked":False,"actionExecuted":False,
            "items":[{"kind":"post","resource":{"type":"post","id":name},
              "suggestedAction":"community.post.comment","contentTrust":"untrusted",
              "matchedInterests":[],"title":"untrusted","preview":"untrusted"}]}


class NativeSocialAgentTests(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory()
        self.addCleanup(t.cleanup)
        self.db=Database(Path(t.name)/"native-social.sqlite")
        with self.db.transaction() as c:
            c.execute("INSERT INTO meta(key,value) VALUES('actor','muse')")
        self.social=SocialOpportunities(self.db)
        self.host=HostHandoff(self.db)
        self.wake=Wakeups(self.db)
        self.muse=MuseConsumer(self.db)

    def admit(self,name="post_1"):
        return self.social.admit(opportunity(name),1000)["activities"][0]["activity_id"]

    def test_best_effort_social_skip_finishes_without_host_ack(self):
        ident=self.admit()
        ticket=self.host.prepare("hatch",1000,capability="best_effort")
        self.assertEqual(ticket["source_type"],"social")
        self.assertEqual(ticket["source_id"],ident)
        self.assertFalse(ticket["host_acceptance_verified"])
        self.assertEqual(self.host.prepare("hatch",1001,capability="best_effort")["status"],
                         "notification_cooled_down")
        pack=self.muse.consume(ticket["wakeup_id"],ticket["notification_generation"],
                               "worker",1002)
        self.assertEqual(pack["status"],"started")
        self.assertEqual(pack["source_type"],"social")
        self.assertEqual(pack["opportunity"]["resource_id"],"post_1")
        self.assertEqual(pack["work_lease_version"],0)
        self.assertFalse(pack["host_accepted_verified"])
        with self.assertRaises(ValueError):
            self.muse.finish(ticket["wakeup_id"],1,"worker",ident,
                pack["scheduler_generation"],0,1003,{"decision":"read_only"})
        done=self.muse.finish(ticket["wakeup_id"],1,"worker",ident,
                pack["scheduler_generation"],0,1003,
                {"decision":"skipped","reason":"no useful contribution"})
        self.assertEqual(done["status"],"finished")
        self.assertEqual(self.db.read(
            "SELECT status FROM schedule_activities WHERE id=?",
            (ident,))[0]["status"],"completed")
        self.assertEqual(self.db.read(
            "SELECT status,confirmed_at FROM wakeup_requests WHERE id=?",
            (ticket["wakeup_id"],))[0]["status"],"obsolete")
        self.assertIsNone(self.db.read(
            "SELECT confirmed_at FROM wakeup_requests WHERE id=?",
            (ticket["wakeup_id"],))[0]["confirmed_at"])

    def test_fenced_social_outbox_requires_confirmation(self):
        ident=self.admit("post_confirm")
        ticket=self.host.prepare("hatch",1000,capability="best_effort")
        pack=self.muse.consume(ticket["wakeup_id"],1,"worker",1002)
        payload={"action":"community.post.comment","version":"1",
            "resource":{"type":"post","id":"post_confirm"},
            "input":{"content":"test"},"requestId":"social-req-123456"}
        out=Outbox(self.db).stage("social:once","muse","platform_action",
            "post_confirm",payload,1003,schedule_context={
                "id":ident,"owner":"worker","generation":pack["scheduler_generation"]})
        with self.assertRaisesRegex(ValueError,"confirmed"):
            self.muse.finish(ticket["wakeup_id"],1,"worker",ident,
                pack["scheduler_generation"],0,1004,{"decision":"posted"})
        with self.db.transaction() as c:
            c.execute("UPDATE outbox SET status='confirmed' WHERE id=?",(out,))
        receipt=self.muse.finish(ticket["wakeup_id"],1,"worker",ident,
            pack["scheduler_generation"],0,1005,{"decision":"posted"})
        self.assertEqual(receipt["confirmed_outbox_ids"],[out])
        self.assertEqual(receipt["remote_effects_confirmed"],1)
        self.assertEqual(self.db.read("SELECT count(*) n FROM outbox")[0]["n"],1)

    def test_formal_work_outranks_social_and_no_unrequested_lease(self):
        self.admit()
        wid=Queue(self.db).enqueue("urgent-owner","human_action",
            {"reason":"reply"},1000,priority=95)
        self.wake.stage(1000)
        ticket=self.host.prepare("hatch",1000,capability="best_effort")
        self.assertEqual(ticket["source_id"],wid)
        self.assertEqual(ticket["source_type"],"work")
        self.assertEqual(self.db.read(
            "SELECT count(*) n FROM wakeup_requests WHERE source_type='social'")[0]["n"],0)

    def test_reliable_social_still_requires_real_ack_evidence(self):
        aid=self.admit()
        ticket=self.host.prepare("hatch",1000,capability="reliable_ack")
        with self.assertRaises(StaleLease):
            self.muse.consume(ticket["wakeup_id"],ticket["lease_version"],"worker",1001)
        self.wake.acknowledge(ticket["wakeup_id"],"hatch",ticket["lease_version"],
                              1001,"accepted",{"host_acceptance":"fixture"})
        pack=self.muse.consume(ticket["wakeup_id"],ticket["lease_version"],"worker",1002)
        self.assertTrue(pack["host_accepted_verified"])
        done=self.muse.finish(ticket["wakeup_id"],ticket["lease_version"],
            "worker",aid,pack["scheduler_generation"],0,1003,
            {"decision":"skipped","reason":"not relevant"})
        self.assertEqual(done["status"],"finished")


if __name__=="__main__":
    unittest.main()
