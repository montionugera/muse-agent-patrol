"""Integration-oriented handoff/backlog tests; no credentials or network."""
import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from patrol.components.attention.index import Attention
from patrol.components.collection.index import Collector, Sync
from patrol.components.config.index import Config
from patrol.components.persistence.index import Database, StaleLease
from patrol.components.promises.index import Promises
from patrol.components.queue.index import Queue, export_pending
from patrol.components.runtime.index import Tick, main
from patrol.components.scheduling.index import Scheduler
from patrol.components.transport.index import Response


class BridgeTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db=Database(Path(self.tmp.name)/"local.sqlite3")
        self.cfg=Config(database=str(self.db.path),allowed_chat_ids=["chat_a"])

    def test_old_history_does_not_make_new_human_work(self):
        with self.db.transaction() as c:
            c.execute("INSERT INTO meta VALUES('actor','self')")
        Sync(self.db).page("messages","chat_a","",
            [{"messageId":"old","cursor":"1","senderId":"someone","content":"old text","createdAt":100}],
            "1",3000)
        Tick(self.db,self.cfg,clock=lambda:3000).run()
        self.assertEqual(self.db.read("SELECT * FROM work_items WHERE type='human_action'"),[])
        self.assertEqual(self.db.read("SELECT * FROM attention_sessions"),[])

    def test_inbox_pointer_fetches_verified_message_once(self):
        calls=[]
        message={"messageId":"msg_1","cursor":"1","senderId":"other",
                 "senderKind":"agent","content":"need an answer","createdAt":1000}
        class Client:
            def request(self,method,path,*args):
                calls.append(path)
                if path=="/api/v1/agents/me":
                    return Response(200,{"id":"self"})
                if path=="/api/v1/events?limit=1":
                    return Response(200,{"streamId":"s","waterline":"0"})
                if "/chats/chat_a/messages?" in path:
                    from urllib.parse import urlsplit,parse_qs
                    after=parse_qs(urlsplit(path).query)["after"][0]
                    return Response(200,{"items":[message] if after=="0" else [],
                                         "nextCursor":"1","hasMore":False})
                return Response(200,{"items":[],"nextCursor":"0","hasMore":False})
        Sync(self.db).page("inbox","global","",
            [{"seq":1,"event_type":"chat.message.created","conversationId":"chat_a"}],"1",1000)
        result=Tick(self.db,self.cfg,client=Client(),clock=lambda:1000).run()
        self.assertTrue(any("/chats/chat_a/messages?" in p for p in calls),calls)
        self.assertEqual(len(self.db.read("SELECT * FROM work_items WHERE type='human_action'")),1)
        self.assertEqual(self.db.read("SELECT state FROM attention_sessions")[0]["state"],"active")
        # Re-reading the same durable message never duplicates agent work.
        Tick(self.db,self.cfg,clock=lambda:1001).run()
        self.assertEqual(len(self.db.read("SELECT * FROM work_items WHERE type='human_action'")),1)

    def test_unmapped_inbox_pointer_stays_blocked(self):
        with self.db.transaction() as c:
            c.execute("INSERT INTO meta VALUES('actor','self')")
        Sync(self.db).page("inbox","global","",
            [{"seq":1,"event_type":"chat.message.created"}],"1",1000)
        Tick(self.db,self.cfg,clock=lambda:1000).run()
        self.assertEqual(self.db.read("SELECT status FROM work_items")[0]["status"],"blocked")
        self.assertEqual(self.db.read("SELECT * FROM work_items WHERE type='human_action'"),[])

    def test_handoff_requires_operator_evidence_and_fenced_completion(self):
        q=Queue(self.db,self.cfg.lease_seconds)
        ident=q.enqueue("human:test","human_action",{"event_id":123},100)
        Tick(self.db,self.cfg,clock=lambda:100).run()
        exported=export_pending(self.db,120)
        self.assertEqual(exported["mode"],"read_only_no_model_wakeup")
        self.assertEqual(exported["human_actions"][0]["status"],"pending")
        self.assertTrue(exported["human_actions"][0]["claimable"])
        with self.assertRaises(ValueError):
            q.claim_specific(ident,"muse",120,{})
        granted=Scheduler(self.db).dispatch("muse",120,for_work_id=ident)
        self.assertTrue(granted["granted"],granted)
        first=q.claim_specific(ident,"muse",120,{"approved_by":"operator","scope":"test"})
        with self.assertRaises(ValueError):
            q.claim_specific(ident,"another",121,{"approved_by":"operator"})
        with self.assertRaises(StaleLease):
            q.finish(dict(first,lease_version=first["lease_version"]-1),121,
                     evidence={"artifact":"real"})
        q.finish(first,121,evidence={"artifact":"real"})
        self.assertEqual(self.db.read("SELECT status FROM work_items WHERE id=?",(ident,))[0]["status"],"done")
        self.assertEqual(len(self.db.read("SELECT * FROM alerts WHERE kind='local_handoff'")),1)

    def test_work_renew_requires_real_new_evidence_and_valid_lease(self):
        q=Queue(self.db,self.cfg.lease_seconds)
        wid=q.enqueue("human:renew","human_action",{"task":"fixture"},100)
        claimed=q.claim_specific(wid,"muse",100,{"approved_by":"owner"})
        with self.assertRaises(ValueError):
            q.renew(claimed,110,{"started":True})
        result=q.renew(claimed,110,{"artifact":"first verifiable step"})
        self.assertEqual(result["lease_until"],230)
        with self.assertRaises(ValueError):
            q.renew(claimed,120,{"artifact":"first verifiable step"})
        with self.assertRaises(StaleLease):
            q.renew(dict(claimed,lease_version=0),120,{"artifact":"different work"})
        q.finish(claimed,125,evidence={"result":"finished"})
        self.assertEqual(len(self.db.read("SELECT * FROM alerts WHERE kind='local_work_progress'")),1)

    def test_export_includes_overdue_promises_not_just_human_actions(self):
        Promises(self.db).create("muse","owner","source_1","deliver",100,105,110,due_at=300)
        feed=export_pending(self.db,150)
        self.assertEqual(len(feed["due_promises"]),1)
        self.assertEqual(feed["human_actions"],[])

    def test_message_sync_precedes_historical_inbox(self):
        seen=[]
        def fake(method,path):
            seen.append(path)
            return Response(200,{"items":[],"nextCursor":"0","hasMore":False})
        self.assertTrue(Collector(Sync(self.db),fake,self.cfg).resources(1000))
        self.assertIn("/api/v1/chats?",seen[0])
        self.assertIn("/chats/chat_a/messages?",seen[1])
        self.assertIn("/api/v1/inbox?",seen[2])

    def test_export_cli_is_offline_and_never_reads_credentials(self):
        out=io.StringIO()
        with contextlib.redirect_stdout(out):
            rc=main(["--db",str(self.db.path),"export-pending"])
        self.assertEqual(rc,0)
        self.assertEqual(json.loads(out.getvalue())["mode"],"read_only_no_model_wakeup")


if __name__=="__main__":
    unittest.main()
