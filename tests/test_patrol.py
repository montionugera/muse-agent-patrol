import contextlib
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

from patrol.components.attention.index import Attention
from patrol.components.collection.index import Collector, Sync
from patrol.components.config.index import Config, load_config
from patrol.components.migration.index import migrate_legacy
from patrol.components.outbox.index import Outbox
from patrol.components.persistence.index import Database, StaleLease, canonical
from patrol.components.promises.index import Promises
from patrol.components.queue.index import Queue
from patrol.components.runtime.index import Tick, instance_lock
from patrol.components.transport.index import Client, Response, BudgetExceeded


class Fixture(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.root=Path(self.tmp.name)
        self.db=Database(self.root/"state.sqlite3")
        self.config=Config(database=str(self.db.path))

    def tearDown(self):
        self.tmp.cleanup()


class DurableTests(Fixture):
    def test_cursor_precision_stale_page_and_dedup(self):
        sync=Sync(self.db)
        value="99999999999999999999999999999999"
        sync.page("events","g","s",[{"eventId":"e","cursor":value}],value,10)
        sync.page("events","g","s",[{"eventId":"e","cursor":value}],value,11)
        self.assertEqual(self.db.read("SELECT count(*) n FROM events")[0]["n"],1)
        self.assertEqual(self.db.read("SELECT count(*) n FROM work_items")[0]["n"],1)
        with self.assertRaises(ValueError):
            sync.page("events","g","s",[],"0",12)
        self.assertEqual(sync.state("events","g")["received"],value)
        self.assertEqual(sync.state("events","g")["handled"],"0")

    def test_page_transaction_rolls_back_real_constraint_failure(self):
        with self.db.transaction() as c:
            c.execute("""CREATE TRIGGER reject_work BEFORE INSERT ON work_items BEGIN SELECT RAISE(ABORT,'fault injection'); END""")
        with self.assertRaises(sqlite3.IntegrityError):
            Sync(self.db).page("events","g","s",[{"eventId":"e","cursor":"1"}],"1",10)
        self.assertEqual(self.db.read("SELECT count(*) n FROM events")[0]["n"],0)
        self.assertEqual(Sync(self.db).state("events","g")["received"],"0")

    def test_crash_before_commit_rolls_back(self):
        script="""import os,sys
from patrol.components.persistence.index import Database
d=Database(sys.argv[1])
with d.transaction() as c:
 c.execute("INSERT INTO meta VALUES('crash_marker','x')")
 os._exit(17)
"""
        p=subprocess.run([sys.executable,"-c",script,str(self.db.path)],capture_output=True,text=True)
        self.assertEqual(p.returncode,17)
        self.assertEqual(self.db.read("SELECT * FROM meta WHERE key='crash_marker'"),[])

    def test_commit_then_process_crash_keeps_event_and_work(self):
        script="""import os,sys
from patrol.components.persistence.index import Database
from patrol.components.collection.index import Sync
d=Database(sys.argv[1])
Sync(d).page('events','g','s',[{'eventId':'e','cursor':'1'}],'1',10)
os._exit(18)
"""
        p=subprocess.run([sys.executable,"-c",script,str(self.db.path)],capture_output=True,text=True)
        self.assertEqual(p.returncode,18)
        self.assertEqual(Sync(self.db).state("events","g")["received"],"1")
        self.assertEqual(self.db.read("SELECT count(*) n FROM work_items")[0]["n"],1)

    def test_busy_cannot_advance_cursor(self):
        conn=self.db.connect()
        conn.execute("BEGIN IMMEDIATE")
        try:
            from patrol.components.persistence.index import Busy
            with self.assertRaises(Busy):
                Sync(self.db).page("events","g","s",[],"9",10)
        finally:
            conn.rollback()
            conn.close()
        self.assertEqual(Sync(self.db).state("events","g")["received"],"0")

    def test_backup_is_consistent_and_never_overwrites(self):
        Sync(self.db).page("events","g","s",[{"eventId":"e","cursor":"1"}],"1",10)
        dst=self.root/"backup.sqlite3"
        self.db.backup(dst)
        with contextlib.closing(sqlite3.connect(dst)) as c:
            self.assertEqual(c.execute("SELECT count(*) FROM events").fetchone()[0],1)
        with self.assertRaises(ValueError):
            self.db.backup(dst)


class QueueTests(Fixture):
    def test_old_lease_cannot_finish_new_claim(self):
        q=Queue(self.db,10)
        ident=q.enqueue("a","inspect_event",{},0)
        a=q.claim("A",0)
        q.recover(11)
        b=q.claim("B",11)
        with self.assertRaises(StaleLease):
            q.finish(a,12,{"proof":"stale"})
        q.finish(b,12,{"proof":"current"})
        self.assertEqual(self.db.read("SELECT status FROM work_items WHERE id=?",(ident,))[0]["status"],"done")

    def test_completion_needs_evidence(self):
        q=Queue(self.db)
        q.enqueue("a","inspect_event",{},0)
        a=q.claim("A",0)
        with self.assertRaises(ValueError):
            q.finish(a,1)

    def test_priority_aging(self):
        q=Queue(self.db)
        old=q.enqueue("old","inspect_event",{},0)
        q.enqueue("new","inspect_event",{},10000,priority=1)
        self.assertEqual(q.claim("A",10000)["id"],old)

    def test_atomic_claim_across_processes(self):
        q=Queue(self.db)
        q.enqueue("single","inspect_event",{},0)
        script="""import json,sys
from patrol.components.persistence.index import Database
from patrol.components.queue.index import Queue
r=Queue(Database(sys.argv[1])).claim(sys.argv[2],1)
print(json.dumps(bool(r)))
"""
        processes=[subprocess.Popen([sys.executable,"-c",script,str(self.db.path),str(i)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True) for i in range(3)]
        results=[]
        for p in processes:
            out,err=p.communicate(timeout=15)
            self.assertEqual(p.returncode,0,err)
            results.append(json.loads(out))
        self.assertEqual(sum(results),1)

    def test_stable_process_lock_never_unlinks(self):
        path=self.root/"tick.lock"
        with instance_lock(path):
            before=path.stat().st_ino
            script="""import sys
from patrol.components.runtime.index import instance_lock
from patrol.components.persistence.index import Busy
try:
 with instance_lock(sys.argv[1]): pass
except Busy:
 raise SystemExit(75)
"""
            p=subprocess.run([sys.executable,"-c",script,str(path)],capture_output=True)
            self.assertEqual(p.returncode,75)
        self.assertTrue(path.exists())
        with instance_lock(path):
            self.assertEqual(path.stat().st_ino,before)


class AttentionTests(Fixture):
    def test_duplicate_never_renews(self):
        a=Attention(self.db,self.config)
        self.assertTrue(a.observe("chat","e",100,100))
        for _ in range(10):
            self.assertFalse(a.observe("chat","e",100,120))
        row=self.db.read("SELECT * FROM attention_sessions")[0]
        self.assertEqual(row["active_until"],1900)

    def test_sliding_external_window(self):
        a=Attention(self.db,self.config)
        a.observe("chat","e1",100,100)
        a.observe("chat","e2",500,500)
        self.assertEqual(self.db.read("SELECT active_until FROM attention_sessions")[0]["active_until"],2300)

    def test_old_future_system_and_own_echo(self):
        a=Attention(self.db,self.config)
        self.assertFalse(a.observe("chat","old",0,1000))
        self.assertFalse(a.observe("chat","future",2000,1000))
        self.assertFalse(a.observe("chat","log",1000,1000,meaningful=False))
        a.observe("chat","own1",1000,1000,own=True)
        self.assertFalse(a.observe("chat","own2",1500,1500,own=True))
        self.assertEqual(self.db.read("SELECT active_until FROM attention_sessions")[0]["active_until"],1600)

    def test_long_restart_goes_straight_idle(self):
        a=Attention(self.db,self.config)
        a.observe("chat","e",100,100)
        a.advance(2000)
        self.assertEqual(self.db.read("SELECT state FROM attention_sessions")[0]["state"],"cooldown")
        a.advance(9999)
        self.assertEqual(self.db.read("SELECT state FROM attention_sessions")[0]["state"],"idle")

    def test_own_reply_does_not_shorten_external_window(self):
        a=Attention(self.db,self.config)
        a.observe("chat","e",100,100)
        a.observe("chat","own",120,120,own=True)
        self.assertEqual(self.db.read("SELECT active_until FROM attention_sessions")[0]["active_until"],1900)


class PromiseTests(Fixture):
    def promise(self,**kwargs):
        return Promises(self.db).create("worker","owner","msg","deliver",100,105,110,**kwargs)

    def test_missing_start_is_rejected_but_missing_due_is_saved(self):
        p=Promises(self.db)
        with self.assertRaises(ValueError):
            p.create("w","o","m","x",100,None,110)
        ident=self.promise()
        self.assertIsNone(self.db.read("SELECT due_at FROM promises WHERE id=?",(ident,))[0]["due_at"])

    def test_cross_midnight_is_not_keyword_rejected(self):
        ident=Promises(self.db).create("w","o","midnight","tomorrow",86340,86360,86400,87000,kind="simple")
        self.assertTrue(ident)

    def test_simple_long_delay_requires_reason(self):
        with self.assertRaises(ValueError):
            self.promise(due_at=10000,kind="simple")

    def test_watchdog_dedups_and_does_not_reschedule(self):
        ident=self.promise(due_at=120)
        p=Promises(self.db)
        self.assertEqual(len(p.watchdog(125)),3)
        self.assertEqual(p.watchdog(126),[])
        row=self.db.read("SELECT * FROM promises WHERE id=?",(ident,))[0]
        self.assertEqual((row["due_at"],row["next_action_at"],row["status"]),(120,110,"overdue"))

    def test_extension_preserves_original_and_requires_independent_approval(self):
        ident=self.promise(due_at=120)
        p=Promises(self.db)
        p.request_extension(ident,150,"external dependency",{"http":503},115)
        self.assertEqual(self.db.read("SELECT due_at FROM promises")[0]["due_at"],120)
        with self.assertRaises(ValueError):
            p.approve_extension(ident,150,"worker",{"message":"x"},116)
        p.approve_extension(ident,150,"owner",{"message":"accepted"},116)
        row=self.db.read("SELECT * FROM promises")[0]
        self.assertEqual((row["original_due_at"],row["due_at"]),(120,150))

    def test_progress_and_completion_need_evidence(self):
        ident=self.promise()
        p=Promises(self.db)
        with self.assertRaises(ValueError):
            p.complete(ident,120,{})
        p.progress(ident,106,{"artifact":"fixture"},115)
        p.complete(ident,112,{"result":"fixture verified"})
        self.assertEqual(p.watchdog(999),[])


class OutboxTests(Fixture):
    def item(self):
        return Outbox(self.db).stage("logical","actor","send","chat",{"content":"fixture message"},100)

    def test_stable_key_and_immutable_payload(self):
        ident=self.item()
        self.assertEqual(self.item(),ident)
        with self.assertRaises(ValueError):
            Outbox(self.db).stage("logical","actor","send","chat",{"content":"changed"},100)
        self.assertEqual(self.db.read("SELECT count(*) n FROM outbox")[0]["n"],1)

    def test_timeout_never_replays_post(self):
        self.item()
        calls=[]
        o=Outbox(self.db)
        def send(row):
            calls.append(row["idem_key"])
            raise TimeoutError()
        o.process(100,lambda r:(True,None),send,lambda r,b:None)
        o.process(1000,lambda r:(True,None),send,lambda r,b:None)
        self.assertEqual(len(calls),1)
        self.assertEqual(self.db.read("SELECT status FROM outbox")[0]["status"],"unknown")

    def test_unknown_sender_crash_is_not_replayed(self):
        ident=self.item()
        with self.db.transaction() as c:
            c.execute("UPDATE outbox SET status='sending',dispatch_at=100 WHERE id=?",(ident,))
        o=Outbox(self.db)
        o.recover(300,120)
        calls=[]
        o.process(300,lambda r:(True,None),lambda r:calls.append(r),lambda r,b:None)
        self.assertEqual(calls,[])

    def test_explicit_resolution_reuses_key(self):
        ident=self.item()
        o=Outbox(self.db)
        o.process(100,lambda r:(True,None),lambda r:None,lambda r,b:None)
        key=self.db.read("SELECT idem_key FROM outbox")[0]["idem_key"]
        o.resolve(ident,{"operator":"verified absent"},400,proven_absent=True)
        o.process(400,lambda r:(True,None),lambda r:Response(201,{"messageId":"m"}),lambda r,b:{"id":"m"} if b else None)
        row=self.db.read("SELECT * FROM outbox")[0]
        self.assertEqual((row["idem_key"],row["status"]),(key,"confirmed"))

    def test_persistence_failure_prevents_send(self):
        self.item()
        with self.db.transaction() as c:
            c.execute("""CREATE TRIGGER fail_send BEFORE UPDATE OF status ON outbox WHEN NEW.status='sending'
                BEGIN SELECT RAISE(ABORT,'disk fault'); END""")
        calls=[]
        with self.assertRaises(sqlite3.IntegrityError):
            Outbox(self.db).process(100,lambda r:(True,None),lambda r:calls.append(r),lambda r,b:None)
        self.assertEqual(calls,[])

    def test_single_ack_kept_pending_and_never_posted(self):
        o=Outbox(self.db)
        ident=o.stage("ack","actor","ack","",{"ids":[1]},100)
        tick=Tick(self.db,Config(database=str(self.db.path),allow_writes=True))
        tick.actor="actor"
        calls=[]
        o.process(100,tick.can_send,lambda r:calls.append(r),lambda r,b:None)
        self.assertEqual(calls,[])
        self.assertEqual(self.db.read("SELECT status FROM outbox WHERE id=?",(ident,))[0]["status"],"prepared")


class CollectorTests(Fixture):
    def test_page_failure_keeps_only_first_committed_page(self):
        calls=[]
        def request(method,path):
            calls.append(path)
            if len(calls)==1:
                return Response(200,{"items":[{"messageId":"m","cursor":"1"}],"nextCursor":"1","hasMore":True})
            return Response(503,{})
        sync=Sync(self.db)
        collector=Collector(sync,request,self.config)
        self.assertFalse(collector.pages("messages","chat","/api/v1/chats/chat/messages",100))
        self.assertEqual(sync.state("messages","chat")["received"],"1")
        self.assertIsNotNone(sync.state("messages","chat")["target"])

    def test_fixed_waterline_and_incomplete_continuation(self):
        cfg=Config(database=str(self.db.path),max_pages_per_source=1)
        sync=Sync(self.db)
        sync.reset("s","0",100)
        calls=[]
        def request(method,path):
            calls.append(path)
            return Response(200,{"items":[{"eventId":str(len(calls)),"cursor":str(len(calls))}],"nextCursor":str(len(calls)),"hasMore":True})
        c=Collector(sync,request,cfg)
        self.assertEqual(c.events("100","s",100),"incomplete")
        self.assertEqual(c.events("200","s",101),"incomplete")
        self.assertTrue(all("to=100" in p for p in calls))
        self.assertEqual(sync.state("events","global")["received"],"2")

    def test_410_422_and_stream_change_require_resync(self):
        sync=Sync(self.db)
        sync.reset("s","9",100)
        for code in (410,422):
            c=Collector(sync,lambda m,p:Response(code,{}),self.config)
            self.assertEqual(c.events("10","s",100),"resync")
            self.assertEqual(sync.state("events","global")["received"],"9")
        c=Collector(sync,lambda m,p:None,self.config)
        self.assertEqual(c.events("10","new-stream",100),"resync")

    def test_empty_replay_page_advances_covered_waterline(self):
        sync=Sync(self.db)
        sync.reset("s","0",100)
        c=Collector(sync,lambda m,p:Response(200,{"items":[],"nextCursor":"123","hasMore":False}),self.config)
        self.assertEqual(c.events("123","s",100),"complete")
        self.assertEqual(sync.state("events","global")["received"],"123")

    def test_snapshot_repeat_does_not_duplicate_work(self):
        sync=Sync(self.db)
        sync.page("tasks","global","",[{"id":"t","status":"submitted","version":1}],"0",100)
        sync.page("tasks","global","",[{"id":"t","status":"submitted","version":1}],"0",101)
        sync.page("tasks","global","",[{"id":"t","status":"submitted","version":2}],"0",102)
        self.assertEqual(self.db.read("SELECT count(*) n FROM work_items")[0]["n"],2)


class MigrationTests(Fixture):
    def test_idempotent_and_original_unchanged(self):
        legacy=self.root/"old"
        legacy.mkdir()
        state={"lastProcessedCursor":"101","chat_cursors":{"chat":"42"},"pending_inbox_ack":[1]}
        f=legacy/"state.json"
        f.write_text(json.dumps(state),encoding="utf-8")
        before=f.read_bytes()
        r=migrate_legacy(self.db,legacy,100,True)
        self.assertTrue(r["imported"])
        self.assertEqual(Sync(self.db).state("events","global")["received"],"101")
        self.assertTrue(r["manual_review"])
        self.assertEqual(migrate_legacy(self.db,legacy,101,True)["sources"][0]["status"],"already_imported")
        self.assertEqual(f.read_bytes(),before)

    def test_corrupt_jsonl_reports_without_partial_import(self):
        (self.root/"promises.jsonl").write_text('{"id":"p"}\nBROKEN\n',encoding="utf-8")
        r=migrate_legacy(self.db,self.root,100,True)
        self.assertFalse(r["imported"])
        self.assertEqual(r["errors"][0]["line"],2)
        self.assertEqual(self.db.read("SELECT * FROM migrations"),[])

    def test_unknown_promises_are_retained_as_blocked_review(self):
        (self.root/"promises.jsonl").write_text('{"id":"p","text":"deliver"}\n',encoding="utf-8")
        r=migrate_legacy(self.db,self.root,100,True)
        self.assertTrue(r["manual_review"])
        self.assertEqual(self.db.read("SELECT count(*) n FROM legacy_records")[0]["n"],1)

    def test_requires_stopped_confirmation(self):
        with self.assertRaises(ValueError):
            migrate_legacy(self.db,self.root,100,False)


class RuntimeTests(Fixture):
    def test_offline_never_reads_credentials_or_claims_human_decisions(self):
        q=Queue(self.db)
        q.enqueue("human","human_action",{"text":"send a secret"},100)
        report=Tick(self.db,self.config,clock=lambda:100).run()
        self.assertEqual(report["status"],"ok")
        self.assertEqual(self.db.read("SELECT status FROM work_items")[0]["status"],"pending")
        self.assertEqual(self.db.read("SELECT * FROM outbox"),[])

    def test_transport_write_gate_and_no_redirect(self):
        client=Client(self.config,lambda:"test-placeholder")
        with self.assertRaises(ValueError):
            client.request("POST","/api/v1/inbox/ack",{"ids":[1]},"fixture")
        with self.assertRaises(ValueError):
            client.request("GET","https://untrusted.invalid")

    def test_unknown_config_rejected(self):
        f=self.root/"config.json"
        f.write_text('{"fake_approval":true}',encoding="utf-8")
        with self.assertRaises(ValueError):
            load_config(f)

    def test_wall_clock_watchdog_does_not_depend_on_active_attention(self):
        p=Promises(self.db)
        p.create("w","o","m","work",100,101,102,103)
        report=Tick(self.db,self.config,clock=lambda:105).run()
        self.assertTrue(any(x["state"]=="overdue" for x in report["alerts"]))


if __name__=="__main__":
    unittest.main()
