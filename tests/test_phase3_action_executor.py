"""Phase 3: isolate Muse intent production from a bounded, authorized executor.

No network access, host hook, real credential, or production SQLite is used.
"""
import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from patrol.components.config.index import Config
from patrol.components.executor.index import ActionSender
from patrol.components.outbox.index import Outbox
from patrol.components.persistence.index import Database
from patrol.components.runtime.index import main, Tick, instance_lock
from patrol.components.transport.index import Response, AuthError


class Phase3Tests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db=Database(Path(self.tmp.name)/"phase3.sqlite3")
        self.cfg=Config(database=str(self.db.path),allow_writes=True,
                        allowed_chat_ids=["owner"])
        with self.db.transaction() as c:
            c.execute("INSERT INTO meta(key,value) VALUES('actor','crewquay-agent')")
        self.outbox=Outbox(self.db)

    def fake_client(self, actor="crewquay-agent", message_id="remote-123"):
        calls=[]
        class Client:
            requests=0
            def request(self,method,path,payload=None,idem_key=None):
                calls.append((method,path,payload,idem_key))
                self.requests+=1
                if method=="GET" and path=="/api/v1/agents/me":
                    return Response(200,{"id":actor})
                if method=="GET" and path=="/api/v1/chats/owner":
                    return Response(200,{"conversationId":"owner","kind":"direct",
                        "canSend":True,"memberCount":2,"isObserver":False,
                        "members":[{"id":"crewquay-agent"},{"id":"owner"}]})
                if method=="GET" and path=="/api/v1/chats/not-authorized":
                    return Response(403,{"error":"no membership"})
                if method=="GET" and path=="/api/v1/chats/unapproved":
                    return Response(403,{"error":"no membership"})
                if method=="POST" and path=="/api/v1/chats/owner/messages":
                    return Response(201,{"messageId":message_id})
                if method=="GET" and path.startswith("/api/v1/chats/owner/messages?"):
                    return Response(200,{"items":[{"messageId":message_id,
                        "senderId":"crewquay-agent","content":"hello"}],
                        "nextCursor":"1","hasMore":False})
                raise AssertionError("unanticipated API "+method+" "+path)
        return Client(),calls

    def direct_client(self, detail, message_id="direct-message-123"):
        calls=[]
        class Client:
            def request(self,method,path,payload=None,idem_key=None):
                calls.append((method,path))
                if method=="GET" and path=="/api/v1/agents/me":
                    return Response(200,{"id":"crewquay-agent"})
                if method=="GET" and path=="/api/v1/chats/direct-one":
                    return Response(200,detail)
                if method=="POST" and path=="/api/v1/chats/direct-one/messages":
                    return Response(201,{"messageId":message_id})
                if method=="GET" and path.startswith("/api/v1/chats/direct-one/messages?"):
                    return Response(200,{"items":[{"messageId":message_id,
                        "senderId":"crewquay-agent","content":"hello"}],
                        "nextCursor":"1","hasMore":False})
                if method=="GET" and path=="/api/v1/chats/other-unknown":
                    return Response(404,{"error":"not found"})
                raise AssertionError("unexpected request "+method+" "+path)
        return Client(),calls

    def stage(self,logical="action-1",actor="crewquay-agent",target="owner",content="hello",producer="hook-worker"):
        return self.outbox.stage(logical,actor,"send",target,
                                 {"content":content},1000,producer_id=producer)

    def test_verified_executor_delivers_hook_produced_message(self):
        ident=self.stage()
        record=self.db.read("SELECT * FROM outbox WHERE id=?",(ident,))[0]
        self.assertEqual(record["actor"],"crewquay-agent")
        provenance=self.db.read("SELECT * FROM outbox_producers WHERE outbox_id=?",(ident,))[0]
        self.assertEqual(provenance["producer_id"],"hook-worker")
        client,calls=self.fake_client()
        result=ActionSender(self.db,self.cfg,client,clock=lambda:1000).drain_once(1,ident)
        self.assertEqual(result["status"],"ok",result)
        self.assertEqual(result["item_status"],"confirmed")
        final=self.db.read("SELECT * FROM outbox WHERE id=?",(ident,))[0]
        self.assertEqual(final["status"],"confirmed")
        self.assertEqual(final["remote_id"],"remote-123")
        self.assertTrue(json.loads(final["result"])["sha256"])
        posts=[x for x in calls if x[0]=="POST"]
        self.assertEqual(len(posts),1)
        self.assertEqual(posts[0][3],final["idem_key"])
        # Repeated triggers NEVER cause a duplicate POST.
        again=ActionSender(self.db,self.cfg,client,clock=lambda:1001).drain_once(1,ident)
        self.assertEqual(again["item_status"],"confirmed")
        self.assertEqual(len([x for x in calls if x[0]=="POST"]),1)

    def test_existing_writable_direct_chat_does_not_need_allowlist(self):
        detail={"conversationId":"direct-one","kind":"direct","memberCount":2,
                "canSend":True,"isObserver":False}
        client,calls=self.direct_client(detail)
        ident=self.stage(logical="direct-open",target="direct-one")
        report=ActionSender(self.db,self.cfg,client,clock=lambda:1000).drain_once(1,ident)
        self.assertEqual(report["item_status"],"confirmed",report)
        self.assertIn(("GET","/api/v1/chats/direct-one"),calls)
        self.assertEqual(sum(method=="POST" for method,_ in calls),1)

    def test_owner_denial_overrides_both_auto_direct_and_explicit_allowlist(self):
        detail={"conversationId":"direct-one","kind":"direct","memberCount":2,
                "canSend":True,"isObserver":False}
        client,calls=self.direct_client(detail)
        cfg=Config(database=str(self.db.path),allow_writes=True,
                   allowed_chat_ids=["direct-one"],blocked_chat_ids=["direct-one"])
        ident=self.stage(logical="owner-block",target="direct-one")
        report=ActionSender(self.db,cfg,client,clock=lambda:1000).drain_once(1,ident)
        self.assertEqual(report["blocked"],1,report)
        self.assertFalse(any(path=="/api/v1/chats/direct-one" for _,path in calls))
        self.assertFalse(any(method=="POST" for method,_ in calls))

    def test_group_and_hub_are_writable_without_any_local_chat_allowlist(self):
        for kind in ("group","community_hub"):
            with self.subTest(kind=kind):
                detail={"conversationId":"direct-one","kind":kind,
                        "memberCount":3,"canSend":True,
                        "isObserver":False}
                # The hub uses its actual fixed conversation ID.
                target="community_hub" if kind=="community_hub" else "direct-one"
                client,calls=self.direct_client({**detail,"conversationId":target})
                if kind=="community_hub":
                    class HubClient:
                        def request(self,method,path,payload=None,idem_key=None):
                            if method=="GET" and path=="/api/v1/agents/me":
                                return Response(200,{"id":"crewquay-agent"})
                            if method=="GET" and path=="/api/v1/chats/community_hub":
                                return Response(200,{**detail,"conversationId":"community_hub"})
                            if method=="POST" and path=="/api/v1/chats/community_hub/messages":
                                return Response(201,{"messageId":"hub_123"})
                            if method=="GET" and path.startswith("/api/v1/chats/community_hub/messages?"):
                                return Response(200,{"items":[{"messageId":"hub_123",
                                    "senderId":"crewquay-agent","content":"hello"}],
                                    "nextCursor":"1","hasMore":False})
                            raise AssertionError(method+" "+path)
                    client=HubClient()
                cfg=Config(database=str(self.db.path),allow_writes=True)
                ident=self.stage(logical="auto-"+kind,target=target)
                report=ActionSender(self.db,cfg,client,clock=lambda:1000).drain_once(1,ident)
                self.assertEqual(report["item_status"],"confirmed",report)

    def test_group_identity_and_privileges_are_verified_by_platform(self):
        base={"conversationId":"direct-one","kind":"direct","memberCount":2,
              "canSend":True,"isObserver":False}
        scenarios=[{"memberCount":3},{"canSend":False},{"isObserver":True},
                   {"conversationId":"other"},{"canSend":"true"},
                   {"kind":"project"}, # missing authoritative project identity
                   {"kind":"community_hub"}]
        for i,changes in enumerate(scenarios):
            with self.subTest(changes=changes):
                client,calls=self.direct_client({**base,**changes})
                ident=self.stage(logical="chat-denied-"+str(i),target="direct-one")
                report=ActionSender(self.db,self.cfg,client,clock=lambda:1000).drain_once(1,ident)
                self.assertEqual(report["blocked"],1,report)
                self.assertFalse(any(method=="POST" for method,_ in calls))
        # The authenticated platform's canSend=true + projectId is the
        # authoritative delegation. The obsolete local project-ID allowlist
        # must not block this otherwise verified group.
        project,calls=self.direct_client({**base,"kind":"project",
                                          "projectId":"project-allowed-by-server"})
        ident=self.stage(logical="project-chat-verified",target="direct-one")
        allowed=ActionSender(self.db,self.cfg,project,clock=lambda:1000).drain_once(1,ident)
        self.assertEqual(allowed["item_status"],"confirmed",allowed)
        client,calls=self.direct_client(base)
        ident=self.stage(logical="chat-lookup-unavailable",target="other-unknown")
        report=ActionSender(self.db,self.cfg,client,clock=lambda:1000).drain_once(1,ident)
        self.assertEqual(report["blocked"],1,report)
        self.assertFalse(any(method=="POST" for method,_ in calls))

    def test_circle_ban_rejects_linked_group_send_by_chat_api(self):
        class CircleGroupClient:
            def __init__(self):
                self.calls=[]
            def request(self,method,path,payload=None,idem_key=None):
                self.calls.append((method,path))
                if method=="GET" and path=="/api/v1/agents/me":
                    return Response(200,{"id":"crewquay-agent"})
                if method=="GET" and path=="/api/v1/chats/direct-one":
                    return Response(200,{"conversationId":"direct-one","kind":"group",
                        "canSend":True,"memberCount":3,"isObserver":False})
                if method=="GET" and path=="/api/v1/agent/resources/circle/cir_forbidden":
                    return Response(200,{"type":"circle","id":"cir_forbidden",
                        "chatId":"direct-one","joined":True})
                raise AssertionError(method+" "+path)
        client=CircleGroupClient()
        cfg=Config(database=str(self.db.path),allow_writes=True,
                   blocked_social_resources=["circle:cir_forbidden"])
        ident=self.stage(logical="circle-as-group",target="direct-one")
        report=ActionSender(self.db,cfg,client,clock=lambda:1000).drain_once(1,ident)
        self.assertEqual(report["blocked"],1,report)
        self.assertFalse(any(method=="POST" for method,_ in client.calls))

    def test_owner_contact_ban_applies_even_to_new_direct_chat(self):
        detail={"conversationId":"direct-one","kind":"direct","memberCount":2,
                "canSend":True,"isObserver":False,
                "members":[{"id":"crewquay-agent"},{"id":"blocked-peer"}]}
        client,calls=self.direct_client(detail)
        cfg=Config(database=str(self.db.path),allow_writes=True,
                   blocked_contact_ids=["blocked-peer"])
        ident=self.stage(logical="contact-deny",target="direct-one")
        report=ActionSender(self.db,cfg,client,clock=lambda:1000).drain_once(1,ident)
        self.assertEqual(report["blocked"],1,report)
        self.assertFalse(any(method=="POST" for method,_ in calls))

    def test_owner_ban_prevents_future_writes_but_not_old_receipt_readback(self):
        detail={"conversationId":"direct-one","kind":"direct","memberCount":2,
                "canSend":True,"isObserver":False}
        ident=self.stage(logical="direct-previously-posted",target="direct-one")
        with self.db.transaction() as c:
            c.execute("UPDATE outbox SET status='unknown',remote_id='direct-message-123' WHERE id=?",
                      (ident,))
        client,calls=self.direct_client(detail)
        cfg=Config(database=str(self.db.path),allow_writes=True,
                   blocked_chat_ids=["direct-one"])
        report=ActionSender(self.db,cfg,client,clock=lambda:1000).drain_once(1,ident)
        self.assertEqual(report["item_status"],"confirmed",report)
        self.assertFalse(any(method=="POST" for method,_ in calls))

    def test_unknown_direct_outbox_reconciles_without_reposting(self):
        detail={"conversationId":"direct-one","kind":"direct","memberCount":2,
                "canSend":True,"isObserver":False}
        ident=self.stage(logical="unknown-direct",target="direct-one")
        with self.db.transaction() as c:
            c.execute("UPDATE outbox SET status='unknown',remote_id='direct-message-123' WHERE id=?",
                      (ident,))
        client,calls=self.direct_client(detail)
        report=ActionSender(self.db,self.cfg,client,clock=lambda:1000).drain_once(1,ident)
        self.assertEqual(report["item_status"],"confirmed",report)
        self.assertFalse(any(method=="POST" for method,_ in calls))

    def test_executor_refuses_mismatched_authenticated_actor(self):
        ident=self.stage()
        client,calls=self.fake_client(actor="different-actor")
        with self.assertRaises(AuthError):
            ActionSender(self.db,self.cfg,client,clock=lambda:1000).drain_once(1,ident)
        self.assertEqual(self.db.read("SELECT status FROM outbox WHERE id=?",(ident,))[0]["status"],"prepared")
        self.assertFalse(any(x[0]=="POST" for x in calls))

    def test_executor_requires_explicit_write_authorization(self):
        ident=self.stage()
        client,calls=self.fake_client()
        readonly=Config(database=str(self.db.path),allowed_chat_ids=["owner"])
        result=ActionSender(self.db,readonly,client,clock=lambda:1000).drain_once(1,ident)
        self.assertEqual(result["status"],"authorization_failed")
        self.assertEqual(calls,[])
        self.assertEqual(self.db.read("SELECT status FROM outbox WHERE id=?",(ident,))[0]["status"],"prepared")

    def test_blocked_head_does_not_starve_later_authorized_action(self):
        for n in range(7):
            self.stage(logical="bad-"+str(n),target="not-authorized",producer="hook-worker")
        good=self.stage(logical="allowed")
        client,calls=self.fake_client()
        result=ActionSender(self.db,self.cfg,client,clock=lambda:1000).drain_once(1)
        self.assertEqual(result["confirmed"],1,result)
        self.assertEqual(result["blocked"],7,result)
        self.assertEqual(self.db.read("SELECT status FROM outbox WHERE id=?",(good,))[0]["status"],"confirmed")
        self.assertTrue(all(row["status"]=="prepared" for row in
            self.db.read("SELECT status FROM outbox WHERE target='not-authorized'")))
        self.assertEqual(len([x for x in calls if x[0]=="POST"]),1)

    def test_producer_does_not_mutate_immutable_staged_action(self):
        first=self.stage()
        second=self.stage()
        self.assertEqual(first,second)
        provenance=self.db.read("SELECT * FROM outbox_producers WHERE outbox_id=?",(first,))[0]
        self.assertEqual(provenance["producer_id"],"hook-worker")
        with self.assertRaises(ValueError):
            self.stage(content="different")
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM outbox")[0]["n"],1)

    def test_cli_auto_binds_verified_actor_and_keeps_producer_separate(self):
        payload=Path(self.tmp.name)/"reply.json"
        payload.write_text('{"content":"hello"}',encoding="utf-8")
        out=io.StringIO()
        with contextlib.redirect_stdout(out):
            exitcode=main(["--db",str(self.db.path),"stage","send",
                           "--logical-key","new-reply","--producer-id","hook-worker",
                           "--target","owner","--payload-file",str(payload)])
        self.assertEqual(exitcode,0,out.getvalue())
        row=self.db.read("SELECT * FROM outbox WHERE logical_key='new-reply'")[0]
        self.assertEqual(row["actor"],"crewquay-agent")
        p=self.db.read("SELECT producer_id FROM outbox_producers WHERE outbox_id=?",(row["id"],))
        self.assertEqual(p[0]["producer_id"],"hook-worker")

    def test_cli_rejects_worker_as_platform_actor_before_enqueue(self):
        payload=Path(self.tmp.name)/"reply.json"
        payload.write_text('{"content":"hello"}',encoding="utf-8")
        out=io.StringIO()
        with contextlib.redirect_stdout(out):
            exitcode=main(["--db",str(self.db.path),"stage","send",
                           "--logical-key","bad-reply","--actor","hook-worker",
                           "--target","owner","--payload-file",str(payload)])
        self.assertEqual(exitcode,1)
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM outbox")[0]["n"],0)

    def test_cli_immediate_dispatch_subprocess_starts_after_lock_release(self):
        payload=Path(self.tmp.name)/"reply.json"
        payload.write_text('{"content":"hello"}',encoding="utf-8")
        commands=[]
        def execute_in_child(command,**kwargs):
            commands.append(command)
            # If stage still held the lock this would raise Busy.
            with instance_lock(str(self.db.path)+".tick.lock"):
                pass
            with self.db.transaction() as c:
                c.execute("""UPDATE outbox SET status='confirmed',
                    remote_id='mock-readback-1',result='{"id":"mock-readback-1"}'
                    WHERE logical_key='immediate'""")
            self.assertIn("outbox-drain",command)
            self.assertIn("--once",command)
            self.assertIn("--id",command)
            return SimpleNamespace(returncode=0,stdout=json.dumps(
                {"status":"ok","item_status":"confirmed","confirmed":1}))
        out=io.StringIO()
        with patch("patrol.components.runtime.composition.cli.subprocess.run",side_effect=execute_in_child):
            with contextlib.redirect_stdout(out):
                code=main(["--db",str(self.db.path),"stage","send",
                           "--logical-key","immediate","--producer-id","hook-worker",
                           "--target","owner","--payload-file",str(payload),
                           "--dispatch-now"])
        self.assertEqual(code,0,out.getvalue())
        self.assertEqual(len(commands),1)
        result=json.loads(out.getvalue())
        self.assertTrue(result["sent"])
        self.assertEqual(result["status"],"confirmed")
        # The mocked child wrote a confirmed DB row; the caller trusts DB,
        # not just what the child claimed on stdout.
        self.assertEqual(self.db.read("SELECT status FROM outbox")[0]["status"],"confirmed")

    def test_cli_dispatch_failure_keeps_prepared_work_without_false_success(self):
        payload=Path(self.tmp.name)/"reply.json"
        payload.write_text('{"content":"hello"}',encoding="utf-8")
        out=io.StringIO()
        with patch("patrol.components.runtime.composition.cli.subprocess.run",
                   return_value=SimpleNamespace(returncode=1,stdout=json.dumps(
                       {"status":"authorization_failed","item_status":"prepared","confirmed":0}))):
            with contextlib.redirect_stdout(out):
                code=main(["--db",str(self.db.path),"stage","send",
                           "--logical-key","durable","--target","owner",
                           "--payload-file",str(payload),"--dispatch-now"])
        self.assertEqual(code,0)
        result=json.loads(out.getvalue())
        self.assertFalse(result["sent"])
        self.assertEqual(result["dispatch"]["status"],"authorization_failed")
        self.assertEqual(self.db.read("SELECT status FROM outbox")[0]["status"],"prepared")

    def test_immediate_dispatch_does_not_trust_forged_child_success(self):
        payload=Path(self.tmp.name)/"reply.json"
        payload.write_text('{"content":"hello"}',encoding="utf-8")
        out=io.StringIO()
        # Child output claims confirmation but neither POST nor durable
        # readback happened. Parent must not claim a message was delivered.
        with patch("patrol.components.runtime.composition.cli.subprocess.run",
                   return_value=SimpleNamespace(returncode=0,stdout=json.dumps(
                       {"status":"ok","item_status":"confirmed","confirmed":1}))):
            with contextlib.redirect_stdout(out):
                code=main(["--db",str(self.db.path),"stage","send",
                           "--logical-key","fake-success","--target","owner",
                           "--payload-file",str(payload),"--dispatch-now"])
        self.assertEqual(code,0)
        result=json.loads(out.getvalue())
        self.assertFalse(result["sent"],result)
        self.assertEqual(result["outbox_status"],"prepared")
        self.assertEqual(self.db.read("SELECT status FROM outbox")[0]["status"],"prepared")

    def test_full_tick_and_executor_share_authorization_policy(self):
        tick=Tick(self.db,self.cfg)
        tick.actor="crewquay-agent"
        row={"actor":"crewquay-agent","target":"unapproved","action":"send",
             "payload":{"content":"hello"}}
        self.assertFalse(tick.can_send(row)[0])
        self.assertFalse(ActionSender(self.db,self.cfg).can_send(row)[0])

    def test_direct_staging_rejects_producer_as_bound_platform_actor(self):
        with self.assertRaises(ValueError):
            self.stage(actor="hook-worker",producer="hook-worker")
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM outbox")[0]["n"],0)

    def test_chat_lane_does_not_consume_notification_action(self):
        message=self.stage(logical="chat-response")
        ack=self.outbox.stage("notif-ack","crewquay-agent","ack","",
                              {"ids":[1]},1000,producer_id="muse-worker")
        client,calls=self.fake_client()
        result=ActionSender(self.db,self.cfg,client,clock=lambda:1000).drain_once(
            limit=5,lane="chat")
        self.assertEqual(result["confirmed"],1,result)
        self.assertEqual(self.db.read("SELECT status FROM outbox WHERE id=?",(message,))[0]["status"],"confirmed")
        self.assertEqual(self.db.read("SELECT status FROM outbox WHERE id=?",(ack,))[0]["status"],"prepared")
        self.assertFalse(any("/inbox/ack" in x[1] for x in calls))

    def test_real_cli_drain_path_uses_one_shot_verified_sender(self):
        ident=self.stage(logical="cli-send")
        config_path=Path(self.tmp.name)/"config.json"
        config_path.write_text(json.dumps({"database":str(self.db.path),
            "allow_writes":True,"allowed_chat_ids":["owner"]}),encoding="utf-8")
        client,calls=self.fake_client()
        out=io.StringIO()
        with patch("patrol.components.runtime.composition.cli.Client",
                   return_value=client), patch(
                   "patrol.components.runtime.composition.cli.credential_provider",
                   return_value=lambda:"never-used"):
            with contextlib.redirect_stdout(out):
                code=main(["--config",str(config_path),"outbox-drain",
                           "--once","--id",ident,"--lane","chat","--limit","1"])
        self.assertEqual(code,0,out.getvalue())
        report=json.loads(out.getvalue())
        self.assertEqual(report["item_status"],"confirmed",report)
        self.assertEqual(len([x for x in calls if x[0]=="POST"]),1)

    def test_unknown_remote_outcome_is_readback_only_and_not_posted_twice(self):
        ident=self.stage(logical="unknown-once")
        with self.db.transaction() as c:
            c.execute("UPDATE outbox SET status='unknown',remote_id='remote-123' WHERE id=?",
                      (ident,))
        client,calls=self.fake_client()
        report=ActionSender(self.db,self.cfg,client,clock=lambda:1000).drain_once(1,ident)
        self.assertEqual(report["item_status"],"confirmed",report)
        self.assertTrue(any(path.startswith("/api/v1/chats/owner/messages?")
                            for method,path,_,__ in calls if method=="GET"))
        self.assertFalse(any(method=="POST" for method,_,__,___ in calls))

    def test_task_thread_cannot_be_sent_via_chat_scope(self):
        ident=self.stage(target="task4-thread")
        client,calls=self.fake_client()
        report=ActionSender(self.db,self.cfg,client,clock=lambda:1000).drain_once(1,ident)
        self.assertEqual(report["status"],"degraded")
        self.assertEqual(report["item_status"],"prepared")
        self.assertFalse(any(x[0]=="POST" for x in calls))


if __name__=="__main__":
    unittest.main()
