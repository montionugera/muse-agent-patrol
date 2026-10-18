"""Server-driven Agent Protocol: fake server and ephemeral SQLite only."""
import hashlib
import tempfile
import unittest
from pathlib import Path

from patrol.components.config.index import Config
from patrol.components.executor.index import ActionSender
from patrol.components.outbox.index import Outbox
from patrol.components.persistence.index import Database, canonical
from patrol.components.transport.index import Response


def envelope(request_id="reply-task-00001", action="task.thread.reply",
             project_type="task", resource_id="task4"):
    return {"action":action,"version":"1",
            "resource":{"type":project_type,"id":resource_id},
            "input":{"content":"Received, updating","mentions":[]},
            "requestId":request_id}


class ProtocolAPI:
    def __init__(self):
        self.calls=[]
        self.committed={}

    def request(self,method,path,payload=None,idem_key=None):
        self.calls.append((method,path,payload,idem_key))
        if method=="GET" and path=="/api/v1/agents/me":
            return Response(200,{"id":"actor"})
        if method=="GET" and path.startswith("/api/v1/agent/runtime-manifest"):
            # Simulate pre-#24 platform; only explicit 404 enables the
            # restrictive legacy fallback. A network exception never does.
            return Response(404,{})
        if method=="GET" and path=="/api/v1/agent/actions":
            return Response(200,{"protocolVersion":"1","actorId":"actor",
                "actions":[{"name":"task.thread.reply","version":"1",
                            "resourceType":"task"},
                           {"name":"task.plan.new-capability","version":"1",
                            "resourceType":"task"},
                           {"name":"community.post.comment","version":"1",
                            "resourceType":"post"},
                           {"name":"community.circle.message","version":"1",
                            "resourceType":"circle"}]})
        if method=="GET" and path.startswith("/api/v1/agent/resources/task/"):
            return Response(200,{"type":"task","id":path.split("/")[-1],
                                 "projectId":"proj"})
        if method=="GET" and path.startswith("/api/v1/agent/resources/post/"):
            return Response(200,{"type":"post","id":path.split("/")[-1]})
        if method=="GET" and path.startswith("/api/v1/agent/resources/circle/"):
            return Response(200,{"type":"circle","id":path.split("/")[-1],
                                 "chatId":"chat_circle_attached","joined":True})
        if method=="POST" and path=="/api/v1/agent/actions/execute":
            request_id=payload["requestId"]
            existing=self.committed.get(request_id)
            if existing:
                return Response(200,existing)
            result={"executionId":"exec_"+request_id,"protocolVersion":"1",
                    "actorId":"actor","action":payload["action"],
                    "version":"1","resource":payload["resource"],
                    "requestId":request_id,"status":"confirmed",
                    "requestHash":hashlib.sha256(canonical(payload).encode()).hexdigest(),
                    "input":payload["input"],"result":{"id":"msg1"}}
            self.committed[request_id]=result
            return Response(200,result)
        if method=="GET" and path.startswith("/api/v1/agent/executions/by-request/"):
            rid=path.split("/")[-1]
            if rid not in self.committed:
                return Response(404,{"error":"not found"})
            return Response(200,self.committed[rid])
        raise AssertionError("Unexpected API "+method+" "+path)


class GenericAgentProtocolTests(unittest.TestCase):
    def setUp(self):
        work=tempfile.TemporaryDirectory()
        self.addCleanup(work.cleanup)
        self.db=Database(Path(work.name)/"protocol.sqlite3")
        with self.db.transaction() as c:
            c.execute("INSERT INTO meta(key,value) VALUES('actor','actor')")
        self.client=ProtocolAPI()
        self.cfg=Config(database=str(self.db.path),allow_writes=True,
            allowed_project_action_ids=["proj"],
            allowed_platform_actions=["task.thread.reply","task.plan.new-capability"])
        self.outbox=Outbox(self.db)

    def stage(self,logical="action-1",payload=None):
        value=payload or envelope()
        return self.outbox.stage(logical,"actor","platform_action",
               value["resource"]["id"],value,1000,producer_id="muse")

    def drain(self,action_id):
        return ActionSender(self.db,self.cfg,self.client,clock=lambda:1000).drain_once(
            limit=1,action_id=action_id,lane="platform")

    def test_generic_action_once_and_confirmed_exact_receipt(self):
        ident=self.stage()
        report=self.drain(ident)
        self.assertEqual(report["item_status"],"confirmed",report)
        state=self.db.read("SELECT * FROM outbox WHERE id=?",(ident,))[0]
        self.assertEqual(state["remote_id"],"exec_reply-task-00001")
        self.assertIn("sha256",state["result"])
        self.drain(ident)
        self.assertEqual(len([x for x in self.client.calls if x[0]=="POST"]),1)

    def test_unknown_crash_result_reconciles_by_request_id_without_post(self):
        value=envelope()
        ident=self.stage(payload=value)
        self.client.request("POST","/api/v1/agent/actions/execute",value,"x")
        self.client.calls.clear()
        with self.db.transaction() as c:
            c.execute("UPDATE outbox SET status='unknown',remote_id=NULL WHERE id=?",(ident,))
        report=self.drain(ident)
        self.assertEqual(report["item_status"],"confirmed",report)
        self.assertEqual(len([x for x in self.client.calls if x[0]=="POST"]),0)

    def test_default_deny_action_scope_does_not_consume(self):
        ident=self.stage()
        cfg=Config(database=str(self.db.path),allow_writes=True,
                   allowed_project_action_ids=["proj"],allowed_platform_actions=[])
        result=ActionSender(self.db,cfg,self.client,clock=lambda:1000).drain_once(1,ident)
        self.assertEqual(result["blocked"],1,result)
        self.assertEqual(len([x for x in self.client.calls if x[0]=="POST"]),0)

    def test_denied_resource_scope_even_with_catalog_permission(self):
        ident=self.stage()
        cfg=Config(database=str(self.db.path),allow_writes=True,
                   allowed_project_action_ids=["other"],
                   allowed_platform_actions=["task.thread.reply"])
        result=ActionSender(self.db,cfg,self.client,clock=lambda:1000).drain_once(1,ident)
        self.assertEqual(result["blocked"],1)
        self.assertFalse(any(x[0]=="POST" for x in self.client.calls))

    def test_new_action_name_needs_no_client_code_mapping(self):
        value=envelope(request_id="new-capability-111",action="task.plan.new-capability")
        ident=self.stage("new-capability",value)
        result=self.drain(ident)
        self.assertEqual(result["item_status"],"confirmed",result)
        self.assertEqual([x[1] for x in self.client.calls if x[0]=="POST"],
                         ["/api/v1/agent/actions/execute"])

    def test_community_social_uses_registry_without_local_action_allowlist(self):
        val=envelope(request_id="community-test-111",action="community.post.comment",
                     project_type="post",resource_id="post_public")
        ident=self.stage("community-allowed",val)
        result=self.drain(ident)
        self.assertEqual(result["item_status"],"confirmed",result)
        self.assertEqual(len([x for x in self.client.calls if x[0]=="POST"]),1)

    def test_owner_blocks_named_community_action_and_resource(self):
        for i,denial in enumerate((
                {"blocked_social_actions":["community.post.comment"]},
                {"blocked_social_resources":["post:post_public"]},
                {"community_social_enabled":False})):
            with self.subTest(denial=denial):
                val=envelope(request_id="community-deny-"+str(i)+"-111",
                    action="community.post.comment",project_type="post",
                    resource_id="post_public")
                ident=self.stage("community-deny-"+str(i),val)
                cfg=Config(database=str(self.db.path),allow_writes=True,**denial)
                self.client.calls.clear()
                report=ActionSender(self.db,cfg,self.client,clock=lambda:1000).drain_once(1,ident)
                self.assertEqual(report["blocked"],1,report)
                self.assertFalse(any(x[0]=="POST" for x in self.client.calls))

    def test_circle_message_owner_resource_ban_cannot_bypass_chat_policy(self):
        val=envelope(request_id="circle-denied-111",action="community.circle.message",
                     project_type="circle",resource_id="cir_private")
        ident=self.stage("circle-denied",val)
        cfg=Config(database=str(self.db.path),allow_writes=True,
                   blocked_social_resources=["circle:cir_private"])
        result=ActionSender(self.db,cfg,self.client,clock=lambda:1000).drain_once(1,ident)
        self.assertEqual(result["blocked"],1,result)
        self.assertFalse(any(x[0]=="POST" for x in self.client.calls))

    def test_circle_linked_chat_denial_blocks_social_action(self):
        val=envelope(request_id="circle-linked-111",action="community.circle.message",
                     project_type="circle",resource_id="cir_allowed")
        ident=self.stage("circle-linked-denied",val)
        cfg=Config(database=str(self.db.path),allow_writes=True,
                   blocked_chat_ids=["chat_circle_attached"])
        report=ActionSender(self.db,cfg,self.client,clock=lambda:1000).drain_once(1,ident)
        self.assertEqual(report["blocked"],1,report)
        self.assertFalse(any(x[0]=="POST" for x in self.client.calls))

    def test_social_ban_does_not_prevent_readonly_reconcile_of_previous_action(self):
        val=envelope(request_id="community-old-111",action="community.post.comment",
                     project_type="post",resource_id="post_public")
        ident=self.stage("community-before-ban",val)
        self.client.request("POST","/api/v1/agent/actions/execute",val,"idem")
        self.client.calls.clear()
        with self.db.transaction() as c:
            c.execute("UPDATE outbox SET status='unknown',remote_id=NULL WHERE id=?",(ident,))
        cfg=Config(database=str(self.db.path),allow_writes=True,
                   blocked_social_resources=["post:post_public"])
        report=ActionSender(self.db,cfg,self.client,clock=lambda:1000).drain_once(1,ident)
        self.assertEqual(report["item_status"],"confirmed",report)
        self.assertFalse(any(x[0]=="POST" for x in self.client.calls))

    def test_payload_rejects_resource_mismatch_and_unstable_id(self):
        with self.assertRaises(ValueError):
            self.outbox.stage("bad","actor","platform_action","wrong",envelope(),1000)
        with self.assertRaises(ValueError):
            self.stage(payload=envelope(request_id="short"))
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM outbox")[0]["n"],0)


if __name__=="__main__":
    unittest.main()
