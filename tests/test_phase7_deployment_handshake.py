"""Native CrewQuay deployment protocol, tested with isolated SQLite and fake HTTP."""
import json
import contextlib
import io
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime, timezone
from pathlib import Path

from patrol.components.collection.index import Sync
from patrol.components.config.index import Config
from patrol.components.deployment.index import Deployment
from patrol.components.executor.index import ActionSender
from patrol.components.outbox.index import Outbox
from patrol.components.runtime.index import main
from patrol.components.persistence.index import Database, StaleLease
from patrol.components.queue.index import Queue
from patrol.components.scheduling.index import Scheduler
from patrol.components.transport.index import Response


def iso(sec):
    return datetime.fromtimestamp(sec,timezone.utc).isoformat()


class FakeServer:
    def __init__(self, service, run):
        self.run=run
        self.service=service
        self.posts=0
        self.verified=False

    def request(self, method, path, payload=None, idem=None):
        if method=="POST":
            self.posts+=1
            self.verified=path.endswith("/ack")
            return Response(200,{"status":"protocol_verified" if self.verified else "awaiting_ack"})
        if method=="GET" and path.endswith(self.run["handshake_id"]):
            return Response(200,{"protocolVersion":"1","runId":self.run["run_id"],
                "actorId":"muse","handshakeId":self.run["handshake_id"],
                "hostHookVerified":False,"expiresAt":iso(1300),
                "status":"protocol_verified" if self.verified else "awaiting_ack",
                "evidence":{"platformSenderId":"crewquay-platform",
                    "synMessageId":"syn1","synAckMessageId":"synack2",
                    "synAckEventId":"event3","synAckEventCursor":"3",
                    "ackMessageId":"ack4" if self.verified else None}})
        return Response(404,{})


class DeploymentTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.db=Database(Path(tmp.name)/"local.sqlite3")
        with self.db.transaction() as c:
            c.execute("INSERT INTO meta(key,value) VALUES('actor','muse')")
        self.deploy=Deployment(self.db)
        self.run=self.deploy.start(1000,"cac7688e")
        self.queue=Queue(self.db,lease_seconds=120)
        self.scheduler=Scheduler(self.db,lease_seconds=120)
        self.server=FakeServer(self.deploy,self.run)
        self.sender=ActionSender(self.db,Config(allow_writes=True),self.server,clock=lambda:1005)
        self.sender.actor="muse"

    def claim(self,work,at):
        active=self.scheduler.dispatch("worker",at,for_work_id=work)
        self.assertTrue(active["granted"],active)
        receipt=self.queue.claim_specific(work,"worker",at+1,{"authorized":"test"})
        context={"id":active["activity"]["id"],"owner":"worker",
                 "generation":active["activity"]["lease_version"]}
        return receipt,context

    def drain(self,at=1005):
        return Outbox(self.db).process(at,self.sender.can_send,self.sender.send,
                                      self.sender.verify,limit=1)

    def begin(self):
        work,ctx=self.claim(self.run["syn_work_id"],1001)
        staged=self.deploy.stage(self.run["id"],"syn",1003,ctx)
        self.assertEqual(staged["status"],"prepared")
        self.assertEqual(self.drain()[0]["status"],"confirmed")
        self.queue.finish(work,1006,{"outbox":staged["outbox_id"]})
        self.deploy.reconcile(1007)
        self.assertEqual(self.deploy.get(self.run["id"])["state"],"awaiting_event")
        return ctx

    def record_event(self,sender="crewquay-platform",challenge="a"*64):
        nonce=self.db.read("SELECT nonce FROM deployment_runs WHERE id=?",
                           (self.run["id"],))[0]["nonce"]
        event={"cursor":"3","eventCursor":"3","eventId":"event3",
            "type":"deployment.syn_ack","resourceId":self.run["handshake_id"],
            "handshakeId":self.run["handshake_id"],"runId":self.run["run_id"],
            "nonce":nonce,"challenge":challenge,
            "senderId":sender,"senderKind":"platform" if sender=="crewquay-platform" else "agent",
            "messageId":"synack2","createdAt":iso(1008)}
        Sync(self.db).page("agent_events","global","stream-id",[event],"3",1008)

    def test_successful_protocol_remains_host_unverified(self):
        self.begin()
        self.record_event()
        transitions=self.deploy.reconcile(1009)
        self.assertEqual(transitions[0]["phase"],"syn_ack_received")
        current=self.deploy.get(self.run["id"])
        self.assertNotIn("challenge",current)
        self.assertFalse(current["deployment_verified"])
        ack_work,ctx=self.claim(current["ack_work_id"],1010)
        staged=self.deploy.stage(self.run["id"],"ack",1012,ctx)
        self.assertEqual(staged["status"],"prepared")
        self.assertEqual(self.drain(1013)[0]["status"],"confirmed")
        self.assertEqual(self.deploy.reconcile(1014)[0]["phase"],"protocol_verified")
        final=self.deploy.get(self.run["id"])
        self.assertFalse(final["deployment_verified"])
        self.assertEqual(final["state"],"protocol_verified")
        self.deploy.reconcile(1100)
        self.assertEqual(self.server.posts,2)

    def test_forged_event_never_stages_ack(self):
        self.begin()
        self.record_event(sender="other-agent")
        self.assertEqual(self.deploy.reconcile(1009),[])
        self.assertEqual(self.deploy.get(self.run["id"])["state"],"awaiting_event")

    def test_expired_server_window_marks_failure(self):
        self.begin()
        changed=self.deploy.reconcile(1300)
        self.assertEqual(changed[0]["phase"],"expired")
        current=self.deploy.get(self.run["id"])
        self.assertEqual(current["failure_code"],"FINAL_ACK_NOT_VERIFIED")
        self.assertFalse(current["deployment_verified"])

    def test_old_generation_does_not_authorize_new_remote_effect(self):
        work,ctx=self.claim(self.run["syn_work_id"],1001)
        self.scheduler.checkpoint(ctx["id"],"worker",ctx["generation"],1003,
                                  {"phase":"pause"},voluntary=True)
        nonce=self.db.read("SELECT nonce FROM deployment_runs WHERE id=?",
                           (self.run["id"],))[0]["nonce"]
        with self.assertRaises(StaleLease):
            Outbox(self.db).stage("deployment:new-intent","muse","deployment_syn",
                self.run["handshake_id"],{"runId":self.run["run_id"],"nonce":nonce,
                                          "hookKind":"HATCH","hookIntervalSeconds":60},
                1004,schedule_context=ctx)
        self.assertEqual(self.server.posts,0)

    def test_unsupported_platform_creates_one_fenced_reportable_bug(self):
        with self.db.transaction() as c:
            c.execute("UPDATE work_items SET status='done' WHERE id=?",
                      (self.run["syn_work_id"],))
        missing=self.deploy.unsupported(1010,"cac7688e")
        self.assertEqual(missing["state"],"failed")
        self.assertEqual(missing["failure_code"],"PLATFORM_HANDSHAKE_UNSUPPORTED")
        self.assertTrue(missing["bug_work_id"])
        claim,ctx=self.claim(missing["bug_work_id"],1011)
        request=self.deploy.stage(missing["id"],"bug",1013,ctx)
        self.assertEqual(request["status"],"prepared")
        same=self.deploy.stage(missing["id"],"bug",1014,ctx)
        self.assertEqual(same["outbox_id"],request["outbox_id"])
        saved=self.db.read("SELECT action,payload FROM outbox WHERE id=?",
                           (request["outbox_id"],))[0]
        self.assertEqual(saved["action"],"deployment_bug")
        self.assertIn("PLATFORM_HANDSHAKE_UNSUPPORTED",json.loads(saved["payload"])["title"])
        self.assertIsNone(self.deploy.get(missing["id"])["bug_id"])
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM outbox WHERE action='deployment_bug'")[0]["n"],1)

    def test_agent_ensure_preserves_identity_across_ordinary_sha_upgrade(self):
        conf=Path(self.db.path).parent/"enabled.json"
        conf.write_text(json.dumps({"database":str(self.db.path),
                                    "allow_writes":True}),encoding="utf-8")
        class Capabilities:
            requested=[]
            def __init__(self,*args,**kwargs): pass
            def request(self,method,path):
                self.requested.append(path)
                return Response(200,{"protocolVersion":"1","supported":True,
                    "actorId":"muse","synAckEventType":"deployment.syn_ack",
                    "platformSenderId":"crewquay-platform","windowSeconds":300})
        out=io.StringIO()
        with patch("patrol.components.runtime.composition.cli.Client",Capabilities), \
             patch("patrol.components.runtime.composition.cli.credential_provider",
                   lambda cfg:lambda:"fake"), \
             contextlib.redirect_stdout(out):
            exit_code=main(["--config",str(conf),"deployment-ensure",
                            "--repo-sha","cac7688e"])
        self.assertEqual(exit_code,0)
        self.assertEqual(Capabilities.requested,[])
        self.assertEqual(json.loads(out.getvalue())["status"],"already_initialized")
        out=io.StringIO()
        with patch("patrol.components.runtime.composition.cli.Client",Capabilities), \
             patch("patrol.components.runtime.composition.cli.credential_provider",
                   lambda cfg:lambda:"fake"), \
             contextlib.redirect_stdout(out):
            code=main(["--config",str(conf),"deployment-ensure",
                       "--repo-sha","abcdef0123456789"])
        self.assertEqual(code,0,out.getvalue())
        result=json.loads(out.getvalue())
        self.assertEqual(result["status"],"already_initialized")
        self.assertEqual(result["deployment"]["id"],self.run["id"])
        self.assertEqual(result["deployment"]["repo_sha"],"cac7688e")
        self.assertEqual(Capabilities.requested,[])
        old=self.db.read("SELECT state,failure_code FROM deployment_runs WHERE id=?",
                         (self.run["id"],))[0]
        self.assertEqual(old["state"],"awaiting_syn")
        self.assertIsNone(old["failure_code"])
        self.assertEqual(self.db.read("SELECT count(*) n FROM deployment_runs")[0]["n"],1)

    def test_restart_idempotent_and_actor_isolation(self):
        again=self.deploy.start(1002,"cac7688e")
        self.assertEqual(again["id"],self.run["id"])
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM deployment_runs")[0]["n"],1)
        with self.db.transaction() as c:
            c.execute("UPDATE meta SET value='different-agent' WHERE key='actor'")
        self.assertEqual(self.deploy.get(self.run["id"])["status"],"not_started")


if __name__=="__main__":
    unittest.main()
