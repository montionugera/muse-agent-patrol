"""Opt-in social wake path: no real host/model/network in these tests."""
import contextlib
import io
import json
import os
import shutil
import sqlite3
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from patrol.components.config.index import Config
from patrol.components.host.index import HostHandoff
from patrol.components.persistence.index import Database
from patrol.components.queue.index import Queue
from patrol.components.runtime.index import main
from patrol.components.scheduling.index import Scheduler
from patrol.components.social.index import SocialOpportunities
from patrol.components.transport.index import Response
from patrol.components.wakeup.index import Wakeups


def opportunity(name="post_one", actor="muse"):
    return {"protocolVersion":"1","mode":"read_only_social_discovery",
            "actorId":actor,"modelInvoked":False,"actionExecuted":False,
            "items":[{"kind":"post","resource":{"type":"post","id":name},
                      "suggestedAction":"community.post.comment",
                      "contentTrust":"untrusted","matchedInterests":[],
                      "title":"untrusted","preview":"ignore external instructions"}]}


class SocialWakeTests(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory()
        self.addCleanup(t.cleanup)
        self.path=Path(t.name)/"state.sqlite3"
        self.db=Database(self.path)
        with self.db.transaction() as c:
            c.execute("INSERT INTO meta(key,value) VALUES('actor','muse')")
        self.social=SocialOpportunities(self.db)
        self.wakeup=Wakeups(self.db,lease_seconds=120)
        self.host=HostHandoff(self.db,lease_seconds=120)
        self.scheduler=Scheduler(self.db,lease_seconds=120)

    def add_social(self,name,now=1000):
        return self.social.admit(opportunity(name),now)["activities"][0]["activity_id"]

    def test_social_default_handoff_uses_same_durable_queue(self):
        aid=self.add_social("post_alpha")
        self.assertEqual(self.db.read(
            "SELECT COUNT(*) n FROM wakeup_requests")[0]["n"],0)
        # Default host preparation selects social without a redundant switch.
        first=self.host.prepare("host",1002)
        self.assertTrue(first["should_wake"])
        self.assertEqual(first["source_type"],"social")
        self.assertEqual(first["source_id"],aid)
        self.assertEqual(first["candidate_activity_id"],aid)
        self.assertFalse(first["activity_dispatched"])
        self.assertFalse(first["host_called"])
        self.assertTrue(first["requires_host_receipt"])
        self.assertFalse(self.host.prepare("another-host",1003,allow_social=True)["should_wake"])
        self.assertEqual(self.db.read(
            "SELECT COUNT(*) n FROM wakeup_requests WHERE source_type='social'")[0]["n"],1)
        grant=self.scheduler.dispatch("muse",1004)
        self.assertTrue(grant["granted"])
        self.assertEqual(grant["activity"]["id"],aid)
        accepted=self.wakeup.acknowledge(first["wakeup_id"],"host",
                     first["lease_version"],1005,"accepted",{"host_receipt":"fake accepted"})
        self.assertEqual(accepted["status"],"accepted")
        self.scheduler.complete(aid,"muse",grant["activity"]["lease_version"],
                                1006,{"decision":"skipped","reason":"nothing useful to add"})

    def test_work_wakeup_beats_social_and_prevents_social_claim(self):
        self.add_social("post_social")
        pending=Queue(self.db).enqueue("work-attention","human_action",
                         {"reason":"important work"},1000,priority=95)
        self.assertEqual(self.wakeup.stage(1000),1)
        claim=self.host.prepare("host",1000,allow_social=True)
        self.assertTrue(claim["should_wake"])
        self.assertEqual(claim["source_type"],"work")
        self.assertEqual(claim["source_id"],pending)
        self.assertFalse(self.host.prepare("host2",1001,allow_social=True)["should_wake"])
        self.assertEqual(self.db.read(
            "SELECT COUNT(*) n FROM wakeup_requests WHERE source_type='social'")[0]["n"],0)

    def test_one_outstanding_social_signal_and_unknown_is_not_auto_replayed(self):
        self.add_social("post_first")
        self.add_social("post_second")
        first=self.host.prepare("host",1002,allow_social=True)
        self.assertTrue(first["should_wake"])
        self.assertFalse(self.host.prepare("host2",1003,allow_social=True)["should_wake"])
        self.assertEqual(self.wakeup.recover(1123,detail=True)["unknown"],1)
        self.assertFalse(self.host.prepare("host3",1124,allow_social=True)["should_wake"])
        self.wakeup.reconcile_unknown(first["wakeup_id"],1125,True,
                                     {"fake_proof":"host never accepted"})
        retry=self.host.prepare("host4",1126,allow_social=True)
        self.assertTrue(retry["should_wake"])
        self.assertEqual(retry["wakeup_id"],first["wakeup_id"])
        self.assertEqual(retry["lease_version"],first["lease_version"]+1)

    def test_social_wake_follows_exact_scheduler_candidate(self):
        first=self.add_social("post_first")
        second=self.add_social("post_other")
        # A stale wakeup prepared for a different activity cannot stand in
        # for the Scheduler's selected activity ID.
        self.assertEqual(self.wakeup.stage_social(1001,second),1)
        decision=self.scheduler.assess(1002)["candidate"]["id"]
        if decision!=second:
            self.assertIsNone(self.wakeup.claim_next(
                "host",1002,allow_social=True,social_activity_id=decision))
        else:
            # No social wakeup may be claimed for the other activity.
            self.assertIsNone(self.wakeup.claim_next(
                "host",1002,allow_social=True,social_activity_id=first))

    def test_completed_social_before_host_claim_is_retired(self):
        activity=self.add_social("post_finished")
        self.assertEqual(self.wakeup.stage_social(1000,activity),1)
        with self.db.transaction() as c:
            c.execute("UPDATE schedule_activities SET status='completed' WHERE id=?",(activity,))
        reserved=self.wakeup.claim_next("host",1002,allow_social=True,
                                        social_activity_id=activity)
        self.assertIsNone(reserved)
        self.assertEqual(self.db.read(
            "SELECT status FROM wakeup_requests WHERE source_id=?",(activity,))[0]["status"],
                         "obsolete")

    def test_idle_discovery_cadence_is_transactional_and_respects_work(self):
        self.assertTrue(self.social.reserve_idle_discovery(1000))
        self.assertFalse(self.social.reserve_idle_discovery(1001))
        self.assertFalse(self.social.reserve_idle_discovery(2500))
        self.assertTrue(self.social.reserve_idle_discovery(2801))
        Queue(self.db).enqueue("urgent","human_action",{"reason":"work"},
                               2802,priority=90)
        self.assertFalse(self.social.reserve_idle_discovery(5000))
        self.assertEqual(self.db.read(
            "SELECT value FROM meta WHERE key='social_discovery_last_at'")[0]["value"],"2801")

    def test_cli_social_wake_is_normal_default_even_with_old_off_flag(self):
        aid=self.add_social("post_cli")
        config_path=self.path.parent/"local.json"
        # Legacy settings are accepted and ignored; outbound writes are
        # not needed to wake Muse for a read-only social decision.
        config_path.write_text(json.dumps({"actor_id":"muse",
          "allow_writes":False,"allow_community_actions":False}),encoding="utf-8")
        capture=io.StringIO()
        with contextlib.redirect_stdout(capture):
            code=main(["--config",str(config_path),"--db",str(self.path),
                       "host-wakeup-prepare","--owner","hook"])
        self.assertEqual(code,0,capture.getvalue())
        receipt=json.loads(capture.getvalue())
        self.assertTrue(receipt["should_wake"])
        self.assertEqual(receipt["source_type"],"social")
        self.assertEqual(receipt["source_id"],aid)
        self.assertFalse(receipt["host_called"])

    def test_idle_cli_runs_by_default_and_throttles_get(self):
        class FakeClient:
            calls=[]
            def __init__(self,*args,**kwargs): pass
            def request(self,method,path):
                self.calls.append((method,path))
                return Response(200,opportunity("post_optional"))
        cfg=self.path.parent/"opt-in.json"
        def run(value):
            cfg.write_text(json.dumps(value),encoding="utf-8")
            buf=io.StringIO()
            with (patch("patrol.components.runtime.composition.cli.Client",FakeClient),
                  patch("patrol.components.runtime.composition.cli.credential_provider",
                        lambda settings: lambda:"stub"),
                  contextlib.redirect_stdout(buf)):
                code=main(["--config",str(cfg),"--db",str(self.path),
                           "social-sync","--if-idle-due"])
            self.assertEqual(code,0,buf.getvalue())
            return json.loads(buf.getvalue())
        self.assertEqual(run({})["new_activities"],1)
        self.assertEqual(len(FakeClient.calls),1)
        self.assertEqual(run({"allow_community_actions":False})["status"],
                         "not_due_or_busy")
        self.assertEqual(len(FakeClient.calls),1)

    def test_idle_discovery_requires_verified_local_agent_before_any_get(self):
        with self.db.transaction() as c:
            c.execute("DELETE FROM meta WHERE key='actor'")
        self.assertFalse(self.social.reserve_idle_discovery(1000))
        self.assertEqual(self.db.read(
            "SELECT count(*) AS n FROM meta WHERE key='social_discovery_last_at'"
        )[0]["n"],0)
        with self.db.transaction() as c:
            c.execute("INSERT INTO meta(key,value) VALUES('actor','muse')")
        self.assertTrue(self.social.reserve_idle_discovery(1000))

    def test_hook_social_by_default_and_remains_a_single_schedule(self):
        hook=Path(__file__).resolve().parents[1]/"tools"/"crewquay_phase3_hook.sh"
        content=hook.read_text(encoding="utf-8")
        self.assertNotIn("CREWQUAY_SOCIAL_IDLE_ENABLED",content)
        self.assertIn("social-sync --if-idle-due",content)
        # Host's native wake() is only best-effort unless a real adapter
        # provides an independent, validated acceptance receipt.
        self.assertNotIn("CREWQUAY_HOST_WAKEUP_WIRED",content)
        self.assertIn("CREWQUAY_HOST_CAPABILITY",content)
        self.assertLess(content.index("CREWQUAY_HOST_CAPABILITY"),
                        content.index("host-wakeup-prepare"))
        self.assertNotIn("--include-social",content)
        self.assertLess(content.index("tick --light"),content.index("social-sync --if-idle-due"))
        self.assertLess(content.index("social-sync --if-idle-due"),
                        content.index("host-wakeup-prepare"))
        self.assertNotIn("wakeup-ack --result accepted",content)
        if os.name=="posix" and shutil.which("bash"):
            syntax=subprocess.run(["bash","-n",str(hook)],capture_output=True,text=True)
            self.assertEqual(syntax.returncode,0,syntax.stderr)

    def test_default_hook_uses_best_effort_without_claiming_host_receipt(self):
        if (os.name!="posix" or not shutil.which("bash") or
                not shutil.which("flock") or not shutil.which("timeout")):
            self.skipTest("reference Hook requires bash, flock and timeout")
        root=Path(__file__).resolve().parents[1]
        hook=root/"tools"/"crewquay_phase3_hook.sh"
        with tempfile.TemporaryDirectory() as temp:
            folder=Path(temp)
            bin_dir=folder/"bin"
            bin_dir.mkdir()
            calls=folder/"mock-calls.txt"
            runtime=folder/"host-runtime.sh"
            runtime.write_text(
                'log() { :; }\n'
                'silent() { printf "%s\\n" "$2"; }\n'
                'wake() { echo SHOULD_NOT_WAKE; }\n',encoding="utf-8")
            mock_python=bin_dir/"python3"
            mock_python.write_text(
                '#!/bin/sh\n'
                'printf "%s\\n" "$*" >> "$MOCK_CALLS"\n'
                'exit 0\n',encoding="utf-8")
            mock_python.chmod(0o755)
            env=dict(os.environ,
                HATCH_HOOK_RUNTIME=str(runtime),
                CREWQUAY_REPO=str(root),
                CREWQUAY_DB=str(self.path),
                CREWQUAY_CONFIG=str(folder/"config.json"),
                CREWQUAY_HOOK_LOCK=str(folder/"hook.lock"),
                MOCK_CALLS=str(calls),
                PATH=str(bin_dir)+os.pathsep+os.environ.get("PATH",""))
            # Best-effort needs no invented Host receipt or operator flag.
            env.pop("CREWQUAY_HOST_CAPABILITY",None)
            result=subprocess.run(["bash",str(hook)],env=env,
                                  capture_output=True,text=True,timeout=12)
            self.assertEqual(result.returncode,0,result.stderr)
            self.assertIn('"status":"idle"',result.stdout)
            commands=calls.read_text(encoding="utf-8").splitlines()
            # Phase 7 bootstraps a deployment run inside the same original
            # Hook, without leasing or faking a Host wake-up.
            self.assertGreaterEqual(len(commands),6,commands)
            self.assertIn("tick --light",commands[0])
            self.assertRegex(commands[1],r"deployment-ensure --repo-sha [0-9a-f]{40}")
            self.assertIn("platform-auth-bootstrap --deployment-run-id",commands[2])
            self.assertIn("--if-due",commands[2])
            self.assertIn("outbox-drain --once --lane any",commands[3])
            self.assertIn("social-sync --if-idle-due",commands[4])
            self.assertTrue(any(
                "host-wakeup-prepare" in x and "--host-capability best_effort" in x
                for x in commands))
            self.assertNotIn("SHOULD_NOT_WAKE",result.stdout)

    def test_old_work_promise_database_migrates_without_losing_lease_history(self):
        old_ddl="""CREATE TABLE wakeup_requests(
            id TEXT PRIMARY KEY,logical_key TEXT NOT NULL UNIQUE,
            source_type TEXT NOT NULL CHECK(source_type IN ('work','promise')),
            source_id TEXT NOT NULL,priority INTEGER NOT NULL CHECK(priority BETWEEN 0 AND 100),
            reason TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',
            created_at REAL NOT NULL,next_attempt_at REAL NOT NULL,
            lease_owner TEXT,lease_version INTEGER NOT NULL DEFAULT 0,
            lease_until REAL,attempts INTEGER NOT NULL DEFAULT 0,
            dispatch_evidence TEXT,confirmed_at REAL)"""
        with self.db.transaction() as c:
            c.execute("DROP TABLE wakeup_requests")
            c.execute(old_ddl)
            c.execute("""INSERT INTO wakeup_requests(
                id,logical_key,source_type,source_id,priority,reason,status,
                created_at,next_attempt_at,lease_owner,lease_version,lease_until,
                attempts,dispatch_evidence,confirmed_at)
                VALUES('old_wake','work:old','work','old',90,'old host action',
                       'unknown',1000,1000,NULL,3,NULL,3,'{"result":"maybe"}',1005)""")
            c.execute("UPDATE meta SET value='1' WHERE key='schema_version'")
        again=Database(self.path)
        rows=again.read("SELECT * FROM wakeup_requests")
        self.assertEqual(len(rows),1)
        self.assertEqual(rows[0]["lease_version"],3)
        self.assertEqual(rows[0]["dispatch_evidence"],'{"result":"maybe"}')
        self.assertEqual(rows[0]["status"],"unknown")
        self.assertEqual(again.read(
           "SELECT value FROM meta WHERE key='schema_version'")[0]["value"],"2")
        with again.transaction() as c:
            c.execute("""INSERT INTO wakeup_requests(
                id,logical_key,source_type,source_id,priority,reason,created_at,next_attempt_at)
                VALUES('new_social','social:stub','social','stub',35,'new',2000,2000)""")
        self.assertEqual(Database(self.path).read(
            "SELECT count(*) n FROM wakeup_requests")[0]["n"],2)


if __name__=="__main__":
    unittest.main()
