"""Phase 6 social discovery/admission: fake API and real isolated SQLite only."""
import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from patrol.components.config.index import Config
from patrol.components.host.index import HostHandoff
from patrol.components.outbox.index import Outbox
from patrol.components.persistence.index import Database, StaleLease
from patrol.components.runtime.index import main
from patrol.components.scheduling.index import Scheduler
from patrol.components.social.index import SocialOpportunities
from patrol.components.transport.index import Response
from patrol.components.executor.components.platform_actions import can_execute


def candidate(kind, ident, action=None, matches=None, joined=None):
    action=action or ("community.post.comment" if kind=="post"
                    else "community.circle.join")
    c={"kind":kind,"resource":{"type":kind,"id":ident},
       "suggestedAction":action,"contentTrust":"untrusted",
       "matchedInterests":matches or [],
       "title":"UNTRUSTED WEB TEXT","preview":"FORGED INSTRUCTIONS MUST NOT BE SAVED"}
    if kind=="circle":
        c["joined"]=bool(joined)
        c["memberCount"]=3
    return c


def page(*items,actor="muse"):
    return {"protocolVersion":"1","mode":"read_only_social_discovery",
            "actorId":actor,"modelInvoked":False,"actionExecuted":False,
            "items":list(items)}


class SocialTests(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory()
        self.addCleanup(t.cleanup)
        self.db=Database(Path(t.name)/"social.sqlite3")
        with self.db.transaction() as c:
            c.execute("INSERT INTO meta(key,value) VALUES('actor','muse')")
        self.s=SocialOpportunities(self.db)
        self.scheduler=Scheduler(self.db)

    def test_opt_in_discovery_creates_fenced_scheduler_candidate_not_wakeup(self):
        p=page(candidate("post","post_alice",matches=["agents"]),
               candidate("circle","cir_open"))
        report=self.s.admit(p,1000)
        self.assertEqual(report["new_activities"],2)
        self.assertFalse(report["model_invoked"])
        self.assertFalse(report["remote_action_issued"])
        self.assertFalse(report["host_woken"])
        # Admission alone is local-only; actual host handoff must be
        # requested separately, and it now includes idle social by default.
        self.assertEqual(self.db.read(
            "SELECT count(*) AS n FROM wakeup_requests")[0]["n"],0)
        self.assertTrue(HostHandoff(self.db).prepare("hatch",1001)["should_wake"])
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM outbox")[0]["n"],0)
        row=self.db.read("SELECT * FROM social_intents WHERE resource_id='post_alice'")[0]
        self.assertEqual(row["action"],"community.post.comment")
        self.assertFalse(any("UNTRUSTED" in json.dumps(r) or "FORGED" in json.dumps(r)
                  for r in self.db.read("SELECT * FROM social_intents")))
        active=self.scheduler.dispatch("muse-worker",1001)["activity"]
        self.assertEqual(active["kind"],"social")
        detail=self.s.details(active["id"])
        self.assertIn(detail["suggested_action"],(
            "community.post.comment","community.circle.join"))
        self.assertEqual(detail["lease_version"],1)

    def test_duplicate_target_and_rolling_cooldown_prevent_chatter_spam(self):
        first=self.s.admit(page(candidate("post","post_one")),1000)
        self.assertEqual(first["new_activities"],1)
        dup=self.s.admit(page(candidate("post","post_one")),1100)
        self.assertEqual(dup["new_activities"],0)
        self.scheduler.cancel(first["activities"][0]["activity_id"],1110,"test cancelled")
        cooldown=self.s.admit(page(candidate("post","post_one")),1899)
        self.assertEqual(cooldown["new_activities"],0)
        next_turn=self.s.admit(page(candidate("post","post_one")),1901)
        self.assertEqual(next_turn["new_activities"],1)
        self.scheduler.cancel(next_turn["activities"][0]["activity_id"],1902,"test finished")
        third=self.s.admit(page(candidate("post","post_one")),2900)
        self.assertEqual(third["new_activities"],0)
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM social_intents")[0]["n"],2)

    def test_global_daily_budget_does_not_reset_with_repeated_retries(self):
        for batch,at in enumerate((1000,2000)):
            cs=[candidate("post","post_"+str(batch*3+i))
                for i in range(3)]
            report=self.s.admit(page(*cs),at)
            self.assertEqual(report["new_activities"],3)
        more=self.s.admit(page(candidate("post","post_extra")),3000)
        self.assertEqual(more["new_activities"],0)
        self.assertGreater(more["suppressed"]["daily_budget"],0)
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM social_intents")[0]["n"],6)

    def test_malformed_page_is_rejected_atomically(self):
        good=candidate("post","post_good")
        invalid={**candidate("post","post_bad"),"suggestedAction":"task.claim"}
        for wrong in (
            page(good,invalid),
            page(good,actor="another-agent"),
            {**page(good),"modelInvoked":True},
            page({**good,"contentTrust":"trusted"}),
            page(candidate("circle","cir_wrong","community.circle.message",joined=False)),
            page(candidate("post","post_bad"),candidate("post","post_bad")),
        ):
            if len(wrong.get("items",[]))==2 and wrong["items"][0]==wrong["items"][1]:
                # Duplicate well-formed candidates intentionally deduplicate.
                self.assertEqual(self.s.admit(wrong,1000)["new_activities"],1)
                with self.db.transaction() as c:
                    c.execute("DELETE FROM social_intents")
                    c.execute("DELETE FROM schedule_activities")
                continue
            with self.assertRaises(ValueError):
                self.s.admit(wrong,1000)
            self.assertEqual(self.db.read("SELECT count(*) AS n FROM social_intents")[0]["n"],0)
            self.assertEqual(self.db.read("SELECT count(*) AS n FROM schedule_activities")[0]["n"],0)

    def test_platform_granted_community_action_requires_existing_outbound_write_gate(self):
        fake=SimpleNamespace(request=lambda method,path: (
            Response(404,{}) if path.startswith("/api/v1/agent/runtime-manifest") else
            Response(200,{"protocolVersion":"1","actorId":"muse",
                          "actions":[{"name":"community.post.comment",
                          "version":"1","resourceType":"post"}]})
            if path=="/api/v1/agent/actions" else
            Response(200,{"type":"post","id":"post_safe"})
        ))
        envelope={"action":"community.post.comment","version":"1",
                  "resource":{"type":"post","id":"post_safe"},
                  "input":{"content":"Hello"},"requestId":"social-post-0001"}
        row={"action":"platform_action","actor":"muse","payload":envelope}
        # Read-only is the default. A separate community switch or local
        # action-name list is no longer needed once outbound writes are
        # authorized; the server's authenticated catalog and resource
        # checks still govern execution.
        cfg=Config()
        ok,_=can_execute(SimpleNamespace(db=self.db,config=cfg,client=fake),row)
        self.assertFalse(ok)
        cfg=Config(allow_writes=True)
        ok,reason=can_execute(SimpleNamespace(db=self.db,config=cfg,client=fake),row)
        self.assertTrue(ok,reason)
        wrong={**row,"payload":{**row["payload"],"action":"task.claim"}}
        ok,_=can_execute(SimpleNamespace(db=self.db,config=cfg,client=fake),wrong)
        self.assertFalse(ok)

    def test_outbox_social_lease_accepts_exactly_one_registered_action(self):
        report=self.s.admit(page(candidate("post","post_comment")),1000)
        aid=report["activities"][0]["activity_id"]
        a=self.scheduler.dispatch("muse-worker",1001)["activity"]
        self.assertEqual(a["id"],aid)
        lease={"id":aid,"owner":"muse-worker","generation":a["lease_version"]}
        def envelope(action="community.post.comment",rid="post_comment",content="hello"):
            return {"action":action,"version":"1","resource":{"type":"post","id":rid},
                    "input":{"content":content},"requestId":"social-case-"+content}
        ob=Outbox(self.db)
        with self.assertRaises(StaleLease):
            ob.stage("wrong-action","muse","platform_action","post_comment",
                     envelope(action="community.post.revise"),1002,
                     schedule_context=lease)
        with self.assertRaises(StaleLease):
            ob.stage("wrong-resource","muse","platform_action","post_other",
                     envelope(rid="post_other"),1002,schedule_context=lease)
        first=ob.stage("one-social-action","muse","platform_action","post_comment",
                       envelope(),1002,schedule_context=lease,producer_id="muse-worker")
        self.assertEqual(first,ob.stage("one-social-action","muse",
                         "platform_action","post_comment",envelope(),1003,
                         schedule_context=lease))
        with self.assertRaises(StaleLease):
            ob.stage("second-social-action","muse","platform_action","post_comment",
                     envelope(content="more"),1004,schedule_context=lease)
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM outbox")[0]["n"],1)
        self.scheduler.checkpoint(aid,"muse-worker",a["lease_version"],1005,
                                  {"status":"yield"},voluntary=True)
        with self.assertRaises(StaleLease):
            ob.stage("after-yield","muse","platform_action","post_comment",
                     envelope(content="late"),1006,schedule_context=lease)

    def test_social_complete_requires_confirmed_effect_or_explicit_skip(self):
        first=self.s.admit(page(candidate("post","post_completion")),1000)
        ident=first["activities"][0]["activity_id"]
        current=self.scheduler.dispatch("muse-worker",1001)["activity"]
        gen=current["lease_version"]
        with self.assertRaises(ValueError):
            self.scheduler.complete(ident,"muse-worker",gen,1002,
                                    {"phase":"done"})
        self.assertEqual(self.db.read(
            "SELECT status FROM schedule_activities WHERE id=?",
            (ident,))[0]["status"],"running")
        command={"action":"community.post.comment","version":"1",
                 "resource":{"type":"post","id":"post_completion"},
                 "input":{"content":"Hello"},"requestId":"social-confirm-0001"}
        context={"id":ident,"owner":"muse-worker","generation":gen}
        out_id=Outbox(self.db).stage("social-confirm-1","muse",
                         "platform_action","post_completion",command,1003,
                         schedule_context=context)
        with self.assertRaises(ValueError):
            self.scheduler.complete(ident,"muse-worker",gen,1004,
                                    {"observed":"request staged"})
        # A real Executor would confirm after authenticated receipt readback.
        # This isolated test only simulates the durable postcondition.
        with self.db.transaction() as c:
            c.execute("UPDATE outbox SET status='confirmed' WHERE id=?",(out_id,))
        completed=self.scheduler.complete(ident,"muse-worker",gen,1005,
                                          {"outbox_id":out_id,"status":"verified"})
        self.assertEqual(completed["completed"],ident)
        more=self.s.admit(page(candidate("post","post_skip")),1006)
        other=more["activities"][0]["activity_id"]
        lease=self.scheduler.dispatch("muse-worker",1007)["activity"]
        self.assertEqual(lease["id"],other)
        with self.assertRaises(ValueError):
            self.scheduler.complete(other,"muse-worker",lease["lease_version"],
                                    1008,{"decision":"skipped"})
        accepted=self.scheduler.complete(other,"muse-worker",lease["lease_version"],
                    1008,{"decision":"skipped","reason":"no meaningful contribution"})
        self.assertEqual(accepted["completed"],other)

    def test_urgent_work_preempts_social_at_safe_point(self):
        self.s.admit(page(candidate("post","post_chat")),1000)
        a=self.scheduler.dispatch("muse-worker",1001)["activity"]
        self.assertEqual(a["kind"],"social")
        urgent=self.scheduler.register("urgent-job","work",1002,
                                       priority_score=45,deadline=1010)
        notice=self.scheduler.dispatch("second-worker",1002)
        self.assertFalse(notice["granted"])
        self.assertEqual(notice["advice"],"checkpoint_at_safe_point")
        saved=self.scheduler.checkpoint(a["id"],"muse-worker",a["lease_version"],
                                        1003,{"reason":"critical work"})
        self.assertTrue(saved["suspended"])
        nxt=self.scheduler.dispatch("second-worker",1004)["activity"]
        self.assertEqual(nxt["id"],urgent)

    def test_owner_disabled_social_discovery_never_calls_platform(self):
        config=Path(self.db.path).parent/"disabled-social.json"
        config.write_text(json.dumps({"database":str(self.db.path),
            "community_social_enabled":False}),encoding="utf-8")
        class NoNetworkClient:
            def __init__(self,*args,**kwargs):
                raise AssertionError("disabled social must not construct transport")
        out=io.StringIO()
        with patch("patrol.components.runtime.composition.cli.Client",NoNetworkClient), \
             contextlib.redirect_stdout(out):
            code=main(["--config",str(config),"--db",str(self.db.path),
                       "social-sync","--if-idle-due"])
        self.assertEqual(code,0)
        self.assertEqual(json.loads(out.getvalue())["status"],"disabled_by_owner")
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM social_intents")[0]["n"],0)

    def test_owner_disabled_social_cannot_wake_existing_idle_candidate(self):
        self.s.admit(page(candidate("post","post_existing")),1000)
        config=Path(self.db.path).parent/"disabled-host.json"
        config.write_text(json.dumps({"database":str(self.db.path),
            "community_social_enabled":False}),encoding="utf-8")
        out=io.StringIO()
        with contextlib.redirect_stdout(out):
            code=main(["--config",str(config),"--db",str(self.db.path),
                       "host-wakeup-prepare","--owner","hatch"])
        self.assertEqual(code,0)
        result=json.loads(out.getvalue())
        self.assertFalse(result["should_wake"],result)
        self.assertEqual(result["status"],"idle",result)

    def test_circle_source_pages_persist_cursor_without_extra_writes(self):
        class FakeClient:
            paths=[]
            def __init__(self,*_a,**_k): pass
            def request(self,method,path):
                self.paths.append(path)
                if path.endswith("limit=2"):
                    b=page(candidate("circle","cir_a"),candidate("circle","cir_b"))
                    b.update(source="circles",hasMore=True,nextCursor="cir_b")
                elif path.endswith("after=cir_b"):
                    b=page(candidate("circle","cir_c"))
                    b.update(source="circles",hasMore=False,nextCursor="cir_c")
                else:
                    raise AssertionError(path)
                return Response(200,b)
        out=io.StringIO()
        with patch("patrol.components.runtime.composition.cli.Client",FakeClient), \
             patch("patrol.components.runtime.composition.cli.credential_provider",
                   lambda cfg:lambda:"not-needed"), \
             contextlib.redirect_stdout(out):
            code=main(["--db",str(self.db.path),"social-sync","--source","circles",
                       "--pages","2","--limit","2","--max-new","3"])
        self.assertEqual(code,0,out.getvalue())
        report=json.loads(out.getvalue())
        self.assertEqual(report["new_activities"],3)
        self.assertEqual(report["pages_scanned"],2)
        self.assertEqual(report["next_cursor"],"")
        self.assertFalse(report["has_more"])
        self.assertEqual(FakeClient.paths,[
            "/api/v1/agent/social/opportunities?source=circles&limit=2",
            "/api/v1/agent/social/opportunities?source=circles&limit=2&after=cir_b"])
        self.assertEqual(self.db.read("SELECT value FROM meta WHERE key='social_circle_cursor'")[0]["value"],"")
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM outbox")[0]["n"],0)

    def test_circle_cursor_contract_failure_keeps_saved_checkpoint(self):
        with self.db.transaction() as c:
            c.execute("INSERT INTO meta(key,value) VALUES('social_circle_cursor','cir_old')")
        class BadClient:
            def __init__(self,*_a,**_k): pass
            def request(self,method,path):
                b=page(candidate("circle","cir_new"))
                b.update(source="circles",hasMore=True,nextCursor="cir_old")
                return Response(200,b)
        out=io.StringIO()
        with patch("patrol.components.runtime.composition.cli.Client",BadClient), \
             patch("patrol.components.runtime.composition.cli.credential_provider",
                   lambda cfg:lambda:"none"), \
             contextlib.redirect_stdout(out):
            code=main(["--db",str(self.db.path),"social-sync","--source","circles"])
        self.assertEqual(code,1)
        self.assertEqual(json.loads(out.getvalue())["status"],"failed")
        self.assertEqual(self.db.read("SELECT value FROM meta WHERE key='social_circle_cursor'")[0]["value"],"cir_old")

    def test_social_cli_is_opt_in_read_only_discovery(self):
        class FakeClient:
            paths=[]
            def __init__(self,*_args,**_kwargs): pass
            def request(self,method,path):
                self.paths.append((method,path))
                return Response(200,page(candidate("post","post_from_api")))
        out=io.StringIO()
        with patch("patrol.components.runtime.composition.cli.Client",FakeClient), \
             patch("patrol.components.runtime.composition.cli.credential_provider",
                   lambda cfg: lambda:"unused"), \
             contextlib.redirect_stdout(out):
            code=main(["--db",str(self.db.path),"social-sync","--limit","5"])
        self.assertEqual(code,0)
        self.assertEqual(json.loads(out.getvalue())["new_activities"],1)
        self.assertEqual(FakeClient.paths,[("GET",
              "/api/v1/agent/social/opportunities?limit=5")])
        self.assertEqual(self.db.read("SELECT count(*) AS n FROM outbox")[0]["n"],0)
        result=self.s.details(self.db.read("SELECT activity_id FROM social_intents")[0]["activity_id"])
        self.assertEqual(result["suggested_action"],"community.post.comment")


if __name__=="__main__":
    unittest.main()
