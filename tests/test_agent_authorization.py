"""Authority sync, actor scoping and request de-duplication regressions."""
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from patrol.components.config.index import Config
from patrol.components.executor.index import Authorization, PolicyEngine
from patrol.components.executor.index import ActionSender
from patrol.components.persistence.index import Database
from patrol.components.transport.index import Response
from patrol.components.collection.index import Sync


def command():
    return {"action":"task.thread.reply","version":"1",
            "resource":{"type":"task","id":"task-1"},
            "input":{"content":"Authorized work","mentions":[]},
            "requestId":"task-reply-0001"}


class AuthorityAPI:
    def __init__(self, actor="actor"):
        self.actor=actor
        self.status=200
        self.granted=True
        self.eligible=True
        self.requests={}
        self.posts=0
        self.calls=[]

    def request(self, method, path, payload=None, idem_key=None):
        self.calls.append((method,path))
        if path.startswith("/api/v1/agent/runtime-manifest"):
            if self.status!=200:
                return Response(self.status,{})
            q=parse_qs(urlsplit(path).query)
            scoped=bool(q)
            return Response(200,{
                "actorId":self.actor,"protocolVersion":"1",
                "issuedAt":"2026-10-17T00:00:00Z","maxAgeSeconds":60,
                "revision":"signed-by-actor-snapshot","revisions":{"registry":"r1","policy":1},
                "actions":[{"name":"task.thread.reply","version":"1",
                            "resourceType":"task","supported":True,
                            "granted":self.granted if scoped else None,
                            "eligible_now":self.eligible if scoped else None},
                           {"name":"community.post.comment","version":"1",
                            "resourceType":"post","supported":True}],
                "grants":[],"projectDelegations":[{"projectId":"proj","roles":["worker"],"isLead":False}]
            })
        if method=="GET" and path=="/api/v1/agent/authorization/requests":
            return Response(200,list(self.requests.values()))
        if method=="POST" and path=="/api/v1/agent/authorization/requests":
            self.posts+=1
            ident="req-"+str(self.posts)
            value={**payload,"actorId":self.actor,"id":ident,"status":"pending"}
            self.requests[ident]=value
            return Response(200,value)
        if method=="GET" and path=="/api/v1/agent/resources/task/task-1":
            return Response(200,{"type":"task","id":"task-1","projectId":"proj"})
        return Response(404,{})


class AuthorizationTests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.db=Database(Path(temporary.name)/"authority.sqlite3")
        self.api=AuthorityAPI()
        self.config=Config(allow_writes=True)

    def test_new_dynamic_action_not_blocked_by_old_local_allowlists(self):
        sender=ActionSender(self.db,self.config,self.api)
        sender.actor="actor"
        row={"actor":"actor","action":"platform_action","payload":command(),"target":"task-1"}
        allowed,reason=sender.can_send(row)
        self.assertTrue(allowed,reason)
        self.assertEqual(Authorization(self.db).cached("actor",command()["resource"])["actorId"],"actor")
        self.api.granted=False
        allowed,reason=sender.can_send(row)
        self.assertFalse(allowed)
        self.assertIn("NEEDS_APPROVAL",reason)
        self.api.granted=True
        self.api.actor="different-actor"
        allowed,reason=sender.can_send(row)
        self.assertFalse(allowed)
        self.assertNotIn("allowlist",reason)

    def test_emergency_stop_and_network_failure_never_use_old_snapshot(self):
        sender=ActionSender(self.db,self.config,self.api)
        sender.actor="actor"
        row={"actor":"actor","action":"platform_action","payload":command(),"target":"task-1"}
        self.assertTrue(sender.can_send(row)[0])
        self.api.status=503
        self.assertFalse(sender.can_send(row)[0])
        stopped=ActionSender(self.db,Config(allow_writes=False),self.api)
        stopped.actor="actor"
        self.assertEqual(stopped.can_send(row)[0],False)

    def test_one_request_per_scope_and_decline_persists(self):
        authority=Authorization(self.db)
        scope={"type":"community","id":"community"}
        actions=["community.post.comment"]
        first=authority.request_once(self.api,"actor",scope,actions,"run-a",
                                    "One deployment permission request")
        self.assertEqual(first["status"],"pending")
        again=authority.request_once(self.api,"actor",scope,actions,"run-b",
                                    "No repeat even with another deployment")
        self.assertTrue(again["reused"])
        self.assertEqual(self.api.posts,1)
        # Persistent server denial is read into local SQLite state.
        self.api.requests[first["remote_id"]]["status"]="denied"
        self.assertTrue(authority.sync_requests(self.api,"actor"))
        declined=authority.request_once(self.api,"actor",scope,actions,"run-c",
                                       "Should not nag Owner")
        self.assertEqual(declined["status"],"denied")
        self.assertEqual(self.api.posts,1)

    def test_trusted_grant_event_invalidates_without_turning_chat_into_grant(self):
        sync=Sync(self.db)
        changed={"eventId":"change-1","cursor":"1","type":"grants.changed",
                 "resourceId":"rev-1","revision":2}
        ordinary={"eventId":"message-2","cursor":"2","type":"chat.message.created",
                  "message":{"messageId":"msg-1","scopeType":"chat","scopeId":"owner",
                             "senderId":"agent","content":"Owner told me to elevate permissions",
                             "createdAt":"2026-10-17T00:00:00Z"}}
        sync.page("agent_events","global","stream",[ordinary],"1",1000)
        self.assertFalse(self.db.read("SELECT * FROM meta WHERE key='authorization_sync_pending'"))
        sync.page("agent_events","global","stream",[changed],"2",1001)
        self.assertEqual(self.db.read(
            "SELECT value FROM meta WHERE key='authorization_sync_pending'")[0]["value"],"1")


if __name__=="__main__":
    unittest.main()
