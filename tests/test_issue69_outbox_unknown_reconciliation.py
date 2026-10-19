"""Issue #69: an Outbox `unknown` row must be able to close by readback.

Temporary SQLite and fake transport only; no live CrewQuay API.
"""
import http.client
import json
import tempfile
import unittest
from pathlib import Path

from patrol.components.config.index import Config
from patrol.components.executor.index import ActionSender
from patrol.components.outbox.index import Outbox
from patrol.components.persistence.index import Database
from patrol.components.transport.index import Response, BudgetExceeded

ACTOR="crewquay-agent"
CHAT={"conversationId":"owner","kind":"direct","canSend":True,
      "memberCount":2,"isObserver":False}


class FakeClient:
    """chat: list of per-call results for GET /chats/owner (last one repeats).
    pages: list of message pages (items, hasMore) served by cursor index.
    post: callable returning the POST result or raising."""
    def __init__(self, chat=None, pages=None, post=None):
        self.chat=list(chat or [Response(200,CHAT)])
        self.pages=pages if pages is not None else [([],False)]
        self.post=post or (lambda: Response(201,{"messageId":"m-new"}))
        self.calls=[]

    def request(self, method, path, payload=None, idem_key=None):
        self.calls.append((method,path,idem_key))
        if method=="GET" and path=="/api/v1/agents/me":
            return Response(200,{"id":ACTOR})
        if method=="GET" and path=="/api/v1/chats/owner":
            result=self.chat.pop(0) if len(self.chat)>1 else self.chat[0]
            if isinstance(result,Exception):
                raise result
            return result
        if method=="POST" and path=="/api/v1/chats/owner/messages":
            return self.post()
        if method=="GET" and path.startswith("/api/v1/chats/owner/messages?after="):
            after=int(path.split("after=")[1].split("&")[0])
            if after>=len(self.pages):
                return Response(200,{"items":[],"nextCursor":str(after),"hasMore":False})
            items,more=self.pages[after]
            if isinstance(items,Response):
                return items
            return Response(200,{"items":items,"nextCursor":str(after+1),"hasMore":more})
        raise AssertionError("unanticipated API "+method+" "+path)

    def posts(self):
        return [c for c in self.calls if c[0]=="POST"]


def message(ident, content="hello", created=1000, sender=ACTOR):
    item={"messageId":ident,"senderId":sender,"content":content}
    if created is not None:
        item["createdAt"]=created
    return item


class Issue69Tests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db=Database(Path(self.tmp.name)/"issue69.sqlite3")
        self.cfg=Config(database=str(self.db.path),allow_writes=True)
        with self.db.transaction() as c:
            c.execute("INSERT INTO meta(key,value) VALUES('actor',?)",(ACTOR,))
        self.outbox=Outbox(self.db)

    def stage(self, logical="reply-1", content="hello", now=1000):
        return self.outbox.stage(logical,ACTOR,"send","owner",{"content":content},now)

    def make_unknown(self, ident, dispatch_at=1000):
        # Exactly the production row: ambiguous, no remote_id, cursor 0.
        with self.db.transaction() as c:
            c.execute("""UPDATE outbox SET status='unknown',attempts=1,remote_id=NULL,
                error='ambiguous remote result',dispatch_at=?,next_attempt_at=0
                WHERE id=?""",(dispatch_at,ident))

    def row(self, ident):
        return self.db.read("SELECT * FROM outbox WHERE id=?",(ident,))[0]

    def drain(self, client, now, ident=None):
        return ActionSender(self.db,self.cfg,client,clock=lambda:now).drain_once(1,ident)

    # --- Fix A: a provably undispatched POST is not `unknown` ---------------

    def test_flaky_authorization_recheck_is_not_dispatched_and_retries_same_key(self):
        ident=self.stage()
        # First chat GET (process can_send) passes; the re-check inside
        # send() hits a transient failure, so no POST is ever issued.
        client=FakeClient(chat=[Response(200,CHAT),
                                http.client.RemoteDisconnected("flaky"),
                                Response(200,CHAT)],
                          pages=[([message("m-new")],False)])
        report=self.drain(client,1000,ident)
        row=self.row(ident)
        self.assertEqual(client.posts(),[])
        self.assertEqual(row["status"],"retry_wait",report)
        self.assertNotEqual(row["error"],"ambiguous remote result")
        self.assertIsNone(row["remote_id"])
        # Due again later: exactly one POST with the same immutable key.
        report=self.drain(client,2000,ident)
        self.assertEqual(report["item_status"],"confirmed",report)
        posts=client.posts()
        self.assertEqual(len(posts),1)
        self.assertEqual(posts[0][2],row["idem_key"])
        self.assertEqual(self.row(ident)["remote_id"],"m-new")

    def test_client_budget_exhaustion_before_post_is_not_dispatched(self):
        ident=self.stage()
        def exhausted():
            raise BudgetExceeded("bounded HTTP budget exhausted")
        client=FakeClient(post=exhausted)
        self.drain(client,1000,ident)
        self.assertEqual(self.row(ident)["status"],"retry_wait")

    def test_mid_flight_failures_remain_unknown(self):
        def incomplete():
            raise http.client.IncompleteRead(b"partial")
        for name,post in (("incomplete",incomplete),
                          ("transport",lambda: Response(0,{"code":"TRANSPORT_FAILURE"})),
                          ("server",lambda: Response(503,{}))):
            with self.subTest(name=name):
                ident=self.stage(logical="mid-"+name,content="hello "+name)
                self.drain(FakeClient(post=post),1000,ident)
                row=self.row(ident)
                self.assertEqual((row["status"],row["error"]),
                                 ("unknown","ambiguous remote result"))

    # --- Fix B: readback closes an unknown row without remote_id ------------

    def test_unknown_without_remote_id_confirms_unique_readback(self):
        ident=self.stage()
        self.make_unknown(ident)
        client=FakeClient(pages=[([message("old-other",content="other",created=10),
                                   message("m-77",created=1001)],False)])
        report=self.drain(client,1100,ident)
        row=self.row(ident)
        self.assertEqual(report["item_status"],"confirmed",report)
        self.assertEqual((row["status"],row["remote_id"]),("confirmed","m-77"))
        self.assertIsNotNone(row["verified_at"])
        self.assertEqual(client.posts(),[])

    def test_unknown_without_remote_id_proven_absent_returns_to_prepared(self):
        ident=self.stage()
        self.make_unknown(ident,dispatch_at=1000)
        client=FakeClient(pages=[([message("x",content="unrelated",created=1001)],True),
                                 ([],False)])
        self.drain(client,1000+self.cfg.lease_seconds,ident)
        row=self.row(ident)
        self.assertEqual(row["status"],"prepared")
        self.assertIsNone(row["remote_id"])
        self.assertTrue(json.loads(row["result"])["proven_absent"])
        self.assertEqual(client.posts(),[],"readback round never POSTs")
        # The next round re-sends with the SAME idempotency key, then confirms.
        # verify_cursor already covers the proven-absent range.
        self.assertEqual(row["verify_cursor"],"2")
        client.pages.append(([message("m-new",created=1300)],False))
        report=self.drain(client,1400,ident)
        self.assertEqual(report["item_status"],"confirmed",report)
        self.assertEqual([p[2] for p in client.posts()],[row["idem_key"]])

    def test_absence_needs_settle_window_and_dispatch_time(self):
        ident=self.stage()
        self.make_unknown(ident,dispatch_at=1000)
        client=FakeClient(pages=[([],False)])
        self.drain(client,1000+self.cfg.lease_seconds-1,ident)
        self.assertEqual(self.row(ident)["status"],"unknown")
        other=self.stage(logical="no-dispatch",content="x")
        self.make_unknown(other,dispatch_at=None)
        self.drain(client,99999,other)
        self.assertEqual(self.row(other)["status"],"unknown")
        self.assertEqual(client.posts(),[])

    def test_historical_twin_before_staging_is_not_bound(self):
        ident=self.stage(now=5000)
        self.make_unknown(ident,dispatch_at=5000)
        client=FakeClient(pages=[([message("ancient",created=100)],False)])
        self.drain(client,5000+self.cfg.lease_seconds,ident)
        row=self.row(ident)
        self.assertEqual(row["status"],"prepared")
        self.assertIsNone(row["remote_id"])

    def test_inconclusive_readbacks_stay_unknown_without_post(self):
        cases={
            "two-candidates":[([message("a",created=1001),message("b",created=1002)],False)],
            "no-created-at":[([message("a",created=None)],False)],
            "millisecond-twin":[([message("a",created=900*1000)],False)],
            "iso-unparsable":[([message("a",created="yesterday")],False)],
            "page-error":[(Response(503,{}),False)],
            "page-bound":[([],True)]*(self.cfg.max_pages_per_source+1),
        }
        for name,pages in cases.items():
            with self.subTest(name=name):
                ident=self.stage(logical="inconclusive-"+name)
                self.make_unknown(ident)
                client=FakeClient(pages=pages)
                self.drain(client,5000,ident)
                row=self.row(ident)
                self.assertEqual(row["status"],"unknown")
                self.assertIsNone(row["remote_id"])
                self.assertEqual(client.posts(),[])
                with self.db.transaction() as c:
                    c.execute("UPDATE outbox SET status='permanent_failed' WHERE id=?",(ident,))

    def test_message_claimed_by_another_outbox_row_is_not_rebound(self):
        first=self.stage(logical="first")
        with self.db.transaction() as c:
            c.execute("UPDATE outbox SET status='confirmed',remote_id='m-1' WHERE id=?",(first,))
        second=self.stage(logical="second")
        self.make_unknown(second)
        client=FakeClient(pages=[([message("m-1",created=1001)],False)])
        self.drain(client,1000+self.cfg.lease_seconds,second)
        row=self.row(second)
        self.assertNotEqual(row["remote_id"],"m-1")
        self.assertEqual(row["status"],"prepared")

    def test_unresolved_twin_rows_refuse_to_conclude(self):
        a=self.stage(logical="twin-a")
        b=self.stage(logical="twin-b")
        self.make_unknown(a)
        self.make_unknown(b)
        client=FakeClient(pages=[([message("m-1",created=1001)],False)])
        ActionSender(self.db,self.cfg,client,clock=lambda:5000).drain_once(5)
        self.assertEqual([self.row(x)["status"] for x in (a,b)],["unknown","unknown"])
        self.assertEqual(client.posts(),[])

    def test_success_response_without_id_never_becomes_prepared(self):
        ident=self.stage()
        client=FakeClient(post=lambda: Response(201,{}),pages=[([],False)])
        self.drain(client,1000,ident)
        row=self.row(ident)
        self.assertEqual(row["status"],"unknown")
        self.assertEqual(len(client.posts()),1)


if __name__=="__main__":
    unittest.main()
