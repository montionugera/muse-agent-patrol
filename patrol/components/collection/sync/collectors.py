import hashlib
import json
from datetime import datetime
from urllib.parse import quote, urlencode
from ...persistence.index import canonical, cursor
from ...queue.index import enqueue_tx, baseline_tx, receipt_tx
from ...transport.index import BudgetExceeded, AuthError


def event_time(value, fallback):
    if isinstance(value,(int,float)) and not isinstance(value,bool):
        return float(value)
    if isinstance(value,str):
        try:
            dt=datetime.fromisoformat(value.replace("Z","+00:00"))
            if dt.tzinfo is not None:
                return dt.timestamp()
        except ValueError:
            pass
    return fallback


class Sync:
    def __init__(self, db, max_event_age_seconds=900, future_skew_seconds=60):
        self.db=db
        self.max_event_age_seconds=max_event_age_seconds
        self.future_skew_seconds=future_skew_seconds

    def state(self, source, scope):
        rows=self.db.read("SELECT * FROM sync_cursors WHERE source=? AND scope=?",(source,scope))
        return rows[0] if rows else {"source":source,"scope":scope,"stream":"","received":"0","handled":"0","target":None,"next_at":0,"failures":0,"covered_at":0}

    def page(self, source, scope, stream, items, next_cursor, now, target=None, next_at=None):
        next_cursor=cursor(next_cursor)
        with self.db.transaction() as c:
            old=c.execute("SELECT * FROM sync_cursors WHERE source=? AND scope=?",(source,scope)).fetchone()
            if old and old["stream"]!=stream:
                # A failed first request may have persisted only retry metadata.
                # No stream or cursor has been committed in that case, so the
                # first valid Agent feed page may establish its stream safely.
                uninitialized_agent_feed=(source=="agent_events" and
                    old["stream"]=="" and old["received"]=="0" and
                    old["handled"]=="0" and old["target"] is None)
                if not uninitialized_agent_feed:
                    raise ValueError("stream reset requires a successful resource resync")
            if old and int(next_cursor)<int(old["received"]):
                raise ValueError("stale page cannot regress cursor")
            for item in items:
                if not isinstance(item,dict):
                    raise ValueError("invalid event object")
                external=item.get("eventId") or item.get("messageId") or item.get("seq") or item.get("updateSeq")
                if external is None:
                    external=hashlib.sha256(canonical(item).encode()).hexdigest()
                # Snapshot status/version changes get distinct keys.
                if source in ("tasks","projects"):
                    external=str(item.get("id",""))+":"+hashlib.sha256(canonical(item).encode()).hexdigest()
                ec=item.get("cursor")
                if ec is not None:
                    ec=cursor(str(ec))
                c.execute("""INSERT INTO events(source,scope,stream,external_key,cursor,payload,observed_at)
                    VALUES(?,?,?,?,?,?,?) ON CONFLICT(source,scope,stream,external_key) DO NOTHING""",
                    (source,scope,stream,str(external),ec,canonical(item),now))
                row=c.execute("SELECT id,payload,observed_at FROM events WHERE source=? AND scope=? AND stream=? AND external_key=?",
                              (source,scope,stream,str(external))).fetchone()
                # First receipt and its event/cursor/inspection are one transaction.
                # Always use the original committed event on a replay: never
                # refresh eligibility because a duplicate arrived later.
                if source=="messages":
                    original=json.loads(row["payload"])
                    receipt_tx(c,row["id"],original,row["observed_at"],
                               self.max_event_age_seconds,self.future_skew_seconds)
                    score=baseline_tx(c,row["id"],original,row["observed_at"])
                elif source=="agent_events":
                    # Authorization events are trusted only as invalidation
                    # triggers. The subsequent manifest is re-authenticated.
                    if item.get("type") in ("grants.changed","capabilities.changed",
                                            "authorization.decided","authorization.revoked"):
                        c.execute("""INSERT INTO meta(key,value)
                            VALUES('authorization_sync_pending','1')
                            ON CONFLICT(key) DO UPDATE SET value='1'""")
                    score=60
                    # A normalized message accompanies a server-visible event.
                    # Persist its first receipt, baseline, and inspector within
                    # the same cursor-advance transaction: no crash gap.
                    # A replay with the same event ID must never replace the
                    # first authorized message snapshot or create a new one.
                    msg=json.loads(row["payload"]).get("message")
                    if isinstance(msg,dict):
                        kind=msg.get("scopeType")
                        scope_id=msg.get("scopeId")
                        mid=msg.get("messageId")
                        if (kind not in ("chat","room","project","task")
                                or not isinstance(scope_id,str) or not scope_id
                                or not isinstance(mid,str) or not mid
                                or not isinstance(msg.get("senderId"),str)
                                or not isinstance(msg.get("content"),str)
                                or not isinstance(msg.get("createdAt"),str)):
                            raise ValueError("invalid authorized event message")
                        mscope=(scope_id if kind in ("chat","room")
                                else kind+":"+scope_id)
                        normalized={"messageId":mid,"senderId":msg["senderId"],
                                    "senderKind":msg.get("senderKind","agent"),
                                    "content":msg["content"],"createdAt":msg["createdAt"],
                                    "scopeType":kind,"scopeId":scope_id}
                        c.execute("""INSERT INTO events(source,scope,stream,external_key,cursor,payload,observed_at)
                            VALUES('messages',?,'',?,NULL,?,?)
                            ON CONFLICT(source,scope,stream,external_key) DO NOTHING""",
                            (mscope,mid,canonical(normalized),now))
                        mrow=c.execute("""SELECT id,payload,observed_at FROM events
                            WHERE source='messages' AND scope=? AND stream='' AND external_key=?""",
                            (mscope,mid)).fetchone()
                        original=json.loads(mrow["payload"])
                        receipt_tx(c,mrow["id"],original,mrow["observed_at"],
                                   self.max_event_age_seconds,self.future_skew_seconds)
                        mscore=baseline_tx(c,mrow["id"],original,mrow["observed_at"])
                        enqueue_tx(c,"event:"+str(mrow["id"]),"inspect_event",
                                   {"event_id":mrow["id"]},now,event_id=mrow["id"],
                                   start_by=now+180,priority=mscore)
                else:
                    score=40 if source in ("events","live") else 10
                enqueue_tx(c,"event:"+str(row[0]),"inspect_event",{"event_id":row[0]},
                           now,event_id=row[0],start_by=now+180,priority=score)
            c.execute("""INSERT INTO sync_cursors(source,scope,stream,received,target,next_at)
                VALUES(?,?,?,?,?,?) ON CONFLICT(source,scope) DO UPDATE SET received=excluded.received,
                stream=excluded.stream,target=excluded.target,next_at=excluded.next_at,failures=0""",
                (source,scope,stream,next_cursor,target,now if next_at is None else next_at))
            if target is None:
                c.execute("UPDATE sync_cursors SET covered_at=? WHERE source=? AND scope=?",(now,source,scope))
            self._handled(c,source,scope,stream)

    @staticmethod
    def _handled(c, source, scope, stream):
        # No higher handled watermark while any earlier event's work is uncompleted.
        pending=c.execute("""SELECT 1 FROM events e JOIN work_items w ON w.event_id=e.id
            WHERE e.source=? AND e.scope=? AND e.stream=? AND w.status!='done' LIMIT 1""",(source,scope,stream)).fetchone()
        if not pending:
            c.execute("UPDATE sync_cursors SET handled=received WHERE source=? AND scope=?",(source,scope))

    def reconcile_handled(self):
        with self.db.transaction() as c:
            for row in c.execute("SELECT source,scope,stream FROM sync_cursors").fetchall():
                self._handled(c,*row)

    def failure(self, source, scope, now, retry_after=0):
        old=self.state(source,scope)
        failures=min(old["failures"]+1,10)
        # Bounded backoff persisted across ticks; no blocking retry loop.
        delay=max(retry_after,min(1800,15*2**failures))
        with self.db.transaction() as c:
            c.execute("""INSERT INTO sync_cursors(source,scope,next_at,failures) VALUES(?,?,?,?)
                ON CONFLICT(source,scope) DO UPDATE SET next_at=excluded.next_at,failures=excluded.failures""",(source,scope,now+delay,failures))

    def reset(self, stream, waterline, now):
        waterline=cursor(waterline)
        with self.db.transaction() as c:
            c.execute("""INSERT INTO sync_cursors(source,scope,stream,received,handled,next_at)
                VALUES('events','global',?,?,?,?) ON CONFLICT(source,scope) DO UPDATE SET stream=excluded.stream,
                received=excluded.received,handled=excluded.handled,target=NULL,next_at=excluded.next_at,failures=0""",
                (stream,waterline,waterline,now))


class Collector:
    def __init__(self, sync, request, config):
        self.sync=sync
        self.request=request
        self.config=config
        self.errors=[]
        self.resync_since=None

    def _visible_chats(self, items, replace=False):
        with self.sync.db.transaction() as c:
            prior=c.execute("SELECT value FROM meta WHERE key='visible_chats'").fetchone()
            ids=set() if replace or not prior else set(json.loads(prior[0]))
            ids.update(v.get("conversationId") or v.get("id") for v in items if isinstance(v,dict))
            ids.discard(None)
            c.execute("INSERT INTO meta VALUES('visible_chats',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(canonical(sorted(ids)),))

    def identity(self):
        r=self.request("GET","/api/v1/agents/me")
        if r.status!=200 or not isinstance(r.body,dict) or not r.body.get("id"):
            raise AuthError("identity could not be verified")
        actor=r.body["id"]
        if self.config.actor_id and actor!=self.config.actor_id:
            raise AuthError("authenticated actor differs from configured actor")
        with self.sync.db.transaction() as c:
            old=c.execute("SELECT value FROM meta WHERE key='actor'").fetchone()
            if old and old[0]!=actor:
                raise AuthError("database is bound to a different actor")
            c.execute("INSERT INTO meta VALUES('actor',?) ON CONFLICT(key) DO NOTHING",(actor,))
        return actor

    def pages(self, source, scope, path, now, force=False, interval=None):
        state=self.sync.state(source,scope)
        if state["failures"] and state["next_at"]>now:
            return False
        if force and self.resync_since is not None and state["covered_at"]>=self.resync_since:
            return True
        if not force and state["next_at"]>now:
            return True
        after=state["received"]
        interval=interval or self.config.normal_scan_seconds
        for _ in range(self.config.max_pages_per_source):
            try:
                response=self.request("GET",path+("&" if "?" in path else "?")+urlencode({"after":after,"limit":100}))
            except BudgetExceeded:
                return False
            if response.status!=200:
                self.sync.failure(source,scope,now,response.retry_after)
                self.errors.append({"source":source,"status":response.status})
                return False
            # A malformed/retrograde server cursor is a failed source,
            # NEVER a successful terminal empty page. Preserve every original
            # event and cursor and give this source a bounded retry backoff;
            # other due sources are still scanned in the same tick.
            try:
                b=response.body
                if isinstance(b,list):
                    items=b
                    nxt=after
                    more=False
                elif isinstance(b,dict) and isinstance(b.get("items"),list):
                    items=b["items"]
                    nxt=str(b.get("nextCursor",after))
                    cursor(nxt)
                    # Inbox/thread pages can omit hasMore: continue to a terminal page.
                    more=bool(b["hasMore"]) if "hasMore" in b else bool(items) and int(nxt)>int(after)
                else:
                    raise ValueError("unrecognized collection response")
                cursor(nxt)
                if int(nxt)<int(after) or (more and int(nxt)<=int(after)):
                    raise ValueError("invalid page cursor: regression or no progress")
                self.sync.page(source,scope,"",items,nxt,now,
                               target="incomplete" if more else None,
                               next_at=now if more else now+interval)
            except (ValueError,TypeError):
                # This is an untrusted upstream response, not an invitation to
                # reset the cursor or mark unseen messages as handled.
                self.sync.failure(source,scope,now)
                self.errors.append({"source":source,"scope":scope,
                                    "code":"INVALID_PAGE_OR_CURSOR"})
                return False
            if source=="chats":
                self._visible_chats(items,replace=after=="0")
            if not more:
                return True
            after=nxt
        return False

    def resources(self, now, force=False, chat_ids=None):
        complete=True
        # Discover visible chats first. Historical inbox may require many pages
        # and must not consume the entire tick before any message is checked.
        complete=self.pages("chats","global","/api/v1/chats",now,force) and complete
        ids=set(self.config.allowed_chat_ids)
        if chat_ids:
            ids.update(chat_ids)
        # Include every currently visible chat, not merely the last-message projection.
        current=self.sync.db.read("SELECT value FROM meta WHERE key='visible_chats'")
        if current:
            ids.update(json.loads(current[0]["value"]))
        # Rotate fairly by the oldest due cursor, not lexicographic chat ID.
        # Active chats may scan ahead of their normal 10-minute deadline,
        # but a persisted 429/backoff always takes precedence.
        def chat_deadline(cid):
            state=self.sync.state("messages",cid)
            due=state["next_at"]
            if chat_ids and cid in chat_ids and not (state["failures"] and due>now):
                due=min(due,now)
            return (due,cid)
        for cid in sorted(ids,key=chat_deadline):
            complete=self.pages("messages",cid,"/api/v1/chats/"+quote(cid,safe="")+"/messages",now,force or bool(chat_ids and cid in chat_ids),
                                self.config.active_scan_seconds if chat_ids and cid in chat_ids else None) and complete
        for source,path in (("inbox","/api/v1/inbox"),("tasks","/api/v1/tasks?mine=true"),("updates","/api/v1/updates")):
            complete=self.pages(source,"global",path,now,force) and complete
        for pid in self.config.project_ids:
            for suffix in ("","/progress","/artifacts"):
                snapshot=self.sync.state("projects",pid+suffix)
                if (force and self.resync_since is not None and snapshot["covered_at"]>=self.resync_since) or (not force and snapshot["next_at"]>now):
                    continue
                r=self.request("GET","/api/v1/projects/"+quote(pid,safe="")+suffix)
                if r.status!=200:
                    complete=False
                    self.errors.append({"source":"project","status":r.status})
                else:
                    body=r.body
                    items=body if isinstance(body,list) else [body]
                    self.sync.page("projects",pid+suffix,"",items,"0",now,next_at=now+self.config.normal_scan_seconds)
        return complete

    def agent_events(self, now):
        """Incremental Phase 5 feed, never a chat/inbox/project polling fallback.

        The server's exact recipient feed is authoritative for a single actor.
        Keep a separate checkpoint from legacy Full Tick event replay. Every
        page atomically persists normalized messages, work and cursor. Cursor
        loss/410/stream changes require explicit recovery, never skip history.
        """
        source,scope="agent_events","global"
        state=self.sync.state(source,scope)
        if state["failures"] and state["next_at"]>now:
            return "backoff"
        after=state["received"]
        target=state["target"]
        stream=state["stream"]
        for _ in range(self.config.light_max_pages_per_source):
            params={"after":after,"limit":self.config.light_events_page_size}
            if target:
                params["to"]=target
            try:
                reply=self.request("GET","/api/v1/agent/events?"+urlencode(params))
            except BudgetExceeded:
                return "incomplete"
            if reply.status in (410,422):
                self.sync.failure(source,scope,now,reply.retry_after)
                self.errors.append({"source":source,"code":"AGENT_EVENTS_RESYNC_REQUIRED",
                                    "http":reply.status})
                return "resync_required"
            if reply.status!=200:
                self.sync.failure(source,scope,now,reply.retry_after)
                self.errors.append({"source":source,"status":reply.status})
                return "failed"
            try:
                b=reply.body
                if not isinstance(b,dict) or not isinstance(b.get("items"),list):
                    raise ValueError("invalid agent event page")
                sid=b.get("streamId")
                nxt=cursor(str(b.get("nextCursor","")))
                upper=cursor(str(b.get("to","")))
                waterline=cursor(str(b.get("waterline","")))
                deleted=cursor(str(b.get("deletedThrough","")))
                more=b.get("hasMore")
                if (int(after)<int(deleted) or (stream and sid!=stream)):
                    self.sync.failure(source,scope,now)
                    self.errors.append({"source":source,
                                        "code":"AGENT_EVENTS_RESYNC_REQUIRED"})
                    return "resync_required"
                if (not isinstance(sid,str) or not sid or type(more) is not bool
                        or (target and upper!=target)
                        or int(upper)>int(waterline)
                        or int(nxt)<int(after) or int(nxt)>int(upper)
                        or (more and int(nxt)==int(after))
                        or (not more and nxt!=upper)):
                    raise ValueError("invalid event feed stream or cursor")
                for item in b["items"]:
                    if not isinstance(item,dict):
                        raise ValueError("invalid event item")
                    if item.get("type")=="event.redacted":
                        continue
                    if item.get("type") in (
                            "chat.message.created","chat.mentioned",
                            "room.message.created","room.mentioned","message.created"):
                        if item.get("contextUnavailable") or not isinstance(item.get("message"),dict):
                            raise ValueError("event message context unavailable")
                    if not isinstance(item.get("type"),str) or not item["type"]:
                        raise ValueError("invalid event type")
                self.sync.page(source,scope,sid,b["items"],nxt,now,
                               target=upper if more else None,
                               next_at=now if more else now+self.config.active_scan_seconds)
            except (ValueError,TypeError,OverflowError):
                self.sync.failure(source,scope,now)
                self.errors.append({"source":source,"code":"INVALID_AGENT_EVENT_PAGE"})
                return "failed"
            if not more:
                return "complete"
            after=nxt
            target=upper
            stream=sid
        return "incomplete"

    def events(self, waterline, stream, now, force=False):
        waterline=cursor(str(waterline))
        state=self.sync.state("events","global")
        if state["stream"]!=stream or int(state["received"])>int(waterline):
            return "resync"
        if state["next_at"]>now and (not force or state["failures"]):
            return "backoff"
        target=state["target"] or waterline
        after=state["received"]
        for _ in range(self.config.max_pages_per_source):
            try:
                r=self.request("GET","/api/v1/events?"+urlencode({"after":after,"to":target,"limit":100}))
            except BudgetExceeded:
                return "incomplete"
            if r.status in (410,422):
                return "resync"
            if r.status!=200:
                self.sync.failure("events","global",now,r.retry_after)
                self.errors.append({"source":"events","status":r.status})
                return "failed"
            try:
                b=r.body
                if not isinstance(b,dict) or not isinstance(b.get("items"),list):
                    raise ValueError("invalid event page")
                nxt=str(b.get("nextCursor",target if not b.get("hasMore") else after))
                cursor(nxt)
                more=bool(b.get("hasMore"))
                if (int(nxt)<int(after) or int(nxt)>int(target)
                        or (more and int(nxt)<=int(after))):
                    raise ValueError("invalid event cursor progression")
                done=not more
                self.sync.page("events","global",stream,b["items"],
                               target if done else nxt,now,
                               target=None if done else target,
                               next_at=now+self.config.normal_scan_seconds if done else now)
            except (ValueError,TypeError):
                self.sync.failure("events","global",now)
                self.errors.append({"source":"events","scope":"global",
                                    "code":"INVALID_EVENT_PAGE_OR_CURSOR"})
                return "failed"
            if done:
                return "complete"
            after=nxt
        return "incomplete"
