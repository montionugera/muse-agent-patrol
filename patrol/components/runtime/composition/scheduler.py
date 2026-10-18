import hashlib
import json
import time
from urllib.parse import quote
from ...attention.index import Attention
from ...deployment.index import Deployment
from ...collection.index import Collector, Sync, event_time
from ...outbox.index import Outbox
from ...executor.index import ActionSender
from ...persistence.index import canonical, new_id, Busy
from ...promises.index import Promises
from ...queue.index import Queue, MessageScores
from ...scheduling.index import Scheduler
from ...transport.index import BudgetExceeded, AuthError, connect_ready
from ...wakeup.index import Wakeups


class Tick(ActionSender):
    def __init__(self, db, config, client=None, clock=time.time, monotonic=time.monotonic, ws_factory=None):
        super().__init__(db,config,client,clock)
        self.db=db
        self.config=config
        self.client=client
        self.clock=clock
        self.monotonic=monotonic
        self.ws_factory=ws_factory
        self.queue=Queue(db,config.lease_seconds)
        self.scheduler=Scheduler(db,config.lease_seconds)
        self.wakeups=Wakeups(db,config.lease_seconds)
        self.scores=MessageScores(db)
        self.promises=Promises(db)
        self.attention=Attention(db,config)
        self.outbox=Outbox(db)
        self.sync=Sync(db,config.max_event_age_seconds,config.future_skew_seconds)
        self.collector=Collector(self.sync,client.request,config) if client else None
        self.actor=None
        self.errors=[]
        self.request_deadline=0

    def execute(self, limit, local_only=False):
        done=0
        for _ in range(limit):
            if self.monotonic()>=self.request_deadline:
                break
            now=self.clock()
            # Reserve every fourth inspection slot for lower-ranked backlog;
            # if no background work is due, use the slot for ordinary work.
            lane="local_messages" if local_only else ("background" if _ % 4==3 else "any")
            item=self.queue.claim(self.run_id,now,lane=lane)
            if not local_only and not item and lane=="background":
                item=self.queue.claim(self.run_id,now)
            if not item:
                break
            if item["type"]!="inspect_event":
                # Defensive fallback if a future Queue implementation admits a
                # model action: release it, never convert it to blocked.
                self.queue.finish(item,now,status="retry_wait",
                                  reason="wrong consumer: requires Muse",retry_at=now+60)
                continue
            try:
                rows=self.db.read("SELECT * FROM events WHERE id=?",(item["payload"]["event_id"],))
                if not rows:
                    raise ValueError("event missing")
                row=rows[0]
                value=json.loads(row["payload"])
                evidence={"event_id":row["id"],"payload_sha256":hashlib.sha256(row["payload"].encode()).hexdigest(),"kind":"durable_resource_snapshot"}
                if row["source"]=="agent_events":
                    kind=value.get("type","")
                    if kind=="event.redacted":
                        evidence["kind"]="redacted_agent_event"
                    elif kind in ("deployment.syn_ack","deployment.protocol_verified"):
                        # Dedicated trusted-control recognizer consumes the
                        # durable Agent Event Feed pointer with strict IDs.
                        # Never synthesize generic chat/social work from it.
                        evidence["kind"]="deployment_control_pointer_persisted"
                    elif isinstance(value.get("message"),dict):
                        # Normalized message, receipt and inspector are
                        # durably persisted in the exact source page txn.
                        evidence["kind"]="server_authorized_message_staged"
                    else:
                        created=event_time(value.get("createdAt"),0)
                        fresh=created>0 and now-self.config.max_event_age_seconds<=created<=now+self.config.future_skew_seconds
                        # Generic events become Muse-decision work. Server
                        # governs visibility; catch-up work has lower priority.
                        priority=75 if fresh else 45
                        self.queue.enqueue("agent:event:"+str(value.get("eventId") or row["external_key"]),
                                           "human_action",
                                           {"event_id":row["id"],"reason":"authorized platform event for Muse decision",
                                            "event_type":kind},
                                           now,start_by=now+180,priority=priority,event_id=row["id"])
                        evidence["kind"]="agent_event_decision_staged"
                elif row["source"] in ("events","legacy_pending","live"):
                    if not self.collector:
                        self.queue.finish(item,now,status="retry_wait",reason="offline: REST confirmation required",retry_at=now+60)
                        continue
                    typ=value.get("type","")
                    tid=value.get("resourceId","")
                    cid=value.get("roomId")
                    if typ=="event.redacted":
                        evidence["kind"]="redacted_pointer"
                    elif cid and ("chat" in typ or "message" in typ or "room" in typ):
                        if not self.collector.pages("messages",cid,"/api/v1/chats/"+quote(cid,safe="")+"/messages",now,force=True):
                            self.queue.finish(item,self.clock(),status="retry_wait",reason="message pagination incomplete",retry_at=now+60)
                            continue
                        evidence["kind"]="REST messages persisted"
                    elif tid.startswith("tsk_"):
                        r=self.client.request("GET","/api/v1/tasks/"+quote(tid,safe=""))
                        if r.status not in (200,403,404):
                            raise RuntimeError("task refresh failed")
                        if r.status==200 and (not isinstance(r.body,dict) or r.body.get("id")!=tid):
                            raise ValueError("task resource identity mismatch")
                        evidence.update(http=r.status,kind="REST task refresh",resource_sha256=hashlib.sha256(canonical(r.body).encode()).hexdigest())
                    else:
                        # Unknown pointers must not be mislabeled as processed.
                        self.queue.finish(item,self.clock(),status="blocked",reason="unsupported resource pointer; explicit handler required")
                        continue
                elif row["source"]=="inbox" and (value.get("event_type") or value.get("type"))=="chat.message.created":
                    # An inbox notification is an untrusted pointer, not proof of
                    # an external message. Only verified REST messages produce human work.
                    cid=value.get("conversationId") or value.get("chatId") or value.get("roomId")
                    if not isinstance(cid,str) or not cid:
                        self.queue.finish(item,self.clock(),status="blocked",
                                          reason="chat notification lacks a verified conversation id")
                        self.db.alert("unmapped_inbox:"+str(row["id"]),"blocked",
                                      {"event_id":row["id"],"reason":"missing chat id"},now)
                        continue
                    if not self.collector:
                        self.queue.finish(item,now,status="retry_wait",
                                          reason="offline inbox pointer needs REST confirmation",retry_at=now+60)
                        continue
                    if not self.collector.pages("messages",cid,
                            "/api/v1/chats/"+quote(cid,safe="")+"/messages",now,force=True):
                        self.queue.finish(item,self.clock(),status="retry_wait",
                                          reason="inbox chat messages not yet fully fetched",retry_at=now+60)
                        continue
                    evidence["kind"]="inbox pointer reconciled by REST messages"
                elif row["source"]=="messages":
                    cid=row["scope"]
                    if not self.actor:
                        # An unverified actor cannot safely classify a message
                        # as someone else's. Preserve the inspector for a later tick.
                        self.queue.finish(item,now,status="retry_wait",
                                          reason="actor identity unavailable",retry_at=now+60)
                        continue
                    external=(value.get("senderId")!=self.actor
                              and value.get("senderKind")!="system"
                              and isinstance(value.get("content"),str)
                              and bool(value["content"].strip()))
                    # Already-ingested messages remain eligible even if queue
                    # congestion made inspection happen after the 15m window.
                    eligible=self.scores.first_receipt_fresh(
                        row["id"],self.config.max_event_age_seconds,
                        self.config.future_skew_seconds)
                    # Attention is still time-bound; old delayed work must not
                    # create a new 30-minute live watch window.
                    self.attention.observe(cid,row["external_key"],
                                           event_time(value.get("createdAt"),0),now,
                                           meaningful=external)
                    if eligible and external:
                        scored=self.db.read("SELECT COALESCE(muse_score,base_score) AS score FROM message_scores WHERE event_id=?",(row["id"],))
                        self.queue.enqueue("human:"+cid+":"+str(row["external_key"]),"human_action",
                                           {"event_id":row["id"],"reason":"fresh on first receipt; authorized agent decision required"},
                                           now,start_by=now+180,priority=scored[0]["score"] if scored else 25,
                                           event_id=row["id"])
                self.queue.finish(item,self.clock(),evidence=evidence)
                done+=1
            except (BudgetExceeded,AuthError):
                self.queue.finish(item,self.clock(),status="retry_wait",reason="request budget or authorization unavailable",retry_at=now+60)
                break
            except Exception:
                self.queue.finish(item,self.clock(),status="retry_wait",reason="resource inspection failed",retry_at=now+min(1800,30*2**min(item["attempts"],5)))
        self.sync.reconcile_handled()
        return done

    def run_light(self):
        """Bounded Phase 5 event-driven turn, with no writes or resource scans.

        /agent/events is the sole remote discovery feed. Resource snapshots,
        historic resync and side-effect execution are outside Light Tick.
        """
        self.run_id=new_id()
        start=self.clock()
        self.request_deadline=self.monotonic()+self.config.tick_budget_seconds
        report={"run_id":self.run_id,"mode":"light","status":"ok","completed_work":0,
                "started_at_utc_epoch":start,"errors":[],"incomplete_sources":[],
                "outbox_attempts":[],"host_wakeups":0,"requests_used":0}
        with self.db.transaction() as c:
            c.execute("INSERT INTO patrol_runs(id,started_at,status) VALUES(?,?,'running')",
                      (self.run_id,start))
        try:
            self.queue.recover(start)
            report["unsupported_work_blocked"]=self.queue.quarantine_unknown(start)
            wakeup_recovery=self.wakeups.recover(start,detail=True)
            report["wakeup_unknown_recovered"]=wakeup_recovery["unknown"]
            report["wakeup_obsolete_recovered"]=wakeup_recovery["obsolete"]
            self.attention.advance(start)
            report["promise_alerts"]=self.promises.watchdog(start)
            known=self.db.read("SELECT value FROM meta WHERE key='actor'")
            self.actor=known[0]["value"] if known else None
            # Local work has a reserved slot before any potentially exhausted GET.
            if known:
                report["completed_work"]+=self.execute(
                    max(1,self.config.max_work_per_tick//2),local_only=True)
            if self.collector:
                self.actor=self.collector.identity()
                if not known:
                    report["completed_work"]+=self.execute(
                        max(1,self.config.max_work_per_tick//2),local_only=True)
                # Phase 5 Light Tick uses exactly ONE remote discovery path:
                # /agent/events. No chat, inbox, task/project pagination,
                # legacy event probe or WebSocket in this execution mode.
                event_result=self.collector.agent_events(self.clock())
                report["event_replay"]=event_result
                report["event_source"]="agent_events"
                # A grants.changed event only invalidates local authority; it
                # never grants write permission by itself. One bounded read
                # fetches a fresh actor-bound snapshot, with safe retry.
                pending=self.db.read(
                    "SELECT value FROM meta WHERE key='authorization_sync_pending'")
                next_at=self.db.read(
                    "SELECT value FROM meta WHERE key='authorization_sync_next_at'")
                if (pending and self.actor and
                        (not next_at or float(next_at[0]["value"])<=self.clock()) and
                        self.monotonic()<self.request_deadline-2):
                    from ...executor.index import Authorization
                    authority=Authorization(self.db)
                    try:
                        snapshot,status=authority.get(self.client,self.actor)
                        if snapshot is not None and authority.sync_requests(self.client,self.actor):
                            with self.db.transaction() as c:
                                c.execute("DELETE FROM meta WHERE key IN "
                                          "('authorization_sync_pending','authorization_sync_next_at')")
                            report["authorization_revision"]=snapshot["revision"]
                        else:
                            with self.db.transaction() as c:
                                c.execute("""INSERT INTO meta(key,value)
                                    VALUES('authorization_sync_next_at',?)
                                    ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
                                    (str(self.clock()+60),))
                            report["incomplete_sources"].append("authorization:refresh_deferred")
                    except (BudgetExceeded,AuthError):
                        report["incomplete_sources"].append("authorization:budget_or_auth")
                    except Exception:
                        report["incomplete_sources"].append("authorization:refresh_failed")
                if event_result!="complete":
                    report["incomplete_sources"].append("agent_events:"+event_result)
                report["errors"].extend(self.collector.errors)
            report["deployment"]=Deployment(self.db).reconcile(self.clock())
            report["completed_work"]+=self.execute(
                self.config.max_work_per_tick,local_only=True)
        except (BudgetExceeded,AuthError) as e:
            report["status"]="deferred" if isinstance(e,BudgetExceeded) else "authorization_failed"
            report["errors"].append({"code":type(e).__name__})
        except Busy:
            # Preserve the CLI's dedicated exit=75 semantics for SQLite contention.
            report["status"]="busy"
            raise
        except Exception as e:
            # The CLI must report a failed tick, never a misleading successful
            # 'partial' if an unexpected upstream payload escapes a collector.
            # Do not serialize error text: it may contain external content.
            report["status"]="failed"
            report["errors"].append({"code":"UNEXPECTED_TICK_ERROR",
                                     "error_type":type(e).__name__})
        finally:
            # These are local, idempotent SQLite transactions, even if HTTP failed.
            # Only register/score local work and request cooperative safe
            # points. The host must explicitly dispatch a model time slice.
            report["scheduler"]=self.scheduler.reconcile(self.clock())
            report["wakeup_staged"]=self.wakeups.stage(self.clock(),
                threshold=self.config.wakeup_min_score)
            report["execution_health"]=self.wakeups.execution_health(self.clock())
            report["wakeup_ready"]=len(self.db.read(
                "SELECT id FROM wakeup_requests WHERE status IN ('pending','retry_wait') AND next_attempt_at<=?",
                (self.clock(),)))
            report["ended_at_utc_epoch"]=self.clock()
            report["requests_used"]=getattr(self.client,"requests",0)
            report["queue"]=self.queue.status()
            if report["status"]=="ok" and report["errors"]:
                report["status"]="degraded"
            elif report["status"]=="ok" and report["incomplete_sources"]:
                report["status"]="partial"
            with self.db.transaction() as c:
                c.execute("UPDATE patrol_runs SET ended_at=?,status=?,report=? WHERE id=?",
                          (self.clock(),report["status"],canonical(report),self.run_id))
        return report

    def run(self, websocket=False, light=False):
        if light:
            return self.run_light()

        self.run_id=new_id()
        start=self.clock()
        self.request_deadline=self.monotonic()+self.config.tick_budget_seconds
        with self.db.transaction() as c:
            c.execute("INSERT INTO patrol_runs(id,started_at,status) VALUES(?,?,'running')",(self.run_id,start))
        ws=None
        report={"run_id":self.run_id,"started_at_utc_epoch":start,"completed_work":0,"alerts":[],"errors":[],"ws_connected":False}
        alert_waterline=self.db.read("SELECT COALESCE(MAX(rowid),0) AS n FROM alerts")[0]["n"]
        try:
            self.queue.recover(start)
            # Full tick is also an entrypoint for recovery: a production
            # executor must not depend on Light running to clear expired leases.
            wakeup_recovery=self.wakeups.recover(start,detail=True)
            report["wakeup_unknown_recovered"]=wakeup_recovery["unknown"]
            report["wakeup_obsolete_recovered"]=wakeup_recovery["obsolete"]
            # Existing v3 databases already contain messages; score them in
            # bounded batches without losing history or modifying remote cursors.
            report["messages_backfilled"]=self.scores.backfill(limit=1000)
            self.outbox.recover(start,self.config.lease_seconds)
            self.attention.advance(start)
            report["alerts"]=self.promises.watchdog(start)
            known=self.db.read("SELECT value FROM meta WHERE key='actor'")
            self.actor=known[0]["value"] if known else None
            due=self.attention.due(start)
            active_ids=[r["target"] for r in due if r["state"]!="idle"]
            collection_due=any(self.sync.state(source,"global")["next_at"]<=start
                               for source in ("inbox","chats","tasks","updates","events"))
            # Every per-chat/project checkpoint is a wake-up source, even when
            # the five global collection cursors are still sleeping.
            scope_due=self.db.read("SELECT 1 FROM sync_cursors WHERE source IN ('messages','projects') AND next_at<=? LIMIT 1",(start,))
            work_due=self.db.read("SELECT 1 FROM work_items WHERE type='inspect_event' AND status IN ('pending','retry_wait') AND next_action_at<=? LIMIT 1",(start,))
            writes_due=self.config.allow_writes and self.db.read("SELECT 1 FROM outbox WHERE status IN ('prepared','unknown','retry_wait') AND next_attempt_at<=? LIMIT 1",(start,))
            needs_network=bool(not known or collection_due or scope_due or active_ids or work_due or writes_due)
            # Reserve local work time before ANY network request if the DB
            # already has a previously verified actor. Identity verification
            # can consume/raise BudgetExceeded during an overloaded first tick.
            # A new DB still verifies the actor before inspecting messages.
            if self.collector and needs_network and not known:
                self.actor=self.collector.identity()
            report["completed_work"]+=self.execute(max(1,self.config.max_work_per_tick//2))
            if self.collector and needs_network and known:
                self.actor=self.collector.identity()
            if self.client and needs_network:
                report["outbox_attempts"]=self.outbox.process(self.clock(),self.can_send,self.send,self.verify)
                ready=None
                if websocket:
                    try:
                        ws,ready=(self.ws_factory or connect_ready)(self.config,self.client.secret)
                        report["ws_connected"]=True
                    except Exception:
                        self.errors.append({"code":"WS_UNAVAILABLE","fallback":"REST"})
                state=self.sync.state("events","global")
                if not ready:
                    if self.monotonic()<self.request_deadline:
                        r=self.client.request("GET","/api/v1/events?limit=1")
                        if r.status==200 and isinstance(r.body,dict) and r.body.get("streamId") and r.body.get("waterline") is not None:
                            ready=r.body
                        else:
                            self.errors.append({"code":"EVENT_WATERLINE_UNAVAILABLE","http":r.status})
                chat_ids=active_ids
                needs_resync=ready and (state["stream"]!=ready["streamId"] or int(state["received"])>int(ready["waterline"]))
                if needs_resync:
                    with self.db.transaction() as c:
                        c.execute("INSERT INTO meta VALUES('resync_since',?) ON CONFLICT(key) DO NOTHING",(str(self.clock()),))
                        self.collector.resync_since=float(c.execute("SELECT value FROM meta WHERE key='resync_since'").fetchone()[0])
                complete=self.collector.resources(self.clock(),force=bool(needs_resync),chat_ids=chat_ids)
                if not complete:
                    report.setdefault("incomplete_sources",[]).append("resource_snapshots")
                if ready:
                    if needs_resync:
                        if complete:
                            self.sync.reset(ready["streamId"],str(ready["waterline"]),self.clock())
                            with self.db.transaction() as c:
                                c.execute("DELETE FROM meta WHERE key='resync_since'")
                            report["resync"]="resources reloaded; cursor rebuilt"
                            report["resync_complete"]=True
                        else:
                            report["resync"]="incomplete; prior cursor retained"
                            report.setdefault("incomplete_sources",[]).append("resync")
                    else:
                        result=self.collector.events(ready["waterline"],ready["streamId"],self.clock())
                        report["event_replay"]=result
                        if result in ("incomplete","backoff","failed","resync"):
                            report.setdefault("incomplete_sources",[]).append("event_stream:"+result)
                        if result=="resync":
                            # Keep old checkpoint. A fresh connection/waterline plus full REST reload
                            # is required on the next tick, recorded explicitly rather than guessing.
                            with self.db.transaction() as c:
                                c.execute("UPDATE sync_cursors SET stream='!resync-required' WHERE source='events' AND scope='global'")
                            self.errors.append({"code":"RESYNC_REQUIRED_NEXT_TICK","checkpoint":"retained"})
                    if ws and (report.get("event_replay")=="complete" or report.get("resync_complete")):
                        for _ in range(64):
                            if self.monotonic()>=self.request_deadline-1:
                                break
                            try:
                                raw=ws.recv()
                            except Exception:
                                break
                            if not raw:
                                break
                            frame=json.loads(raw)
                            if frame.get("eventId") and frame.get("cursor"):
                                # Persist live pointers without jumping the replay checkpoint.
                                checkpoint=self.sync.state("events","global")
                                self.sync.page("events","global",ready["streamId"],[frame],checkpoint["received"],self.clock(),target=checkpoint["target"],next_at=checkpoint["next_at"])
                for row in due:
                    self.attention.checked(row["target"],self.clock())
                report["completed_work"]+=self.execute(self.config.max_work_per_tick)
                report["errors"].extend(self.collector.errors)
            report["errors"].extend(self.errors)
            report["scheduler"]=self.scheduler.reconcile(self.clock())
            report["wakeup_staged"]=self.wakeups.stage(self.clock(),threshold=self.config.wakeup_min_score)
            report["host_wakeups"]=0
            report["ended_at_utc_epoch"]=self.clock()
            report["queue"]=self.queue.status()
            report["status"]="degraded" if report["errors"] else "partial" if report.get("incomplete_sources") else "ok"
            return report
        except (BudgetExceeded,AuthError) as e:
            report["status"]="deferred" if isinstance(e,BudgetExceeded) else "authorization_failed"
            report["errors"].append({"code":type(e).__name__})
            return report
        except Busy:
            report["status"]="busy"
            raise
        except Exception as e:
            report["status"]="failed"
            report["errors"].append({"code":"UNEXPECTED_TICK_ERROR",
                                     "error_type":type(e).__name__})
            return report
        finally:
            if "wakeup_staged" not in report:
                # A deferred network tick can still have completed local work.
                report["scheduler"]=self.scheduler.reconcile(self.clock())
                report["wakeup_staged"]=self.wakeups.stage(
                    self.clock(),threshold=self.config.wakeup_min_score)
                report["host_wakeups"]=0
            if ws:
                ws.close()
            report["execution_health"]=self.wakeups.execution_health(self.clock())
            report["alerts"]=[{"kind":r["kind"],**json.loads(r["detail"])}
                              for r in self.db.read("SELECT kind,detail FROM alerts WHERE rowid>? ORDER BY rowid",(alert_waterline,))]
            with self.db.transaction() as c:
                c.execute("UPDATE patrol_runs SET ended_at=?,status=?,report=? WHERE id=?",(self.clock(),report.get("status","failed"),canonical(report),self.run_id))
