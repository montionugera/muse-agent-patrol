"""Bounded, credential-verified Outbox delivery; never generates decisions.

One verified CrewQuay actor performs all remote API writes. The Muse worker
that drafted an action is only an audit producer, not the platform principal.
"""
import hashlib
import json
import time
from urllib.parse import quote

from ...attention.index import Attention
from ...collection.index import Collector, Sync, event_time
from ...persistence.index import canonical
from ...outbox.index import Outbox, NotDispatched
from ...transport.index import AuthError, BudgetExceeded
from .task_actions import TASK_ACTIONS, task_scope, task_path, task_payload, verify_task
from .platform_actions import can_execute, verify_execution
from .authorization import live_action_authority


class ActionSender:
    def __init__(self, db, config, client=None, clock=time.time):
        self.db=db
        self.config=config
        self.client=client
        self.clock=clock
        self.outbox=Outbox(db)
        self.sync=Sync(db,config.max_event_age_seconds,config.future_skew_seconds)
        self.attention=Attention(db,config)
        self.actor=None

    def _chat_allowed(self, target):
        """Resolve current CrewQuay chat authority; Owner denials always win.

        Chat IDs and local allowlists do not establish membership or permission.
        Do not trust a chat_ prefix, a sender's requested target or stale
        synced conversation metadata as proof of current server grants.
        """
        if target in self.config.blocked_chat_ids:
            return False,"chat explicitly blocked by Owner"
        if self.client is None:
            return False,"chat authorization lookup unavailable"
        try:
            response=self.client.request("GET","/api/v1/chats/"+quote(target,safe=""))
        except Exception:
            return False,"chat authorization lookup unavailable"
        if response.status!=200 or not isinstance(response.body,dict):
            return False,"chat not currently authorized by platform"
        detail=response.body
        kind=detail.get("kind")
        if (detail.get("conversationId")!=target
                or kind not in ("direct","group","community_hub","project")
                or detail.get("canSend") is not True
                or detail.get("isObserver") is True):
            return False,"chat not currently writable by platform"
        if kind=="community_hub":
            if target!="community_hub":
                return False,"hub conversation identity mismatch"
            if not self.config.community_social_enabled:
                return False,"community social activity disabled by Owner"
        elif kind=="direct":
            if type(detail.get("memberCount")) is not int or detail["memberCount"]!=2:
                return False,"direct conversation must have two members"
            # When the Owner blocks a peer, every present and future DM with
            # that peer must be denied, including those not named in a chat ID
            # denylist. Require bounded, well-formed member identity evidence.
            if self.config.blocked_contact_ids:
                members=detail.get("members")
                if not isinstance(members,list) or len(members)!=2 or not all(
                        isinstance(m,dict) and isinstance(m.get("id"),str)
                        for m in members):
                    return False,"direct participants unavailable for Owner policy"
                if self.actor not in [m["id"] for m in members]:
                    return False,"direct actor not a current participant"
                if any(m["id"] in self.config.blocked_contact_ids for m in members):
                    return False,"direct contact explicitly blocked by Owner"
        elif kind=="group":
            # A community circle is backed by a normal group conversation.
            # Map the Owner's forbidden circle IDs to their authoritative
            # chatId, so a group send cannot bypass the circle action policy.
            for key in self.config.blocked_social_resources:
                if not key.startswith("circle:"):
                    continue
                circle_id=key[len("circle:"):]
                try:
                    found=self.client.request("GET",
                        "/api/v1/agent/resources/circle/"+
                        quote(circle_id,safe=""))
                except Exception:
                    return False,"blocked circle membership lookup unavailable"
                if (found.status!=200 or not isinstance(found.body,dict)
                        or found.body.get("type")!="circle"
                        or found.body.get("id")!=circle_id
                        or not isinstance(found.body.get("chatId"),str)):
                    return False,"blocked circle chat mapping unavailable"
                if found.body["chatId"]==target:
                    return False,"linked circle explicitly blocked by Owner"
        elif kind=="project":
            # Authenticated CrewQuay chat metadata already says canSend and
            # is checked at every write. Former local project whitelist
            # duplicated the platform's current project membership authority.
            if not detail.get("projectId"):
                return False,"platform project chat has no project identity"
        return True,None

    def can_send(self,row):
        if not self.config.allow_writes or not self.actor or self.actor!=row["actor"]:
            return False,"explicit write authorization and current matching actor required"
        if row["action"] in ("deployment_syn","deployment_ack","deployment_bug"):
            run=self.db.read("SELECT * FROM deployment_runs WHERE actor=? AND handshake_id=?",
                             (row["actor"],row["target"]))
            if not run:
                return False,"no matching registered deployment run"
            r=run[0]
            if row["action"]=="deployment_syn":
                if (r["run_id"]!=row["payload"]["runId"] or
                        r["nonce"]!=row["payload"]["nonce"] or
                        r["state"] not in ("awaiting_syn","syn_staged","awaiting_event","syn_ack_received")):
                    return False,"SYN not authorized in deployment phase"
            elif row["action"]=="deployment_ack":
                p=row["payload"]
                if (r["state"] not in ("syn_ack_received","ack_staged","protocol_verified") or
                        r["nonce"]!=p["nonce"] or r["challenge"]!=p["challenge"] or
                        r["syn_ack_event_id"]!=p["eventId"] or
                        r["syn_ack_event_cursor"]!=p["eventCursor"] or
                        r["syn_ack_message_id"]!=p["synAckMessageId"] or
                        r["expires_at"] is None or self.clock()>=r["expires_at"]):
                    return False,"ACK requires current unexpired authenticated event"
            else:
                if (r["state"] not in ("failed","expired") or
                        not r["failure_code"] or
                        r["failure_code"] not in row["payload"]["title"] or
                        r["run_id"] not in row["payload"]["stepsToReproduce"]):
                    return False,"Bug requires actual durable failed deployment"
            return True,None
        if row["action"] in ("send","read"):
            allowed, reason=self._chat_allowed(row["target"])
            if not allowed:
                return False,reason
        if row["action"] in ("project_message","task_thread_reply","room_message"):
            operation={"project_message":("project.message.send","project"),
                       "task_thread_reply":("task.thread.reply","task"),
                       "room_message":("room.message.send","room")}
            name,kind=operation[row["action"]]
            current,reason,legacy=live_action_authority(
                self,row,name,kind,row["target"])
            if not current:
                return False,reason
            if legacy:
                if not self.config.allowed_project_action_ids:
                    return False,"legacy project scope not explicitly authorized"
                if kind=="project":
                    project_id=row["target"]
                else:
                    endpoint=("tasks" if kind=="task" else "rooms")
                    try:
                        scope=self.client.request("GET","/api/v1/"+endpoint+"/"+
                                                  quote(row["target"],safe=""))
                    except Exception:
                        return False,"legacy resource authorization lookup unavailable"
                    if scope.status!=200 or not isinstance(scope.body,dict):
                        return False,"legacy resource not currently accessible"
                    project_id=scope.body.get("projectId")
                if project_id not in self.config.allowed_project_action_ids:
                    return False,"legacy resource outside Owner-approved project scope"
        if row["action"]=="platform_action":
            return can_execute(self,row)
        if row["action"] in TASK_ACTIONS:
            return task_scope(self,row)
        if row["action"]=="read":
            state=self.sync.state("messages",row["target"])
            if state["covered_at"]<=0 or state["target"] is not None or int(row["payload"]["cursor"])>int(state["handled"]):
                return False,"read cursor is not covered by durably handled messages"
        if row["action"]=="ack":
            if len(row["payload"]["ids"])==1 and not self.config.ack_single_enabled:
                return False,"single ACK compatibility gate; retained without POST"
            # Every ACK id needs durable inbox coverage and explicit message dependencies.
            for seq in row["payload"]["ids"]:
                rows=self.db.read("SELECT payload,observed_at FROM events WHERE source='inbox' AND external_key=?",(str(seq),))
                if not rows:
                    return False,"inbox item not durable"
                item=json.loads(rows[0]["payload"])
                typ=item.get("event_type") or item.get("type")
                dependencies=self.config.ack_chat_map.get(typ)
                if dependencies is None:
                    return False,"explicit ACK dependency mapping required"
                for cid in dependencies:
                    state=self.sync.state("messages",cid)
                    if state["covered_at"]<rows[0]["observed_at"] or state["target"] is not None:
                        return False,"dependent messages not durably covered"
        return True,None

    def send(self,row):
        if row["action"]=="deployment_bug":
            path="/api/v1/bugs"
        elif row["action"]=="deployment_syn":
            path="/api/v1/agent/deployment/handshakes"
        elif row["action"]=="deployment_ack":
            path="/api/v1/agent/deployment/handshakes/"+quote(row["target"],safe="")+"/ack"
        elif row["action"]=="ack":
            path="/api/v1/inbox/ack"
        elif row["action"]=="project_message":
            path="/api/v1/projects/"+quote(row["target"],safe="")+"/messages"
        elif row["action"]=="task_thread_reply":
            path="/api/v1/tasks/"+quote(row["target"],safe="")+"/thread/messages"
        elif row["action"]=="room_message":
            path="/api/v1/rooms/"+quote(row["target"],safe="")+"/messages"
        elif row["action"]=="platform_action":
            path="/api/v1/agent/actions/execute"
        elif row["action"] in TASK_ACTIONS:
            path=task_path(row)
        else:
            path="/api/v1/chats/"+quote(row["target"],safe="")+("/messages" if row["action"]=="send" else "/read")
        # Authorization is checked immediately before every network write.
        # A failed re-check (e.g. a flaky chat GET) never reached the POST, so
        # it is a retry of the same key, not an ambiguous remote result.
        allowed,reason=self.can_send(row)
        if not allowed:
            raise NotDispatched("authorization changed: "+str(reason))
        payload=task_payload(row) if row["action"] in TASK_ACTIONS else row["payload"]
        try:
            return self.client.request("POST",path,payload,row["idem_key"])
        except (AuthError,BudgetExceeded,ValueError) as exc:
            # The transport raises these only before opening a connection;
            # anything else (e.g. IncompleteRead) remains ambiguous.
            raise NotDispatched(type(exc).__name__) from exc

    def verify(self,row,response):
        if row["action"]=="deployment_bug":
            # Exact Bug ID must come from the original HTTP response (or
            # already persisted remote_id); never guess an existing report.
            bug=response.get("bugId") if isinstance(response,dict) else None
            if bug is None and isinstance(response,dict):
                bug=response.get("id")
            bug=bug or row.get("remote_id")
            if not isinstance(bug,str) or not bug.startswith("BUG-"):
                return None
            try:
                detail=self.client.request("GET","/api/v1/bugs/"+quote(bug,safe=""))
            except Exception:
                return None
            if (detail.status!=200 or not isinstance(detail.body,dict) or
                    detail.body.get("bugId")!=bug or
                    detail.body.get("reporterId")!=row["actor"] or
                    detail.body.get("title")!=row["payload"]["title"] or
                    detail.body.get("component")!="agent-deployment-hatch"):
                return None
            return {"id":bug,"source":"CrewQuay exact Bug ID and reporter readback"}
        if row["action"] in ("deployment_syn","deployment_ack"):
            # A successful HTTP 200 alone is insufficient: only platform
            # authenticated readback of the same actor and protocol record.
            try:
                remote=self.client.request("GET","/api/v1/agent/deployment/handshakes/"+
                                           quote(row["target"],safe=""))
            except Exception:
                return None
            if remote.status!=200 or not isinstance(remote.body,dict):
                return None
            body=remote.body
            evidence=body.get("evidence")
            if (body.get("protocolVersion")!="1" or
                    body.get("actorId")!=row["actor"] or
                    body.get("handshakeId")!=row["target"] or
                    body.get("hostHookVerified") is not False or
                    not isinstance(evidence,dict) or
                    evidence.get("platformSenderId")!="crewquay-platform" or
                    not isinstance(evidence.get("synMessageId"),str) or
                    not isinstance(evidence.get("synAckMessageId"),str) or
                    evidence["synMessageId"]==evidence["synAckMessageId"]):
                return None
            if row["action"]=="deployment_syn":
                if (body.get("runId")!=row["payload"]["runId"] or
                        body.get("status") not in ("awaiting_ack","protocol_verified") or
                        not isinstance(body.get("expiresAt"),str)):
                    return None
                return {"id":row["target"],"source":"CrewQuay authenticated deployment SYN status",
                        "expires_at":body["expiresAt"]}
            payload=row["payload"]
            if (body.get("status")!="protocol_verified" or
                    evidence.get("synAckMessageId")!=payload["synAckMessageId"] or
                    evidence.get("synAckEventId")!=payload["eventId"] or
                    evidence.get("synAckEventCursor")!=payload["eventCursor"] or
                    not isinstance(evidence.get("ackMessageId"),str) or
                    len({evidence["synMessageId"],evidence["synAckMessageId"],
                         evidence["ackMessageId"]})!=3):
                return None
            return {"id":evidence["ackMessageId"],
                    "source":"CrewQuay authoritative final ACK status",
                    "handshake_id":row["target"]}
        # Reconciling a previously submitted effect is read-only. Owner
        # prohibitions prevent NEW writes but must not erase evidence of a
        # possible earlier write; the server still checks GET visibility.
        if row["action"] in ("project_message","task_thread_reply","room_message"):
            allowed,_=self.can_send(row)
            if not allowed:
                return None
            # Remote response alone is not proof: verify the exact message
            # identity and content in the corresponding authorized resource.
            if response is None and row.get("remote_id"):
                response={"id":row["remote_id"]}
            if not isinstance(response,dict):
                return None
            message=response.get("message",response)
            ident=message.get("messageId") or message.get("id")
            if not ident:
                return None
            if row["action"]=="project_message":
                endpoint="/api/v1/projects/"+quote(row["target"],safe="")+"/messages"
            elif row["action"]=="task_thread_reply":
                endpoint="/api/v1/tasks/"+quote(row["target"],safe="")+"/thread"
            else:
                endpoint="/api/v1/rooms/"+quote(row["target"],safe="")+"/messages"
            after=str(row.get("verify_cursor") or "0")
            for _ in range(self.config.max_pages_per_source):
                r=self.client.request("GET",endpoint+"?after="+after+"&limit=100")
                if r.status!=200 or not isinstance(r.body,dict):
                    return None
                for item in r.body.get("items",[]):
                    actual_id=item.get("messageId") or item.get("id")
                    sender=item.get("actorId") or item.get("senderAgentId")
                    if (actual_id==ident and sender==row["actor"] and
                            item.get("content")==row["payload"]["content"]):
                        return {"id":ident,"source":"REST collaboration readback",
                                "sha256":hashlib.sha256(canonical(item).encode()).hexdigest()}
                nxt=str(r.body.get("nextCursor",after))
                try:
                    if int(nxt)<int(after):
                        return None
                except ValueError:
                    return None
                self.outbox.verified_page(row["id"],nxt)
                if not r.body.get("hasMore") or nxt==after:
                    break
                after=nxt
            return None
        if row["action"]=="platform_action":
            return verify_execution(self,row)
        if row["action"] in TASK_ACTIONS:
            return verify_task(self,row,response)
        if row["action"]=="send" and response is None and not row.get("remote_id"):
            return self._reconcile_unattributed_send(row)
        if response is None and row.get("remote_id"):
            response={"messageId":row["remote_id"]}
        if row["action"]=="send" and isinstance(response,dict):
            message=response.get("message",response)
            ident=message.get("messageId") or message.get("id")
            if not ident:
                return None
            # A collector may already have advanced beyond this success message.
            # Bounded replay may defer verification, but cannot confirm a text twin.
            after=row.get("verify_cursor","0")
            for _ in range(self.config.max_pages_per_source):
                r=self.client.request("GET","/api/v1/chats/"+quote(row["target"],safe="")+"/messages?after="+after+"&limit=100")
                if r.status!=200 or not isinstance(r.body,dict):
                    return None
                for item in r.body.get("items",[]):
                    if (item.get("messageId") or item.get("id"))==ident and item.get("senderId")==row["actor"] and item.get("content")==row["payload"]["content"]:
                        self.attention.observe(row["target"],"own:"+ident,event_time(item.get("createdAt"),self.clock()),self.clock(),own=True)
                        return {"id":ident,"source":"REST message readback","sha256":hashlib.sha256(canonical(item).encode()).hexdigest()}
                nxt=str(r.body.get("nextCursor",after))
                if int(nxt)<int(after):
                    return None
                self.outbox.verified_page(row["id"],nxt)
                if not r.body.get("hasMore") or nxt==after:
                    break
                after=nxt
        # ACK/read require an operator's evidence-backed resolution until an exact
        # endpoint-specific readback contract is supplied. Never replay them blindly.
        return None

    def _reconcile_unattributed_send(self,row):
        """Close an `unknown` chat send that never received a remote ID.

        Similar text alone is not proof. Bind only a UNIQUE message with the
        exact sender and content, created after staging and not owned by any
        other Outbox row. Absence is proven only by a complete, all-200 scan
        after the in-flight window; anything else stays unknown.
        """
        if self.db.read("""SELECT 1 FROM outbox WHERE id<>? AND actor=? AND target=?
                AND payload_hash=? AND status IN ('prepared','retry_wait','sending','unknown')
                LIMIT 1""",(row["id"],row["actor"],row["target"],row["payload_hash"])):
            return None
        claimed={r["remote_id"] for r in self.db.read("""SELECT remote_id FROM outbox
            WHERE id<>? AND remote_id IS NOT NULL""",(row["id"],))}
        staged=row["created_at"]-self.config.future_skew_seconds
        after=str(row.get("verify_cursor") or "0")
        found=[]
        for _ in range(self.config.max_pages_per_source):
            r=self.client.request("GET","/api/v1/chats/"+quote(row["target"],safe="")+"/messages?after="+after+"&limit=100")
            if r.status!=200 or not isinstance(r.body,dict):
                return None
            for item in r.body.get("items",[]):
                if item.get("senderId")!=row["actor"] or item.get("content")!=row["payload"]["content"]:
                    continue
                ident=item.get("messageId") or item.get("id")
                created=event_time(item.get("createdAt"),None)
                # Unparsable or future times (e.g. epoch milliseconds) cannot
                # be ordered against staging: fail closed.
                if (not ident or created is None or
                        created>self.clock()+self.config.future_skew_seconds):
                    return None
                if created>=staged and str(ident) not in claimed:
                    found.append((str(ident),item,created))
            nxt=str(r.body.get("nextCursor",after))
            try:
                if int(nxt)<int(after):
                    return None
            except ValueError:
                return None
            if not found:
                self.outbox.verified_page(row["id"],nxt)
            if not r.body.get("hasMore") or nxt==after:
                break
            after=nxt
        else:
            return None
        if len(found)==1:
            ident,item,created=found[0]
            self.attention.observe(row["target"],"own:"+ident,created,self.clock(),own=True)
            return {"id":ident,"source":"REST readback: unique unclaimed exact sender/content after staging",
                    "unclaimed":True,"sha256":hashlib.sha256(canonical(item).encode()).hexdigest()}
        if found or row.get("dispatch_at") is None or self.clock()-row["dispatch_at"]<self.config.lease_seconds:
            return None
        return {"proven_absent":True,"source":"complete REST readback without exact sender/content after staging",
                "chat":row["target"],"scanned_through":nxt,"dispatch_at":row["dispatch_at"]}

    def drain_once(self, limit=3, action_id=None, lane="any"):
        """Deliver only bounded, due durable Outbox actions; NO full patrol.

        Authentication and platform identity are checked before network writes.
        No model calls, prompt interpretation, automatic authorization or
        credential printing occur here. A crash-ambiguous POST is reconciled,
        never blindly retried.
        """
        if type(limit) is not int or not 1<=limit<=50:
            raise ValueError("limit must be between 1 and 50")
        if action_id is not None and (not isinstance(action_id,str) or not action_id):
            raise ValueError("nonempty Outbox ID required")
        if lane not in ("any","chat","notifications","collaboration","tasks","platform"):
            raise ValueError("unsupported action lane")
        now=self.clock()
        report={"mode":"outbox_once","status":"idle","outbox_attempts":[],
                "confirmed":0,"unknown":0,"blocked":0,
                "remote_writes_allowed":bool(self.config.allow_writes)}
        if not self.config.allow_writes:
            report["status"]="authorization_failed"
            return report
        self.outbox.recover(now,self.config.lease_seconds)
        selection=" AND id=?" if action_id is not None else ""
        scope={"any":"","chat":" AND action='send'",
               "notifications":" AND action IN ('ack','read')",
               "collaboration":" AND action IN ('project_message','task_thread_reply','room_message')",
               "tasks":" AND action IN ('task_claim','task_progress','task_submit','submission_review','project_decision','artifact_upload')",
               "platform":" AND action='platform_action'"}[lane]
        params=(now,action_id) if action_id is not None else (now,)
        # Check due work before loading credentials or making an API request.
        due=self.db.read("""SELECT id FROM outbox WHERE
            status IN ('prepared','retry_wait','unknown') AND next_attempt_at<=?"""+
            selection+scope+" ORDER BY created_at LIMIT 1",params)
        if not due:
            if action_id:
                row=self.db.read("SELECT status FROM outbox WHERE id=?",(action_id,))
                report["item_status"]=row[0]["status"] if row else "not_found"
                if report["item_status"]=="not_found":
                    report["status"]="not_found"
            return report
        if self.client is None:
            report["status"]="authorization_failed"
            return report
        # An executor identity is the authenticated platform actor, never a
        # handoff-provided producer id. A DB bound to another actor is rejected.
        self.actor=Collector(self.sync,self.client.request,self.config).identity()
        report["verified_actor"]=self.actor
        report["outbox_attempts"]=self.outbox.process(
            self.clock(),self.can_send,self.send,self.verify,limit=limit,
            action_id=action_id,lane=lane)
        if action_id:
            row=self.db.read("SELECT status FROM outbox WHERE id=?",(action_id,))
            report["item_status"]=row[0]["status"] if row else "not_found"
        report["confirmed"]=sum(x.get("status")=="confirmed"
                                for x in report["outbox_attempts"])
        report["unknown"]=sum(x.get("status")=="unknown"
                              for x in report["outbox_attempts"])
        report["blocked"]=sum(x.get("status")=="blocked"
                              for x in report["outbox_attempts"])
        if (report["blocked"] or report["unknown"]
                or any(x["status"] not in ("confirmed","blocked") for x in report["outbox_attempts"])):
            report["status"]="degraded"
        elif report["outbox_attempts"]:
            report["status"]="ok"
        elif action_id and report.get("item_status")=="confirmed":
            report["status"]="ok"
        else:
            report["status"]="partial"
        return report
