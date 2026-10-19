import hashlib
import base64
import binascii
import json
from ...persistence.index import canonical, new_id, cursor, StaleLease


class NotDispatched(RuntimeError):
    """Raised by a sender only when no network write was attempted."""


class Outbox:
    """Discovery never stages writes; actions need an explicit local operator."""
    def __init__(self, db):
        self.db=db

    def agent_effects_tx(self,c,activity_id):
        """Read exact local remote-receipt status under the Agent transaction.

        This never issues a platform request and never promotes prepared or
        unknown to confirmed. The Executor owns verification and readback.
        """
        return [dict(r) for r in c.execute("""SELECT o.id,o.status,o.remote_id
            FROM scheduled_outbox so JOIN outbox o ON o.id=so.outbox_id
            WHERE so.schedule_id=? ORDER BY o.created_at,o.id""",
            (activity_id,)).fetchall()]

    def stage(self, logical, actor, action, target, payload, now, producer_id=None, schedule_context=None):
        if action not in ("send","ack","read","project_message","task_thread_reply","room_message",
                          "task_claim","task_progress","task_submit","submission_review","project_decision","artifact_upload","platform_action",
                          "deployment_syn","deployment_ack","deployment_bug") or not actor or not logical:
            raise ValueError("unsupported or unattributed action")
        if producer_id is not None and (not isinstance(producer_id,str) or not producer_id.strip() or len(producer_id)>128):
            raise ValueError("invalid audit producer ID")
        if action=="send" and (set(payload)!={"content"} or not isinstance(payload["content"],str) or not 1<=len(payload["content"])<=8000):
            raise ValueError("send payload must contain only content")
        if action in ("project_message","task_thread_reply","room_message"):
            if (not isinstance(payload,dict) or set(payload)!={"content","mentions"}
                    or not isinstance(payload["content"],str)
                    or not 1<=len(payload["content"])<=8000
                    or not isinstance(payload["mentions"],list)
                    or len(payload["mentions"])>10
                    or any(not isinstance(m,str) or not 1<=len(m)<=80 for m in payload["mentions"])):
                raise ValueError("collaboration message requires content and bounded mentions")
            if not isinstance(target,str) or not target.strip() or len(target)>120:
                raise ValueError("collaboration action requires a resource ID")
        if action=="platform_action":
            import re
            if (not isinstance(payload,dict) or
                    set(payload)!={"action","version","resource","input","requestId"}
                    or not isinstance(payload["action"],str)
                    or re.fullmatch(r"[a-z][a-z0-9_.-]{2,99}",payload["action"]) is None
                    or payload["version"]!="1"
                    or not isinstance(payload["resource"],dict)
                    or set(payload["resource"])!={"type","id"}
                    or payload["resource"]["type"] not in ("project","task","room","submission","community","circle","post")
                    or not isinstance(payload["resource"]["id"],str)
                    or not 1<=len(payload["resource"]["id"])<=120
                    or payload["resource"]["id"]!=target
                    or not isinstance(payload["input"],dict)
                    or not isinstance(payload["requestId"],str)
                    or re.fullmatch(r"[A-Za-z0-9._:-]{8,128}",payload["requestId"]) is None
                    or len(canonical(payload).encode("utf-8"))>3000000):
                raise ValueError("invalid Agent Action Protocol v1 envelope")
        if action=="deployment_bug":
            if (not isinstance(payload,dict) or
                    set(payload)!={"title","component","severity","affectedPlatforms",
                                   "labels","environment","stepsToReproduce",
                                   "expectedBehavior","actualBehavior","evidence","relatedApiPath"}
                    or payload["component"]!="agent-deployment-hatch"
                    or payload["severity"]!="high"
                    or any(not isinstance(payload[k],str) or
                           not 1<=len(payload[k])<=limit
                           for k,limit in (("title",200),("environment",4000),
                                           ("stepsToReproduce",8000),
                                           ("expectedBehavior",8000),("actualBehavior",8000),
                                           ("evidence",12000)))
                    or payload["affectedPlatforms"]!=["CrewQuay","HATCH"]
                    or payload["labels"]!=["deployment","handshake","agent"]
                    or not isinstance(target,str) or not target.startswith("dph_")):
                raise ValueError("invalid bounded deployment Bug report")
        if action in ("deployment_syn","deployment_ack"):
            import re
            if not isinstance(payload,dict) or not isinstance(target,str) or not re.fullmatch(r"dph_[0-9a-f]{64}",target):
                raise ValueError("deployment must target canonical handshake ID")
            if action=="deployment_syn":
                if (set(payload)!={"runId","nonce","hookKind","hookIntervalSeconds"} or
                        not isinstance(payload["runId"],str) or
                        re.fullmatch(r"[A-Za-z0-9._:-]{8,100}",payload["runId"]) is None or
                        not isinstance(payload["nonce"],str) or
                        re.fullmatch(r"[A-Za-z0-9_-]{16,128}",payload["nonce"]) is None or
                        payload["hookKind"]!="HATCH" or
                        type(payload["hookIntervalSeconds"]) is not int or
                        payload["hookIntervalSeconds"]!=60):
                    raise ValueError("invalid CrewQuay deployment SYN contract")
            else:
                if (set(payload)!={"nonce","challenge","synAckMessageId","eventId","eventCursor"} or
                        not isinstance(payload["nonce"],str) or
                        re.fullmatch(r"[A-Za-z0-9_-]{16,128}",payload["nonce"]) is None or
                        not isinstance(payload["challenge"],str) or
                        re.fullmatch(r"[a-f0-9]{64}",payload["challenge"]) is None or
                        any(not isinstance(payload[k],str) or not 1<=len(payload[k])<=120
                            for k in ("synAckMessageId","eventId"))):
                    raise ValueError("invalid CrewQuay deployment ACK contract")
                cursor(payload["eventCursor"])
        if action=="task_claim":
            if not isinstance(payload,dict) or set(payload)!={"expectedVersion"} or type(payload["expectedVersion"]) is not int or payload["expectedVersion"]<=0:
                raise ValueError("task claim requires positive expectedVersion")
        if action=="task_progress":
            if (not isinstance(payload,dict) or set(payload)!={"expectedVersion","progress"}
                    or type(payload["expectedVersion"]) is not int or payload["expectedVersion"]<=0
                    or not isinstance(payload["progress"],str) or not 1<=len(payload["progress"])<=2000):
                raise ValueError("task progress requires expectedVersion and progress")
        if action=="task_submit":
            if (not isinstance(payload,dict) or set(payload)!={"assignmentId","baseTaskVersion","summary","notes","artifacts"}
                    or not isinstance(payload["assignmentId"],str) or not 1<=len(payload["assignmentId"])<=80
                    or type(payload["baseTaskVersion"]) is not int or payload["baseTaskVersion"]<=0
                    or not isinstance(payload["summary"],str) or not 1<=len(payload["summary"])<=10000
                    or not isinstance(payload["notes"],str) or len(payload["notes"])>10000
                    or not isinstance(payload["artifacts"],list) or len(payload["artifacts"])>20
                    or any(not isinstance(x,dict) or set(x)!={"artifactId","sha256"}
                           or not isinstance(x["artifactId"],str) or not 1<=len(x["artifactId"])<=80
                           or not isinstance(x["sha256"],str) or len(x["sha256"])!=64
                           or any(c not in "0123456789abcdef" for c in x["sha256"])
                           for x in payload["artifacts"])):
                raise ValueError("invalid task submission payload")
        if action=="submission_review":
            if (not isinstance(payload,dict)
                    or set(payload)!={"taskId","decision","feedback","requiredChanges"}
                    or not isinstance(payload["taskId"],str) or not 1<=len(payload["taskId"])<=120
                    or payload["decision"] not in ("approve","request_revision")
                    or not isinstance(payload["feedback"],str) or not 1<=len(payload["feedback"])<=10000
                    or not isinstance(payload["requiredChanges"],list) or len(payload["requiredChanges"])>30
                    or any(not isinstance(x,str) or not 1<=len(x)<=2000 for x in payload["requiredChanges"])):
                raise ValueError("invalid submission review payload")
        if action=="artifact_upload":
            if (not isinstance(payload,dict) or set(payload)!={"filename","mediaType","contentBase64"}
                    or not isinstance(payload["filename"],str) or not 1<=len(payload["filename"])<=100
                    or payload["mediaType"] not in ("text/plain","text/markdown","application/json",
                                                   "application/pdf","image/png","image/jpeg")
                    or not isinstance(payload["contentBase64"],str)
                    or not 1<=len(payload["contentBase64"])<=2800000):
                raise ValueError("invalid project artifact body")
            try:
                binary=base64.b64decode(payload["contentBase64"],validate=True)
            except (ValueError,binascii.Error):
                raise ValueError("invalid artifact base64") from None
            if not 1<=len(binary)<=2*1024*1024:
                raise ValueError("artifact exceeds 2MiB limit")
        if action=="project_decision":
            if (not isinstance(payload,dict) or set(payload)!={"expectedPlannerEpoch","content"}
                    or type(payload["expectedPlannerEpoch"]) is not int or payload["expectedPlannerEpoch"]<=0
                    or not isinstance(payload["content"],str) or not 1<=len(payload["content"])<=10000):
                raise ValueError("invalid project decision payload")
        if action=="ack" and (set(payload)!={"ids"} or not isinstance(payload["ids"],list) or not 1<=len(payload["ids"])<=100 or any(type(n)is not int or n<=0 or n>9007199254740991 for n in payload["ids"])):
            raise ValueError("invalid ACK payload")
        if action=="read":
            if set(payload)!={"cursor"}:
                raise ValueError("invalid read payload")
            cursor(payload["cursor"])
        encoded=canonical(payload)
        sha=hashlib.sha256(encoded.encode()).hexdigest()
        ident=new_id()
        with self.db.transaction() as c:
            bound=c.execute("SELECT value FROM meta WHERE key='actor'").fetchone()
            if bound and actor!=bound[0]:
                raise ValueError("outbox action actor does not match verified CrewQuay actor; producer is audit-only")
            row=c.execute("SELECT * FROM outbox WHERE logical_key=?",(logical,)).fetchone()
            if row:
                if (row["actor"],row["action"],row["target"],row["payload_hash"])!=(actor,action,target,sha):
                    raise ValueError("logical action has different immutable request")
                return row["id"]
            # New model-authored business effects must not bypass the
            # Scheduler merely by omitting --schedule-generation. The Hook's
            # receipt/read compensation remains a separate system lane.
            # Existing immutable logical keys are returned above, preserving
            # readback after a yielded generation.
            if schedule_context is None and action not in ("ack","read"):
                adopted=c.execute("SELECT 1 FROM schedule_activities LIMIT 1").fetchone()
                if adopted:
                    raise StaleLease("new business action requires a live scheduler generation")
            # A genuinely new remote action must be fenced in the SAME
            # transaction as insertion. Existing immutable actions are looked
            # up first, even if the original activity has since yielded.
            if schedule_context is not None:
                if (not isinstance(schedule_context,dict)
                        or set(schedule_context)!={"id","owner","generation"}
                        or not isinstance(schedule_context["id"],str)
                        or not isinstance(schedule_context["owner"],str)
                        or type(schedule_context["generation"]) is not int):
                    raise ValueError("invalid schedule fencing context")
                live=c.execute("""SELECT id,kind,work_id FROM schedule_activities
                    WHERE id=? AND status='running' AND lease_owner=?
                    AND lease_version=? AND lease_until>? AND preempt_requested=0""",
                    (schedule_context["id"],schedule_context["owner"],
                     schedule_context["generation"],now)).fetchone()
                if live is None:
                    raise StaleLease("scheduler generation is suspended, preempt requested, stale or expired")
                if live["work_id"] and action not in ("ack","read"):
                    work=c.execute("""SELECT 1 FROM work_items WHERE id=?
                        AND status='running' AND lease_owner=?
                        AND lease_until>?""",
                        (live["work_id"],schedule_context["owner"],now)).fetchone()
                    if work is None:
                        raise StaleLease("scheduled work needs a live matching Queue lease")
                if live["kind"]=="social":
                    intent=c.execute("""SELECT resource_type,resource_id,action
                        FROM social_intents WHERE activity_id=?""",
                        (schedule_context["id"],)).fetchone()
                    # Phase 6.1 also supports manually registered, general
                    # social activities. Preserve their existing Outbox
                    # semantics; tighten only opportunities issued by the
                    # new bounded discovery component and admission ledger.
                    if intent is not None:
                        if (action!="platform_action" or
                                payload["action"]!=intent["action"] or
                                payload["resource"]!={"type":intent["resource_type"],
                                                      "id":intent["resource_id"]}):
                            raise StaleLease("social opportunity may stage only its registered action and resource")
                        # Exactly one distinct remote effect per discovered
                        # opportunity, even if the model retries its plan.
                        if c.execute("""SELECT 1 FROM scheduled_outbox
                             WHERE schedule_id=? LIMIT 1""",
                             (schedule_context["id"],)).fetchone():
                            raise StaleLease("social opportunity already staged its one permitted action")
            if action in ("deployment_syn","deployment_ack","deployment_bug"):
                # The exact deployment's specific human_action Queue lease is
                # mandatory even before the Scheduler was adopted. A generic
                # maintenance/social activity cannot forge control messages.
                col=("syn_work_id" if action=="deployment_syn" else
                     "ack_work_id" if action=="deployment_ack" else "bug_work_id")
                run=c.execute("SELECT * FROM deployment_runs WHERE handshake_id=? AND actor=?",
                              (target,actor)).fetchone()
                phase={"deployment_syn":"syn","deployment_ack":"ack",
                       "deployment_bug":"bug"}[action]
                expected_states=({"deployment_syn":("awaiting_syn","syn_staged"),
                                  "deployment_ack":("syn_ack_received","ack_staged"),
                                  "deployment_bug":("failed","expired")})[action]
                if (not run or logical!="deployment:"+run["id"]+":"+phase
                        or run["state"] not in expected_states
                        or schedule_context is None or
                        live["work_id"]!=run[col] or
                        (action=="deployment_syn" and
                         (payload["runId"]!=run["run_id"] or payload["nonce"]!=run["nonce"])) or
                        (action=="deployment_ack" and
                         (payload["nonce"]!=run["nonce"] or payload["challenge"]!=run["challenge"]
                          or payload["synAckMessageId"]!=run["syn_ack_message_id"] or
                          payload["eventId"]!=run["syn_ack_event_id"] or
                          payload["eventCursor"]!=run["syn_ack_event_cursor"])) or
                        (action=="deployment_bug" and
                         (run["state"] not in ("failed","expired") or
                          run["failure_code"] not in payload["title"] or
                          run["run_id"] not in payload["stepsToReproduce"]))):
                    raise StaleLease("deployment action requires its own live fenced work and exact platform evidence")
            c.execute("""INSERT INTO outbox(id,actor,action,target,logical_key,idem_key,payload,payload_hash,status,created_at,next_attempt_at)
                VALUES(?,?,?,?,?,?,?,?,'prepared',?,?)""",(ident,actor,action,target,logical,new_id(),encoded,sha,now,now))
            if producer_id is not None:
                c.execute("""INSERT INTO outbox_producers(outbox_id,producer_id,recorded_at)
                    VALUES(?,?,?)""",(ident,producer_id,now))
            if schedule_context is not None:
                c.execute("""INSERT INTO scheduled_outbox(outbox_id,schedule_id,lease_generation,owner,staged_at)
                    VALUES(?,?,?,?,?)""",(ident,schedule_context["id"],
                        schedule_context["generation"],schedule_context["owner"],now))
            return ident

    def recover(self, now, lease_seconds):
        with self.db.transaction() as c:
            c.execute("""UPDATE outbox SET status='unknown',error='sender interrupted; reconcile remote result',
                dispatch_owner=NULL WHERE status='sending' AND dispatch_at<=?""",(now-lease_seconds,))

    def verified_page(self, ident, next_cursor):
        next_cursor=cursor(next_cursor)
        with self.db.transaction() as c:
            old=c.execute("SELECT verify_cursor FROM outbox WHERE id=? AND status IN ('sending','unknown')",(ident,)).fetchone()
            if old and int(next_cursor)>=int(old[0]):
                c.execute("UPDATE outbox SET verify_cursor=? WHERE id=?",(next_cursor,ident))

    def process(self, now, can_send, send, verify, limit=3, action_id=None, lane="any"):
        """Process up to limit due actions without allowing blocked rows to starve
        later authorized work. An action_id narrows an immediate drain to one
        immutable request. The scan is bounded even under poisoned backlogs.
        """
        if type(limit) is not int or not 1<=limit<=50:
            raise ValueError("limit must be 1..50")
        if action_id is not None and (not isinstance(action_id,str) or not action_id):
            raise ValueError("nonempty Outbox ID required")
        if lane not in ("any","chat","notifications","collaboration","tasks","platform"):
            raise ValueError("unsupported action lane")
        report=[]
        scan_limit=max(100,limit*20)
        selection=" AND id=?" if action_id is not None else ""
        scope={"any":"","chat":" AND action='send'",
               "notifications":" AND action IN ('ack','read')",
               "collaboration":" AND action IN ('project_message','task_thread_reply','room_message')",
               "tasks":" AND action IN ('task_claim','task_progress','task_submit','submission_review','project_decision','artifact_upload')",
               "platform":" AND action='platform_action'"}[lane]
        params=(now,action_id,scan_limit) if action_id is not None else (now,scan_limit)
        rows=self.db.read("""SELECT * FROM outbox
            WHERE status IN ('prepared','retry_wait','unknown')
            AND next_attempt_at<=?"""+selection+scope+
            " ORDER BY created_at LIMIT ?",params)
        consumed=0
        for row in rows:
            row["payload"]=json.loads(row["payload"])
            if row["status"]=="unknown":
                # This never retries the POST: only readback/reconciliation.
                try:
                    result=verify(row,None)
                except Exception:
                    result=None
                if result and result.get("proven_absent") is True:
                    # Complete readback proved no remote write: the SAME
                    # immutable request/key becomes eligible again.
                    try:
                        self.resolve(row["id"],result,now,proven_absent=True)
                    except ValueError:
                        pass
                elif result:
                    self._confirmed(row["id"],result,verified_at=now)
                else:
                    with self.db.transaction() as c:
                        c.execute("UPDATE outbox SET next_attempt_at=? WHERE id=? AND status='unknown'",
                                  (now+300,row["id"]))
                consumed+=1
            else:
                allowed,reason=can_send(row)
                if not allowed:
                    self.db.alert("outbox_block:"+row["id"],"outbox_blocked",
                                  {"id":row["id"],"reason":reason},now)
                    # Defer without altering actor/target/idempotency.
                    # A rejected head must not starve later authorized items.
                    with self.db.transaction() as c:
                        c.execute("""UPDATE outbox SET next_attempt_at=?
                            WHERE id=? AND status IN ('prepared','retry_wait')
                            AND next_attempt_at<=?""",(now+300,row["id"],now))
                    report.append({"id":row["id"],"action":row["action"],
                                   "status":"blocked"})
                    continue
                token=new_id()
                with self.db.transaction() as c:
                    won=c.execute("""UPDATE outbox SET status='sending',
                        dispatch_owner=?,dispatch_at=?,attempts=attempts+1
                        WHERE id=? AND status IN ('prepared','retry_wait')
                        AND next_attempt_at<=?""",
                        (token,now,row["id"],now)).rowcount
                if not won:
                    continue
                consumed+=1
                dispatched=True
                try:
                    response=send(row)
                except NotDispatched as exc:
                    dispatched=False
                    response=None
                    self._result(row["id"],token,"retry_wait",now+60,
                                 "not dispatched: "+str(exc)[:200])
                except Exception:
                    response=None
                if not dispatched:
                    pass
                elif response and 200<=response.status<300:
                    # Save remote identity BEFORE readback: ambiguous crashes
                    # must be verified by ID and not blindly reposted.
                    if isinstance(response.body,dict):
                        message=response.body.get("message",response.body)
                        remote=message.get("messageId") or message.get("id") or message.get("bugId")
                        if remote:
                            with self.db.transaction() as c:
                                c.execute("UPDATE outbox SET remote_id=? WHERE id=? AND dispatch_owner=?",
                                          (str(remote),row["id"],token))
                    try:
                        result=verify(row,response.body)
                    except Exception:
                        result=None
                    if result and result.get("proven_absent") is not True:
                        self._confirmed(row["id"],result,token,verified_at=now)
                    else:
                        self._result(row["id"],token,"unknown",now+300,
                                     "success response unconfirmed")
                elif response and response.status==429:
                    # A definitive server-side rate rejection has not
                    # committed the requested business write. Retain the SAME
                    # immutable key/payload and honor the bounded retry delay.
                    self._result(row["id"],token,"retry_wait",
                                 now+max(30,response.retry_after),
                                 "HTTP 429 rate limited; retry same key")
                elif response and response.status in (400,401,403,404,409,422):
                    error=response.body.get("error") if isinstance(response.body,dict) else None
                    code=error.get("code") if isinstance(error,dict) else None
                    suffix=" DUPLICATE_SOCIAL_CONTENT" if code=="DUPLICATE_SOCIAL_CONTENT" else ""
                    self._result(row["id"],token,"permanent_failed",now,
                                 "HTTP "+str(response.status)+suffix)
                else:
                    self._result(row["id"],token,"unknown",now+300,
                                 "ambiguous remote result")
            state=self.db.read("SELECT status FROM outbox WHERE id=?",(row["id"],))
            report.append({"id":row["id"],"action":row["action"],
                           "status":state[0]["status"] if state else "missing"})
            if consumed>=limit:
                break
        return report

    def _confirmed(self, ident, evidence, token=None, verified_at=None):
        with self.db.transaction() as c:
            sql="""UPDATE outbox SET status='confirmed',result=?,remote_id=?,
                verified_at=?,dispatch_owner=NULL,error=NULL WHERE id=?"""
            args=[canonical(evidence),str(evidence.get("id","")),verified_at,ident]
            if token:
                sql+=" AND dispatch_owner=? AND status='sending'"
                args.append(token)
            else:
                sql+=" AND status='unknown'"
            if evidence.get("unclaimed") is True:
                # Readback without a stored remote ID may bind only a message
                # no other Outbox row already owns (checked atomically).
                sql+=" AND NOT EXISTS (SELECT 1 FROM outbox o WHERE o.remote_id=? AND o.id<>?)"
                args+=[str(evidence.get("id","")),ident]
            c.execute(sql,args)

    def _result(self, ident, token, status, next_at, error):
        with self.db.transaction() as c:
            c.execute("""UPDATE outbox SET status=?,next_attempt_at=?,error=?,dispatch_owner=NULL
                WHERE id=? AND dispatch_owner=? AND status='sending'""",(status,next_at,error,ident,token))
        if status in ("unknown","permanent_failed"):
            self.db.alert("outbox:"+ident+":"+status,status,{"outbox_id":ident,"reason":error},next_at)

    def resolve(self, ident, evidence, now, confirmed=False, proven_absent=False):
        if not isinstance(evidence,dict) or not evidence or confirmed==proven_absent:
            raise ValueError("choose one evidence-backed resolution")
        with self.db.transaction() as c:
            n=c.execute("""UPDATE outbox SET status=?,result=?,next_attempt_at=?,
                verified_at=?,error=NULL
                WHERE id=? AND status='unknown'""",
                ("confirmed" if confirmed else "prepared",canonical(evidence),now,
                 now if confirmed else None,ident)).rowcount
            if n!=1:
                raise ValueError("unknown Outbox item required")
