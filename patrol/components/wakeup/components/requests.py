"""Durable wakeup intent outbox. NEVER calls a model, Cron, Hook or remote API."""
import json
from ...persistence.index import canonical, new_id, StaleLease
from ...scheduling.index import activity_priority


class Wakeups:
    def __init__(self, db, lease_seconds=120):
        self.db=db
        self.lease_seconds=lease_seconds

    def stage(self, now, threshold=70, limit=50):
        if type(threshold) is not int or not 0<=threshold<=100 or type(limit) is not int or not 1<=limit<=200:
            raise ValueError("invalid wakeup threshold/limit")
        added=0
        with self.db.transaction() as c:
            # Only actual agent-decision work, not mere uninspected messages.
            # Blocked/denied work cannot be executed; only due runnable work wakes Muse.
            works=c.execute("""SELECT w.id,w.priority,w.start_by,w.due_at,e.source,
                    a.status AS activity_status,a.base_priority,a.deadline,
                    a.ready_since,a.virtual_runtime,a.switch_cost
                FROM work_items w LEFT JOIN events e ON e.id=w.event_id
                LEFT JOIN schedule_activities a ON a.work_id=w.id
                WHERE w.type='human_action'
                  AND w.status IN ('pending','retry_wait')
                  AND w.next_action_at<=?
                  AND (w.priority>=? OR
                       (w.start_by IS NOT NULL AND w.start_by<=?) OR
                       (w.due_at IS NOT NULL AND w.due_at<=?) OR
                       (a.status IN ('ready','suspended')
                        AND w.due_at IS NOT NULL AND a.deadline<=?))
                  AND (w.event_id IS NULL OR e.source IN ('messages','agent_events'))
                  AND NOT EXISTS(SELECT 1 FROM promises p WHERE p.work_item_id=w.id)
                ORDER BY w.priority DESC,w.created_at LIMIT ?""",
                (now,threshold,now,now,now+3600,min(800,max(limit*4,limit)))).fetchall()
            for w in works:
                # Use the same dynamic deadline/fairness policy as Scheduler;
                # a low base score can become urgent near its real deadline.
                effective=max(float(w["priority"]),activity_priority(w,now)) if (
                    w["activity_status"] in ("ready","suspended")
                ) else float(w["priority"])
                due=bool(w["priority"]>=threshold or
                         (w["start_by"] is not None and w["start_by"]<=now) or
                         (w["due_at"] is not None and w["due_at"]<=now) or
                         (w["deadline"] is not None and w["deadline"]<=now+3600))
                if effective<threshold and not due:
                    continue
                if effective<threshold:
                    effective=threshold
                effective=min(100,max(0,int(round(effective))))
                k="work:"+w["id"]
                n=c.execute("""INSERT INTO wakeup_requests(
                    id,logical_key,source_type,source_id,priority,reason,created_at,next_attempt_at)
                    VALUES(?,?,'work',?,?,?, ?,?)
                    ON CONFLICT(logical_key) DO NOTHING""",
                    (new_id(),k,w["id"],effective,
                     "authorized agent decision needed; not platform approval",now,now)).rowcount
                added+=n
            # Only due or near-due promises, never every dormant future promise.
            rows=c.execute("""SELECT id,next_action_at,status FROM promises
                WHERE status!='done' AND (next_action_at<=? OR
                    (first_action_at IS NULL AND start_by<=?) OR
                    (due_at IS NOT NULL AND due_at<=?))
                ORDER BY next_action_at LIMIT ?""",(now+120,now,now,limit)).fetchall()
            for p in rows:
                # A new next-action deadline creates a new logical signal after progress.
                k="promise:"+p["id"]+":"+str(p["next_action_at"])
                n=c.execute("""INSERT INTO wakeup_requests(
                    id,logical_key,source_type,source_id,priority,reason,created_at,next_attempt_at)
                    VALUES(?,?,'promise',?,100,?, ?,?)
                    ON CONFLICT(logical_key) DO NOTHING""",
                    (new_id(),k,p["id"],"next promise action due",now,now)).rowcount
                added+=n
        return added

    def stage_social(self, now, activity_id):
        """Put ONE already-admitted, idle social activity on the SAME wake queue.

        No model invocation and no outbound API call. A prior accepted or
        ambiguous social wake blocks further social wakeups until the
        earlier activity terminates, preventing stacked idle wake storms.
        """
        with self.db.transaction() as c:
            running=c.execute("SELECT 1 FROM schedule_activities "
                              "WHERE status='running' LIMIT 1").fetchone()
            if running:
                return 0
            outstanding=c.execute("""SELECT 1 FROM wakeup_requests w
                JOIN schedule_activities a ON a.id=w.source_id
                WHERE w.source_type='social'
                  AND w.status IN ('pending','retry_wait','leased','unknown','accepted')
                  AND a.status IN ('ready','running','suspended')
                LIMIT 1""").fetchone()
            if outstanding:
                return 0
            act=c.execute("""SELECT a.id,s.selection_score FROM schedule_activities a
                JOIN social_intents s ON s.activity_id=a.id
                WHERE a.id=? AND a.status IN ('ready','suspended')
                  AND NOT EXISTS (SELECT 1 FROM wakeup_requests w
                                  WHERE w.source_type='social' AND w.source_id=a.id)
                LIMIT 1""",(activity_id,)).fetchone()
            if not act:
                return 0
            return c.execute("""INSERT INTO wakeup_requests(
                id,logical_key,source_type,source_id,priority,reason,
                created_at,next_attempt_at)
                VALUES(?,?,'social',?,?,?, ?,?)
                ON CONFLICT(logical_key) DO NOTHING""",
                (new_id(),"social:"+act["id"],act["id"],
                 min(35,act["selection_score"]),
                 "bounded idle social activity for host decision",now,now)).rowcount

    def _source_alive(self, c, row):
        if row["source_type"]=="work":
            return bool(c.execute("""SELECT 1 FROM work_items WHERE id=?
                AND type='human_action' AND status IN
                ('pending','retry_wait')""",
                (row["source_id"],)).fetchone())
        if row["source_type"]=="social":
            return bool(c.execute("""SELECT 1 FROM schedule_activities a
                JOIN social_intents s ON s.activity_id=a.id
                WHERE a.id=? AND a.status IN ('ready','suspended')
                  AND a.logical_key IS NOT NULL""",
                (row["source_id"],)).fetchone())
        if row["source_type"]=="promise":
            return bool(c.execute("""SELECT 1 FROM promises WHERE id=? AND status!='done'
                AND ('promise:'||id||':'||CAST(next_action_at AS TEXT))=?""",
                (row["source_id"],row["logical_key"])).fetchone())
        return False

    def export(self, now, limit=50):
        if type(limit) is not int or not 1<=limit<=200:
            raise ValueError("limit must be 1..200")
        rows=self.db.read("""SELECT id,logical_key,source_type,source_id,priority,reason,
            status,created_at,next_attempt_at,lease_until,attempts
            FROM wakeup_requests WHERE status IN ('pending','retry_wait','unknown','leased')
            ORDER BY CASE WHEN status IN ('pending','retry_wait') AND next_attempt_at<=? THEN 0 ELSE 1 END,
            priority DESC,created_at LIMIT ?""",(now,limit))
        for row in rows:
            row["claimable"]=row["status"] in ("pending","retry_wait") and row["next_attempt_at"]<=now
        return {"mode":"read_only_no_host_dispatch","generated_at":now,
                "warning":"A wakeup intention is not a host wakeup, worker execution, or authorization.",
                "requests":rows}

    def recover(self, now, detail=False):
        """Reconcile expired delivery leases against the CURRENT work state.

        Once the business work is completed (or a promise was closed/rekeyed),
        there is no remaining wakeup to deliver: retire the expired lease as
        obsolete. Otherwise, delivery may have occurred before a crash and
        remains unknown; NEVER silently retry that request.
        """
        totals={"unknown":0,"obsolete":0}
        with self.db.transaction() as c:
            rows=c.execute("""SELECT * FROM wakeup_requests
                WHERE status='leased' AND (lease_until IS NULL OR lease_until<=?)""",
                (now,)).fetchall()
            for row in rows:
                new_status="unknown" if self._source_alive(c,row) else "obsolete"
                count=c.execute("""UPDATE wakeup_requests SET status=?,
                    lease_owner=NULL,lease_until=NULL WHERE id=? AND status='leased'
                    AND (lease_until IS NULL OR lease_until<=?)""",
                    (new_status,row["id"],now)).rowcount
                totals[new_status]+=count
        return totals if detail else totals["unknown"]+totals["obsolete"]

    def claim(self, ident, owner, now):
        if not ident or not isinstance(owner,str) or not owner.strip():
            raise ValueError("specific ID and nonempty worker identity required")
        with self.db.transaction() as c:
            row=c.execute("SELECT * FROM wakeup_requests WHERE id=?",(ident,)).fetchone()
            if not row:
                raise ValueError("unknown wakeup request")
            if not self._source_alive(c,row):
                # Never report "obsolete" while an active delivery lease
                # remains persisted. An expired lease may be safely retired
                # because the underlying business work is already complete.
                if row["status"]=="obsolete":
                    return {"id":ident,"status":"obsolete","host_called":False}
                n=c.execute("""UPDATE wakeup_requests SET status='obsolete',
                    lease_owner=NULL,lease_until=NULL WHERE id=?
                    AND (status IN ('pending','retry_wait','unknown')
                         OR (status='leased' AND
                             (lease_until IS NULL OR lease_until<=?)))""",
                    (ident,now)).rowcount
                if n:
                    return {"id":ident,"status":"obsolete","host_called":False}
                raise StaleLease("active or confirmed wakeup cannot be retired by claim")
            n=c.execute("""UPDATE wakeup_requests SET status='leased',lease_owner=?,
                lease_until=?,lease_version=lease_version+1,attempts=attempts+1
                WHERE id=? AND status IN ('pending','retry_wait') AND next_attempt_at<=?""",
                (owner,now+self.lease_seconds,ident,now)).rowcount
            if n!=1:
                raise StaleLease("already delivered, unknown, not due, or leased")
            x=dict(c.execute("SELECT * FROM wakeup_requests WHERE id=?",(ident,)).fetchone())
            return {"id":x["id"],"lease_owner":x["lease_owner"],
                    "lease_version":x["lease_version"],"lease_until":x["lease_until"],
                    "source_type":x["source_type"],"source_id":x["source_id"],
                    "host_called":False}

    def claim_next(self, owner, now, limit=100, allow_social=False,
                   social_activity_id=None, focus_work_id=None):
        """Atomically reserve the highest-priority due host signal.

        Unlike export -> claim, selection and lease acquisition happen inside
        one SQLite transaction, so overlapping Hook turns cannot duplicate a
        dispatch. This does not call the host or assert delivery.
        """
        if (not isinstance(owner,str) or not 1<=len(owner)<=128
                or type(limit) is not int or not 1<=limit<=200):
            raise ValueError("bounded host owner and selection limit required")
        with self.db.transaction() as c:
            # Retire obsolete intents independently of Scheduler's focused
            # selection; a finished work must not remain pending forever.
            candidates=c.execute("""SELECT * FROM wakeup_requests
                WHERE status IN ('pending','retry_wait') AND next_attempt_at<=?
                ORDER BY created_at,id LIMIT 100""",(now,)).fetchall()
            for old in candidates:
                if not self._source_alive(c,old):
                    c.execute("""UPDATE wakeup_requests SET status='obsolete',
                        lease_owner=NULL,lease_until=NULL WHERE id=?
                        AND status IN ('pending','retry_wait')""",(old["id"],))
            rows=c.execute("""SELECT * FROM wakeup_requests
                WHERE status IN ('pending','retry_wait') AND next_attempt_at<=?
                  AND (source_type!='social' OR
                       (? AND source_id=?))
                  AND (? IS NULL OR (source_type='work' AND source_id=?))
                ORDER BY priority DESC,created_at,id LIMIT ?""",
                (now,1 if allow_social else 0,social_activity_id or "",
                 focus_work_id,focus_work_id,limit)).fetchall()
            for row in rows:
                if not self._source_alive(c,row):
                    c.execute("""UPDATE wakeup_requests SET status='obsolete',
                        lease_owner=NULL,lease_until=NULL
                        WHERE id=? AND status IN ('pending','retry_wait')""",
                        (row["id"],))
                    continue
                # A future retry is not executable even if an earlier wakeup
                # intent remained due. Preserve it for its actual retry time.
                if row["source_type"]=="work":
                    source=c.execute("SELECT status,next_action_at FROM work_items WHERE id=?",
                                     (row["source_id"],)).fetchone()
                    if source and source["status"]=="retry_wait" and source["next_action_at"]>now:
                        continue
                n=c.execute("""UPDATE wakeup_requests SET status='leased',
                    lease_owner=?,lease_until=?,lease_version=lease_version+1,
                    attempts=attempts+1
                    WHERE id=? AND status IN ('pending','retry_wait')
                    AND next_attempt_at<=?""",
                    (owner,now+self.lease_seconds,row["id"],now)).rowcount
                if n:
                    leased=c.execute("SELECT * FROM wakeup_requests WHERE id=?",
                                     (row["id"],)).fetchone()
                    return {"id":leased["id"],"source_type":leased["source_type"],
                            "source_id":leased["source_id"],"lease_owner":owner,
                            "lease_version":leased["lease_version"],
                            "lease_until":leased["lease_until"],"host_called":False}
        return None

    def prepare_best_effort(self, source_id, now, capability_source="hatch-native-wake/v1"):
        """Durably throttle a wake *intention* without consuming the Host ACK lease.

        Wake may crash before/after a real call, so the outcome is always
        unknown until the worker starts. No fake accepted Host receipt is made.
        Old unknown delivery rows remain untouched and can be used to resume.
        """
        if not isinstance(source_id,str) or not source_id or not isinstance(capability_source,str) or not capability_source:
            raise ValueError("valid host intent source and capability origin required")
        with self.db.transaction() as c:
            work=c.execute("""SELECT status,next_action_at FROM work_items
                WHERE id=? AND type='human_action'""",(source_id,)).fetchone()
            if not work or work["status"] not in ("pending","retry_wait") or work["next_action_at"]>now:
                return None
            row=c.execute("""SELECT * FROM wakeup_requests
                WHERE source_type='work' AND source_id=?
                  AND status IN ('pending','retry_wait','unknown')
                ORDER BY created_at LIMIT 1""",(source_id,)).fetchone()
            if not row or (row["status"]=="retry_wait" and row["next_attempt_at"]>now):
                return None
            prev=c.execute("SELECT * FROM host_wakeup_notifications WHERE wakeup_id=?",
                           (row["id"],)).fetchone()
            if prev and (prev["next_notice_at"]>now or prev["worker_started_at"] is not None):
                return None
            # One durable unique key per request. Overlapping Hooks cannot
            # issue duplicate advice; the next call stays cooled down.
            gen=prev["generation"]+1 if prev else 1
            attempts=prev["attempts"]+1 if prev else 1
            delay=min(900,60*(2**min(4,attempts-1)))
            c.execute("""INSERT INTO host_wakeup_notifications(
                wakeup_id,generation,attempts,intent_at,next_notice_at,
                capability,capability_source,outcome)
                VALUES(?,?,?,?,?,'best_effort',?,'unknown')
                ON CONFLICT(wakeup_id) DO UPDATE SET
                generation=excluded.generation,attempts=excluded.attempts,
                intent_at=excluded.intent_at,next_notice_at=excluded.next_notice_at,
                capability_source=excluded.capability_source,outcome='unknown'""",
                (row["id"],gen,attempts,now,now+delay,capability_source))
            return {"wakeup_id":row["id"],"source_id":source_id,
                    "source_type":"work","lease_version":gen,
                    "notification_generation":gen,"next_notice_at":now+delay,
                    "notification_attempt":attempts,
                    "notification_outcome":"unknown",
                    "host_capability":"best_effort",
                    "capability_source":capability_source,
                    "requires_host_receipt":False,"host_called":False,
                    "host_acceptance_verified":False}

    @staticmethod
    def _worker_intent_valid(c, wake, generation):
        if wake["status"]=="accepted" and wake["lease_version"]==generation:
            return "reliable_ack"
        if wake["source_type"]!="work" or wake["status"] not in ("pending","retry_wait","unknown"):
            return None
        notice=c.execute("""SELECT generation FROM host_wakeup_notifications
            WHERE wakeup_id=? AND capability='best_effort'""",(wake["id"],)).fetchone()
        return "best_effort" if notice and notice["generation"]==generation else None

    def acknowledge(self, ident, owner, version, now, result, evidence):
        if result not in ("accepted","confirmed_failed","unknown"):
            raise ValueError("invalid dispatch result")
        if not isinstance(evidence,dict) or not evidence:
            raise ValueError("host receipt or failure evidence required")
        dest={"accepted":"accepted","confirmed_failed":"retry_wait","unknown":"unknown"}[result]
        retry=now+60 if result=="confirmed_failed" else now
        with self.db.transaction() as c:
            n=c.execute("""UPDATE wakeup_requests SET status=?,dispatch_evidence=?,
                confirmed_at=?,next_attempt_at=?,lease_owner=NULL,lease_until=NULL
                WHERE id=? AND status='leased' AND lease_owner=?
                AND lease_version=? AND lease_until>?""",
                (dest,canonical(evidence),now,retry,ident,owner,version,now)).rowcount
            if n!=1:
                raise StaleLease("wakeup delivery lease expired or superseded")
        return {"id":ident,"status":dest,"business_work_completed":False}

    def reconcile_unknown(self, ident, now, proven_absent, evidence):
        if proven_absent is not True or not isinstance(evidence,dict) or not evidence:
            raise ValueError("explicit verified non-delivery proof required")
        with self.db.transaction() as c:
            n=c.execute("""UPDATE wakeup_requests SET status='retry_wait',next_attempt_at=?,
                dispatch_evidence=? WHERE id=? AND status='unknown'""",
                (now,canonical(evidence),ident)).rowcount
            if n!=1:
                raise ValueError("unknown delivery state required")
        return {"id":ident,"status":"retry_wait","host_called":False}


    def agent_intent_tx(self,c,ident,generation):
        """Return only a currently valid reliable ACK or best-effort intent."""
        wake=c.execute("SELECT * FROM wakeup_requests WHERE id=?",(ident,)).fetchone()
        mode=self._worker_intent_valid(c,wake,generation) if wake else None
        if not mode:
            raise StaleLease("Host receipt or notification intent superseded")
        return {**dict(wake),"host_capability":mode}

    def agent_started_tx(self,c,ident,generation,worker,activity_id,
                         activity_generation):
        """Check existing independent Worker start/progress; no Host ACK inferred."""
        row=c.execute("""SELECT * FROM wakeup_execution_receipts WHERE wakeup_id=?""",
                      (ident,)).fetchone()
        if (not row or row["status"] not in ("started","progress")
                or row["worker"]!=worker or row["activity_id"]!=activity_id
                or row["wakeup_generation"]!=generation
                or row["activity_generation"]>activity_generation):
            raise StaleLease("missing current verified Worker start/progress")
        return dict(row)

    def agent_heartbeat_tx(self,c,ident,generation,worker,activity_id,
                           activity_generation,work_lease_version,now,evidence):
        """Persist monotonic liveness without advancing business progress."""
        self.agent_started_tx(c,ident,generation,worker,activity_id,activity_generation)
        prev=c.execute("SELECT * FROM worker_lease_heartbeats WHERE wakeup_id=?",
                       (ident,)).fetchone()
        if prev and (prev["worker"]!=worker or
                     prev["wakeup_generation"]!=generation or
                     prev["activity_id"]!=activity_id or
                     prev["activity_generation"]>activity_generation or
                     prev["work_lease_version"]!=work_lease_version or
                     evidence["observation_seq"]<=prev["observation_seq"] or
                     evidence["host_session"]!=json.loads(prev["evidence"])["host_session"]):
            raise StaleLease("stale, replayed or different Host liveness observation")
        c.execute("""INSERT INTO worker_lease_heartbeats(
            wakeup_id,worker,wakeup_generation,activity_id,activity_generation,
            work_lease_version,observation_seq,last_observed_at,evidence)
            VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(wakeup_id) DO UPDATE SET
            activity_generation=excluded.activity_generation,
            observation_seq=excluded.observation_seq,
            last_observed_at=excluded.last_observed_at,evidence=excluded.evidence""",
            (ident,worker,generation,activity_id,activity_generation,
             work_lease_version,evidence["observation_seq"],now,canonical(evidence)))

    def agent_finish_tx(self,c,ident,generation,worker,activity_id,
                        activity_generation,now,evidence,remote_ids,host_capability):
        """Finish Worker receipt and retire best-effort advice atomically."""
        self.agent_started_tx(c,ident,generation,worker,activity_id,activity_generation)
        n=c.execute("""UPDATE wakeup_execution_receipts SET status='finished',
            activity_generation=?,updated_at=?,finished_at=?,evidence=?
            WHERE wakeup_id=? AND worker=? AND status IN ('started','progress')""",
            (activity_generation,now,now,
             canonical({"work_id":evidence.get("work_id"),
                        "confirmed_outbox_ids":remote_ids,
                        "decision":evidence.get("decision")}),ident,worker)).rowcount
        if n!=1:
            raise StaleLease("Worker completion receipt lost generation")
        if host_capability=="best_effort":
            n=c.execute("""UPDATE wakeup_requests SET status='obsolete'
                WHERE id=? AND status IN ('pending','retry_wait','unknown')""",
                (ident,)).rowcount
            if n!=1:
                raise StaleLease("best-effort intent changed during Worker completion")

    def record_execution(self, ident, wakeup_generation, activity_id,
                         activity_generation, worker, phase, now, evidence):
        """Worker-owned receipts are separate from HATCH's host acceptance.

        This call does NOT invoke a model or certify a remote platform action.
        Only a live Scheduler lease may attest start/progress. Completion
        requires independently completed Scheduler state.
        """
        if (phase not in ("started","progress","finished")
                or type(wakeup_generation) is not int or wakeup_generation<1
                or type(activity_generation) is not int or activity_generation<1
                or not isinstance(worker,str) or not 1<=len(worker)<=128
                or not isinstance(evidence,dict) or not evidence):
            raise ValueError("bounded execution receipt and worker evidence required")
        payload=canonical(evidence)
        if len(payload.encode("utf-8"))>4096:
            raise ValueError("worker execution evidence exceeds 4 KiB")
        with self.db.transaction() as c:
            wake=c.execute("SELECT * FROM wakeup_requests WHERE id=?",(ident,)).fetchone()
            if not wake or not self._worker_intent_valid(c,wake,wakeup_generation):
                raise StaleLease("current reliable Host receipt or best-effort worker intent required")
            activity=c.execute("SELECT * FROM schedule_activities WHERE id=?",
                               (activity_id,)).fetchone()
            if not activity:
                raise StaleLease("unknown Scheduler activity")
            if wake["source_type"]=="work":
                matched=activity["work_id"]==wake["source_id"]
            elif wake["source_type"]=="social":
                matched=activity["id"]==wake["source_id"]
            else:
                promise=c.execute("SELECT work_item_id FROM promises WHERE id=?",
                                  (wake["source_id"],)).fetchone()
                matched=bool(promise and (
                    activity["work_id"]==promise["work_item_id"] or
                    activity["logical_key"]==
                    "promise_due:"+wake["logical_key"][len("promise:"):]
                ))
            if not matched:
                raise StaleLease("wakeup source does not belong to activity")
            prior=c.execute("SELECT * FROM wakeup_execution_receipts WHERE wakeup_id=?",
                            (ident,)).fetchone()
            if phase in ("started","progress"):
                if (activity["status"]!="running" or activity["lease_owner"]!=worker
                        or activity["lease_version"]!=activity_generation
                        or activity["lease_until"]<=now):
                    raise StaleLease("worker execution needs a live scheduler generation")
            elif (activity["status"]!="completed"
                  or activity["lease_version"]!=activity_generation+1):
                raise StaleLease("completion needs a finished scheduler generation")
            if prior:
                if (prior["worker"]!=worker
                        or prior["activity_id"]!=activity_id
                        or prior["wakeup_generation"]!=wakeup_generation
                        or activity_generation<prior["activity_generation"]
                        or (activity_generation>prior["activity_generation"]
                            and phase!="progress")):
                    raise StaleLease("worker or generation stale; resumed slice must attest progress")
                # After a safe-point yield, the same verified worker may
                # record progress under a *new*, currently live Scheduler
                # generation, without rewriting the original host wakeup.
                if prior["status"]=="finished":
                    return {"wakeup_id":ident,"phase":"finished","already_recorded":True}
                if phase=="started":
                    return {"wakeup_id":ident,"phase":prior["status"],"already_recorded":True}
                c.execute("""UPDATE wakeup_execution_receipts
                    SET status=?,activity_generation=?,updated_at=?,
                        finished_at=?,evidence=?
                    WHERE wakeup_id=?""",
                    (phase,activity_generation,now,
                     now if phase=="finished" else None,payload,ident))
            else:
                if phase!="started":
                    raise ValueError("execution must be started before progress or finish")
                c.execute("""INSERT INTO wakeup_execution_receipts(
                    wakeup_id,wakeup_generation,activity_id,activity_generation,
                    worker,status,started_at,updated_at,evidence)
                    VALUES(?,?,?,?,?,'started',?,?,?)""",
                    (ident,wakeup_generation,activity_id,activity_generation,
                     worker,now,now,payload))
                c.execute("""UPDATE host_wakeup_notifications
                    SET worker_started_at=COALESCE(worker_started_at,?)
                    WHERE wakeup_id=? AND generation=?""",(now,ident,wakeup_generation))
        return {"wakeup_id":ident,"phase":phase,"already_recorded":False,
                "remote_action_confirmed":False}

    def execution_health(self, now, threshold=300, limit=100):
        """Idempotent, non-dispatching audit of stalled accepted wakeups.

        An accepted host wake is never automatically replayed just because
        this watchdog did not observe model/worker progress.
        """
        if (type(threshold) is not int or not 60<=threshold<=86400
                or type(limit) is not int or not 1<=limit<=500):
            raise ValueError("invalid bounded execution health scan")
        counts={"accepted_without_start":0,"started_without_progress":0,
                "new_alerts":0,"host_invoked":False}
        with self.db.transaction() as c:
            rows=c.execute("""SELECT w.*,x.status AS execution_status,
                x.updated_at AS execution_at
                FROM wakeup_requests w
                LEFT JOIN wakeup_execution_receipts x ON x.wakeup_id=w.id
                WHERE w.status='accepted'
                  AND COALESCE(x.updated_at,w.confirmed_at,w.created_at)<=?
                  AND (x.status IS NULL OR x.status!='finished')
                  AND NOT EXISTS(
                      SELECT 1 FROM alerts a
                      WHERE a.logical_key=('wakeup_execution:'||w.id||':'||
                          CAST(w.lease_version AS TEXT)||':'||
                          CASE WHEN x.status IS NULL THEN 'accepted_without_start'
                               ELSE 'started_without_progress' END)
                  )
                ORDER BY COALESCE(x.updated_at,w.confirmed_at,w.created_at),w.id
                LIMIT ?""",(now-threshold,limit)).fetchall()
            for row in rows:
                if not self._source_alive(c,row):
                    continue
                kind=("accepted_without_start" if row["execution_status"] is None
                      else "started_without_progress")
                counts[kind]+=1
                key="wakeup_execution:"+row["id"]+":"+str(row["lease_version"])+":"+kind
                detail={"wakeup_id":row["id"],"source_type":row["source_type"],
                        "source_id":row["source_id"],"reason":kind,
                        "requires_host_reconciliation":True,
                        "auto_retried":False}
                counts["new_alerts"]+=c.execute("""INSERT INTO alerts(
                    logical_key,kind,detail,created_at) VALUES(?,?,?,?)
                    ON CONFLICT(logical_key) DO NOTHING""",
                    (key,kind,canonical(detail),now)).rowcount
        return counts


    def reconcile_accepted_no_start(self, ident, now, proven_not_started, evidence):
        """Operator/host-authenticated absence proof permits a controlled retry.

        An accepted wake cannot be assumed absent from missing heartbeat alone.
        Never downgrade a wake that has any worker execution receipt.
        """
        if (proven_not_started is not True or not isinstance(evidence,dict)
                or not evidence or len(canonical(evidence).encode("utf-8"))>4096):
            raise ValueError("explicit bounded non-execution proof is required")
        with self.db.transaction() as c:
            wake=c.execute("SELECT * FROM wakeup_requests WHERE id=?",
                           (ident,)).fetchone()
            if not wake or wake["status"]!="accepted":
                raise StaleLease("only an accepted wake may be reconciled")
            if c.execute("SELECT 1 FROM wakeup_execution_receipts WHERE wakeup_id=?",
                         (ident,)).fetchone():
                raise StaleLease("worker execution already observed; cannot retry host wake")
            if not self._source_alive(c,wake):
                raise StaleLease("source no longer needs a host wake")
            # Preserve original delivery evidence. Append the independent
            # non-execution proof instead of overwriting the host receipt.
            c.execute("""INSERT INTO alerts(logical_key,kind,detail,created_at)
                VALUES(?,?,?,?)""",
                ("wakeup_reconcile_accepted:"+ident+":"+str(wake["lease_version"]),
                 "accepted_no_start_reconciled",
                 canonical({"wakeup_id":ident,"generation":wake["lease_version"],
                            "absence_proof":evidence}),now))
            c.execute("""UPDATE wakeup_requests SET status='retry_wait',
                next_attempt_at=? WHERE id=? AND status='accepted'""",(now,ident))
        return {"id":ident,"status":"retry_wait","host_called":False,
                "original_host_acceptance_preserved":True}
