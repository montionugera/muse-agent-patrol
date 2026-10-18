import json
from ...persistence.index import canonical, new_id, StaleLease


def enqueue_tx(c, logical, kind, payload, now, priority=0, start_by=None, due_at=None, event_id=None):
    ident = new_id()
    c.execute("""INSERT INTO work_items(id,event_id,logical_key,type,payload,status,priority,created_at,start_by,next_action_at,due_at)
                 VALUES(?,?,?,?,?,'pending',?,?,?,?,?) ON CONFLICT(logical_key) DO NOTHING""",
              (ident, event_id, logical, kind, canonical(payload), priority, now, start_by, now, due_at))
    return c.execute("SELECT id FROM work_items WHERE logical_key=?", (logical,)).fetchone()[0]


class Queue:
    def __init__(self, database, lease_seconds=120):
        self.db = database
        self.lease_seconds = lease_seconds

    def enqueue(self, logical, kind, payload, now, **kwargs):
        with self.db.transaction() as c:
            return enqueue_tx(c, logical, kind, payload, now, **kwargs)

    def recover(self, now):
        """Reclaim expired leases and boundedly repair historical Tick-blocked human work.

        Only the exact legacy reason is eligible: Owner denials, Outbox
        receipts, event cursors and all other blocked reasons are untouched.
        """
        with self.db.transaction() as c:
            c.execute("""UPDATE work_items SET status='pending',lease_owner=NULL,lease_until=NULL,
                         next_action_at=?,block_reason='expired local lease; reconcile Outbox before any remote effect'
                         WHERE status='running' AND lease_until<=?""", (now, now))
            legacy=c.execute("""SELECT id FROM work_items
                WHERE type='human_action' AND status='blocked'
                  AND block_reason='no authorized executor for human_action'
                  AND lease_owner IS NULL
                ORDER BY created_at,id LIMIT 100""").fetchall()
            for row in legacy:
                n=c.execute("""UPDATE work_items SET status='pending',
                    block_reason=NULL,next_action_at=?
                    WHERE id=? AND type='human_action' AND status='blocked'
                    AND block_reason='no authorized executor for human_action'
                    AND lease_owner IS NULL""",(now,row["id"])).rowcount
                if n:
                    c.execute("""INSERT INTO alerts(logical_key,kind,detail,created_at)
                        VALUES(?,?,?,?) ON CONFLICT(logical_key) DO NOTHING""",
                        ("reclassified_human_action:"+row["id"],"work_reclassified",
                         canonical({"work_id":row["id"],"from":"legacy_tick_block",
                                    "to":"pending","remote_action_issued":False}),now))

    def claim(self, owner, now, lane="any"):
        """Highest scored ready work, reserving optional slots for background.

        A capped aging bonus prevents old tasks from dominating fresh high scores.
        The scheduler dedicates every fourth slot to lower-priority backlog.
        """
        if lane not in ("any","background","local_messages"):
            raise ValueError("invalid queue lane")
        background_filter="""
            AND NOT EXISTS (
                SELECT 1 FROM message_scores ms WHERE ms.event_id=work_items.event_id
                AND COALESCE(ms.muse_score,ms.base_score)>=70)
        """ if lane=="background" else (
            """ AND type='inspect_event' AND EXISTS (
                SELECT 1 FROM events e WHERE e.id=work_items.event_id
                 AND e.source IN ('messages','agent_events'))
            """ if lane=="local_messages" else "")
        # Tick has exactly one deterministic handler: inspect_event.
        # Model-decision work is owned by Scheduler -> HATCH -> Muse.
        background_filter=" AND type='inspect_event' "+background_filter
        with self.db.transaction() as c:
            row = c.execute("""SELECT * FROM work_items WHERE status IN ('pending','retry_wait')
                AND next_action_at<=? """+background_filter+"""
                ORDER BY (
                    CASE WHEN EXISTS (
                        SELECT 1 FROM events e
                        WHERE e.id=work_items.event_id AND e.source='messages'
                    ) THEN COALESCE((
                        SELECT COALESCE(ms.muse_score,ms.base_score)
                        FROM message_scores ms WHERE ms.event_id=work_items.event_id
                    ),20)
                    ELSE priority END
                    + MIN(20,MAX(0,CAST((? - created_at)/600 AS INTEGER)))
                ) DESC, created_at ASC LIMIT 1""",(now,now)).fetchone()
            if not row:
                return None
            c.execute("""UPDATE work_items SET status='running',lease_owner=?,lease_until=?,
                         lease_version=lease_version+1,attempts=attempts+1 WHERE id=?""",
                      (owner, now+self.lease_seconds, row["id"]))
            result = dict(c.execute("SELECT * FROM work_items WHERE id=?", (row["id"],)).fetchone())
            result["payload"] = json.loads(result["payload"])
            return result

    def quarantine_unknown(self, now, limit=25):
        """Boundedly flag truly unsupported work kinds; never touch Muse work."""
        if type(limit) is not int or not 1<=limit<=100:
            raise ValueError("invalid quarantine batch")
        with self.db.transaction() as c:
            rows=c.execute("""SELECT id,type FROM work_items
                WHERE type NOT IN ('inspect_event','human_action')
                AND status IN ('pending','retry_wait') AND next_action_at<=?
                AND lease_owner IS NULL
                ORDER BY created_at,id LIMIT ?""",(now,limit)).fetchall()
            for row in rows:
                c.execute("""UPDATE work_items SET status='blocked',
                    block_reason=? WHERE id=? AND status IN ('pending','retry_wait')
                    AND lease_owner IS NULL""",
                    ("no authorized executor for "+row["type"],row["id"]))
                c.execute("""INSERT INTO alerts(logical_key,kind,detail,created_at)
                    VALUES(?,?,?,?) ON CONFLICT(logical_key) DO NOTHING""",
                    ("missing_executor:"+row["id"],"blocked",
                     canonical({"work_id":row["id"],"type":row["type"]}),now))
            return len(rows)

    def claim_specific(self, ident, owner, now, authorization_evidence):
        """Explicit local handoff. Evidence is an operator assertion, not platform approval."""
        if not owner or not ident or not isinstance(authorization_evidence,dict) or not authorization_evidence:
            raise ValueError("owner, work id, and documented operator authorization required")
        with self.db.transaction() as c:
            if authorization_evidence.get("kind")=="accepted_host_wakeup":
                wake=c.execute("""SELECT id FROM wakeup_requests
                    WHERE id=? AND source_type='work' AND source_id=?
                    AND status='accepted' AND lease_version=?""",
                    (authorization_evidence.get("wakeup_id"),ident,
                     authorization_evidence.get("wakeup_generation"))).fetchone()
                if wake is None:
                    raise StaleLease("Muse handoff requires an accepted matching host wakeup")
            elif authorization_evidence.get("kind")=="best_effort_host_wakeup":
                # Worker identity and *live* Scheduler lease are still required;
                # this check establishes intent provenance, NOT Host accepted.
                wake=c.execute("""SELECT w.id FROM wakeup_requests w
                    JOIN host_wakeup_notifications n ON n.wakeup_id=w.id
                    WHERE w.id=? AND w.source_type='work' AND w.source_id=?
                      AND w.status IN ('pending','retry_wait','unknown')
                      AND n.capability='best_effort' AND n.generation=?""",
                    (authorization_evidence.get("wakeup_id"),ident,
                     authorization_evidence.get("wakeup_generation"))).fetchone()
                if wake is None:
                    raise StaleLease("Muse handoff needs a current best-effort notification intent")
            # Scheduler-controlled work must be claimed by the same worker
            # in the current live time slice; legacy unscheduled work is
            # unchanged. Prevent claiming the underlying Queue in suspension.
            activity=c.execute("""SELECT id,status,lease_owner,lease_version,
                    lease_until,preempt_requested,weight FROM schedule_activities
                    WHERE work_id=?""",(ident,)).fetchone()
            source=c.execute("SELECT status FROM work_items WHERE id=?",(ident,)).fetchone()
            if source and source["status"]=="running":
                raise ValueError("human work unavailable or already leased")
            manual_unblock=bool(
                activity and source and
                source["status"] in ("blocked","needs_approval") and
                activity["status"] in ("ready","suspended") and
                not c.execute("SELECT 1 FROM schedule_activities WHERE status='running' LIMIT 1").fetchone()
            )
            # An explicitly evidenced operator handoff of blocked work
            # precedes scheduling; it does not grant remote permissions.
            if activity and not manual_unblock and (
                activity["status"]!="running" or
                activity["lease_owner"]!=owner or
                activity["lease_until"] is None or activity["lease_until"]<=now or
                activity["preempt_requested"]
            ):
                raise StaleLease("work claim requires the active scheduler lease")
            n=c.execute("""UPDATE work_items SET status='running',lease_owner=?,lease_until=?,
                lease_version=lease_version+1,attempts=attempts+1
                WHERE id=? AND type='human_action' AND status IN
                    ('pending','retry_wait','blocked','needs_approval')
                AND next_action_at<=?""",(owner,now+self.lease_seconds,ident,now)).rowcount
            if n!=1:
                raise ValueError("human work unavailable or already leased")
            row=dict(c.execute("SELECT * FROM work_items WHERE id=?",(ident,)).fetchone())
            if manual_unblock:
                # An operator can explicitly clear a blocked work item without
                # an existing model slice. Reserve its Scheduler lease in this
                # SAME transaction so the resulting action still needs fencing.
                slice_seconds=min(30.0,max(1.0,self.lease_seconds-1.0))
                n=c.execute("""UPDATE schedule_activities SET status='running',
                    lease_owner=?,lease_version=lease_version+1,
                    lease_until=?,slice_until=?,last_started_at=?,
                    last_charged_at=?,last_updated=?,preempt_requested=0,
                    preempt_mode=NULL,preempt_reason=NULL
                    WHERE id=? AND status IN ('ready','suspended')""",
                    (owner,now+self.lease_seconds,now+slice_seconds,now,now,now,
                     activity["id"])).rowcount
                if n!=1:
                    raise StaleLease("manual work handoff could not reserve scheduler")
            if activity:
                lease=c.execute("""SELECT id,lease_version FROM schedule_activities
                    WHERE work_id=? AND status='running' AND lease_owner=?""",
                    (ident,owner)).fetchone()
                if lease:
                    row["scheduler_activity_id"]=lease["id"]
                    row["scheduler_generation"]=lease["lease_version"]
            # Append-only evidence of the claimed local handoff, including lease generation.
            # This is not a CrewQuay permission check or remote task claim.
            detail={"work_id":ident,"owner":owner,"lease_version":row["lease_version"],
                    "operator_evidence":authorization_evidence}
            c.execute("INSERT INTO alerts(logical_key,kind,detail,created_at) VALUES(?,?,?,?)",
                      ("handoff:"+ident+":"+str(row["lease_version"]),"local_handoff",canonical(detail),now))
            row["payload"]=json.loads(row["payload"])
            return row

    def renew(self, item, now, evidence):
        """Renew a live local lease only with new nontrivial progress evidence."""
        if not isinstance(evidence,dict) or not evidence or (
                set(evidence).issubset({"started","working","in_progress","status"})
                and all(isinstance(v,(str,bool)) for v in evidence.values())):
            raise ValueError("lease renewal requires evidence of actual progress")
        serialized=canonical(evidence)
        with self.db.transaction() as c:
            row=c.execute("""SELECT evidence FROM work_items WHERE id=? AND type='human_action'
                AND status='running' AND lease_owner=? AND lease_version=? AND lease_until>?""",
                (item["id"],item["lease_owner"],item["lease_version"],now)).fetchone()
            if not row:
                raise StaleLease("cannot renew expired or superseded lease")
            if row["evidence"]==serialized:
                raise ValueError("repeating the same evidence cannot prolong a lease")
            c.execute("""UPDATE work_items SET lease_until=?,evidence=?,
                first_action_at=COALESCE(first_action_at,?),last_progress_at=?
                WHERE id=? AND lease_owner=? AND lease_version=? AND status='running'""",
                (now+self.lease_seconds,serialized,now,now,
                 item["id"],item["lease_owner"],item["lease_version"]))
            detail={"work_id":item["id"],"lease_version":item["lease_version"],
                    "owner":item["lease_owner"],"progress":evidence}
            c.execute("INSERT INTO alerts(logical_key,kind,detail,created_at) VALUES(?,?,?,?)",
                ("work_progress:"+new_id(),"local_work_progress",canonical(detail),now))
            return {"lease_until":now+self.lease_seconds,"last_progress_at":now}

    def finish(self, item, now, evidence=None, status="done", reason=None, retry_at=None):
        if status not in ("done", "blocked", "needs_approval", "retry_wait"):
            raise ValueError("invalid work result")
        if status == "done" and (not isinstance(evidence,dict) or not evidence):
            raise ValueError("completion requires evidence")
        with self.db.transaction() as c:
            n = c.execute("""UPDATE work_items SET status=?,evidence=?,block_reason=?,next_action_at=?,
                first_action_at=CASE WHEN ? IS NOT NULL THEN COALESCE(first_action_at,?) ELSE first_action_at END,
                last_progress_at=CASE WHEN ? IS NOT NULL THEN ? ELSE last_progress_at END,
                lease_owner=NULL,lease_until=NULL WHERE id=? AND status='running'
                AND lease_owner=? AND lease_version=? AND lease_until>?""",
                (status, canonical(evidence) if evidence else None, reason, retry_at or now,
                 1 if evidence else None, now, 1 if evidence else None, now, item["id"], item["lease_owner"], item["lease_version"], now)).rowcount
            if n != 1:
                raise StaleLease("expired or superseded worker cannot finish")

    def resume(self, ident, now, evidence):
        if not evidence:
            raise ValueError("resume requires evidence of resolved condition")
        with self.db.transaction() as c:
            c.execute("""UPDATE work_items SET status='pending',next_action_at=?,block_reason=NULL
                         WHERE id=? AND status IN ('blocked','needs_approval')""", (now, ident))

    def status(self):
        return self.db.read("SELECT status,count(*) AS count FROM work_items GROUP BY status")
