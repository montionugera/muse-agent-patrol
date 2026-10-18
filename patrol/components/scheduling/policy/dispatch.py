"""SQLite-owned cooperative dispatch and lease-generation fencing.

Scheduling a work ID never grants a CrewQuay role or claims the original Queue.
Remote side effects live in Outbox and must be reconciled independently.
"""
import json
import math
from .scoring import priority, intrinsic_priority, quantum, preemption
from .checkpoint import validate as validate_checkpoint
from ...persistence.index import canonical, new_id, StaleLease

KINDS=frozenset(("work","promise","social","research","maintenance"))
ACTIVE=("ready","suspended")


class Scheduler:
    def __init__(self, db, lease_seconds=120):
        if type(lease_seconds) is not int or lease_seconds<30:
            raise ValueError("scheduler lease_seconds must be >=30")
        self.db=db
        self.lease_seconds=lease_seconds

    @staticmethod
    def _ready(c,now):
        return [dict(r) for r in c.execute(
            """SELECT a.* FROM schedule_activities a
               LEFT JOIN work_items w ON w.id=a.work_id
               WHERE a.status IN ('ready','suspended')
                 AND (a.work_id IS NULL OR (
                      w.status IN ('pending','retry_wait')
                      AND w.next_action_at<=?))
               ORDER BY a.created_at,a.id""",(now,))]

    @staticmethod
    def _current(c):
        r=c.execute("SELECT * FROM schedule_activities WHERE status='running'").fetchone()
        return dict(r) if r else None

    @staticmethod
    def _best(rows, now):
        if not rows:
            return None
        # The bounded 60-point waiting bonus must not make a stale
        # priority-45 job outrank a newly arrived priority-85 Owner message.
        # Within a moderate intrinsic-priority band, however, age/virtual
        # runtime still decide ordering, so old similar-priority work lives.
        highest=max(intrinsic_priority(row,now) for row in rows)
        eligible=(row for row in rows
                  if intrinsic_priority(row,now)>=highest-24)
        return max(eligible,key=lambda r:(
            priority(r,now),-float(r["created_at"]),r["id"]))

    @staticmethod
    def _public(row,now):
        if row is None:
            return None
        result={key:row[key] for key in (
            "id","logical_key","kind","work_id","status","base_priority",
            "weight","deadline","lease_owner","lease_version","lease_until",
            "slice_until","preempt_requested","preempt_mode","preempt_reason",
            "cpu_seconds","virtual_runtime","created_at","ready_since","last_updated"
        )}
        result["score"]=priority(row,now,running=row["status"]=="running")
        result["checkpoint"]=json.loads(row["checkpoint"]) if row["checkpoint"] else None
        return result

    def register(self,logical,kind,now,*,priority_score=50,weight=50,
                 deadline=None,switch_cost=4,work_id=None):
        if (not isinstance(logical,str) or not 1<=len(logical)<=160
                or kind not in KINDS or type(priority_score) is not int
                or not 0<=priority_score<=100 or type(weight) is not int
                or not 1<=weight<=100 or type(switch_cost) is not int
                or not 0<=switch_cost<=30
                or type(now) not in (int,float) or not math.isfinite(now)
                or (deadline is not None and
                    (type(deadline) not in (int,float) or not math.isfinite(deadline)))
                or (work_id is not None and
                    (not isinstance(work_id,str) or not work_id or kind!="work"))):
            raise ValueError("invalid bounded activity registration")
        with self.db.transaction() as c:
            if work_id and not c.execute("SELECT 1 FROM work_items WHERE id=?",(work_id,)).fetchone():
                raise ValueError("unknown local work; cannot create platform assignment")
            old=c.execute("SELECT * FROM schedule_activities WHERE logical_key=?",(logical,)).fetchone()
            if old:
                if (old["kind"],old["work_id"],old["base_priority"],old["weight"],
                    old["deadline"],old["switch_cost"]) != (
                        kind,work_id,priority_score,weight,deadline,switch_cost):
                    raise ValueError("immutable activity registration mismatch")
                return old["id"]
            ident=new_id()
            c.execute("""INSERT INTO schedule_activities
                (id,logical_key,kind,work_id,status,base_priority,weight,deadline,
                 switch_cost,created_at,ready_since,last_updated)
                VALUES(?,?,?,?,'ready',?,?,?,?,?,?,?)""",
                      (ident,logical,kind,work_id,priority_score,weight,deadline,
                       switch_cost,now,now,now))
            return ident

    def sync_work(self,now,limit=100,focus_work_id=None):
        """Idempotently reflect local human-decision work; no model or claim.

        A bounded sync always includes a specifically requested work item
        before older backlog, without reserving it or changing its priority.
        """
        if type(limit) is not int or not 1<=limit<=500:
            raise ValueError("sync limit must be 1..500")
        if focus_work_id is not None and (
                not isinstance(focus_work_id,str) or not 1<=len(focus_work_id)<=128):
            raise ValueError("bounded local work ID required")
        added=0
        with self.db.transaction() as c:
            rows=c.execute("""SELECT w.id,w.priority,w.created_at,w.start_by,w.due_at,
                         p.due_at AS promise_due, e.source AS event_source,
                         COALESCE(ms.muse_score,ms.base_score) AS message_score
                         FROM work_items w LEFT JOIN promises p
                           ON p.work_item_id=w.id AND p.status!='done'
                         LEFT JOIN events e ON e.id=w.event_id
                         LEFT JOIN message_scores ms ON ms.event_id=w.event_id
                         LEFT JOIN schedule_activities a ON a.work_id=w.id
                         WHERE w.type='human_action' AND w.status IN
                           ('pending','retry_wait','blocked','needs_approval')
                         ORDER BY CASE WHEN w.id=? THEN -1 ELSE 0 END,
                             CASE WHEN w.status IN ('pending','retry_wait')
                                       AND w.next_action_at<=? THEN 0 ELSE 1 END,
                             CASE WHEN a.id IS NULL THEN 0 ELSE 1 END,
                             CASE WHEN e.source='messages' AND
                                   COALESCE(ms.muse_score,ms.base_score) IS NOT NULL
                                  THEN COALESCE(ms.muse_score,ms.base_score)
                                  ELSE w.priority END DESC,
                             COALESCE(a.last_updated,w.created_at),w.created_at
                         LIMIT ?""",(focus_work_id or "",now,limit)).fetchall()
            for row in rows:
                key="work:"+row["id"]
                score=(row["message_score"] if row["event_source"]=="messages"
                       and row["message_score"] is not None else row["priority"])
                score=min(100,max(0,score))
                # A generic low-score event's suggested start_by (often
                # now+180s) is not a formal delivery deadline. Treating it
                # as EDF urgency turns stale events into false emergencies.
                deadline=(row["promise_due"] if row["promise_due"] is not None else
                          row["due_at"] if row["due_at"] is not None else
                          row["start_by"] if score>=70 else None)
                # A work item has a unique Scheduler activity even when a
                # previous operator registered it under a custom logical key.
                # Looking up only "work:<id>" would attempt a duplicate insert.
                existing=c.execute("SELECT id,status FROM schedule_activities WHERE work_id=?",
                                   (row["id"],)).fetchone()
                if existing:
                    if existing["status"] in ("ready","suspended","running"):
                        c.execute("""UPDATE schedule_activities
                            SET base_priority=?,deadline=?,last_updated=?
                            WHERE id=?""",(score,deadline,now,existing["id"]))
                    continue
                c.execute("""INSERT INTO schedule_activities
                  (id,logical_key,kind,work_id,status,base_priority,weight,
                   deadline,switch_cost,created_at,ready_since,last_updated)
                  VALUES(?,?, 'work',?,'ready',?,?,?,?,?,?,?)""",
                  (new_id(),key,row["id"],score,50,deadline,4,now,now,now))
                added+=1
            # Non-human_action promises have a separate host wake source, but
            # previously had no runnable Scheduler activity after wakeup.
            # Derive a one-shot activity per due next-action checkpoint.
            # Human_action promises already have a linked work activity.
            due_promises=c.execute("""SELECT p.id,p.next_action_at
                FROM promises p JOIN work_items w ON w.id=p.work_item_id
                WHERE p.status!='done' AND w.type!='human_action'
                  AND (p.next_action_at<=? OR
                       (p.first_action_at IS NULL AND p.start_by<=?) OR
                       (p.due_at IS NOT NULL AND p.due_at<=?))
                ORDER BY p.next_action_at,p.id LIMIT ?""",
                (now+120,now,now,limit)).fetchall()
            for p in due_promises:
                logical="promise_due:"+p["id"]+":"+str(p["next_action_at"])
                if c.execute("SELECT 1 FROM schedule_activities WHERE logical_key=?",
                             (logical,)).fetchone():
                    continue
                c.execute("""INSERT INTO schedule_activities
                    (id,logical_key,kind,work_id,status,base_priority,weight,
                     deadline,switch_cost,created_at,ready_since,last_updated)
                    VALUES(?,?,'promise',NULL,'ready',95,60,?,4,?,?,?)""",
                    (new_id(),logical,p["next_action_at"],now,now,now))
                added+=1
            # Advance old checkpoints only when their promise has recorded
            # progress (new next_action_at) or has been completed. A lease
            # generation is fenced at the same local transaction boundary.
            c.execute("""UPDATE schedule_activities SET status='completed',
                lease_owner=NULL,lease_until=NULL,slice_until=NULL,
                lease_version=lease_version+1,preempt_requested=0,
                preempt_mode=NULL,last_updated=?,
                preempt_reason='promise advanced or completed'
                WHERE kind='promise' AND logical_key LIKE 'promise_due:%'
                AND status IN ('ready','running','suspended')
                AND NOT EXISTS(
                    SELECT 1 FROM promises p WHERE p.status!='done'
                    AND schedule_activities.logical_key=
                       ('promise_due:'||p.id||':'||CAST(p.next_action_at AS TEXT))
                )""",(now,))
            # External workers retain their own Queue fencing. A locally
            # completed work item invalidates an unleased scheduled copy,
            # but cannot undo anything already dispatched to Outbox.
            c.execute("""UPDATE schedule_activities SET status='completed',
                lease_owner=NULL,lease_until=NULL,slice_until=NULL,
                preempt_requested=0,preempt_mode=NULL,
                lease_version=lease_version+1,last_updated=?,
                preempt_reason='source work already completed'
                WHERE work_id IN (SELECT id FROM work_items WHERE status='done')
                AND status IN ('ready','suspended','running')""",(now,))
        return added

    @staticmethod
    def _charge(c,row,now):
        elapsed=max(0.0,min(float(now),float(row["lease_until"])) -
                    float(row["last_charged_at"] or row["last_started_at"] or now))
        if elapsed>0:
            c.execute("""UPDATE schedule_activities
                SET cpu_seconds=cpu_seconds+?,
                    virtual_runtime=virtual_runtime+?,
                    last_charged_at=? WHERE id=?""",
                (elapsed,elapsed*100.0/float(row["weight"]),now,row["id"]))
        return elapsed

    def _recover(self,c,now):
        """Fence only truly expired leases; never revoke an active worker.

        The historical active-Scheduler recovery is augmented by an orphan
        scan: a ready/suspended Scheduler activity cannot run while its
        associated Queue work is left 'running' by an older crashed handoff.
        Reclaim that Queue lease only after its own expiry, within the same
        transaction as subsequent scheduling. Outbox side effects are not
        modified or replayed.
        """
        recovered=0
        active=self._current(c)
        if active and active["lease_until"]<=now:
            self._charge(c,active,now)
            # A crashed Scheduler fences its matching Queue lease, even
            # when the Queue lease nominally expires a little later.
            if active["work_id"]:
                c.execute("""UPDATE work_items SET status='pending',
                    lease_owner=NULL,lease_until=NULL,
                    lease_version=lease_version+1,next_action_at=?,
                    block_reason='scheduler lease expired; reconcile Outbox'
                    WHERE id=? AND status='running' AND lease_owner=?""",
                    (now,active["work_id"],active["lease_owner"]))
            c.execute("""UPDATE schedule_activities SET status='suspended',
                lease_owner=NULL,lease_until=NULL,slice_until=NULL,
                lease_version=lease_version+1,ready_since=?,last_updated=?,
                preempt_requested=0,preempt_mode=NULL,
                preempt_reason='expired lease; reconcile Outbox before new effects'
                WHERE id=? AND status='running'""",(now,now,active["id"]))
            recovered+=1
        orphans=c.execute("""SELECT w.id FROM work_items w
            JOIN schedule_activities a ON a.work_id=w.id
            WHERE a.status IN ('ready','suspended')
              AND w.status='running' AND w.lease_until<=?""",
            (now,)).fetchall()
        for orphan in orphans:
            c.execute("""UPDATE work_items SET status='pending',
                lease_owner=NULL,lease_until=NULL,
                lease_version=lease_version+1,next_action_at=?,
                block_reason='orphaned expired Queue lease; reconcile Outbox'
                WHERE id=? AND status='running' AND lease_until<=?""",
                (now,orphan["id"],now))
        recovered+=len(orphans)
        return recovered

    def _assess_tx(self,c,now,mutate):
        current=self._current(c)
        contenders=self._ready(c,now)
        best=self._best(contenders,now)
        mode=preemption(current,best,now) if current else None
        if current and mutate:
            # A request is not a latched cancellation. Withdraw it if the
            # challenger disappeared (or no longer clears hysteresis).
            if mode and (not current["preempt_requested"] or current["preempt_mode"]!=mode):
                c.execute("""UPDATE schedule_activities
                    SET preempt_requested=1,preempt_mode=?,preempt_reason=?,
                        last_updated=? WHERE id=? AND status='running'""",
                    (mode,"a higher-value eligible activity is ready",now,current["id"]))
                current=self._current(c)
            elif not mode and current["preempt_requested"]:
                c.execute("""UPDATE schedule_activities SET preempt_requested=0,
                    preempt_mode=NULL,preempt_reason=NULL,last_updated=?
                    WHERE id=? AND status='running'""",(now,current["id"]))
                current=self._current(c)
        return {"running":self._public(current,now),
                "candidate":self._public(best,now),
                "preemption":mode,
                "advice":"checkpoint_at_safe_point" if current and mode else
                         "continue" if current else "ready_to_dispatch" if best else "idle",
                "remote_side_effects":"not managed by scheduler"}

    def reconcile(self,now,limit=100):
        """On a Patrol tick, refresh waiting activities and request a safe point.

        Does not claim local work or invoke a model. The host may react to the
        returned advice on its next wakeup. Never modifies Outbox rows.
        """
        added=self.sync_work(now,limit)
        with self.db.transaction() as c:
            expired=self._recover(c,now)
            decision=self._assess_tx(c,now,True)
        return {"work_registered":added,"expired_leases_recovered":expired,
                "decision":decision,"model_invoked":False,
                "remote_action_issued":False}

    def assess(self,now):
        """Read-only scheduling diagnosis; does not dispatch models."""
        with self.db.transaction() as c:
            return self._assess_tx(c,now,False)

    def dispatch(self,owner,now,for_work_id=None):
        """Refresh actionable work then grant the top eligible time slice.

        An optional work ID is a *selection guard*, not a priority or
        authorization override. An active lease must yield or expire normally.
        If a different candidate outranks the requested work, report why and
        leave it untouched. This prevents an operator from accidentally
        claiming a low-priority activity while replying to urgent messages.
        """
        if not isinstance(owner,str) or not 1<=len(owner)<=128:
            raise ValueError("nonempty bounded worker owner required")
        if for_work_id is not None and (
                not isinstance(for_work_id,str) or not 1<=len(for_work_id)<=128):
            raise ValueError("bounded local work ID required")
        # tick --light and real Host wakeup are separate processes. Do not
        # assume the latest human_action has already been imported into the
        # Scheduler when a just-awakened worker calls schedule-next.
        refreshed=self.sync_work(now,limit=500,focus_work_id=for_work_id)
        with self.db.transaction() as c:
            expired=self._recover(c,now)
            view=self._assess_tx(c,now,True)
            info={}
            selected=None
            if for_work_id is not None:
                work=c.execute("""SELECT id,type,status,next_action_at,lease_owner,
                    lease_until FROM work_items WHERE id=?""",
                    (for_work_id,)).fetchone()
                if not work or work["type"]!="human_action":
                    info={"requested_work_id":for_work_id,
                          "hint_status":"unknown_or_nonhuman_work"}
                else:
                    activity=c.execute("""SELECT * FROM schedule_activities
                        WHERE work_id=?""",(for_work_id,)).fetchone()
                    if not activity:
                        reason="activity_not_registered"
                    elif activity["status"] not in ACTIVE:
                        reason="activity_"+activity["status"]
                    elif work["status"] not in ("pending","retry_wait"):
                        reason="work_"+work["status"]
                    elif work["next_action_at"]>now:
                        reason="work_not_due"
                    else:
                        selected=dict(activity)
                        reason="eligible"
                    info={"requested_work_id":for_work_id,
                          "hint_status":reason,
                          "requested_activity":self._public(activity,now) if activity else None,
                          "work_status":work["status"],
                          "work_next_action_at":work["next_action_at"]}
            if view["running"]:
                return {**view,**info,"granted":False,
                        "lease_recovered":expired,"activities_registered":refreshed}
            best=self._best(self._ready(c,now),now)
            if best is None or (for_work_id is not None and selected is None):
                return {**view,**info,"granted":False,
                        "lease_recovered":expired,"activities_registered":refreshed}
            if for_work_id is not None and selected["id"]!=best["id"]:
                return {**view,**info,"granted":False,
                        "hint_status":"outscored_by_other_activity",
                        "lease_recovered":expired,"activities_registered":refreshed}
            n=c.execute("""UPDATE schedule_activities SET status='running',
                lease_owner=?,lease_version=lease_version+1,
                lease_until=?,slice_until=?,last_started_at=?,
                last_charged_at=?,preempt_requested=0,preempt_mode=NULL,
                preempt_reason=NULL,last_updated=?
                WHERE id=? AND status IN ('ready','suspended')""",
                (owner,now+self.lease_seconds,now+min(quantum(best["weight"]),self.lease_seconds-1.0),
                 now,now,now,best["id"])).rowcount
            if n!=1:
                raise StaleLease("another scheduler dispatched this activity")
            active=self._current(c)
            return {"granted":True,"activity":self._public(active,now),
                    **info,"lease_recovered":expired,
                    "activities_registered":refreshed,
                    "warning":"scheduler lease is not work-claim or CrewQuay authorization"}

    @staticmethod
    def _owned(c,ident,owner,generation,now):
        if type(generation) is not int or generation<=0 or not owner:
            raise StaleLease("current generation and worker identity required")
        row=c.execute("""SELECT * FROM schedule_activities
            WHERE id=? AND status='running' AND lease_owner=?
            AND lease_version=? AND lease_until>?""",
            (ident,owner,generation,now)).fetchone()
        if row is None:
            raise StaleLease("activity is no longer owned by this lease generation")
        return dict(row)

    def agent_owned_tx(self,c,ident,owner,generation,now,*,kind,work_id=None):
        """Public transaction port; caller owns BEGIN IMMEDIATE.

        Never grant a new lease here. Validate the already-running Scheduler
        generation under the SAME transaction as Queue and Worker receipts.
        """
        row=self._owned(c,ident,owner,generation,now)
        if row["kind"]!=kind or (work_id is not None and row["work_id"]!=work_id):
            raise StaleLease("Scheduler activity does not match Agent's work intent")
        return row

    def agent_renew_tx(self,c,row,owner,generation,now):
        """Extend only the live hard lease, NEVER the fair/preemption slice."""
        expiry=now+self.lease_seconds
        n=c.execute("""UPDATE schedule_activities SET lease_until=?,
            last_updated=? WHERE id=? AND status='running' AND lease_owner=?
            AND lease_version=? AND lease_until>?""",
            (expiry,now,row["id"],owner,generation,now)).rowcount
        if n!=1:
            raise StaleLease("Scheduler renewal lost generation")
        return expiry

    def agent_finish_tx(self,c,row,owner,generation,now,evidence):
        """Commit a verified Agent completion inside the caller transaction."""
        self.agent_owned_tx(c,row["id"],owner,generation,now,
                            kind=row["kind"],work_id=row["work_id"])
        self._charge(c,row,now)
        n=c.execute("""UPDATE schedule_activities SET status='completed',
            checkpoint=?,lease_owner=NULL,lease_until=NULL,slice_until=NULL,
            lease_version=lease_version+1,preempt_requested=0,
            preempt_mode=NULL,last_updated=? WHERE id=? AND status='running'
            AND lease_owner=? AND lease_version=?""",
            (canonical(evidence),now,row["id"],owner,generation)).rowcount
        if n!=1:
            raise StaleLease("Scheduler Agent completion lost generation")

    def checkpoint(self,ident,owner,generation,now,data,*,voluntary=False):
        if (not isinstance(data,dict) or not data or
                len(canonical(data).encode("utf-8"))>32768):
            raise ValueError("nonempty checkpoint <=32KiB required")
        validate_checkpoint(data)
        with self.db.transaction() as c:
            row=self._owned(c,ident,owner,generation,now)
            self._charge(c,row,now)
            row=dict(c.execute("SELECT * FROM schedule_activities WHERE id=?",
                               (ident,)).fetchone())
            contenders=self._best(self._ready(c,now),now)
            mode=preemption(row,contenders,now)
            # Re-evaluate at the actual safe point. A withdrawn or completed
            # challenger must not force an obsolete yield.
            suspended=bool(voluntary or mode)
            if suspended:
                if row["work_id"]:
                    # Release only the work lease owned by this scheduler
                    # worker. A different live owner is a conflicting claim.
                    work=c.execute("""SELECT status,lease_owner,lease_until
                        FROM work_items WHERE id=?""",(row["work_id"],)).fetchone()
                    if work and work["status"]=="running":
                        if work["lease_owner"]!=owner and work["lease_until"]>now:
                            raise StaleLease("linked local work is leased to another owner")
                        c.execute("""UPDATE work_items SET status='pending',
                            lease_owner=NULL,lease_until=NULL,
                            lease_version=lease_version+1,next_action_at=?,
                            block_reason='checkpoint yielded; reconcile Outbox'
                            WHERE id=? AND status='running'""",(now,row["work_id"]))
                c.execute("""UPDATE schedule_activities SET
                    status='suspended',checkpoint=?,lease_owner=NULL,
                    lease_until=NULL,slice_until=NULL,
                    lease_version=lease_version+1,
                    preempt_requested=0,preempt_mode=NULL,
                    preempt_reason=?,ready_since=?,last_updated=?
                    WHERE id=?""",
                    (canonical(data),"voluntary yield" if voluntary else
                     row["preempt_mode"] or mode or "safe-point yield",
                     now,now,ident))
            else:
                # Renewing a lease must not silently extend an unfinished
                # fairness slice, even if checkpoints are very frequent.
                next_slice=(row["slice_until"] if now<row["slice_until"]
                    else now+min(quantum(row["weight"]),self.lease_seconds-1.0))
                c.execute("""UPDATE schedule_activities SET checkpoint=?,
                    lease_until=?,slice_until=?,preempt_requested=0,
                    preempt_mode=NULL,preempt_reason=NULL,last_updated=?
                    WHERE id=?""",
                    (canonical(data),now+self.lease_seconds,
                     next_slice,now,ident))
            new=dict(c.execute("SELECT * FROM schedule_activities WHERE id=?",
                               (ident,)).fetchone())
            return {"suspended":suspended,"activity":self._public(new,now),
                    "next_step":"dispatch_again" if suspended else "continue",
                    "warning":"previously staged remote effects remain in Outbox"}

    def restore(self,ident,now):
        """Read-only restoration pack; caller must independently obtain a lease.

        Checkpoint text is untrusted context, never instructions or permission.
        Remote resource references must be re-read with current authorization.
        Existing Outbox IDs are inspected but never resent here.
        """
        if not isinstance(ident,str) or not ident or len(ident)>128:
            raise ValueError("bounded schedule activity ID required")
        rows=self.db.read("SELECT * FROM schedule_activities WHERE id=?",(ident,))
        if not rows:
            raise ValueError("unknown schedule activity")
        row=rows[0]
        data=json.loads(row["checkpoint"]) if row["checkpoint"] else None
        fmt=validate_checkpoint(data) if data is not None else "none"
        pending=data.get("pending_outbox_ids",[]) if fmt=="v1" else []
        results=[]
        for outbox_id in pending:
            lookup=self.db.read("SELECT status,remote_id FROM outbox WHERE id=?",
                                (outbox_id,))
            results.append({"id":outbox_id,"status":lookup[0]["status"] if lookup else "missing",
                            "remote_id":lookup[0]["remote_id"] if lookup else None})
        work=None
        if row["work_id"]:
            w=self.db.read("SELECT status,next_action_at,lease_version FROM work_items WHERE id=?",
                           (row["work_id"],))
            work=w[0] if w else {"status":"missing"}
        return {"activity_id":ident,"kind":row["kind"],"status":row["status"],
                "checkpoint_format":fmt,"checkpoint":data,
                "outbox_receipts":results,"work":work,
                "scheduler_generation":row["lease_version"],
                "next_step":"obtain_new_schedule_next_lease" if row["status"] in ACTIVE else
                            "inspect_current_running_lease" if row["status"]=="running" else
                            "terminal_do_not_resume",
                "requires_remote_resource_revalidation":True,
                "requires_outbox_reconciliation":any(
                    v["status"] not in ("confirmed","resolved") for v in results),
                "model_state_restored":False,
                "side_effects_executed":False}

    def complete(self,ident,owner,generation,now,evidence):
        if not isinstance(evidence,dict) or not evidence:
            raise ValueError("activity completion requires documented evidence")
        if len(canonical(evidence).encode("utf-8"))>32768:
            raise ValueError("evidence too large")
        with self.db.transaction() as c:
            row=self._owned(c,ident,owner,generation,now)
            if row["work_id"]:
                work=c.execute("SELECT status FROM work_items WHERE id=?",
                               (row["work_id"],)).fetchone()
                if not work or work["status"]!="done":
                    raise ValueError("linked local work must be completed with separate work-finish evidence")
            if row["kind"]=="promise" and row["logical_key"].startswith("promise_due:"):
                # An activity cannot make an outstanding commitment silently
                # disappear; record actual promise progress or completion first.
                current=c.execute("""SELECT 1 FROM promises p
                    WHERE p.status!='done'
                    AND ('promise_due:'||p.id||':'||
                        CAST(p.next_action_at AS TEXT))=?""",
                    (row["logical_key"],)).fetchone()
                if current:
                    raise ValueError("promise progress or completion evidence required")
            if row["kind"]=="social" and c.execute(
                    "SELECT 1 FROM social_intents WHERE activity_id=?",
                    (ident,)).fetchone():
                effects=c.execute("""SELECT o.status FROM outbox o
                    JOIN scheduled_outbox so ON so.outbox_id=o.id
                    WHERE so.schedule_id=?""",(ident,)).fetchall()
                if effects:
                    if any(effect["status"]!="confirmed" for effect in effects):
                        raise ValueError("social remote action requires confirmed Outbox receipt")
                elif (evidence.get("decision")!="skipped" or
                      not isinstance(evidence.get("reason"),str) or
                      not 1<=len(evidence["reason"].strip())<=500):
                    raise ValueError("unposted social opportunity must be explicitly skipped with a reason")
            self._charge(c,row,now)
            c.execute("""UPDATE schedule_activities SET
                status='completed',checkpoint=?,lease_owner=NULL,
                lease_until=NULL,slice_until=NULL,lease_version=lease_version+1,
                preempt_requested=0,preempt_mode=NULL,last_updated=?
                WHERE id=?""",(canonical(evidence),now,ident))
            return {"completed":ident,"remote_action_issued":False}

    def cancel(self,ident,now,reason):
        if not isinstance(reason,str) or not reason.strip() or len(reason)>500:
            raise ValueError("nonempty bounded cancellation reason required")
        with self.db.transaction() as c:
            n=c.execute("""UPDATE schedule_activities SET status='cancelled',
                lease_version=lease_version+1,preempt_reason=?,last_updated=?
                WHERE id=? AND status IN ('ready','suspended')""",
                (reason,now,ident)).rowcount
            if not n:
                raise ValueError("activity not cancellable while running or terminal")
            return {"cancelled":ident,"remote_action_issued":False}

    def snapshot(self,now,limit=50):
        if type(limit) is not int or not 1<=limit<=200:
            raise ValueError("snapshot limit must be 1..200")
        rows=self.db.read("""SELECT * FROM schedule_activities
            ORDER BY CASE status WHEN 'running' THEN 0
             WHEN 'ready' THEN 1 WHEN 'suspended' THEN 2 ELSE 3 END,
             created_at,id LIMIT ?""",(limit,))
        return {"activities":[self._public(r,now) for r in rows],
                "decision":self.assess(now),
                "model_invoked":False,"host_woken":False}
