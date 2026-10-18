import json
import math
from ...persistence.index import canonical, new_id
from ...queue.index import enqueue_tx


class Promises:
    # A commitment can have a long delivery deadline, but must keep making short,
    # inspectable next steps. A deadline is never a license to go idle until tomorrow.
    MAX_NEXT_ACTION_DELAY = 15 * 60
    DEFAULT_DUE_LIMITS = {"simple": 15 * 60, "normal": 60 * 60, "human_action": 60 * 60, "complex": 4 * 60 * 60}
    def __init__(self, db):
        self.db=db

    @staticmethod
    def _time(value):
        if isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value) or value<0:
            raise ValueError("finite UTC epoch time required")

    def create(self, actor, recipient, source_message_id, text, now, start_by, next_action_at,
               due_at=None, work_id=None, kind="human_action", payload=None, objective_reason=None):
        if not actor or not recipient or not source_message_id or not text:
            raise ValueError("promise provenance required")
        for value in (now,start_by,next_action_at):
            self._time(value)
        if due_at is not None:
            self._time(due_at)
        if start_by is None or next_action_at is None or start_by<now or next_action_at<now:
            raise ValueError("explicit start_by and next_action_at required")
        if next_action_at-now>self.MAX_NEXT_ACTION_DELAY:
            raise ValueError("next action must occur within 15 minutes, regardless of delivery deadline")
        if due_at is not None and due_at<start_by:
            raise ValueError("deadline precedes start")
        start_limit={"simple":120,"normal":180,"complex":300}.get(kind,180)
        # An external dependency can justify a later delivery, not postponing
        # the *first* checkpoint. Inspect and record the dependency immediately.
        if start_by-now>start_limit:
            raise ValueError("first action must start within 2-5 minutes; document external dependencies as progress")
        due_limit=self.DEFAULT_DUE_LIMITS.get(kind,60*60)
        if due_at is not None and due_at-now>due_limit and not objective_reason:
            raise ValueError("long delivery deadline requires an objective explanation; start and follow-up remain immediate")
        ident=new_id()
        with self.db.transaction() as c:
            old=c.execute("SELECT id FROM promises WHERE source_message_id=?",(source_message_id,)).fetchone()
            if old:
                return old[0]
            if work_id is None:
                work_id=enqueue_tx(c,"promise:"+source_message_id,kind,payload or {"source_message_id":source_message_id},now,start_by=start_by,due_at=due_at)
            elif not c.execute("SELECT id FROM work_items WHERE id=?",(work_id,)).fetchone():
                raise ValueError("unknown work item")
            c.execute("""INSERT INTO promises(id,work_item_id,actor,recipient,source_message_id,text,created_at,
                start_by,original_due_at,due_at,next_action_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (ident,work_id,actor,recipient,source_message_id,text,now,start_by,due_at,due_at,next_action_at))
            self._history(c,ident,"created",now,{"reason":objective_reason,"due_at":due_at})
            return ident

    @staticmethod
    def _history(c, ident, action, now, detail):
        c.execute("INSERT INTO promise_history(promise_id,action,at,detail) VALUES(?,?,?,?)",(ident,action,now,canonical(detail)))

    def progress(self, ident, now, evidence, next_action_at):
        self._time(now)
        self._time(next_action_at)
        if not isinstance(evidence,dict) or not evidence or next_action_at<=now:
            raise ValueError("progress requires structured evidence and next action")
        if next_action_at-now>self.MAX_NEXT_ACTION_DELAY:
            raise ValueError("progress cannot postpone the next action beyond 15 minutes")
        # Merely claiming to be working is not evidence of real progress.
        if set(evidence).issubset({"started","working","in_progress","status"}) and all(
                isinstance(value,(str,bool)) for value in evidence.values()):
            raise ValueError("self-reported activity alone is not progress evidence")
        with self.db.transaction() as c:
            if not c.execute("SELECT id FROM promises WHERE id=? AND status!='done'",(ident,)).fetchone():
                raise ValueError("unknown or completed promise")
            c.execute("""UPDATE promises SET first_action_at=COALESCE(first_action_at,?),last_progress_at=?,
                progress_evidence=?,next_action_at=? WHERE id=?""",(now,now,canonical(evidence),next_action_at,ident))
            self._history(c,ident,"progress",now,evidence)

    def agent_progress_tx(self,c,ident,now,evidence,next_action_at):
        """Advance the exact promise in the Agent's fenced SQLite transaction."""
        self._time(now)
        self._time(next_action_at)
        if (not isinstance(evidence,dict) or not evidence
                or next_action_at<=now
                or next_action_at-now>self.MAX_NEXT_ACTION_DELAY):
            raise ValueError("promise progress requires evidence and next action within 15 minutes")
        row=c.execute("SELECT * FROM promises WHERE id=? AND status!='done'",
                      (ident,)).fetchone()
        if not row:
            raise ValueError("unknown or completed promise")
        c.execute("""UPDATE promises SET first_action_at=COALESCE(first_action_at,?),
            last_progress_at=?,progress_evidence=?,next_action_at=?
            WHERE id=? AND status!='done'""",
            (now,now,canonical(evidence),next_action_at,ident))
        self._history(c,ident,"progress",now,evidence)
        return dict(row)

    def agent_complete_tx(self,c,ident,now,evidence,*,linked_work_finished=False):
        """Close the promise only after exact Worker/Outbox checks by Consumer."""
        row=c.execute("SELECT * FROM promises WHERE id=? AND status!='done'",
                      (ident,)).fetchone()
        if not row or not isinstance(evidence,dict) or not evidence:
            raise ValueError("current promise completion proof required")
        work=c.execute("SELECT status,type FROM work_items WHERE id=?",
                       (row["work_item_id"],)).fetchone()
        if not work:
            raise ValueError("promise source work missing")
        if linked_work_finished:
            if work["type"]!="human_action" or work["status"]!="done":
                raise ValueError("human_action promise requires finished Queue lease")
        else:
            if work["type"]=="human_action" or work["status"]=="running":
                raise ValueError("cannot complete leased or unconfirmed human work")
            n=c.execute("""UPDATE work_items SET status='done',evidence=?,
                first_action_at=COALESCE(first_action_at,?),last_progress_at=?,
                lease_owner=NULL,lease_until=NULL WHERE id=?
                AND status IN ('pending','retry_wait','blocked','needs_approval')""",
                (canonical(evidence),now,now,row["work_item_id"])).rowcount
            if n!=1:
                raise ValueError("promise source work is not finishable")
        c.execute("""UPDATE promises SET status='done',completed_evidence=?,
            first_action_at=COALESCE(first_action_at,?),last_progress_at=?
            WHERE id=? AND status!='done'""",
            (canonical(evidence),now,now,ident))
        self._history(c,ident,"completed",now,evidence)
        return dict(row)

    def request_extension(self, ident, due_at, reason, evidence, now):
        self._time(now)
        self._time(due_at)
        if not reason or not evidence:
            raise ValueError("extension needs reason and evidence")
        with self.db.transaction() as c:
            row=c.execute("SELECT * FROM promises WHERE id=? AND status!='done'",(ident,)).fetchone()
            if not row:
                raise ValueError("unknown promise")
            if due_at is None or due_at<=max(now,row["start_by"]) or (row["due_at"] is not None and due_at<=row["due_at"]):
                raise ValueError("extension must be after current deadline, start and now")
            self._history(c,ident,"extension_requested",now,{"old_due_at":row["due_at"],"new_due_at":due_at,"reason":reason,"evidence":evidence})

    def approve_extension(self, ident, due_at, approved_by, approval_evidence, now):
        self._time(now)
        self._time(due_at)
        with self.db.transaction() as c:
            row=c.execute("SELECT * FROM promises WHERE id=? AND status!='done'",(ident,)).fetchone()
            if not row or approved_by==row["actor"] or not approved_by or not approval_evidence:
                raise ValueError("independent approval identity and evidence required")
            if approved_by!=row["recipient"]:
                raise ValueError("only the original recipient may approve in this local workflow")
            if due_at is None or due_at<=max(now,row["start_by"]) or (row["due_at"] is not None and due_at<=row["due_at"]):
                raise ValueError("approved extension must be a future deadline")
            request=c.execute("SELECT detail FROM promise_history WHERE promise_id=? AND action='extension_requested' ORDER BY id DESC LIMIT 1",(ident,)).fetchone()
            if not request or json.loads(request[0])["new_due_at"]!=due_at:
                raise ValueError("matching extension request required")
            if row["due_at"] is not None and now>=row["due_at"]:
                self._history(c,ident,"overdue_before_extension",now,{"missed_due_at":row["due_at"]})
            self._history(c,ident,"extension_approved",now,{"old_due_at":row["due_at"],"new_due_at":due_at,"approved_by":approved_by,"approval_evidence":approval_evidence})
            c.execute("UPDATE promises SET due_at=? WHERE id=?",(due_at,ident))

    def complete(self, ident, now, evidence):
        if not isinstance(evidence,dict) or not evidence:
            raise ValueError("completion requires structured evidence")
        with self.db.transaction() as c:
            row=c.execute("SELECT * FROM promises WHERE id=? AND status!='done'",(ident,)).fetchone()
            if not row:
                raise ValueError("unknown or completed promise")
            active=c.execute("SELECT status FROM work_items WHERE id=?",(row["work_item_id"],)).fetchone()
            if active and active["status"]=="running":
                raise ValueError("cannot manually complete an actively leased work item")
            self._history(c,ident,"completed",now,evidence)
            c.execute("UPDATE promises SET status='done',completed_evidence=? WHERE id=?",(canonical(evidence),ident))
            c.execute("UPDATE work_items SET status='done',evidence=?,last_progress_at=? WHERE id=?",
                      (canonical(evidence),now,row["work_item_id"]))

    def watchdog(self, now):
        notices=[]
        for row in self.db.read("SELECT * FROM promises WHERE status!='done'"):
            tags=[]
            if row["first_action_at"] is None and now>=row["start_by"]:
                tags.append("start_overdue")
            if now>=row["next_action_at"]:
                tags.append("stalled")
            if row["due_at"] is not None and now>=row["due_at"]:
                tags.append("overdue")
            with self.db.transaction() as c:
                for tag in tags:
                    interval=row["next_action_at"] if tag=="stalled" else (row["due_at"] if tag=="overdue" else row["start_by"])
                    key="promise:"+row["id"]+":"+tag+":"+str(interval)
                    detail={"promise_id":row["id"],"work_id":row["work_item_id"],"state":tag}
                    added=c.execute("INSERT INTO alerts VALUES(?,?,?,?) ON CONFLICT(logical_key) DO NOTHING",(key,tag,canonical(detail),now)).rowcount
                    if added:
                        notices.append(detail)
                        self._history(c,row["id"],tag,now,detail)
                    c.execute("UPDATE work_items SET priority=MAX(priority,100) WHERE id=? AND status IN ('pending','retry_wait')",(row["work_item_id"],))
                if tags:
                    c.execute("UPDATE promises SET status=? WHERE id=?",("overdue" if "overdue" in tags else tags[0],row["id"]))
        return notices
