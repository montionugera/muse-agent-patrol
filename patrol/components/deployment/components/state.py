"""CrewQuay platform-native deployment protocol, not a substitute for Host evidence.

Every transition is durable. Nothing here invokes a model, sends HTTP, or
claims that a manually run light tick proves an actual 60s HATCH invocation.
"""
import hashlib
import json
import re
import secrets
from datetime import datetime
from ...persistence.index import canonical, new_id
from ...queue.index import enqueue_tx


def handshake_id(actor, run_id):
    return "dph_" + hashlib.sha256(canonical([actor, run_id]).encode()).hexdigest()


def epoch(value):
    if not isinstance(value, str):
        raise ValueError("expected UTC datetime")
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("timezone required")
    return dt.timestamp()


def sanitize(row):
    """Never print the stored nonce/challenge or the private event body."""
    keys = ("id", "actor", "run_id", "handshake_id", "repo_sha", "state",
            "created_at", "updated_at", "expires_at", "syn_work_id",
            "ack_work_id", "syn_outbox_id", "ack_outbox_id",
            "syn_ack_event_id", "syn_ack_event_cursor", "syn_ack_message_id",
            "failure_code", "bug_work_id", "bug_outbox_id", "bug_id")
    result = {k: row[k] for k in keys}
    result["deployment_verified"] = False
    result["reason"] = ("Host 60-second natural wake/real execution evidence not independently verified"
                        if row["state"] == "protocol_verified" else None)
    return result


class Deployment:
    def __init__(self, db):
        self.db = db

    def _actor(self, c):
        row = c.execute("SELECT value FROM meta WHERE key='actor'").fetchone()
        if not row or not row["value"]:
            raise ValueError("verified CrewQuay actor identity required")
        return row["value"]

    def start(self, now, repo_sha):
        if not isinstance(repo_sha, str) or not re.fullmatch(r"[0-9a-f]{7,40}", repo_sha):
            raise ValueError("actual lowercase Git repository SHA required")
        with self.db.transaction() as c:
            actor = self._actor(c)
            current = c.execute("""SELECT * FROM deployment_runs WHERE actor=?
                AND state NOT IN ('protocol_verified','expired','failed')
                ORDER BY created_at DESC LIMIT 1""", (actor,)).fetchone()
            if current:
                # Ordinary Git SHA changes are version evidence only, not a
                # new deployment identity. Never supersede the active protocol
                # handshake or orphan its staged SYN/ACK Outbox requests.
                # Explicit critical host/protocol/actor change needs a
                # separately authorized verification run.
                return sanitize(current)
            run_id = "deploy-" + secrets.token_hex(16)
            nonce = secrets.token_hex(16)
            ident = new_id()
            wid = enqueue_tx(c, "deployment:syn:" + run_id, "human_action",
                {"reason": "start authenticated deployment handshake", "deployment_run_id": ident},
                now, priority=75)
            c.execute("""INSERT INTO deployment_runs
                (id,actor,run_id,nonce,handshake_id,repo_sha,state,created_at,
                 updated_at,syn_work_id)
                VALUES(?,?,?,?,?,?,'awaiting_syn',?,?,?)""",
                (ident,actor,run_id,nonce,handshake_id(actor,run_id),
                 repo_sha,now,now,wid))
            return sanitize(c.execute("SELECT * FROM deployment_runs WHERE id=?", (ident,)).fetchone())

    @staticmethod
    def _bug_work(c, row, now, code):
        # A failure is actionable work for the existing HATCH wakeup lane,
        # never a privileged network write on the read-only Tick process.
        wid=enqueue_tx(c, "deployment:bug:"+row["run_id"], "human_action",
            {"reason":"report failed CrewQuay deployment handshake",
             "deployment_run_id":row["id"],"failure_code":code},
            now,priority=75)
        c.execute("""UPDATE deployment_runs SET bug_work_id=?,
            failure_code=?,updated_at=? WHERE id=?""",(wid,code,now,row["id"]))
        return wid

    def unsupported(self, now, repo_sha):
        """Record a missing capability as a real reportable failed run."""
        if not isinstance(repo_sha,str) or not re.fullmatch(r"[0-9a-f]{7,40}",repo_sha):
            raise ValueError("actual Git commit SHA required")
        with self.db.transaction() as c:
            actor=self._actor(c)
            run_id="deploy-"+secrets.token_hex(16)
            ident=new_id()
            record={"id":ident,"run_id":run_id}
            c.execute("""INSERT INTO deployment_runs(id,actor,run_id,nonce,
                handshake_id,repo_sha,state,created_at,updated_at)
                VALUES(?,?,?,?,?,?,'failed',?,?)""",
                (ident,actor,run_id,secrets.token_hex(16),
                 handshake_id(actor,run_id),repo_sha,now,now))
            self._bug_work(c,record,now,"PLATFORM_HANDSHAKE_UNSUPPORTED")
            return sanitize(c.execute("SELECT * FROM deployment_runs WHERE id=?",
                                      (ident,)).fetchone())

    def get(self, ident=None):
        with self.db.transaction() as c:
            actor = self._actor(c)
            if ident:
                row = c.execute("SELECT * FROM deployment_runs WHERE id=? AND actor=?",
                                (ident,actor)).fetchone()
            else:
                row = c.execute("SELECT * FROM deployment_runs WHERE actor=? ORDER BY created_at DESC LIMIT 1",
                                (actor,)).fetchone()
            return sanitize(row) if row else {"status": "not_started", "deployment_verified": False}

    def stage(self, ident, phase, now, schedule_context, producer_id=None):
        """Prepare the exact immutable SYN/ACK under the current local lease.

        Caller must independently dispatch the Outbox; prepared != sent.
        """
        from ...outbox.index import Outbox
        if phase not in ("syn","ack","bug"):
            raise ValueError("invalid deployment phase")
        with self.db.transaction() as c:
            actor=self._actor(c)
            row=c.execute("SELECT * FROM deployment_runs WHERE id=? AND actor=?",
                          (ident,actor)).fetchone()
            if not row:
                raise ValueError("unknown deployment run")
            if phase=="bug":
                if row["state"] not in ("failed","expired") or not row["bug_work_id"]:
                    raise ValueError("no failed deployment awaiting Bug report")
                code=row["failure_code"] or "FINAL_ACK_NOT_VERIFIED"
                payload={
                    "title":"Agent deployment handshake failed: "+code,
                    "component":"agent-deployment-hatch",
                    "severity":"high",
                    "affectedPlatforms":["CrewQuay","HATCH"],
                    "labels":["deployment","handshake","agent"],
                    "environment":"Patrol "+row["repo_sha"]+" (operator host details withheld)",
                    "stepsToReproduce":"Deploy Patrol under one native 60s HATCH Hook; start run "+row["run_id"]+"; inspect durable phase evidence.",
                    "expectedBehavior":"Authenticated platform SYN-ACK and verified final ACK with real Hook execution.",
                    "actualBehavior":"Deployment run failed with "+code+".",
                    "evidence":"runId="+row["run_id"]+"; phase="+row["state"]+
                               "; prior SYN/ACK outbox IDs="+str(row["syn_outbox_id"])+
                               "/"+str(row["ack_outbox_id"]),
                    "relatedApiPath":"/api/v1/agent/deployment/handshakes"}
                action="deployment_bug"
            elif phase=="syn":
                if row["state"] not in ("awaiting_syn","syn_staged"):
                    raise ValueError("SYN cannot be sent from current phase")
                payload={"runId":row["run_id"],"nonce":row["nonce"],
                         "hookKind":"HATCH","hookIntervalSeconds":60}
                action="deployment_syn"
            else:
                if row["state"] not in ("syn_ack_received","ack_staged"):
                    raise ValueError("ACK requires persisted authenticated SYN-ACK")
                if row["expires_at"] is None or now>=row["expires_at"]:
                    raise ValueError("platform challenge expired")
                if not row["syn_ack_event_id"] or not row["challenge"]:
                    raise ValueError("no verified platform event")
                payload={"nonce":row["nonce"],"challenge":row["challenge"],
                         "synAckMessageId":row["syn_ack_message_id"],
                         "eventId":row["syn_ack_event_id"],
                         "eventCursor":row["syn_ack_event_cursor"]}
                action="deployment_ack"
            # An existing outbox request is always immutable and can be read
            # back after its generation expires, never replaced with a new key.
            linked=(row["syn_outbox_id"] if phase=="syn" else
                    row["ack_outbox_id"] if phase=="ack" else row["bug_outbox_id"])
            if linked:
                return {"outbox_id":linked,"status":"existing_prepared_or_dispatched",
                        "deployment_run_id":ident,"phase":phase}
        oid=Outbox(self.db).stage("deployment:"+ident+":"+phase,
                                 actor,action,row["handshake_id"],
                                 payload,now,producer_id=producer_id,
                                 schedule_context=schedule_context)
        with self.db.transaction() as c:
            field=("syn_outbox_id" if phase=="syn" else
                   "ack_outbox_id" if phase=="ack" else "bug_outbox_id")
            if phase=="bug":
                c.execute("UPDATE deployment_runs SET bug_outbox_id=?,updated_at=? WHERE id=?",
                          (oid,now,ident))
            else:
                c.execute("UPDATE deployment_runs SET "+field+"=?,state=?,updated_at=? WHERE id=?",
                          (oid,phase+"_staged",now,ident))
        return {"outbox_id":oid,"status":"prepared","deployment_run_id":ident,"phase":phase}

    def reconcile(self, now):
        """Only local SQLite/event work. Safe on every existing light Tick."""
        updated=[]
        with self.db.transaction() as c:
            actor = c.execute("SELECT value FROM meta WHERE key='actor'").fetchone()
            if not actor:
                return updated
            runs = c.execute("""SELECT * FROM deployment_runs WHERE actor=? AND
                state NOT IN ('failed','expired','protocol_verified')""",(actor[0],)).fetchall()
            for item in runs:
                row=dict(item)
                if row["syn_outbox_id"] and row["state"]=="syn_staged":
                    s=c.execute("SELECT status,result FROM outbox WHERE id=?",
                                (row["syn_outbox_id"],)).fetchone()
                    if s and s["status"]=="confirmed":
                        proof=json.loads(s["result"])
                        if proof.get("expires_at"):
                            expiry=epoch(proof["expires_at"])
                            c.execute("""UPDATE deployment_runs
                                SET state='awaiting_event',expires_at=?,updated_at=? WHERE id=?""",
                                (expiry,now,row["id"]))
                            row["state"]="awaiting_event";row["expires_at"]=expiry
                if row["state"]=="awaiting_event":
                    # Consume exactly the first stored authorized platform
                    # pointer for THIS actor, run, handshake and event cursor.
                    events=c.execute("""SELECT payload,cursor FROM events
                        WHERE source='agent_events' AND scope='global'
                          AND json_extract(payload,'$.type')='deployment.syn_ack'
                          AND json_extract(payload,'$.resourceId')=?
                        ORDER BY id LIMIT 10""",(row["handshake_id"],)).fetchall()
                    for event in events:
                        data=json.loads(event["payload"])
                        if (data.get("handshakeId")!=row["handshake_id"] or
                            data.get("runId")!=row["run_id"] or
                            data.get("nonce")!=row["nonce"] or
                            data.get("senderId")!="crewquay-platform" or
                            data.get("senderKind")!="platform" or
                            data.get("eventId") is None or
                            data.get("eventCursor")!=event["cursor"] or
                            not isinstance(data.get("challenge"),str) or
                            not re.fullmatch(r"[0-9a-f]{64}",data["challenge"]) or
                            not isinstance(data.get("messageId"),str) or
                            not data["messageId"]):
                            continue
                        try:
                            occurred=epoch(data.get("createdAt"))
                        except (TypeError,ValueError):
                            continue
                        if not (row["created_at"]<=occurred<=row["expires_at"] and
                                now<row["expires_at"]):
                            continue
                        wid=enqueue_tx(c,"deployment:ack:"+row["run_id"],
                            "human_action",
                            {"reason":"verify platform SYN-ACK and send fenced ACK",
                             "deployment_run_id":row["id"]},
                            now,priority=95,start_by=min(now+120,row["expires_at"]))
                        c.execute("""UPDATE deployment_runs SET
                            state='syn_ack_received',syn_ack_event_id=?,
                            syn_ack_event_cursor=?,syn_ack_message_id=?,
                            challenge=?,ack_work_id=?,updated_at=? WHERE id=?""",
                            (data["eventId"],event["cursor"],data["messageId"],
                             data["challenge"],wid,now,row["id"]))
                        row["state"]="syn_ack_received"
                        updated.append({"id":row["id"],"phase":"syn_ack_received"})
                        break
                if row["state"]=="ack_staged" and row["ack_outbox_id"]:
                    a=c.execute("SELECT status FROM outbox WHERE id=?",
                                (row["ack_outbox_id"],)).fetchone()
                    if a and a["status"]=="confirmed":
                        c.execute("""UPDATE deployment_runs SET state='protocol_verified',
                            updated_at=? WHERE id=?""",(now,row["id"]))
                        row["state"]="protocol_verified"
                        updated.append({"id":row["id"],"phase":"protocol_verified"})
                if (row["expires_at"] is not None and now>=row["expires_at"] and
                    row["state"] not in ("protocol_verified","failed","expired")):
                    self._bug_work(c,row,now,"FINAL_ACK_NOT_VERIFIED")
                    c.execute("UPDATE deployment_runs SET state='expired' WHERE id=?", (row["id"],))
                    updated.append({"id":row["id"],"phase":"expired"})
            # A confirmed exact Bug receipt is a separate durable fact.
            reports=c.execute("""SELECT id,bug_outbox_id FROM deployment_runs
                WHERE actor=? AND bug_outbox_id IS NOT NULL AND bug_id IS NULL""",
                (actor[0],)).fetchall()
            for report in reports:
                out=c.execute("SELECT status,result FROM outbox WHERE id=?",
                              (report["bug_outbox_id"],)).fetchone()
                if out and out["status"]=="confirmed":
                    proof=json.loads(out["result"])
                    if proof.get("id"):
                        c.execute("UPDATE deployment_runs SET bug_id=?,updated_at=? WHERE id=?",
                                  (proof["id"],now,report["id"]))
                        updated.append({"id":report["id"],"phase":"bug_reported"})
        return updated
