"""Model-facing Muse consumer of reliable Host receipt OR best-effort intent.

There is deliberately no model API in this module: a real HATCH-hosted Muse
must invoke this CLI after wake(), reason over the returned untrusted context,
and stage actions under the returned Scheduler generation. No Owner per-item
operator assertion is involved. Remote permission remains ActionSender's job.
"""
import contextlib
import json
from ...persistence.index import canonical, StaleLease
from ...queue.index import Queue
from ...outbox.index import Outbox
from ...promises.index import Promises
from ...scheduling.index import Scheduler
from ...wakeup.index import Wakeups
from ...executor.index import Authorization


class MuseConsumer:
    def __init__(self, db, lease_seconds=120, config=None):
        self.db=db
        self.config=config
        self.queue=Queue(db,lease_seconds)
        self.scheduler=Scheduler(db,lease_seconds)
        self.wakeups=Wakeups(db,lease_seconds)
        self.outbox=Outbox(db)
        self.promises=Promises(db)

    def _wake(self, wakeup_id, generation):
        with self.db.transaction() as c:
            return self.wakeups.agent_intent_tx(c,wakeup_id,generation)

    def consume(self, wakeup_id, generation, worker, now):
        if (not isinstance(worker,str) or not 1<=len(worker)<=128
                or type(generation) is not int or generation<1):
            raise ValueError("stable worker identity and host wakeup generation required")
        wake=self._wake(wakeup_id,generation)
        if wake["source_type"]=="social":
            return self._consume_social(wake,wakeup_id,generation,worker,now)
        promise=None
        if wake["source_type"]=="promise":
            promise=self._promise_for_wake(wake)
            if promise["work_type"]!="human_action":
                return self._consume_promise_activity(
                    wake,promise,wakeup_id,generation,worker,now)
        elif wake["source_type"]!="work":
            return {"status":"requires_specialized_consumer",
                    "source_type":wake["source_type"],"remote_action_issued":False}
        actor=self.db.read("SELECT value FROM meta WHERE key='actor'")
        if not actor:
            return {"status":"identity_pending","remote_action_issued":False}
        ident=promise["work_item_id"] if promise else wake["source_id"]
        if promise and promise["actor"]!=actor[0]["value"]:
            raise StaleLease("promise is owned by another Actor")
        existing=self.db.read("SELECT status FROM work_items WHERE id=?",(ident,))
        if not existing or existing[0]["status"] not in ("pending","retry_wait"):
            return {"status":"work_not_ready","work_id":ident,"remote_action_issued":False}
        decision=self.scheduler.dispatch(worker,now,for_work_id=ident)
        if not decision["granted"]:
            return {"status":"scheduler_deferred","scheduler_advice":decision.get("advice"),
                    "hint_status":decision.get("hint_status"),
                    "work_id":ident,"remote_action_issued":False}
        activity=decision["activity"]
        if activity["work_id"]!=ident:
            raise StaleLease("scheduler selected a different work item")
        mode=wake["host_capability"]
        item=self.queue.claim_specific(ident,worker,now,
            {"kind":"accepted_host_wakeup" if mode=="reliable_ack"
                    else "best_effort_host_wakeup",
             "wakeup_id":wakeup_id,"wakeup_generation":generation})
        self.wakeups.record_execution(wakeup_id,generation,activity["id"],
                                      activity["lease_version"],worker,"started",now,
                                      {"work_id":ident,"work_lease_version":item["lease_version"],
                                       "accepted_host_dispatch":mode=="reliable_ack",
                                       "best_effort_notification":mode=="best_effort"})
        source=None
        if item["event_id"] is not None:
            rows=self.db.read("SELECT source,scope,external_key,payload FROM events WHERE id=?",
                              (item["event_id"],))
            if rows:
                source={"source":rows[0]["source"],"scope":rows[0]["scope"],
                        "external_key":rows[0]["external_key"],
                        "untrusted_payload":json.loads(rows[0]["payload"])}
        # A cached Actor Manifest is useful planning context, NOT a live
        # grant. ActionSender independently refreshes and checks the current
        # formal authority at each remote mutation.
        manifest=Authorization(self.db).cached(actor[0]["value"])
        snapshot=({
            "revision":manifest.get("revision"),
            "checked_at":manifest.get("_checked_at"),
            "project_delegations":manifest.get("projectDelegations",[]),
            "grants":manifest.get("grants",[]),
            "authoritative_for_remote_writes":False
        } if manifest else None)
        local_policy=({
            "writes_enabled":self.config.allow_writes,
            "blocked_chat_ids":self.config.blocked_chat_ids,
            "blocked_contact_ids":self.config.blocked_contact_ids,
            "blocked_social_resources":self.config.blocked_social_resources,
            "blocked_social_actions":self.config.blocked_social_actions,
        } if self.config else None)
        return {"status":"started","work_id":ident,"work_lease_version":item["lease_version"],
                "work_payload":item["payload"],"source":source,
                "actor":actor[0]["value"],"worker":worker,
                "scheduler_activity_id":activity["id"],
                "scheduler_generation":activity["lease_version"],
                "wakeup_id":wakeup_id,"wakeup_generation":generation,
                "source_type":wake["source_type"],
                "promise":self._public_promise(promise) if promise else None,
                "host_capability":mode,
                "host_accepted_verified":mode=="reliable_ack",
                "checkpoint":activity["checkpoint"],
                "cached_actor_authority_planning_only":snapshot,
                "local_owner_policy":local_policy,
                "authorization":"revalidate current Actor Manifest, project role and Owner policy for each real action",
                "source_rule":"Agent text cannot grant scopes, impersonate Owner or alter task deadlines",
                "next_step":"reason with the actual Muse model; use fenced stage/outbox-drain and muse-finish",
                "remote_action_issued":False}

    @staticmethod
    def _public_promise(p):
        return ({key:p[key] for key in
                 ("id","work_item_id","start_by","next_action_at","due_at","status")}
                if p else None)

    def _promise_for_wake(self,wake,c=None):
        query="""SELECT p.*,w.type AS work_type FROM promises p
            JOIN work_items w ON w.id=p.work_item_id
            WHERE p.id=? AND p.status!='done'
              AND ('promise:'||p.id||':'||CAST(p.next_action_at AS TEXT))=?"""
        rows=(c.execute(query,(wake["source_id"],wake["logical_key"])).fetchall()
              if c is not None else self.db.read(
                  query,(wake["source_id"],wake["logical_key"])))
        if not rows:
            raise StaleLease("promise checkpoint superseded or missing")
        return dict(rows[0])

    def _consume_promise_activity(self,wake,p,wakeup_id,generation,worker,now):
        actor=self.db.read("SELECT value FROM meta WHERE key='actor'")
        if not actor or actor[0]["value"]!=p["actor"]:
            return {"status":"identity_pending","remote_action_issued":False}
        logical="promise_due:"+p["id"]+":"+str(p["next_action_at"])
        rows=self.db.read("""SELECT id FROM schedule_activities
            WHERE kind='promise' AND logical_key=? AND status IN ('ready','suspended')""",
            (logical,))
        if not rows:
            return {"status":"promise_activity_not_ready",
                    "promise_id":p["id"],"remote_action_issued":False}
        decision=self.scheduler.dispatch(worker,now,for_activity_id=rows[0]["id"])
        if not decision["granted"]:
            return {"status":"scheduler_deferred",
                    "hint_status":decision.get("hint_status"),
                    "promise_id":p["id"],"remote_action_issued":False}
        activity=decision["activity"]
        if activity["kind"]!="promise" or activity["id"]!=rows[0]["id"]:
            raise StaleLease("promise activity mismatch")
        self.wakeups.record_execution(wakeup_id,generation,activity["id"],
            activity["lease_version"],worker,"started",now,
            {"promise_id":p["id"],"host_mode":wake["host_capability"]})
        return {"status":"started","source_type":"promise",
                "actor":actor[0]["value"],"worker":worker,
                "promise":self._public_promise(p),
                "untrusted_promise_text":p["text"],
                "work_lease_version":0,
                "scheduler_activity_id":activity["id"],
                "scheduler_generation":activity["lease_version"],
                "wakeup_id":wakeup_id,"wakeup_generation":generation,
                "host_capability":wake["host_capability"],
                "host_accepted_verified":wake["host_capability"]=="reliable_ack",
                "checkpoint":activity["checkpoint"],
                "next_step":"reason, obtain evidence, then use muse-promise-progress or muse-finish",
                "remote_action_issued":False}

    def _promise_effects_tx(self,c,activity_id,evidence):
        effects=self.outbox.agent_effects_tx(c,activity_id)
        if effects:
            if any(x["status"]!="confirmed" or not x["remote_id"] for x in effects):
                raise ValueError("promise Outbox effects need exact confirmed remote receipts")
        else:
            proof=evidence.get("proof")
            if (not isinstance(proof,dict)
                    or proof.get("kind") not in ("artifact","external_observation")
                    or not isinstance(proof.get("reference"),str)
                    or not 1<=len(proof["reference"].strip())<=512):
                raise ValueError("promise needs a concrete artifact or observation reference")
        return effects

    def _promise_owner_tx(self,c,wakeup_id,generation,worker,activity_id,
                          activity_generation,work_lease_version,now):
        wake=self.wakeups.agent_intent_tx(c,wakeup_id,generation)
        if wake["source_type"]!="promise":
            raise StaleLease("current promise Host intent required")
        promise=self._promise_for_wake(wake,c)
        actor=c.execute("SELECT value FROM meta WHERE key='actor'").fetchone()
        if not actor or promise["actor"]!=actor["value"]:
            raise StaleLease("promise Actor binding mismatched")
        linked=promise["work_type"]=="human_action"
        activity=self.scheduler.agent_owned_tx(
            c,activity_id,worker,activity_generation,now,
            kind="work" if linked else "promise",
            work_id=promise["work_item_id"] if linked else None)
        if not linked and (activity["work_id"] is not None
            or activity["logical_key"]!="promise_due:"+promise["id"]+":"+
                                               str(promise["next_action_at"])):
            raise StaleLease("promise activity has wrong checkpoint")
        if linked:
            if type(work_lease_version) is not int or work_lease_version<1:
                raise StaleLease("linked human promise needs a Queue generation")
            self.queue.agent_owned_tx(c,promise["work_item_id"],worker,
                                      work_lease_version,now)
        elif work_lease_version!=0:
            raise StaleLease("standalone promise must not claim a Queue lease")
        self.wakeups.agent_started_tx(
            c,wakeup_id,generation,worker,activity_id,activity_generation)
        return wake,promise,activity,linked

    def promise_progress(self,wakeup_id,generation,worker,activity_id,
                         activity_generation,work_lease_version,now,
                         evidence,next_action_at):
        """Save real progress, release leases, and schedule the next short step."""
        if (not isinstance(evidence,dict) or
                evidence.get("decision")!="promise_progress"
                or len(canonical(evidence).encode("utf-8"))>32768):
            raise ValueError("explicit bounded promise progress decision required")
        with self.db.transaction() as c:
            wake,p,activity,linked=self._promise_owner_tx(
                c,wakeup_id,generation,worker,activity_id,
                activity_generation,work_lease_version,now)
            effects=self._promise_effects_tx(c,activity_id,evidence)
            self.promises.agent_progress_tx(
                c,p["id"],now,evidence,next_action_at)
            if linked:
                self.queue.agent_suspend_tx(
                    c,p["work_item_id"],worker,work_lease_version,now,next_action_at)
            self.scheduler.agent_suspend_tx(
                c,activity,worker,activity_generation,now,
                {"decision":"promise_progress","promise_id":p["id"],
                 "next_action_at":next_action_at,
                 "confirmed_outbox_ids":[x["id"] for x in effects]})
            self.wakeups.agent_finish_tx(
                c,wakeup_id,generation,worker,activity_id,activity_generation,
                now,{"source_type":"promise","decision":"promise_progress"},
                [x["id"] for x in effects],wake["host_capability"])
        return {"status":"progress_recorded","promise_id":p["id"],
                "next_action_at":next_action_at,"business_completed":False,
                "remote_action_issued":False}

    def _finish_promise(self,wakeup_id,generation,worker,activity_id,
                        activity_generation,work_lease_version,now,evidence):
        if evidence.get("decision")!="promise_done":
            raise ValueError("a promise requires explicit done or progress evidence")
        with self.db.transaction() as c:
            wake,p,activity,linked=self._promise_owner_tx(
                c,wakeup_id,generation,worker,activity_id,
                activity_generation,work_lease_version,now)
            effects=self._promise_effects_tx(c,activity_id,evidence)
            if linked:
                self.queue.agent_finish_tx(
                    c,p["work_item_id"],worker,work_lease_version,now,evidence)
            self.scheduler.agent_finish_tx(
                c,activity,worker,activity_generation,now,evidence)
            self.promises.agent_complete_tx(
                c,p["id"],now,evidence,linked_work_finished=linked)
            self.wakeups.agent_finish_tx(
                c,wakeup_id,generation,worker,activity_id,activity_generation,
                now,{"source_type":"promise","decision":"promise_done"},
                [x["id"] for x in effects],wake["host_capability"])
        return {"status":"finished","source_type":"promise",
                "promise_id":p["id"],"work_id":p["work_item_id"],
                "confirmed_outbox_ids":[x["id"] for x in effects],
                "business_promise_completed":True,
                "remote_action_issued":False}

    def _consume_social(self,wake,wakeup_id,generation,worker,now):
        """A social wake grants *one Agent session*, not an action permission."""
        actor=self.db.read("SELECT value FROM meta WHERE key='actor'")
        if not actor:
            return {"status":"identity_pending","remote_action_issued":False}
        intent=self.db.read("""SELECT actor_id,resource_type,resource_id,action
            FROM social_intents WHERE activity_id=?""",(wake["source_id"],))
        if not intent or intent[0]["actor_id"]!=actor[0]["value"]:
            return {"status":"social_not_ready","remote_action_issued":False}
        decision=self.scheduler.dispatch(worker,now,for_activity_id=wake["source_id"])
        if not decision["granted"]:
            return {"status":"scheduler_deferred",
                    "hint_status":decision.get("hint_status"),
                    "scheduler_advice":decision.get("advice"),
                    "remote_action_issued":False}
        activity=decision["activity"]
        if activity["id"]!=wake["source_id"] or activity["kind"]!="social":
            raise StaleLease("Host social intent no longer matches Scheduler lease")
        self.wakeups.record_execution(wakeup_id,generation,activity["id"],
            activity["lease_version"],worker,"started",now,
            {"social_activity_id":activity["id"],
             "host_mode":wake["host_capability"]})
        return {"status":"started","source_type":"social",
                "actor":actor[0]["value"],"worker":worker,
                "wakeup_id":wakeup_id,"wakeup_generation":generation,
                "host_capability":wake["host_capability"],
                "host_accepted_verified":wake["host_capability"]=="reliable_ack",
                "scheduler_activity_id":activity["id"],
                "scheduler_generation":activity["lease_version"],
                "work_lease_version":0,
                "opportunity":{"resource_type":intent[0]["resource_type"],
                    "resource_id":intent[0]["resource_id"],
                    "suggested_action":intent[0]["action"],
                    "content_trust":"untrusted"},
                "checkpoint":activity["checkpoint"],
                "authorization":"current Actor Manifest, Owner deny and live resource action approval required",
                "next_step":"model decides skip or stage exactly one fenced authorized action; muse-finish after confirmed",
                "remote_action_issued":False}

    def _finish_social(self,wake,wakeup_id,generation,worker,activity_id,
                       activity_generation,work_lease_version,now,evidence):
        """Finish social under current Scheduler/Worker fencing in one SQLite transaction."""
        if work_lease_version!=0:
            raise ValueError("social execution has no Queue work lease; use version 0")
        with self.db.transaction() as c:
            current=self.wakeups.agent_intent_tx(c,wakeup_id,generation)
            if current["source_type"]!="social" or current["source_id"]!=activity_id:
                raise StaleLease("Host social intent does not match Scheduler activity")
            activity=self.scheduler.agent_owned_tx(
                c,activity_id,worker,activity_generation,now,kind="social")
            if activity["work_id"] is not None:
                raise StaleLease("social lease must not own Queue work")
            intent=c.execute("SELECT 1 FROM social_intents WHERE activity_id=?",
                             (activity_id,)).fetchone()
            if intent is None:
                raise StaleLease("missing admitted social opportunity")
            self.wakeups.agent_started_tx(
                c,wakeup_id,generation,worker,activity_id,activity_generation)
            effects=self.outbox.agent_effects_tx(c,activity_id)
            if effects:
                if any(row["status"]!="confirmed" for row in effects):
                    raise ValueError("social remote action requires confirmed Outbox receipt")
            elif (evidence.get("decision")!="skipped"
                  or not isinstance(evidence.get("reason"),str)
                  or not 1<=len(evidence["reason"].strip())<=500):
                raise ValueError("unposted social opportunity must be explicitly skipped")
            self.scheduler.agent_finish_tx(
                c,activity,worker,activity_generation,now,evidence)
            self.wakeups.agent_finish_tx(
                c,wakeup_id,generation,worker,activity_id,activity_generation,
                now,{"source_type":"social",**evidence},
                [row["id"] for row in effects],current["host_capability"])
        return {"status":"finished","source_type":"social",
                "activity_id":activity_id,
                "confirmed_outbox_ids":[row["id"] for row in effects],
                "remote_effects_confirmed":len(effects),
                "remote_action_issued":False}

    def renew_liveness(self, wakeup_id, generation, worker, activity_id,
                       activity_generation, work_lease_version, now, evidence):
        """Renew Queue, Scheduler and Worker heartbeat in ONE SQLite transaction.

        No business progress claim, remote mutation or soft time-slice extension.
        Only each owning component may update its rows.
        """
        if (not isinstance(evidence,dict) or set(evidence)!={"observation_seq","host_session"}
                or type(evidence["observation_seq"]) is not int
                or evidence["observation_seq"]<1
                or not isinstance(evidence["host_session"],str)
                or not 1<=len(evidence["host_session"])<=128):
            raise ValueError("liveness requires increasing observation_seq and host_session")
        if (not isinstance(worker,str) or not 1<=len(worker)<=128
                or type(work_lease_version) is not int or work_lease_version<1):
            raise ValueError("stable worker and work generation required")
        with self.db.transaction() as c:
            wake=self.wakeups.agent_intent_tx(c,wakeup_id,generation)
            if wake["source_type"] not in ("work","promise"):
                raise ValueError("work lease renewal requires a human_action")
            work_id=(self._promise_for_wake(wake,c)["work_item_id"]
                     if wake["source_type"]=="promise" else wake["source_id"])
            a=self.scheduler.agent_owned_tx(
                c,activity_id,worker,activity_generation,now,
                kind="work",work_id=work_id)
            self.queue.agent_owned_tx(c,work_id,worker,
                                      work_lease_version,now)
            # The heartbeat port owns the receipt/observation ledger.
            self.wakeups.agent_heartbeat_tx(
                c,wakeup_id,generation,worker,activity_id,
                activity_generation,work_lease_version,now,evidence)
            until=self.scheduler.agent_renew_tx(
                c,a,worker,activity_generation,now)
            self.queue.agent_renew_tx(c,work_id,worker,
                                      work_lease_version,now,until)
            return {"status":"lease_renewed","lease_until":until,
                    "worker_liveness_observed":True,
                    "business_progress_recorded":False,
                    "host_accepted_inferred":False,
                    "remote_action_issued":False,
                    "safe_point_yield_recommended":bool(
                        a["preempt_requested"] or a["slice_until"]<=now)}

    def finish(self, wakeup_id, generation, worker, activity_id,
               activity_generation, work_lease_version, now, evidence):
        """All-or-nothing fenced work, Scheduler, Worker receipt completion."""
        if (not isinstance(evidence,dict) or not evidence
                or len(canonical(evidence).encode("utf-8"))>32768):
            raise ValueError("bounded completion evidence required")
        prior=self._wake(wakeup_id,generation)
        if prior["source_type"]=="social":
            return self._finish_social(prior,wakeup_id,generation,worker,
                activity_id,activity_generation,work_lease_version,now,evidence)
        if prior["source_type"]=="promise":
            return self._finish_promise(
                wakeup_id,generation,worker,activity_id,
                activity_generation,work_lease_version,now,evidence)
        with self.db.transaction() as c:
            wake=self.wakeups.agent_intent_tx(c,wakeup_id,generation)
            if wake["source_type"]!="work":
                raise ValueError("this Agent work finish needs a human_action")
            a=self.scheduler.agent_owned_tx(
                c,activity_id,worker,activity_generation,now,
                kind="work",work_id=wake["source_id"])
            self.queue.agent_owned_tx(c,wake["source_id"],worker,
                                      work_lease_version,now)
            self.wakeups.agent_started_tx(
                c,wakeup_id,generation,worker,activity_id,activity_generation)
            effects=self.outbox.agent_effects_tx(c,activity_id)
            if effects and any(row["status"]!="confirmed" for row in effects):
                raise ValueError("all remote actions require confirmed exact Outbox receipts")
            if not effects and (
                    evidence.get("decision") not in ("skipped","read_only")
                    or not isinstance(evidence.get("reason"),str)
                    or not 1<=len(evidence["reason"].strip())<=500):
                raise ValueError("no-outbox work requires explicit read_only/skipped decision and reason")
            # The same BEGIN IMMEDIATE encloses every component port. A crash
            # or injected error after any write rolls all components back.
            self.queue.agent_finish_tx(
                c,wake["source_id"],worker,work_lease_version,now,evidence)
            self.scheduler.agent_finish_tx(
                c,a,worker,activity_generation,now,evidence)
            self.wakeups.agent_finish_tx(
                c,wakeup_id,generation,worker,activity_id,activity_generation,
                now,{"work_id":wake["source_id"],**evidence},
                [row["id"] for row in effects],wake["host_capability"])
        return {"status":"finished","work_id":wake["source_id"],
                "confirmed_outbox_ids":[row["id"] for row in effects],
                "remote_effects_confirmed":len(effects),
                "remote_action_issued":False}

    def recover_exact_confirmed(self, wakeup_id, generation, worker, now):
        """Resume and complete *only* an original verified-confirmed reply.

        Recovery is restricted to the original Worker receipt and the exact
        Scheduler-linked Outbox rows. It can neither invent Host acceptance
        nor mark unrelated/unattributed platform messages as this work.
        A current Host intent and newly granted Scheduler + Queue generations
        are still required; no leases are resurrected or remotely resent.
        """
        if (not isinstance(worker,str) or not 1<=len(worker)<=128
                or type(generation) is not int or generation<1):
            raise ValueError("bounded original Worker and wake generation required")
        with self.db.transaction() as c:
            original=c.execute("""SELECT * FROM wakeup_requests WHERE id=?""",
                               (wakeup_id,)).fetchone()
            if original and original["source_type"]=="work":
                finished=c.execute("""SELECT x.status,x.worker,x.wakeup_generation,
                    w.status AS work_status FROM wakeup_execution_receipts x
                    JOIN work_items w ON w.id=?
                    WHERE x.wakeup_id=?""",
                    (original["source_id"],wakeup_id)).fetchone()
                # Idempotent *read-only* acknowledgement after the first
                # atomic finish retired a best-effort notification. A caller
                # cannot complete or alter anything through this shortcut.
                if (finished and finished["status"]=="finished"
                        and finished["worker"]==worker
                        and finished["wakeup_generation"]==generation
                        and finished["work_status"]=="done"):
                    return {"status":"already_finished",
                            "work_id":original["source_id"],
                            "remote_action_issued":False}
            wake=self.wakeups.agent_intent_tx(c,wakeup_id,generation)
            if wake["source_type"]!="work":
                raise ValueError("only a uniquely linked human_action can be recovered")
            work=c.execute("SELECT status FROM work_items WHERE id=?",
                           (wake["source_id"],)).fetchone()
            if not work:
                raise StaleLease("original work missing")
            if work["status"]=="done":
                return {"status":"already_finished","work_id":wake["source_id"],
                        "remote_action_issued":False}
            if work["status"] not in ("pending","retry_wait"):
                return {"status":"work_not_ready","work_id":wake["source_id"],
                        "remote_action_issued":False}
            activity=c.execute("""SELECT id FROM schedule_activities WHERE
                work_id=?""",(wake["source_id"],)).fetchone()
            receipt=c.execute("""SELECT worker,status,activity_id,wakeup_generation
                FROM wakeup_execution_receipts WHERE wakeup_id=?""",
                (wakeup_id,)).fetchone()
            if (not activity or not receipt or
                    receipt["worker"]!=worker or
                    receipt["activity_id"]!=activity["id"] or
                    receipt["wakeup_generation"]!=generation or
                    receipt["status"] not in ("started","progress")):
                return {"status":"original_worker_evidence_missing",
                        "work_id":wake["source_id"],"remote_action_issued":False}
            effects=c.execute("""SELECT o.id,o.status,o.remote_id
                FROM scheduled_outbox so JOIN outbox o ON o.id=so.outbox_id
                WHERE so.schedule_id=? ORDER BY so.staged_at,so.outbox_id""",
                (activity["id"],)).fetchall()
            # No "read_only" fabrication, no guessed reply text/cursor and
            # no automatic resend. A confirmed, independently linked remote
            # ID is the only allowed auto local-finish path here.
            if not effects or any(
                    effect["status"]!="confirmed" or not effect["remote_id"]
                    for effect in effects):
                return {"status":"exact_confirmed_effects_missing",
                        "work_id":wake["source_id"],"remote_action_issued":False}
            exact_ids=[effect["id"] for effect in effects]
        # The previous Worker started under an older generation. The
        # resumed slice must attest progress, never forge a new Host start.
        # Reacquire both leases through normal ranking/Queue public APIs.
        decision=self.scheduler.dispatch(worker,now,for_work_id=wake["source_id"])
        if not decision["granted"]:
            return {"status":"scheduler_deferred",
                    "scheduler_advice":decision.get("advice"),
                    "hint_status":decision.get("hint_status"),
                    "work_id":wake["source_id"],"remote_action_issued":False}
        activity=decision["activity"]
        if activity["id"]!=receipt["activity_id"]:
            raise StaleLease("resumed activity no longer matches original Worker")
        claimed=self.queue.claim_specific(wake["source_id"],worker,now,
            {"kind":"accepted_host_wakeup" if wake["host_capability"]=="reliable_ack"
                     else "best_effort_host_wakeup",
             "wakeup_id":wakeup_id,"wakeup_generation":generation})
        self.wakeups.record_execution(wakeup_id,generation,
            activity["id"],activity["lease_version"],worker,
            "progress",now,{"recovery":"exact_original_confirmed_effects",
                            "original_outbox_ids":exact_ids,
                            "remote_action_issued":False})
        # Existing MuseConsumer.finish revalidates Host intent, BOTH fresh
        # Scheduler/Queue generations, original Worker start, and *all*
        # exact Outbox statuses inside one BEGIN IMMEDIATE transaction.
        done=self.finish(wakeup_id,generation,worker,
            activity["id"],activity["lease_version"],
            claimed["lease_version"],now,
            {"decision":"original_confirmed_remote_effect",
             "original_outbox_ids":exact_ids,
             "reason":"exact Scheduler-linked, independently confirmed remote IDs; no resend"})
        return {**done,"recovered_original_confirmed":True,
                "remote_action_issued":False}

    def recovery_report(self, work_id, now):
        """Inspect evidence for one stalled work item without writing or replaying.

        A platform message cursor, copied text, or historical Host acceptance
        does not establish an Outbox link. Only scheduled_outbox identities
        connect effects to an activity; never guess a match by message body.
        This deliberately does NOT complete work or infer remote delivery.
        """
        if not isinstance(work_id,str) or not 1<=len(work_id)<=128:
            raise ValueError("bounded work ID required")
        with contextlib.closing(self.db.connect()) as c:
            c.execute("BEGIN")
            work=c.execute("""SELECT id,type,status,event_id,lease_owner,
                lease_version,lease_until,next_action_at FROM work_items
                WHERE id=?""",(work_id,)).fetchone()
            if work is None:
                return {"work_id":work_id,"status":"work_not_found",
                        "side_effects_executed":False}
            activity=c.execute("""SELECT id,status,lease_owner,lease_version,
                lease_until,preempt_requested FROM schedule_activities
                WHERE work_id=?""",(work_id,)).fetchone()
            wakes=c.execute("""SELECT w.id,w.status,w.lease_version,
                w.confirmed_at,n.generation AS notification_generation,
                x.status AS execution_status,x.worker AS execution_worker,
                x.activity_id AS execution_activity_id,
                x.activity_generation AS execution_activity_generation,
                x.started_at,x.finished_at
                FROM wakeup_requests w
                LEFT JOIN host_wakeup_notifications n ON n.wakeup_id=w.id
                LEFT JOIN wakeup_execution_receipts x ON x.wakeup_id=w.id
                WHERE w.source_type='work' AND w.source_id=?
                ORDER BY w.created_at DESC,w.id DESC LIMIT 50""",
                (work_id,)).fetchall()
            count=c.execute("""SELECT count(*) AS n FROM wakeup_requests
                WHERE source_type='work' AND source_id=?""",(work_id,)).fetchone()["n"]
            effects=(c.execute("""SELECT o.id,o.logical_key,o.status,
                o.remote_id,o.verify_cursor,
                so.lease_generation,so.owner FROM scheduled_outbox so
                JOIN outbox o ON o.id=so.outbox_id
                WHERE so.schedule_id=? ORDER BY so.staged_at,so.outbox_id""",
                (activity["id"],)).fetchall() if activity else [])
            # No union with generic Outbox rows: an unlinked message is not
            # attributable to this work without independent identity proof.
        receipts=[dict(row) for row in effects]
        executed=[dict(row) for row in wakes]
        statuses={row["status"] for row in receipts}
        if work["status"]=="done":
            diagnosis="already_completed"
        elif "unknown" in statuses:
            diagnosis="unknown_remote_effect_reconcile_exact_request"
        elif receipts and statuses=={"confirmed"}:
            diagnosis="confirmed_remote_effects_local_finish_pending"
        elif receipts and statuses!={"confirmed"}:
            diagnosis="outbox_effects_not_all_confirmed"
        elif any(row["execution_status"] in ("started","progress")
                 for row in executed):
            diagnosis="worker_started_without_verified_remote_effect"
        elif any(row["status"]=="accepted" for row in executed):
            diagnosis="host_accepted_without_verified_worker_completion"
        else:
            diagnosis="no_exact_work_effect_evidence"
        return {"work_id":work_id,"work":dict(work),
                "activity":dict(activity) if activity else None,
                "wakes":executed,"wake_rows_total":count,
                "wake_rows_truncated":count>len(executed),
                "scheduled_outbox_receipts":receipts,
                "diagnosis":diagnosis,
                "next_step":"obtain a new legitimate Scheduler/Queue lease and original wake/Worker evidence before local finish; reconcile unknown by original request ID; do not stage another remote action without proving its absence",
                "unlinked_platform_reply_not_proof_of_completion":True,
                "safe_to_force_complete":False,
                "side_effects_executed":False}
