"""Compose existing Wakeup and Scheduler facilities for a host Hook.

This is a durable handoff *reservation*, not proof a Muse was woken,
an activity was dispatched, or platform work was completed.
"""
from ...scheduling.index import Scheduler
from ...wakeup.index import Wakeups


class HostHandoff:
    def __init__(self, db, lease_seconds=120):
        self.scheduler=Scheduler(db,lease_seconds)
        self.wakeups=Wakeups(db,lease_seconds)

    def prepare(self, owner, now, allow_social=True, capability="reliable_ack"):
        if capability not in ("reliable_ack","best_effort","unavailable"):
            raise ValueError("invalid verified Host capability")
        if capability=="unavailable":
            return {"should_wake":False,"host_called":False,
                    "status":"host_unavailable","activity_dispatched":False,
                    "host_capability":"unavailable"}
        """Reserve one due intent and provide bounded, non-content routing hints.

        The Hook invokes wake() only after a successful reservation. Its host
        must subsequently acknowledge actual acceptance using wakeup-ack with
        verifiable evidence; a missing receipt becomes unknown at lease expiry.
        The Muse worker, not the Hook, owns schedule-next and model execution.
        """
        decision=self.scheduler.reconcile(now)["decision"]
        # A host wake is not a Scheduler preemption. The running Muse must
        # reach a checkpoint/yield safe point before another activity can be
        # delivered; otherwise an accepted wake would be stranded.
        if decision["running"] is not None:
            return {"should_wake":False,"host_called":False,
                    "status":"scheduler_busy","activity_dispatched":False,
                    "scheduler_advice":decision["advice"]}
        # Idle social is opt-in only. The same Hook and wakeup table handle
        # it, but never wake for social while another activity is running
        # or an eligible higher-value formal task is the best candidate.
        social_eligible=bool(
            allow_social and decision["running"] is None and
            decision["candidate"] and decision["candidate"]["kind"]=="social")
        social_id=decision["candidate"]["id"] if social_eligible else None
        if social_id:
            self.wakeups.stage_social(now,social_id)
        # Do not wake a lower-ranked work item while Scheduler would deny
        # its for-work-id dispatch. Preemption is checkpointed, not stolen.
        focus_work=(decision["candidate"]["work_id"]
                    if decision["candidate"] and
                       decision["candidate"]["kind"]=="work" else None)
        if capability=="best_effort":
            # Never consume a reliable-ACK delivery lease on hosts that can
            # only notify the agent. The durable throttled advice is not an ACK.
            source_type="social" if social_eligible else "work"
            source_id=social_id if social_eligible else focus_work
            if not source_id:
                return {"should_wake":False,"host_called":False,
                        "status":"no_actionable_agent_activity",
                        "activity_dispatched":False,
                        "host_capability":"best_effort",
                        "scheduler_advice":decision["advice"]}
            notice=self.wakeups.prepare_best_effort(
                source_id,now,source_type=source_type)
            if notice is None:
                return {"should_wake":False,"host_called":False,
                        "status":"notification_cooled_down","activity_dispatched":False,
                        "host_capability":"best_effort",
                        "scheduler_advice":decision["advice"]}
            return {"should_wake":True,"host_called":False,
                    "status":"notification_intent","activity_dispatched":False,
                    **notice,"scheduler_advice":decision["advice"],
                    "candidate_activity_id":decision["candidate"]["id"]}
        leased=self.wakeups.claim_next(
            owner,now,allow_social=social_eligible,
            social_activity_id=social_id,focus_work_id=focus_work)
        if leased is None:
            return {"should_wake":False,"host_called":False,
                    "status":"idle","activity_dispatched":False,
                    "scheduler_advice":decision["advice"]}
        candidate=decision["candidate"]
        current=decision["running"]
        return {"should_wake":True,"host_called":False,
                "status":"reserved","activity_dispatched":False,
                "wakeup_id":leased["id"],"source_type":leased["source_type"],
                "source_id":leased["source_id"],"lease_owner":leased["lease_owner"],
                "lease_version":leased["lease_version"],
                "lease_until":leased["lease_until"],
                "scheduler_advice":decision["advice"],
                "candidate_activity_id":candidate["id"] if candidate else None,
                "running_activity_id":current["id"] if current else None,
                "requires_host_receipt":True}
