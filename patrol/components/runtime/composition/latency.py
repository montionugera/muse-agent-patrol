"""Bounded, read-only latency evidence. Never synthesize Host or remote timestamps."""
import math


def _seconds(end, start):
    if end is None or start is None or end < start:
        return None
    return round(float(end)-float(start), 3)


def _percentile(values, quantile):
    values=sorted(values)
    if not values:
        return None
    n=(len(values)-1)*quantile
    lo=int(math.floor(n))
    hi=int(math.ceil(n))
    return round(values[lo]+(values[hi]-values[lo])*(n-lo),3)


class LatencyReport:
    """Keep observation source and model-vs-host uncertainty explicit."""

    METRICS=("event_to_collection","work_to_worker","intent_to_worker",
             "reliable_accept_to_worker","outbox_to_verified",
             "work_to_verified","message_to_verified","work_to_finish")

    def __init__(self,db):
        self.db=db

    def snapshot(self,now,limit=100):
        if type(limit) is not int or not 1<=limit<=500:
            raise ValueError("bounded latency sample limit must be 1..500")
        with self.db.transaction() as c:
            rows=[dict(r) for r in c.execute("""SELECT w.id AS work_id,w.status AS work_status,
                    w.created_at AS work_created_at,
                    e.observed_at AS event_collected_at,
                    mr.message_created_at AS message_created_at,
                    wa.id AS wakeup_id,
                    CASE WHEN wa.status='accepted' THEN wa.confirmed_at END AS
                        reliable_host_accepted_at,
                    hn.intent_at AS best_effort_intent_at,
                    x.started_at AS worker_started_at,
                    x.finished_at AS worker_finished_at,
                    s.id AS activity_id
                FROM work_items w
                LEFT JOIN events e ON e.id=w.event_id
                LEFT JOIN message_receipts mr ON mr.event_id=w.event_id
                LEFT JOIN wakeup_requests wa ON wa.source_type='work'
                     AND wa.source_id=w.id
                LEFT JOIN host_wakeup_notifications hn ON hn.wakeup_id=wa.id
                LEFT JOIN wakeup_execution_receipts x ON x.wakeup_id=wa.id
                LEFT JOIN schedule_activities s ON s.work_id=w.id
                WHERE w.type='human_action'
                ORDER BY w.created_at DESC,w.id DESC LIMIT ?""",(limit,)).fetchall()]
            # Reuse the SAME SQLite read transaction for an immutable snapshot
            # across work, activity and Outbox, without sending or scheduling.
            ids=[r["activity_id"] for r in rows if r["activity_id"]]
            effects={}
            for ident in ids:
                effects[ident]=[dict(x) for x in c.execute("""
                    SELECT o.id,o.created_at,o.verified_at,o.status
                    FROM scheduled_outbox so JOIN outbox o ON o.id=so.outbox_id
                    WHERE so.schedule_id=? ORDER BY o.created_at,o.id""",(ident,))]
        records=[]
        for r in rows:
            linked=effects.get(r["activity_id"],[])
            fully_verified=(bool(linked) and
                all(x["status"]=="confirmed" and x["verified_at"] is not None
                    for x in linked))
            verified_at=max(x["verified_at"] for x in linked) if fully_verified else None
            start_at=min(x["created_at"] for x in linked) if linked else None
            metrics={
                "event_to_collection":_seconds(r["event_collected_at"],r["message_created_at"]),
                "work_to_worker":_seconds(r["worker_started_at"],r["work_created_at"]),
                "intent_to_worker":_seconds(r["worker_started_at"],r["best_effort_intent_at"]),
                "reliable_accept_to_worker":_seconds(r["worker_started_at"],r["reliable_host_accepted_at"]),
                "outbox_to_verified":_seconds(verified_at,start_at),
                "work_to_verified":_seconds(verified_at,r["work_created_at"]),
                "message_to_verified":_seconds(verified_at,r["message_created_at"]),
                "work_to_finish":_seconds(r["worker_finished_at"],r["work_created_at"])}
            records.append({"work_id":r["work_id"],"work_status":r["work_status"],
                "activity_id":r["activity_id"],"wakeup_id":r["wakeup_id"],
                "outbox_count":len(linked),
                "outbox_statuses":sorted({x["status"] for x in linked}),
                "fully_verified_with_local_timestamp":fully_verified,
                "metrics_seconds":metrics})
        aggregates={}
        for name in self.METRICS:
            values=[r["metrics_seconds"][name] for r in records
                    if r["metrics_seconds"][name] is not None]
            aggregates[name]={"samples":len(values),"p50_seconds":_percentile(values,.5),
                "p95_seconds":_percentile(values,.95),
                "max_seconds":round(max(values),3) if values else None,
                "small_sample":bool(values) and len(values)<20}
        return {"generated_at":now,"limit":limit,"work_sample_size":len(records),
                "unfinished_work_count":sum(r["work_status"]!="done" for r in records),
                "metrics":aggregates,"samples":records,
                "limitations":[
                    "Local best_effort_intent_at is not actual Host wake delivery or acceptance.",
                    "Exact Outbox verified_at records local readback, not remote server commit time.",
                    "Historical confirmed rows without verified_at are intentionally excluded from verified latency.",
                    "Missing timestamps remain null; P95 with fewer than 20 observations is exploratory.",
                    "Work-to-worker includes polling, scheduling, Host, and model-start wait; it is not pure queue wait."],
                "side_effects_executed":False}
