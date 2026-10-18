"""Pure bounded, interpretable scheduler heuristic; no database access."""
import math


def intrinsic_priority(row, now):
    """Base priority plus genuine deadline urgency, excluding aging/fairness."""
    deadline=row["deadline"]
    urgent=0.0
    if deadline is not None:
        remaining=float(deadline)-now
        urgent=75.0 if remaining<=0 else max(0.0,75.0*(1.0-remaining/3600.0))
    return float(row["base_priority"])+urgent


def priority(row, now, *, running=False):
    core=intrinsic_priority(row,now)
    ready_since=float(row["ready_since"])
    # Waiting credit is bounded (<=60) and belongs to the slice that won
    # dispatch. Dropping it to zero immediately after grant lets another
    # equally old activity win the next tick and fence work-claim before the
    # Agent starts. Freeze the earned credit at last_started_at while running;
    # do not accrue additional waiting credit during execution. On yield the
    # dispatcher resets ready_since, so the next turn earns credit afresh.
    # This is finite fairness, not starvation freedom under arbitrary load.
    started=(row["last_started_at"] if "last_started_at" in row.keys() else None)
    age_until=(min(now,float(started)) if running and started is not None
               else ready_since if running else now)
    wait=max(0.0,age_until-ready_since)
    aging=min(30.0,wait/90.0)+min(30.0,max(0.0,wait-900.0)/30.0)
    fair=min(34.0,max(0.0,float(row["virtual_runtime"])/60.0))
    # Do not thrash on small priority changes. Existing work retains some
    # continuity, while new resumptions pay a measured local restore penalty.
    restore=0 if running else min(15.0,float(row["switch_cost"]))
    continuity=8.0 if running else 0.0
    return round(core+aging-fair-restore+continuity,3)


def quantum(weight):
    """Finite, weight-sensitive cooperative time slice, in seconds."""
    return max(20.0,min(120.0,30.0+float(weight)*0.6))


def preemption(running, challenger, now):
    """Return requested safe-point mode, not permission to kill a model."""
    if challenger is None:
        return None
    active_score=priority(running,now,running=True)
    other_score=priority(challenger,now)
    remaining=(float(challenger["deadline"])-now
               if challenger["deadline"] is not None else math.inf)
    # A task with a hard deadline in the next 30 seconds must first get
    # its safe point; long-waiting background work cannot displace it.
    running_deadline=running["deadline"]
    if (running_deadline is not None and float(running_deadline)-now<=30
            and (challenger["deadline"] is None or float(challenger["deadline"])-now>30)):
        return None
    # A large gap supersedes hysteresis; near-deadline work has an
    # urgency-sensitive override; expired slices permit fair rotation.
    if remaining<=30 and other_score>=active_score-8:
        return "urgent_safe_point"
    # Waiting credit preserves a newly granted slice, but must not mask a
    # genuinely more important fresh Owner/action. Compare intrinsic urgency
    # independently of age; a large base/deadline gap is preemptible even
    # when the running slice won with maximum waiting credit.
    if intrinsic_priority(challenger,now)>=intrinsic_priority(running,now)+25:
        return "medium_safe_point"
    if other_score>=active_score+12+min(8.0,float(running["switch_cost"])/2):
        return "medium_safe_point"
    if now>=float(running["slice_until"]) and other_score>=active_score-12:
        return "soft_safe_point"
    return None
