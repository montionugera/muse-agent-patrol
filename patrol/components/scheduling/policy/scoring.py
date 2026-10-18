"""Pure bounded, interpretable scheduler heuristic; no database access."""
import math


def priority(row, now, *, running=False):
    base=float(row["base_priority"])
    deadline=row["deadline"]
    urgent=0.0
    if deadline is not None:
        remaining=float(deadline)-now
        urgent=75.0 if remaining<=0 else max(0.0, 75.0*(1.0-remaining/3600.0))
    ready_since=float(row["ready_since"])
    # The initial aging ramp is bounded to limit short-term thrashing. A
    # second, slower credit helps moderate priority gaps. Both cap at 30:
    # aging saturates at 60 after 45 minutes, so six-day-old work cannot
    # accumulate the ~17000-point bonus that caused issue #45's immediate
    # grant -> preempt -> StaleLease livelock. This is bounded fairness,
    # NOT a guarantee against starvation under an endless stream of much
    # higher-priority work. Each granted slice resets ready_since on yield.
    wait=max(0.0,now-ready_since)
    aging=0.0 if running else min(30.0,wait/90.0)+min(30.0,max(0.0,wait-900.0)/30.0)
    fair=min(34.0,max(0.0,float(row["virtual_runtime"])/60.0))
    # Do not thrash on small priority changes. Existing work retains some
    # continuity, while new resumptions pay a measured local restore penalty.
    restore=0 if running else min(15.0,float(row["switch_cost"]))
    continuity=8.0 if running else 0.0
    return round(base+urgent+aging-fair-restore+continuity,3)


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
    if other_score>=active_score+12+min(8.0,float(running["switch_cost"])/2):
        return "medium_safe_point"
    if now>=float(running["slice_until"]) and other_score>=active_score-12:
        return "soft_safe_point"
    return None
