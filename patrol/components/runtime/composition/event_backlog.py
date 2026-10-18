"""Strictly read-only and bounded Agent event backlog census (#65).

No migration, tick, work lease, business status, cursor or Outbox mutation.
The platform owns event semantics and the live Actor owns any cleanup.
"""
from collections import Counter
import json
import sqlite3
from pathlib import Path
from urllib.parse import quote

def backlog_report(db,now,limit=1000):
    """Read-only and bounded pending census. No work status or cursor updates.

    Emits aggregated statistics only: no user text, tokens, per-message IDs,
    Actor or resource IDs. A recommendation is *not* permission to close
    an old work item: source business status and Outbox still need review.
    """
    if type(limit) is not int or not 1<=limit<=5000:
        raise ValueError("backlog report limit must be 1..5000")
    statuses=("pending","retry_wait","blocked","needs_approval","running")
    by_type={
        r["type"]:{r["status"]:r["n"] for r in db.read(
            """SELECT status,COUNT(*) AS n FROM work_items
               WHERE type=? GROUP BY status""",(r["type"],))}
        for r in db.read("SELECT DISTINCT type FROM work_items")
    }
    active=db.read("""SELECT w.type,w.status,w.created_at,w.lease_owner,
                e.source,e.payload
             FROM work_items w LEFT JOIN events e ON e.id=w.event_id
             WHERE w.status IN (?,?,?,?,?)
             ORDER BY w.created_at,w.id LIMIT ?""",(*statuses,limit+1))
    truncated=len(active)>limit
    rows=active[:limit]
    groups=Counter()
    ages=Counter()
    routes=Counter()
    for r in rows:
        try:
            event=json.loads(r["payload"]) if r["payload"] else {}
        except (TypeError,ValueError):
            event={}
        kind=event.get("type")
        source=r["source"] or "local"
        typ=r["type"]
        name=kind if isinstance(kind,str) and kind else "unknown"
        groups[(typ,r["status"],source,name)]+=1
        age=max(0,float(now)-float(r["created_at"]))
        band=("under_15m" if age<900 else
              "15m_to_1h" if age<3600 else
              "1h_to_24h" if age<86400 else "over_24h")
        ages[(typ,band)]+=1
        if typ=="human_action" and source=="agent_events":
            # Observed event names only. Never convert a dry-run guess
            # into an instruction to close or resume an old lease.
            routes[name]+=1
    return {
        "status":"read_only",
        "as_of_epoch":now,
        "sample_limit":limit,
        "active_rows_examined":len(rows),
        "truncated":truncated,
        "total_work_by_type_status":by_type,
        "active_by_event":[
            {"work_type":t,"work_status":s,"source":src,"event_type":kind,"count":n}
            for (t,s,src,kind),n in sorted(groups.items())
        ],
        "active_age_buckets":[
            {"work_type":t,"age":age,"count":n}
            for (t,age),n in sorted(ages.items())
        ],
        "existing_agent_event_type_sample":dict(sorted(routes.items())),
        "changes_applied":0,
        "safe_to_bulk_close":False,
        "note":"Old records require source/lease/Outbox evidence; no automatic migration."
    }

def read_only_backlog_report(database_path,now,limit=1000):
    """Inspect a *pre-existing* SQLite DB without opening migration or a Tick lock.

    SQLite mode=ro enforces no accidental writes including from future query
    changes. Never create a new DB or sidecar state on missing installations.
    """
    path=Path(database_path).expanduser().resolve(strict=True)
    if not path.is_file():
        raise ValueError("existing private runtime SQLite database required")
    uri="file:"+quote(str(path),safe="/")+ "?mode=ro"
    con=sqlite3.connect(uri,uri=True,timeout=2)
    try:
        con.row_factory=sqlite3.Row
        con.execute("PRAGMA query_only=ON")

        class Reader:
            def read(self,statement,params=()):
                if not statement.lstrip().upper().startswith("SELECT"):
                    raise ValueError("only read-only SELECT statements allowed")
                return [dict(x) for x in con.execute(statement,params)]

        return backlog_report(Reader(),now,limit)
    finally:
        con.close()
