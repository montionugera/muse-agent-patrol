"""SQLite connection/transaction ownership. No network calls inside transactions."""
import contextlib
import json
import os
import re
import sqlite3
import time
import uuid
from pathlib import Path


class Busy(RuntimeError):
    pass


class StaleLease(RuntimeError):
    pass


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def new_id():
    return uuid.uuid4().hex


def cursor(value):
    # Keep arbitrary-precision cursors as strings at every public boundary.
    if not isinstance(value, str) or not re.fullmatch(r"0|[1-9][0-9]*", value):
        raise ValueError("cursor must be a canonical decimal string")
    return value


SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS patrol_runs(
 id TEXT PRIMARY KEY, started_at REAL NOT NULL, ended_at REAL,
 status TEXT NOT NULL, report TEXT NOT NULL DEFAULT '{}');
CREATE TABLE IF NOT EXISTS sync_cursors(
 source TEXT NOT NULL, scope TEXT NOT NULL, stream TEXT NOT NULL DEFAULT '',
 received TEXT NOT NULL DEFAULT '0', handled TEXT NOT NULL DEFAULT '0',
 target TEXT, next_at REAL NOT NULL DEFAULT 0, failures INTEGER NOT NULL DEFAULT 0,
 covered_at REAL NOT NULL DEFAULT 0,
 PRIMARY KEY(source,scope));
CREATE TABLE IF NOT EXISTS events(
 id INTEGER PRIMARY KEY, source TEXT NOT NULL, scope TEXT NOT NULL, stream TEXT NOT NULL,
 external_key TEXT NOT NULL, cursor TEXT, payload TEXT NOT NULL, observed_at REAL NOT NULL,
 UNIQUE(source,scope,stream,external_key));
CREATE TABLE IF NOT EXISTS work_items(
 id TEXT PRIMARY KEY, event_id INTEGER, logical_key TEXT NOT NULL UNIQUE,
 type TEXT NOT NULL, payload TEXT NOT NULL, status TEXT NOT NULL,
 priority INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL,
 start_by REAL, next_action_at REAL NOT NULL, due_at REAL, attempts INTEGER NOT NULL DEFAULT 0,
 lease_owner TEXT, lease_version INTEGER NOT NULL DEFAULT 0, lease_until REAL,
 first_action_at REAL, last_progress_at REAL, evidence TEXT, block_reason TEXT);
CREATE INDEX IF NOT EXISTS work_due ON work_items(status,next_action_at);
-- Additive v1 schema extension, safe for existing local databases.
CREATE TABLE IF NOT EXISTS message_scores(
 event_id INTEGER PRIMARY KEY REFERENCES events(id) ON DELETE CASCADE,
 base_score INTEGER NOT NULL CHECK(base_score BETWEEN 0 AND 100),
 muse_score INTEGER CHECK(muse_score IS NULL OR muse_score BETWEEN 0 AND 100),
 scored_by TEXT, reason TEXT, scored_at REAL,
 baseline_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS message_score_history(
 id INTEGER PRIMARY KEY, event_id INTEGER NOT NULL REFERENCES events(id),
 scorer TEXT NOT NULL, score INTEGER NOT NULL CHECK(score BETWEEN 0 AND 100),
 reason TEXT NOT NULL, evidence TEXT NOT NULL, scored_at REAL NOT NULL);
CREATE INDEX IF NOT EXISTS message_score_unrated ON message_scores(muse_score,event_id);
-- Immutable first-observation freshness, separate from when a queued inspector runs.
-- Existing databases receive this table additively; no cursor or event reset.
CREATE TABLE IF NOT EXISTS message_receipts(
 event_id INTEGER PRIMARY KEY REFERENCES events(id) ON DELETE CASCADE,
 observed_at REAL NOT NULL,
 message_created_at REAL,
 fresh_at_receipt INTEGER NOT NULL CHECK(fresh_at_receipt IN (0,1)));

CREATE TABLE IF NOT EXISTS wakeup_requests(
 id TEXT PRIMARY KEY, logical_key TEXT NOT NULL UNIQUE,
 source_type TEXT NOT NULL CHECK(source_type IN ('work','promise','social')),
 source_id TEXT NOT NULL, priority INTEGER NOT NULL CHECK(priority BETWEEN 0 AND 100),
 reason TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
 created_at REAL NOT NULL, next_attempt_at REAL NOT NULL,
 lease_owner TEXT, lease_version INTEGER NOT NULL DEFAULT 0, lease_until REAL,
 attempts INTEGER NOT NULL DEFAULT 0, dispatch_evidence TEXT, confirmed_at REAL);
CREATE INDEX IF NOT EXISTS wakeup_due ON wakeup_requests(status,next_attempt_at,priority);
CREATE TABLE IF NOT EXISTS promises(
 id TEXT PRIMARY KEY, work_item_id TEXT NOT NULL REFERENCES work_items(id),
 actor TEXT NOT NULL, recipient TEXT NOT NULL, source_message_id TEXT NOT NULL UNIQUE,
 text TEXT NOT NULL, created_at REAL NOT NULL, start_by REAL NOT NULL,
 original_due_at REAL, due_at REAL, next_action_at REAL NOT NULL,
 first_action_at REAL, last_progress_at REAL, progress_evidence TEXT,
 status TEXT NOT NULL DEFAULT 'open', completed_evidence TEXT);
CREATE TABLE IF NOT EXISTS promise_history(
 id INTEGER PRIMARY KEY, promise_id TEXT NOT NULL REFERENCES promises(id),
 action TEXT NOT NULL, at REAL NOT NULL, detail TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS attention_sessions(
 target TEXT PRIMARY KEY, state TEXT NOT NULL, active_until REAL NOT NULL,
 cooldown_until REAL NOT NULL, next_check_at REAL NOT NULL,
 last_activity_at REAL NOT NULL, own_until REAL NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS attention_consumed(
 target TEXT NOT NULL, event_key TEXT NOT NULL, PRIMARY KEY(target,event_key));
-- Phase 6: additive, local scheduler state; never platform task grants.
CREATE TABLE IF NOT EXISTS schedule_activities(
 id TEXT PRIMARY KEY,logical_key TEXT NOT NULL UNIQUE,
 kind TEXT NOT NULL CHECK(kind IN ('work','promise','social','research','maintenance')),
 work_id TEXT UNIQUE REFERENCES work_items(id),
 status TEXT NOT NULL CHECK(status IN ('ready','running','suspended','completed','cancelled')),
 base_priority INTEGER NOT NULL CHECK(base_priority BETWEEN 0 AND 100),
 weight INTEGER NOT NULL CHECK(weight BETWEEN 1 AND 100),
 deadline REAL, switch_cost INTEGER NOT NULL CHECK(switch_cost BETWEEN 0 AND 30),
 created_at REAL NOT NULL,ready_since REAL NOT NULL,last_updated REAL NOT NULL,
 -- A voluntary maintenance yield defers only its own next selection; an
 -- actual safe-point preemption remains resumable without artificial delay.
 resume_after REAL NOT NULL DEFAULT 0,
 voluntary_yield_streak INTEGER NOT NULL DEFAULT 0,
 last_started_at REAL,last_charged_at REAL,
 cpu_seconds REAL NOT NULL DEFAULT 0,virtual_runtime REAL NOT NULL DEFAULT 0,
 lease_owner TEXT,lease_version INTEGER NOT NULL DEFAULT 0,
 lease_until REAL,slice_until REAL,
 preempt_requested INTEGER NOT NULL DEFAULT 0 CHECK(preempt_requested IN (0,1)),
 preempt_mode TEXT,preempt_reason TEXT,checkpoint TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS schedule_one_running
 ON schedule_activities(status) WHERE status='running';
CREATE INDEX IF NOT EXISTS schedule_available ON
 schedule_activities(status,ready_since);
-- Optional Phase 6 social opportunity admission ledger, not a new worker
-- queue. Only typed IDs/action names are retained, never external prose.
CREATE TABLE IF NOT EXISTS social_intents(
 activity_id TEXT PRIMARY KEY REFERENCES schedule_activities(id),
 actor_id TEXT NOT NULL,resource_type TEXT NOT NULL
   CHECK(resource_type IN ('post','circle')),
 resource_id TEXT NOT NULL,action TEXT NOT NULL,
 created_at REAL NOT NULL,selection_score INTEGER NOT NULL
   CHECK(selection_score BETWEEN 0 AND 100));
CREATE INDEX IF NOT EXISTS social_intents_target
 ON social_intents(actor_id,resource_type,resource_id,created_at);
CREATE INDEX IF NOT EXISTS social_intents_budget
 ON social_intents(actor_id,created_at);
CREATE TABLE IF NOT EXISTS scheduled_outbox(
 outbox_id TEXT PRIMARY KEY REFERENCES outbox(id) ON DELETE CASCADE,
 schedule_id TEXT NOT NULL REFERENCES schedule_activities(id),
 lease_generation INTEGER NOT NULL,owner TEXT NOT NULL,staged_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox(
 id TEXT PRIMARY KEY, actor TEXT NOT NULL, action TEXT NOT NULL, target TEXT NOT NULL,
 logical_key TEXT NOT NULL UNIQUE, idem_key TEXT NOT NULL UNIQUE, payload TEXT NOT NULL,
 payload_hash TEXT NOT NULL, status TEXT NOT NULL, created_at REAL NOT NULL,
 next_attempt_at REAL NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
 remote_id TEXT, result TEXT, error TEXT, dispatch_owner TEXT, dispatch_at REAL,
 verify_cursor TEXT NOT NULL DEFAULT '0', verified_at REAL);
-- Agent workers draft actions, but only the verified CrewQuay actor may send.
-- Separate immutable production provenance avoids treating a Hook worker
-- name as a platform execution identity. Safe additive table for old v1 DBs.
CREATE TABLE IF NOT EXISTS deployment_runs(
  id TEXT PRIMARY KEY, actor TEXT NOT NULL,run_id TEXT NOT NULL,
  nonce TEXT NOT NULL,handshake_id TEXT NOT NULL UNIQUE,
  repo_sha TEXT NOT NULL,state TEXT NOT NULL,created_at REAL NOT NULL,
  updated_at REAL NOT NULL,expires_at REAL,
  syn_work_id TEXT REFERENCES work_items(id),
  ack_work_id TEXT REFERENCES work_items(id),
  bug_work_id TEXT REFERENCES work_items(id),
  bug_outbox_id TEXT REFERENCES outbox(id),
  syn_outbox_id TEXT REFERENCES outbox(id),
  ack_outbox_id TEXT REFERENCES outbox(id),
  syn_ack_event_id TEXT,syn_ack_event_cursor TEXT,
  syn_ack_message_id TEXT,challenge TEXT,
  failure_code TEXT,bug_id TEXT,
  UNIQUE(actor,run_id));
CREATE INDEX IF NOT EXISTS deployment_actor_due ON deployment_runs(actor,state,updated_at);
CREATE TABLE IF NOT EXISTS outbox_producers(
 outbox_id TEXT PRIMARY KEY REFERENCES outbox(id) ON DELETE CASCADE,
 producer_id TEXT NOT NULL, recorded_at REAL NOT NULL);
CREATE INDEX IF NOT EXISTS outbox_ready ON outbox(status,next_attempt_at,created_at);
CREATE TABLE IF NOT EXISTS alerts(
 logical_key TEXT PRIMARY KEY, kind TEXT NOT NULL, detail TEXT NOT NULL, created_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS migrations(
 source_hash TEXT NOT NULL, source_name TEXT NOT NULL, imported_at REAL NOT NULL,
 record_count INTEGER NOT NULL, PRIMARY KEY(source_name,source_hash));
CREATE TABLE IF NOT EXISTS legacy_records(
 source_hash TEXT NOT NULL, line_no INTEGER NOT NULL, payload TEXT NOT NULL,
 PRIMARY KEY(source_hash,line_no));
-- Actor authority is independent of old allowlists, work queues and cursors.
CREATE TABLE IF NOT EXISTS platform_manifests(
 actor TEXT NOT NULL, resource_type TEXT NOT NULL DEFAULT '',
 resource_id TEXT NOT NULL DEFAULT '', revision TEXT NOT NULL,
 payload TEXT NOT NULL, checked_at REAL NOT NULL,
 PRIMARY KEY(actor,resource_type,resource_id));
CREATE TABLE IF NOT EXISTS permission_requests(
 id TEXT PRIMARY KEY, actor TEXT NOT NULL, scope TEXT NOT NULL,
 actions TEXT NOT NULL, remote_id TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('pending','approved','denied')),
 updated_at REAL NOT NULL);
CREATE INDEX IF NOT EXISTS permission_requests_actor
 ON permission_requests(actor,status);
"""


class Database:
    """Public persistence capability. Tables form a documented shared transaction schema."""
    def __init__(self, path, journal="DELETE", busy_ms=1000):
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.journal = journal.upper()
        if self.journal not in ("WAL", "DELETE"):
            raise ValueError("journal must be WAL or DELETE")
        version = sqlite3.sqlite_version_info
        patched = version >= (3, 51, 3) or ((3, 44, 6) <= version < (3, 45, 0)) or ((3, 50, 7) <= version < (3, 51, 0))
        if self.journal == "WAL" and not patched:
            raise ValueError("WAL requires SQLite 3.51.3+ or a verified supported fixed branch; use DELETE otherwise")
        self.busy_ms = busy_ms
        with contextlib.closing(self.connect()) as c:
            mode = c.execute("PRAGMA journal_mode=" + self.journal).fetchone()[0]
            if mode.upper() != self.journal:
                raise RuntimeError("requested journal mode unavailable")
            c.executescript(SCHEMA)
            row = c.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
            if row and row[0] not in ("1","2"):
                raise RuntimeError("unsupported database schema")
            # Existing installations have v1 wakeup_requests with a SQLite
            # CHECK limited to work/promise. Rebuild ONLY that table and its
            # index in one transaction; preserve every row and lease receipt.
            ddl=c.execute("SELECT sql FROM sqlite_master WHERE type='table' "
                          "AND name='wakeup_requests'").fetchone()[0]
            if "'social'" not in ddl:
                c.execute("BEGIN IMMEDIATE")
                try:
                    c.execute("""CREATE TABLE wakeup_requests_v2(
                        id TEXT PRIMARY KEY, logical_key TEXT NOT NULL UNIQUE,
                        source_type TEXT NOT NULL CHECK(source_type IN ('work','promise','social')),
                        source_id TEXT NOT NULL,priority INTEGER NOT NULL
                        CHECK(priority BETWEEN 0 AND 100),
                        reason TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',
                        created_at REAL NOT NULL,next_attempt_at REAL NOT NULL,
                        lease_owner TEXT,lease_version INTEGER NOT NULL DEFAULT 0,
                        lease_until REAL,attempts INTEGER NOT NULL DEFAULT 0,
                        dispatch_evidence TEXT,confirmed_at REAL)""")
                    columns=("id,logical_key,source_type,source_id,priority,reason,"
                             "status,created_at,next_attempt_at,lease_owner,"
                             "lease_version,lease_until,attempts,dispatch_evidence,"
                             "confirmed_at")
                    c.execute("INSERT INTO wakeup_requests_v2("+columns+") "
                              "SELECT "+columns+" FROM wakeup_requests")
                    c.execute("DROP TABLE wakeup_requests")
                    c.execute("ALTER TABLE wakeup_requests_v2 RENAME TO wakeup_requests")
                    c.execute("CREATE INDEX wakeup_due ON wakeup_requests"
                              "(status,next_attempt_at,priority)")
                    c.commit()
                except BaseException:
                    c.rollback()
                    raise
            # Additive, idempotent migration for existing per-Agent SQLite DBs.
            # Never clear legacy activities or their generation/Outbox identity.
            schedule_cols={col["name"] for col in
                c.execute("PRAGMA table_info(schedule_activities)")}
            if "resume_after" not in schedule_cols:
                c.execute("ALTER TABLE schedule_activities "
                          "ADD COLUMN resume_after REAL NOT NULL DEFAULT 0")
            if "voluntary_yield_streak" not in schedule_cols:
                c.execute("ALTER TABLE schedule_activities "
                          "ADD COLUMN voluntary_yield_streak INTEGER NOT NULL DEFAULT 0")
            # Verification time is a local exact readback, never a guessed remote time.
            # Preserve historical confirmed rows as NULL on additive upgrade.
            outbox_cols={x["name"] for x in c.execute("PRAGMA table_info(outbox)")}
            if "verified_at" not in outbox_cols:
                c.execute("ALTER TABLE outbox ADD COLUMN verified_at REAL")
            # Execution receipts are deliberately separate from host delivery
            # leases. Create only AFTER the older wakeup-table rebuild, so
            # v1 -> v2 migration does not drop a referenced parent table.
            c.execute("""CREATE TABLE IF NOT EXISTS wakeup_execution_receipts(
                wakeup_id TEXT PRIMARY KEY REFERENCES wakeup_requests(id),
                wakeup_generation INTEGER NOT NULL,
                activity_id TEXT NOT NULL REFERENCES schedule_activities(id),
                activity_generation INTEGER NOT NULL,
                worker TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('started','progress','finished')),
                started_at REAL NOT NULL,updated_at REAL NOT NULL,
                finished_at REAL,evidence TEXT NOT NULL)""")
            c.execute("""CREATE INDEX IF NOT EXISTS wakeup_execution_due
                ON wakeup_execution_receipts(status,updated_at)""")
            # An unacknowledged HATCH wake is a best-effort notification, not a
            # delivery lease or accepted Host receipt. Keep this ledger separate
            # from historical wakeup_requests, including leased/unknown rows.
            c.execute("""CREATE TABLE IF NOT EXISTS host_wakeup_notifications(
                wakeup_id TEXT PRIMARY KEY REFERENCES wakeup_requests(id),
                generation INTEGER NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0,
                intent_at REAL NOT NULL,
                next_notice_at REAL NOT NULL,
                capability TEXT NOT NULL CHECK(capability='best_effort'),
                capability_source TEXT NOT NULL,
                outcome TEXT NOT NULL CHECK(outcome='unknown'),
                worker_started_at REAL)""")
            # Heartbeats prove only that the fenced Worker called back; they
            # do not assert business progress or confirmed remote effects.
            c.execute("""CREATE TABLE IF NOT EXISTS worker_lease_heartbeats(
                wakeup_id TEXT PRIMARY KEY REFERENCES wakeup_requests(id),
                worker TEXT NOT NULL,
                wakeup_generation INTEGER NOT NULL,
                activity_id TEXT NOT NULL REFERENCES schedule_activities(id),
                activity_generation INTEGER NOT NULL,
                work_lease_version INTEGER NOT NULL,
                observation_seq INTEGER NOT NULL,
                last_observed_at REAL NOT NULL,
                evidence TEXT NOT NULL)""")
            c.execute("INSERT INTO meta(key,value) VALUES('schema_version','2') "
                      "ON CONFLICT(key) DO UPDATE SET value=excluded.value")
        if os.name == "posix":
            os.chmod(self.path, 0o600)

    def connect(self):
        c = sqlite3.connect(str(self.path), timeout=self.busy_ms / 1000, isolation_level=None)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA foreign_keys=ON")
        c.execute("PRAGMA synchronous=FULL")
        c.execute("PRAGMA busy_timeout=" + str(int(self.busy_ms)))
        return c

    @contextlib.contextmanager
    def transaction(self):
        c = self.connect()
        try:
            c.execute("BEGIN IMMEDIATE")
            yield c
            c.commit()
        except sqlite3.OperationalError as e:
            c.rollback()
            if "locked" in str(e) or "busy" in str(e):
                raise Busy("database busy; no cursor or work was committed") from None
            raise
        except BaseException:
            c.rollback()
            raise
        finally:
            c.close()

    def read(self, sql, args=()):
        with contextlib.closing(self.connect()) as c:
            return [dict(r) for r in c.execute(sql, args)]

    def alert(self, key, kind, detail, now):
        with self.transaction() as c:
            c.execute("INSERT INTO alerts VALUES(?,?,?,?) ON CONFLICT(logical_key) DO NOTHING",
                      (key, kind, canonical(detail), now))

    def backup(self, destination):
        dest = Path(destination).resolve()
        if dest == self.path or dest.exists():
            raise ValueError("backup destination must be a new file")
        with contextlib.closing(self.connect()) as src, contextlib.closing(sqlite3.connect(str(dest))) as dst:
            src.backup(dst)
            if dst.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise RuntimeError("backup integrity failed")
        if os.name == "posix":
            os.chmod(dest, 0o600)
        return str(dest)

    def integrity(self):
        return self.read("PRAGMA integrity_check")
