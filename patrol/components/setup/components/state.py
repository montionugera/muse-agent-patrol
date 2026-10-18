"""One-at-a-time, resumable Agent-led setup; SQLite is NOT an authority grant.

A step becomes verified only after a supported real readback. No automatic
approval, PAT creation, Hook installation, prompt spam or per-Tick scheduling.
"""
import hashlib
import json
import os
import secrets
import sqlite3
import time
from pathlib import Path

from .manifest import Manifest
from .probes import Probes
from .secrets import private_dirs, read_secret, SecretUnavailable

_ORDER=("runtime_paths","crewquay_identity","crewquay_scopes",
        "github_credential","github_repositories","github_permissions",
        "native_hatch","deployment_handshake")
_FINAL={"verified","skipped_optional"}
_REQUESTABLE={"SECURE_SECRET_INPUT_REQUIRED","GITHUB_TOKEN_REVOKED_OR_REJECTED",
              "GITHUB_REPOSITORY_NOT_ACCESSIBLE","GITHUB_REPO_AUTH_REJECTED"}
_DESCRIPTIONS={
  "runtime_paths":"Agent must prepare a private runtime config/SQLite with the expected Actor.",
  "crewquay_identity":"Provision this Agent's own CrewQuay credential through the trusted private host channel.",
  "crewquay_scopes":"Agent checks current CrewQuay Actor Manifest and formal scoped grants.",
  "github_credential":"Create or update a fine-grained PAT in GitHub for only the required repositories.",
  "github_repositories":"Select the intended repositories for the PAT in the official GitHub UI.",
  "github_permissions":"Verify allowed actions through real GitHub operations; rulesets enforce main protection.",
  "native_hatch":"Agent registers/verifies the single native HATCH 60s Hook through the real host interface.",
  "deployment_handshake":"Agent completes independent CrewQuay protocol and real Host Worker evidence."}
_SOURCES={"crewquay_identity":"secret://crewquay/agent",
          "github_credential":"secret://github/pat"}


class Setup:
    def __init__(self,manifest,*,probes=None,clock=time.time):
        if not isinstance(manifest,Manifest):
            raise ValueError("validated deployment manifest required")
        self.manifest=manifest
        self.clock=clock
        self.probes=probes or Probes(manifest,clock=clock)
        self.path=manifest.state_dir/"setup.sqlite3"

    def _connect(self):
        private_dirs(self.manifest.state_dir,create=True)
        if self.path.is_symlink():
            raise ValueError("setup database symlink refused")
        c=sqlite3.connect(str(self.path),timeout=5,isolation_level=None)
        c.row_factory=sqlite3.Row
        c.execute("PRAGMA busy_timeout=5000")
        c.execute("PRAGMA synchronous=FULL")
        c.execute("""CREATE TABLE IF NOT EXISTS setup_meta(
            key TEXT PRIMARY KEY,value TEXT NOT NULL)""")
        c.execute("""CREATE TABLE IF NOT EXISTS setup_steps(
            step TEXT PRIMARY KEY,status TEXT NOT NULL,reason TEXT NOT NULL,
            action TEXT NOT NULL,revision TEXT NOT NULL,
            asked_at REAL,observed_at REAL NOT NULL,tries INTEGER NOT NULL,
            credential_ref TEXT,credential_fingerprint TEXT)""")
        c.execute("INSERT OR IGNORE INTO setup_meta(key,value) VALUES('installation_id',?)",
                  (self.manifest.data["agent"]["id"]+"@"+
                   self.manifest.data["agent"]["host_instance"],))
        c.execute("INSERT OR IGNORE INTO setup_meta(key,value) VALUES('fingerprint_salt',?)",
                  (secrets.token_hex(32),))
        if os.name=="posix":
            os.chmod(self.path,0o600)
        return c

    def _required(self,step):
        if step.startswith("github_"):
            return self.manifest.required("github")
        if step.startswith("crewquay_") or step=="deployment_handshake":
            return self.manifest.required("crewquay")
        return self.manifest.required("runtime")

    def _fingerprint(self,c,step):
        ref=(_SOURCES.get(step) if step!="github_credential" else
             self.manifest.data["github"]["credential_ref"])
        if step=="crewquay_identity":
            ref=self.manifest.data["crewquay"]["credential_ref"]
        if ref is None:
            return None,None
        try:
            secret=read_secret(self.manifest.state_dir,ref)
        except (SecretUnavailable,OSError):
            return ref,None
        salt=bytes.fromhex(c.execute("SELECT value FROM setup_meta WHERE key='fingerprint_salt'").fetchone()[0])
        digest=hashlib.blake2b(secret.encode(),key=salt,digest_size=16).hexdigest()
        # Only the salted fingerprint is stored. Drop the credential string.
        del secret
        return ref,digest

    def _set(self,c,step,check,now):
        prev=c.execute("SELECT * FROM setup_steps WHERE step=?",(step,)).fetchone()
        ref,fingerprint=self._fingerprint(c,step)
        same=(prev and prev["reason"]==check["reason"] and
              prev["status"]==check["status"] and
              prev["revision"]==self.manifest.revision and
              prev["credential_fingerprint"]==fingerprint)
        asked=(prev["asked_at"] if same else None) if prev else None
        tries=prev["tries"]+1 if prev else 1
        c.execute("""INSERT INTO setup_steps(step,status,reason,action,
           revision,asked_at,observed_at,tries,credential_ref,credential_fingerprint)
           VALUES(?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(step) DO UPDATE SET status=excluded.status,
             reason=excluded.reason,action=excluded.action,
             revision=excluded.revision,asked_at=excluded.asked_at,
             observed_at=excluded.observed_at,tries=excluded.tries,
             credential_ref=excluded.credential_ref,
             credential_fingerprint=excluded.credential_fingerprint""",
             (step,check["status"],check["reason"],check["next_action"],
              self.manifest.revision,asked,now,tries,ref,fingerprint))
        return (not same or not asked)

    def init(self):
        c=self._connect()
        try:
            with c:
                key=self.manifest.data["agent"]["id"]+"@"+self.manifest.data["agent"]["host_instance"]
                inst=c.execute("SELECT value FROM setup_meta WHERE key='installation_id'").fetchone()[0]
                if inst!=key:
                    raise ValueError("setup state belongs to another Agent or host")
                c.execute("""INSERT INTO setup_meta(key,value) VALUES('schema_revision',?)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
                   (self.manifest.revision,))
            return self.status()
        finally:
            c.close()

    def status(self):
        c=self._connect()
        try:
            rows={x["step"]:dict(x) for x in c.execute("SELECT * FROM setup_steps").fetchall()}
            report=[]
            for step in _ORDER:
                row=rows.get(step)
                current=(row is not None and row["revision"]==self.manifest.revision)
                status=row["status"] if current else "not_checked"
                report.append({"step":step,"required":self._required(step),
                    "status":status,
                    "reason":row["reason"] if current else "NOT_VERIFIED",
                    "action":row["action"] if current else "setup_verify",
                    "asked_at":row["asked_at"] if current else None,
                    "observed_at":row["observed_at"] if current else None,
                    "credential_ref":row["credential_ref"] if current else None,
                    "credential_fingerprint":row["credential_fingerprint"] if current else None})
            required_complete=all(x["status"]=="verified" for x in report if x["required"])
            optional_complete=all(x["status"] in _FINAL for x in report if not x["required"])
            return {"status":"ready_for_external_e2e" if required_complete and optional_complete
                    else "setup_incomplete",
                    "installation":self.manifest.data["agent"],
                    "schema_version":1,"revision":self.manifest.revision,
                    "steps":report,"required_verified":required_complete,
                    "all_steps_resolved":optional_complete,
                    "host_deployment_verified":False,
                    "setup_is_not_platform_authorization":True,
                    "token_values_persisted":False}
        finally:
            c.close()

    def _check_one(self,step,now):
        c=self._connect()
        try:
            current=c.execute("SELECT * FROM setup_steps WHERE step=?",(step,)).fetchone()
            if (current and current["status"]=="denied" and
                    current["revision"]==self.manifest.revision):
                return {"step":step,"required":self._required(step),
                        "status":"denied","reason":"OWNER_DECLINED_PREVIOUS_REQUEST",
                        "next_action":"await_trusted_owner_decision",
                        "prompt_new":False}
            if (current and current["status"]=="skipped_optional" and
                    current["revision"]==self.manifest.revision):
                return {"step":step,"required":False,"status":"skipped_optional",
                        "reason":"OPTIONAL_DEFERRED","next_action":"none","prompt_new":False}
        finally:
            c.close()
        check=self.probes.check(step)
        if not isinstance(check,dict) or set(check)!={"status","reason","next_action"}:
            raise ValueError("invalid safe setup probe result")
        if check["status"] not in ("verified","unverified","awaiting_user","awaiting_agent","blocked","error"):
            raise ValueError("unrecognized setup verification status")
        c=self._connect()
        try:
            with c:
                ask_new=self._set(c,step,check,now)
                is_prompt=check["status"]=="awaiting_user" and check["reason"] in _REQUESTABLE
                if is_prompt and ask_new:
                    c.execute("UPDATE setup_steps SET asked_at=? WHERE step=?",(now,step))
            return {"step":step,"required":self._required(step),"status":check["status"],
                    "reason":check["reason"],"next_action":check["next_action"],
                    "prompt_new":bool(is_prompt and ask_new),
                    "instructions":_DESCRIPTIONS[step],
                    "never_request_secret_in_chat":True}
        finally:
            c.close()

    def next(self):
        now=self.clock()
        # Check one unsatisfied step per call. No Hook invocation, no model,
        # no repetitive message for an unchanged missing credential.
        for step in _ORDER:
            result=self._check_one(step,now)
            if result["status"] not in _FINAL:
                return {"status":"action_required","current":result,
                        "owner_contact_required":result["prompt_new"],
                        "subsequent_steps_not_prompted":True}
        return {"status":"setup_steps_resolved",
                "host_deployment_verified":False,
                "meaning":"run real HATCH/Platform E2E before claiming ready"}

    def verify(self):
        now=self.clock()
        # Diagnostics do not deliver Owner requests. In particular, do NOT
        # mark all missing credentials 'asked' when the Agent runs verify:
        # next() remains the sole one-item-at-a-time notification gate.
        observations=[]
        c=self._connect()
        try:
            for step in _ORDER:
                check=self.probes.check(step)
                if (not isinstance(check,dict) or
                        set(check)!={"status","reason","next_action"}):
                    raise ValueError("invalid safe setup probe result")
                with c:
                    current=c.execute("SELECT status,revision FROM setup_steps WHERE step=?",
                                      (step,)).fetchone()
                    if current and current["revision"]==self.manifest.revision and (
                            current["status"] in ("denied","skipped_optional")):
                        observations.append({"step":step,"status":current["status"],
                                             "prompt_new":False})
                        continue
                    self._set(c,step,check,now)
                observations.append({"step":step,**check,"prompt_new":False})
        finally:
            c.close()
        return {"observations":observations,"state":self.status(),
                "owner_contact_required":False,"remote_action_issued":False}

    def skip_optional(self,step):
        if step not in _ORDER or self._required(step):
            raise ValueError("cannot skip a required or unknown setup step")
        c=self._connect()
        try:
            now=self.clock()
            with c:
                self._set(c,step,{"status":"skipped_optional","reason":"OPTIONAL_DEFERRED",
                                 "next_action":"none"},now)
            return {"step":step,"status":"skipped_optional",
                    "grants_changed":False,"remote_action_issued":False}
        finally:
            c.close()

    def deny(self,step):
        # Denial can only reduce local capabilities, never grant anything.
        # The trusted host should obtain an explicit Owner decision before
        # calling this. No 'approve' parameter/API exists here.
        if step not in _ORDER:
            raise ValueError("unknown setup step")
        c=self._connect()
        try:
            with c:
                self._set(c,step,{"status":"denied","reason":"OWNER_DECLINED_PREVIOUS_REQUEST",
                                 "next_action":"await_trusted_owner_decision"},self.clock())
            return {"step":step,"status":"denied","grants_changed":False}
        finally:
            c.close()
