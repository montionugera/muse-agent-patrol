"""Direct PAT-backed GitHub operations: fenced, policy-scoped, locally audited.

This is NOT CrewQuay Outbox or a second scheduler. An ambiguous GitHub write
is not blindly retried; the Agent must read back/reconcile original identity.
"""
import hashlib
import json
import os
import re
import sqlite3
import time
from pathlib import Path
from urllib.parse import quote

from ...setup.components.manifest import Manifest
from ...setup.components.secrets import read_secret, private_dirs
from .transport import GitHubAPI

_KEY=re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._:-]{7,127}$")
_SHA=re.compile(r"^[a-fA-F0-9]{40}$")
_BRANCH=re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._/-]{1,100}$")


def valid_branch(name):
    return (isinstance(name,str) and _BRANCH.fullmatch(name) is not None
            and not any(x in name for x in ("..","//","@{","\\"))
            and not name.endswith((".","/")) and
            not name.startswith(("refs/","heads/")))


class GitHubOperations:
    def __init__(self,manifest,*,transport=None,clock=time.time):
        if not isinstance(manifest,Manifest):
            raise ValueError("validated deployment manifest required")
        self.manifest=manifest
        self.transport=transport or GitHubAPI()
        self.clock=clock
        self.path=manifest.state_dir/"github-actions.sqlite3"

    def _open(self):
        private_dirs(self.manifest.state_dir,create=True)
        if self.path.is_symlink():
            raise ValueError("GitHub audit sqlite symlink refused")
        c=sqlite3.connect(str(self.path),timeout=5,isolation_level=None)
        c.row_factory=sqlite3.Row
        c.execute("PRAGMA busy_timeout=5000")
        c.execute("PRAGMA synchronous=FULL")
        c.execute("""CREATE TABLE IF NOT EXISTS github_actions(
            logical_key TEXT PRIMARY KEY, operation TEXT NOT NULL,
            repository TEXT NOT NULL, payload_sha256 TEXT NOT NULL,
            status TEXT NOT NULL, created_at REAL NOT NULL,
            updated_at REAL NOT NULL, http_status INTEGER,
            remote_id TEXT)""")
        if os.name=="posix":
            os.chmod(self.path,0o600)
        return c

    def _authorized(self,repo,operation):
        g=self.manifest.data["github"]
        if g["auth_type"]!="fine_grained_pat":
            raise ValueError("existing GitHub App must use its own verified adapter")
        if repo not in g["repositories"]:
            raise ValueError("GitHub repository outside declared Agent installation")
        role=self.manifest.data["agent"]["role"]
        perms=g["permissions"]
        policy=g["policy"]
        rights={"create_issue":"issues","create_branch":"contents",
                "create_pull":"pull_requests","merge_pull":"pull_requests"}
        if operation not in rights or perms.get(rights[operation])!="write":
            raise ValueError("operation not expected by this Agent's declared minimum policy")
        if operation=="create_pull" and not policy["allow_pr_create"]:
            raise ValueError("this Agent must not open PR")
        if operation=="merge_pull" and (role!="planner" or not policy["allow_pr_merge"]):
            raise ValueError("only a policy-authorized Planner may request a merge")
        if policy["allow_direct_main_push"]:
            raise ValueError("local branch protection expectation invalid")
        # A local flag is never sufficient to override GitHub server rulesets.
        db=self.manifest.state_dir/"setup.sqlite3"
        if db.is_file():
            with sqlite3.connect("file:"+str(db)+"?mode=ro",uri=True) as c:
                denied=c.execute("""SELECT count(*) FROM setup_steps
                    WHERE step LIKE 'github_%'
                      AND status IN ('denied','skipped_optional')""").fetchone()[0]
                if denied:
                    raise ValueError("Owner-declined or disabled GitHub feature")

    @staticmethod
    def _build(operation,payload):
        if not isinstance(payload,dict):
            raise ValueError("GitHub action needs typed JSON payload")
        if operation=="create_issue":
            if (set(payload)!={"title","body"} or
                    not isinstance(payload["title"],str) or not 1<=len(payload["title"])<=200 or
                    not isinstance(payload["body"],str) or len(payload["body"])>10000):
                raise ValueError("bounded issue title and body required")
            return "/issues",payload
        if operation=="create_branch":
            if (set(payload)!={"branch","sha"} or
                    not valid_branch(payload["branch"]) or payload["branch"] in ("main","master") or
                    not isinstance(payload["sha"],str) or not _SHA.fullmatch(payload["sha"])):
                raise ValueError("valid non-main branch and exact source SHA required")
            return "/git/refs",{"ref":"refs/heads/"+payload["branch"],"sha":payload["sha"]}
        if operation=="create_pull":
            if (set(payload)!={"head","base","title","body"} or
                    not valid_branch(payload["head"]) or
                    payload["head"] in ("main","master") or payload["base"]!="main" or
                    not isinstance(payload["title"],str) or not 1<=len(payload["title"])<=200 or
                    not isinstance(payload["body"],str) or len(payload["body"])>10000):
                raise ValueError("bounded feature-branch PR into main required")
            return "/pulls",payload
        if operation=="merge_pull":
            if (set(payload)!={"number","expected_head_sha"} or
                    type(payload["number"]) is not int or not 1<=payload["number"]<=100000000 or
                    not isinstance(payload["expected_head_sha"],str) or
                    not _SHA.fullmatch(payload["expected_head_sha"])):
                raise ValueError("merge requires exact PR number and observed head SHA")
            return "/pulls/"+str(payload["number"])+"/merge",{
                "sha":payload["expected_head_sha"],"merge_method":"merge"}
        raise ValueError("unsupported GitHub operation")

    @staticmethod
    def _verified(operation,payload,response,observed):
        if not isinstance(response,dict) or not isinstance(observed,dict):
            return False,None
        if operation=="create_branch":
            ref="refs/heads/"+payload["branch"]
            good=(observed.get("ref")==ref and
                  isinstance(observed.get("object"),dict) and
                  observed["object"].get("sha")==payload["sha"])
            return good,ref if good else None
        if operation in ("create_issue","create_pull"):
            number=response.get("number")
            remote_id=response.get("id")
            good=(type(number) is int and number>0 and type(remote_id) is int and
                  observed.get("id")==remote_id and
                  observed.get("title")==payload["title"])
            if good and operation=="create_pull":
                head=observed.get("head")
                base=observed.get("base")
                good=(isinstance(head,dict) and isinstance(base,dict)
                      and head.get("ref")==payload["head"] and base.get("ref")=="main")
            return good,str(number) if good else None
        if operation=="merge_pull":
            good=(response.get("merged") is True and observed.get("merged") is True
                  and type(observed.get("number")) is int
                  and observed["number"]==payload["number"])
            return good,str(payload["number"]) if good else None
        return False,None

    @staticmethod
    def _readback_path(operation,payload,response):
        if not isinstance(response,dict):
            return None
        if operation=="create_branch":
            return "/git/ref/heads/"+quote(payload["branch"],safe="/")
        if operation in ("create_issue","create_pull"):
            number=response.get("number")
            if type(number) is not int or number<=0:
                return None
            return ("/issues/" if operation=="create_issue" else "/pulls/")+str(number)
        if operation=="merge_pull":
            return "/pulls/"+str(payload["number"])
        return None

    def action(self,logical_key,repo,operation,payload):
        if not isinstance(logical_key,str) or not _KEY.fullmatch(logical_key):
            raise ValueError("stable unique logical action key required (8..128)")
        self._authorized(repo,operation)
        route,body=self._build(operation,payload)
        # Fail BEFORE writing a 'sending' intent if a secret is unavailable
        # or a read-only authentication check fails. Only the real mutation
        # requires the durable crash-unknown fence.
        try:
            token=read_secret(self.manifest.state_dir,
                              self.manifest.data["github"]["credential_ref"])
        except (OSError,ValueError) as e:
            reason=getattr(e,"reason","SECURE_GITHUB_SECRET_UNAVAILABLE")
            return {"status":"blocked","reason":reason,
                    "new_request_sent":False,"remote_effect_confirmed":False}
        identity,me=self.transport.request("GET","/user",token)
        access,repository=self.transport.request("GET","/repos/"+repo,token)
        if (identity!=200 or not isinstance(me,dict) or
                not isinstance(me.get("login"),str) or
                access!=200 or not isinstance(repository,dict) or
                repository.get("full_name","").lower()!=repo.lower()):
            return {"status":"denied" if identity in (401,403) or
                    access in (401,403,404) else "preflight_deferred",
                    "reason":"GITHUB_PREWRITE_AUTH_OR_REPOSITORY_UNVERIFIED",
                    "new_request_sent":False,"remote_effect_confirmed":False}
        encoded=json.dumps([operation,repo,payload],sort_keys=True,
                           ensure_ascii=False,separators=(",",":"))
        digest=hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        c=self._open()
        try:
            now=self.clock()
            with c:
                old=c.execute("SELECT * FROM github_actions WHERE logical_key=?",
                              (logical_key,)).fetchone()
                if old:
                    if (old["operation"]!=operation or old["repository"]!=repo
                            or old["payload_sha256"]!=digest):
                        raise ValueError("immutable GitHub logical key has different action")
                    return {"status":old["status"],"remote_id":old["remote_id"],
                            "reused":True,"new_request_sent":False,
                            "requires_remote_reconciliation":old["status"] in ("sending","unknown")}
                # Durable before HTTP. A process crash keeps 'sending':
                # never replay a possibly successful remote side effect.
                c.execute("""INSERT INTO github_actions(
                    logical_key,operation,repository,payload_sha256,status,
                    created_at,updated_at) VALUES(?,?,?,?,'sending',?,?)""",
                    (logical_key,operation,repo,digest,now,now))
        finally:
            c.close()
        result="unknown"
        remote_id=None
        status=0
        try:
            method="PUT" if operation=="merge_pull" else "POST"
            status,response=self.transport.request(
                method,"/repos/"+repo+route,token,body)
            if status in (200,201) and isinstance(response,dict):
                path=self._readback_path(operation,payload,response)
                checked,readback=self.transport.request(
                    "GET","/repos/"+repo+path,token) if path else (0,None)
                ok,remote_id=(self._verified(operation,payload,response,readback)
                              if checked==200 else (False,None))
                result="confirmed" if ok else "unknown"
            elif status in (400,401,403,404,422):
                result="denied"
            else:
                result="unknown"
        except (ValueError,OSError):
            result="unknown"  # Never automatically repost an ambiguous write.
        c=self._open()
        try:
            with c:
                c.execute("""UPDATE github_actions SET status=?,updated_at=?,
                    remote_id=?,http_status=? WHERE logical_key=? AND status='sending'""",
                    (result,self.clock(),remote_id,status,logical_key))
        finally:
            c.close()
        return {"status":result,"remote_id":remote_id,"http_status":status,
                "reused":False,"new_request_sent":status in (200,201,400,401,403,404,422),
                "requires_remote_reconciliation":result=="unknown",
                "github_server_authoritative":True,
                "crewquay_action_issued":False}
