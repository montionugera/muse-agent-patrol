"""Read-only, non-destructive host/CrewQuay/GitHub setup observations.

No probe may submit requests, create repository content, install HATCH, grant
scopes, or interpret a guessed permission as a verified write grant.
"""
import json
import sqlite3
import time
import urllib.error
import urllib.request
from pathlib import Path
from .secrets import SecretUnavailable, private_dirs, read_secret


def observation(status,reason,action="none"):
    return {"status":status,"reason":reason,"next_action":action}


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,req,fp,code,msg,headers,newurl):
        return None


def github_get(path,token,opener=None):
    """Fixed GitHub HTTPS origin; never interpolate arbitrary request targets."""
    if not path.startswith("/") or "//" in path or "?" in path or "#" in path:
        raise ValueError("invalid GitHub API path")
    req=urllib.request.Request(
        "https://api.github.com"+path,
        headers={"Authorization":"Bearer "+token,
                 "Accept":"application/vnd.github+json",
                 "X-GitHub-Api-Version":"2022-11-28",
                 "User-Agent":"muse-agent-setup/1"},method="GET")
    client=opener or urllib.request.build_opener(
        urllib.request.ProxyHandler({}),_NoRedirect)
    try:
        with client.open(req,timeout=6) as response:
            if response.status!=200:
                return response.status,None
            body=response.read(1024*1024+1)
            if len(body)>1024*1024:
                return 422,None
            return 200,json.loads(body)
    except urllib.error.HTTPError as e:
        # Never expose API responses or headers: auth messages may leak tokens.
        return e.code,None
    except (urllib.error.URLError,TimeoutError,OSError,ValueError,TypeError):
        return 0,None


class Probes:
    def __init__(self,manifest,*,github_reader=github_get,clock=time.time):
        self.manifest=manifest
        self.github_reader=github_reader
        self.clock=clock

    def _runtime(self):
        r=self.manifest.data["runtime"]
        state=self.manifest.state_dir
        try:
            private_dirs(state)
        except SecretUnavailable as e:
            return observation("awaiting_agent",e.reason,"agent_prepare_private_state")
        if not Path(r["config_path"]).expanduser().is_file():
            return observation("awaiting_agent","RUNTIME_CONFIG_MISSING","agent_prepare_runtime_config")
        if not Path(r["database"]).expanduser().is_file():
            return observation("awaiting_agent","RUNTIME_DATABASE_MISSING","agent_initialize_patrol_sqlite")
        try:
            from ...config.index import load_config
            cfg=load_config(str(Path(r["config_path"]).expanduser()))
            if (Path(cfg.database).expanduser()!=Path(r["database"]).expanduser()
                    or cfg.base_url!=self.manifest.data["crewquay"]["base_url"]
                    or cfg.actor_id!=self.manifest.data["crewquay"]["actor_id"]):
                return observation("error","RUNTIME_MANIFEST_BINDING_MISMATCH",
                                   "agent_reconcile_runtime_config")
            return observation("verified","RUNTIME_CONFIG_VALIDATED")
        except (ValueError,OSError,TypeError,KeyError):
            return observation("error","RUNTIME_CONFIG_INVALID","agent_repair_runtime_config")

    def _identity(self):
        status=self._runtime()
        if status["status"]!="verified":
            return observation("unverified","RUNTIME_IDENTITY_NOT_READY","agent_prepare_runtime_config")
        ref=self.manifest.data["crewquay"]["credential_ref"]
        try:
            read_secret(self.manifest.state_dir,ref)
        except SecretUnavailable as e:
            return observation("awaiting_user" if e.reason=="SECURE_SECRET_INPUT_REQUIRED"
                               else "blocked",e.reason,
                               "request_one_secure_crewquay_credential_import")
        # File presence alone does NOT prove CrewQuay authentication.
        db=Path(self.manifest.data["runtime"]["database"]).expanduser()
        try:
            with sqlite3.connect("file:"+str(db)+"?mode=ro",uri=True,timeout=2) as c:
                row=c.execute("SELECT value FROM meta WHERE key='actor'").fetchone()
            if not row or row[0]!=self.manifest.data["crewquay"]["actor_id"]:
                return observation("unverified","ACTOR_NOT_BOUND_TO_EXPECTED_ID",
                                   "agent_sync_authenticated_actor")
        except sqlite3.Error:
            return observation("unverified","ACTOR_BINDING_NOT_READABLE",
                               "agent_sync_authenticated_actor")
        return observation("unverified","ACTOR_BINDING_LOCAL_NOT_FRESH_API",
                           "agent_sync_authenticated_actor")

    def _scopes(self):
        # Scope aliases in a deployment plan are intents, not CrewQuay's
        # authoritative typed Action grants. Only the platform can certify.
        return observation("unverified","CREWQUAY_LIVE_GRANTS_REQUIRED",
                           "agent_platform_auth_sync_and_compare_requested_actions")

    def _github_token(self):
        g=self.manifest.data["github"]
        if g["auth_type"]!="fine_grained_pat":
            return observation("unverified","EXISTING_GITHUB_APP_ADAPTER_REQUIRED",
                               "agent_verify_existing_github_app")
        try:
            token=read_secret(self.manifest.state_dir,g["credential_ref"])
        except SecretUnavailable as e:
            return observation("awaiting_user" if e.reason=="SECURE_SECRET_INPUT_REQUIRED"
                               else "blocked",e.reason,
                               "request_one_secure_github_pat_import")
        status,data=self.github_reader("/user",token)
        if status==200 and isinstance(data,dict) and isinstance(data.get("login"),str):
            return observation("verified","GITHUB_AUTHENTICATED_READ_ONLY")
        if status in (401,403):
            return observation("awaiting_user","GITHUB_TOKEN_REVOKED_OR_REJECTED",
                               "request_one_github_pat_update")
        if status==0 or status in (429,500,502,503,504):
            return observation("error","GITHUB_AUTH_TEMPORARILY_UNAVAILABLE",
                               "agent_retry_safe_github_read")
        return observation("unverified","GITHUB_AUTH_UNVERIFIED",
                           "agent_verify_github_credential")

    def _github_repositories(self):
        g=self.manifest.data["github"]
        status=self._github_token()
        if status["status"]!="verified":
            return observation("unverified","GITHUB_TOKEN_NOT_VERIFIED",status["next_action"])
        token=read_secret(self.manifest.state_dir,g["credential_ref"])
        for repo in g["repositories"]:
            code,data=self.github_reader("/repos/"+repo,token)
            if code==404:
                return observation("awaiting_user","GITHUB_REPOSITORY_NOT_ACCESSIBLE",
                                   "request_one_repository_access_adjustment")
            if code in (401,403):
                return observation("awaiting_user","GITHUB_REPO_AUTH_REJECTED",
                                   "request_one_repository_access_adjustment")
            if code!=200 or not isinstance(data,dict) or data.get("full_name","").lower()!=repo.lower():
                return observation("unverified","GITHUB_REPOSITORY_READ_UNVERIFIED",
                                   "agent_retry_safe_repository_read")
        return observation("verified","GITHUB_REPOSITORY_READ_VERIFIED")

    def _github_permissions(self):
        # A repository GET cannot prove Contents/Issues/PR write permissions
        # or server branch rules. Never mark granted from a response header.
        status=self._github_repositories()
        if status["status"]!="verified":
            return observation("unverified","GITHUB_REPOSITORY_PREREQUISITE_MISSING",
                               status["next_action"])
        return observation("unverified","GITHUB_WRITE_PERMISSIONS_AND_RULESETS_UNVERIFIED",
                           "agent_verify_on_actual_authorized_operation")

    def _hatch(self):
        return observation("unverified","NATIVE_HATCH_REGISTRATION_NOT_OBSERVED",
                           "agent_check_real_native_hook_registration_and_3_ticks")

    def _handshake(self):
        # platform protocol_verified on its own does not prove Host autonomy.
        return observation("unverified","PLATFORM_AND_REAL_HOST_HANDSHAKE_NOT_OBSERVED",
                           "agent_run_protocol_and_live_host_acceptance")

    def check(self,step):
        funcs={"runtime_paths":self._runtime,"crewquay_identity":self._identity,
               "crewquay_scopes":self._scopes,
               "github_credential":self._github_token,
               "github_repositories":self._github_repositories,
               "github_permissions":self._github_permissions,
               "native_hatch":self._hatch,"deployment_handshake":self._handshake}
        if step not in funcs:
            raise ValueError("unknown setup step")
        return funcs[step]()
