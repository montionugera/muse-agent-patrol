"""Versioned, public *expectations* for a single Agent installation.

This is not a runtime Config and never grants scopes or contains credentials.
JSON is deliberately used instead of YAML to avoid a new parser and ambiguous
types; the public example + JSON Schema are the declarative contract.
"""
import hashlib
import json
import re
from pathlib import Path
from urllib.parse import urlsplit

_SLUG=re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{1,63}$")
_REPO=re.compile(r"^[A-Za-z0-9_.-]{1,100}/[A-Za-z0-9_.-]{1,100}$")
_SCOPE=re.compile(r"^[a-z][a-z0-9_.:-]{1,100}$")
_REF=re.compile(r"^secret://(crewquay|github)/[a-z][a-z0-9_-]{0,63}$")
_PERMISSIONS={"contents","issues","pull_requests","actions","workflows"}
_ALLOWED_ROLES={"planner","worker","tester"}


def _object_pairs(pairs):
    result={}
    for k,v in pairs:
        if k in result:
            raise ValueError("duplicate manifest key: "+k)
        result[k]=v
    return result


def _shape(value, keys, required, label):
    if not isinstance(value,dict):
        raise ValueError(label+" must be an object")
    extra=set(value)-set(keys)
    missing=set(required)-set(value)
    if extra or missing:
        raise ValueError(label+" unsupported/missing keys: "+",".join(sorted(extra|missing)))


def _string(value, label, pattern=None):
    if not isinstance(value,str) or not value:
        raise ValueError(label+" must be a nonempty string")
    if pattern and not pattern.fullmatch(value):
        raise ValueError(label+" has invalid format")
    return value


def _path(value,label):
    if not isinstance(value,(str,Path)) or not str(value) or "\x00" in str(value):
        raise ValueError("invalid "+label)
    p=Path(value).expanduser()
    if not p.is_absolute() or ".." in p.parts:
        raise ValueError(label+" must be an absolute/private expanded path")
    return p


def _https_origin(value,label):
    p=urlsplit(_string(value,label))
    if (p.scheme!="https" or not p.hostname or p.username or p.password
            or p.query or p.fragment or p.path not in ("","/")):
        raise ValueError(label+" must be a credential-free HTTPS origin")


class Manifest:
    def __init__(self,data):
        _shape(data,("schema_version","agent","crewquay","github","runtime"),
               ("schema_version","agent","crewquay","github","runtime"),"manifest")
        if type(data["schema_version"]) is not int or data["schema_version"]!=1:
            raise ValueError("unsupported deployment schema_version")
        a=data["agent"]
        _shape(a,("id","role","host_instance"),("id","role","host_instance"),"agent")
        _string(a["id"],"agent.id",_SLUG)
        _string(a["host_instance"],"agent.host_instance",_SLUG)
        if a["role"] not in _ALLOWED_ROLES:
            raise ValueError("unsupported Agent role")
        c=data["crewquay"]
        _shape(c,("base_url","actor_id","credential_ref","required_scopes","required"),
               ("base_url","actor_id","credential_ref","required_scopes","required"),"crewquay")
        _https_origin(c["base_url"],"crewquay.base_url")
        _string(c["actor_id"],"crewquay.actor_id",_SLUG)
        if not _REF.fullmatch(c["credential_ref"]) or not c["credential_ref"].startswith("secret://crewquay/"):
            raise ValueError("CrewQuay credential must be a logical secret reference")
        if not isinstance(c["required_scopes"],list) or not c["required_scopes"] or any(
                not isinstance(x,str) or not _SCOPE.fullmatch(x) for x in c["required_scopes"]):
            raise ValueError("invalid crewquay.required_scopes")
        if len(set(c["required_scopes"]))!=len(c["required_scopes"]):
            raise ValueError("duplicate CrewQuay requested scopes")
        g=data["github"]
        _shape(g,("auth_type","credential_ref","repositories","permissions","policy","required"),
               ("auth_type","credential_ref","repositories","permissions","policy","required"),"github")
        if g["auth_type"] not in ("fine_grained_pat","github_app"):
            raise ValueError("unsupported github.auth_type")
        if (not _REF.fullmatch(g["credential_ref"]) or
                not g["credential_ref"].startswith("secret://github/")):
            raise ValueError("GitHub credential must be a logical secret reference")
        if not isinstance(g["repositories"],list) or not g["repositories"] or any(
                not isinstance(x,str) or not _REPO.fullmatch(x) or ".." in x
                for x in g["repositories"]) or len(set(g["repositories"]))!=len(g["repositories"]):
            raise ValueError("invalid or duplicated GitHub repositories")
        _shape(g["permissions"],_PERMISSIONS,(),"github.permissions")
        if not g["permissions"] or any(x not in ("read","write") for x in g["permissions"].values()):
            raise ValueError("invalid GitHub expected permission level")
        p=g["policy"]
        _shape(p,("allow_pr_create","allow_pr_merge","allow_direct_main_push"),
               ("allow_pr_create","allow_pr_merge","allow_direct_main_push"),"github.policy")
        if any(type(v) is not bool for v in p.values()):
            raise ValueError("GitHub policy flags must be boolean")
        if p["allow_direct_main_push"]:
            raise ValueError("direct main push must be protected by GitHub rulesets; cannot enable in manifest")
        if a["role"] in ("worker","tester") and p["allow_pr_merge"]:
            raise ValueError("Worker/Tester cannot be configured to merge PRs")
        r=data["runtime"]
        _shape(r,("state_dir","config_path","database","hook","required"),
               ("state_dir","config_path","database","hook","required"),"runtime")
        state=_path(r["state_dir"],"runtime.state_dir")
        path=_path(r["config_path"],"runtime.config_path")
        database=_path(r["database"],"runtime.database")
        if (a["id"] not in state.parts or a["host_instance"] not in state.parts):
            raise ValueError("state_dir must isolate both agent id and host instance")
        # Paths must be private to this installation. Do not follow symlinks
        # to a different Agent's state tree.
        if path==database or state not in path.parents or state not in database.parents:
            raise ValueError("Runtime config/database must be distinct files under state_dir")
        _shape(r["hook"],("provider","interval_seconds"),("provider","interval_seconds"),"runtime.hook")
        if r["hook"]["provider"]!="hatch" or type(r["hook"]["interval_seconds"]) is not int or r["hook"]["interval_seconds"]!=60:
            raise ValueError("only the single native 60-second HATCH Hook is supported")
        for name in ("crewquay","github","runtime"):
            if type(data[name]["required"]) is not bool:
                raise ValueError(name+".required must be a boolean")
        self.data=data
        self.revision=hashlib.sha256(json.dumps(
            data,sort_keys=True,separators=(",",":"),ensure_ascii=False).encode()).hexdigest()
        self.state_dir=state

    @classmethod
    def load(cls,path):
        p=_path(path,"manifest path")
        if p.is_symlink():
            raise ValueError("manifest symlinks are not supported")
        with p.open(encoding="utf-8") as f:
            return cls(json.load(f,object_pairs_hook=_object_pairs))

    def required(self,name):
        return self.data[name]["required"]

    def step_names(self):
        return ("runtime_paths","crewquay_identity","crewquay_scopes",
                "github_credential","github_repositories","github_permissions",
                "native_hatch","deployment_handshake")
