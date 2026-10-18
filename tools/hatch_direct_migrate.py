#!/usr/bin/env python3
"""Install the canonical Hook directly at one existing native HATCH script path.

The installed executable is NEW canonical code. The old file exists only in
a private rollback backup. Never loads legacy Hook logic or modifies HATCH
registry, timer, Agent SQLite, permission grants or Outbox.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
import sys

SOURCE=Path(__file__).resolve().with_name("crewquay_hatch_hook.sh")
REQUIRED={"schema_version","agent_id","host_id","repo","db","config","hatch_runtime"}

def sha(data):
    return hashlib.sha256(data).hexdigest()

def safe_file(path, *, private=False):
    path=Path(path).absolute()
    info=path.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid!=os.geteuid()
        or bool(info.st_mode & 0o022) or
        (private and stat.S_IMODE(info.st_mode)!=0o600)):
        raise ValueError("file ownership/type/permissions are not safe")
    return path, info

def binding_input(path):
    path,info=safe_file(path,private=True)
    if info.st_size>4096:
        raise ValueError("binding exceeds size bound")
    data=json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data,dict) or set(data)!=REQUIRED or type(data["schema_version"]) is not int or data["schema_version"]!=1:
        raise ValueError("unrecognized binding format")
    fields=REQUIRED-{"schema_version"}
    if not all(isinstance(data[k],str) and 0<len(data[k])<=1024 and
               all(ord(c)>=32 for c in data[k]) for k in fields):
        raise ValueError("invalid binding values")
    paths=("repo","db","config","hatch_runtime")
    if not all(Path(data[k]).is_absolute() for k in paths):
        raise ValueError("binding paths must be absolute")
    if not Path(data["repo"]).is_dir() or not all(
        Path(data[k]).is_file() for k in ("db","config","hatch_runtime")):
        raise ValueError("bound files do not exist")
    config=json.loads(Path(data["config"]).read_text(encoding="utf-8"))
    if (not isinstance(config,dict) or config.get("actor_id")!=data["agent_id"]
        or not isinstance(config.get("database"),str)
        or Path(config["database"]).resolve()!=Path(data["db"]).resolve()):
        raise ValueError("Actor/database mismatch; refusing migration")
    # HATCH runtime comes from this host, never an old copied Hook or invented
    # wake adapter. Inspecting path existence is only preflight, not an ACK.
    return data

def target_info(target,expected):
    target,meta=safe_file(target)
    if not target.is_absolute() or not target.parent.is_dir():
        raise ValueError("bad target")
    if (not isinstance(expected,str) or len(expected)!=64 or
        any(c not in "0123456789abcdef" for c in expected)):
        raise ValueError("full previous SHA-256 required")
    blob=target.read_bytes()
    if len(blob)>65536 or sha(blob)!=expected:
        raise ValueError("target changed since native HATCH audit")
    if meta.st_mode & 0o022:
        raise ValueError("group-writable target forbidden")
    return target,meta,blob

def atomic_bytes(path, data, mode=0o600):
    fd,temporary=tempfile.mkstemp(prefix="."+path.name+".",dir=path.parent)
    try:
        with os.fdopen(fd,"wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary,mode)
        os.replace(temporary,path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)

def write_exclusive(path,data):
    fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,"wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())

def migrate(*, installed,expected_sha,binding_spec,native_task_id,native_command_path,apply=False):
    target,meta,old=target_info(installed,expected_sha)
    if (not isinstance(native_task_id,str) or not 1<=len(native_task_id)<=128
        or any(ord(c)<33 for c in native_task_id)
        or Path(native_command_path).absolute()!=target):
        raise ValueError("exact existing native task and invocation path required")
    binding=binding_input(binding_spec)
    canonical=SOURCE.read_bytes()
    if not canonical.startswith(b"#!/usr/bin/env bash\n# MUSE_HATCH_HOOK_ABI=1"):
        raise ValueError("canonical Hook missing expected ABI")
    if sha(canonical)==expected_sha:
        raise ValueError("canonical already installed; no legacy migration needed")
    sidecar=Path(str(target)+".binding.json")
    backup=Path(str(target)+".backup-"+expected_sha[:12])
    journal=Path(str(target)+".migration.json")
    if any(p.exists() or p.is_symlink() for p in (sidecar,journal)):
        raise ValueError("existing binding/migration journal requires explicit reconciliation")
    report={"status":"ready_to_migrate" if not apply else "migrated",
            "previous_sha256":expected_sha,"canonical_sha256":sha(canonical),
            "native_task_id_asserted":native_task_id,
            "native_registration_verified":False,
            "env_injection_required":False,
            "new_executable_only":True,
            "legacy_hook_executed":False}
    if not apply:
        return report
    if backup.is_symlink():
        raise ValueError("unsafe rollback backup")
    if backup.exists():
        b,_=safe_file(backup,private=True)
        if sha(b.read_bytes())!=expected_sha:
            raise ValueError("backup does not match previous executable")
    else:
        write_exclusive(backup,old)
    mode=stat.S_IMODE(meta.st_mode)
    spec=json.dumps(binding,sort_keys=True,separators=(",",":")).encode()
    record=json.dumps({"schema_version":1,"native_task_id":native_task_id,
        "original_sha256":expected_sha,"canonical_sha256":sha(canonical),
        "original_mode":mode,"backup":str(backup)},sort_keys=True).encode()
    try:
        write_exclusive(sidecar,spec)
        write_exclusive(journal,record)
        # Preserve original script until binding is safely installed.
        if target.is_symlink() or sha(target.read_bytes())!=expected_sha:
            raise ValueError("target changed during migration")
        atomic_bytes(target,canonical,mode=(mode|stat.S_IXUSR)&~0o022)
        if sha(target.read_bytes())!=sha(canonical):
            raise RuntimeError("canonical replace was not verified")
    except Exception:
        # If target was not swapped, leave the old executable intact.
        if sha(target.read_bytes())==expected_sha:
            for path in (sidecar,journal):
                if path.exists():
                    path.unlink()
        raise
    return {**report,"backup_verified":sha(backup.read_bytes())==expected_sha,
            "binding_mode":"0600","rollback_available":True}

def rollback(*,installed,expected_sha):
    target,_,_=target_info(installed,expected_sha)
    journal=Path(str(target)+".migration.json")
    journal,_=safe_file(journal,private=True)
    record=json.loads(journal.read_text())
    if record.get("canonical_sha256")!=expected_sha:
        raise ValueError("wrong deployed SHA for rollback")
    backup=Path(record["backup"])
    if backup!=Path(str(target)+".backup-"+record["original_sha256"][:12]):
        raise ValueError("rollback archive does not match target")
    backup,_=safe_file(backup,private=True)
    old=backup.read_bytes()
    if sha(old)!=record["original_sha256"]:
        raise ValueError("rollback archive SHA mismatch")
    # Never reintroduce group-writable old file; restore safe executable mode.
    mode=(int(record["original_mode"])|stat.S_IXUSR)&~0o022
    atomic_bytes(target,old,mode)
    return {"status":"rolled_back","restored_sha256":sha(old),
            "native_registration_verified":False,
            "outbox_replayed":False,"old_code_ran_during_migration":False}

def main(argv=None):
    parser=argparse.ArgumentParser(description="HATCH direct replacement of one registered Agent Hook")
    sub=parser.add_subparsers(dest="command",required=True)
    for cmd in ("plan","apply"):
        p=sub.add_parser(cmd)
        p.add_argument("--installed",required=True)
        p.add_argument("--expected-sha",required=True)
        p.add_argument("--binding-spec",required=True)
        p.add_argument("--native-task-id",required=True)
        p.add_argument("--native-command-path",required=True)
    p=sub.add_parser("rollback")
    p.add_argument("--installed",required=True)
    p.add_argument("--expected-sha",required=True)
    args=parser.parse_args(argv)
    try:
        if args.command=="rollback":
            result=rollback(installed=args.installed,expected_sha=args.expected_sha)
        else:
            result=migrate(installed=args.installed,
                expected_sha=args.expected_sha,binding_spec=args.binding_spec,
                native_task_id=args.native_task_id,
                native_command_path=args.native_command_path,
                apply=args.command=="apply")
        print(json.dumps(result,sort_keys=True))
        return 0
    except (ValueError,OSError,RuntimeError,KeyError,TypeError):
        print(json.dumps({"status":"failed","native_registration_verified":False}))
        return 2

if __name__=="__main__":
    sys.exit(main())
