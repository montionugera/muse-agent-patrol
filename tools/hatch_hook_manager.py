#!/usr/bin/env python3
"""Agent-owned HATCH Hook drift inspection and safe existing-file upgrade.

Does NOT register/enable a HATCH job, change any scheduler, run patrol,
read credentials, create an extra timer or claim natural Host verification.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile


CANONICAL=Path(__file__).resolve().with_name("crewquay_hatch_hook.sh")
MARKER=b"MUSE_HATCH_HOOK_ABI=1"
HISTORICAL_MARKER=b"Native HATCH 60s Hook adapter:"
COMPAT_MARKER=b"MUSE_HATCH_HOOK_COMPAT=1"
MAX_SCRIPT=65536


def digest(blob):
    return hashlib.sha256(blob).hexdigest()


def inspect(installed,canonical=CANONICAL):
    """Check the exact currently installed script, not HATCH registry state."""
    target=Path(installed).expanduser().absolute()
    if target.is_symlink() or not target.is_file():
        raise ValueError("existing non-symlink regular HATCH Hook file required")
    if target.stat().st_size>MAX_SCRIPT:
        raise ValueError("Hook script exceeds bounded size")
    original=target.read_bytes()
    source=Path(canonical).read_bytes()
    if not source.startswith(b"#!/usr/bin/env bash") or MARKER not in source[:256]:
        raise ValueError("trusted canonical Hook source required")
    recognized=(MARKER in original[:256]
                or HISTORICAL_MARKER in original[:512]
                or COMPAT_MARKER in original[:256])
    current=digest(original)
    desired=digest(source)
    return {"status":"in_sync" if current==desired else
                     "drift" if recognized else "unmanaged",
            "current_sha256":current,"canonical_sha256":desired,
            "hook_abi":1,"recognized_previous":recognized,
            "native_registration_verified":False,
            "natural_tick_verified":False,"real_worker_started_verified":False,
            "remote_delivery_verified":False}


def sync(installed,expected_sha,canonical=CANONICAL):
    """Fail-closed CAS replace; never overwrite unknown/native registry state."""
    target=Path(installed).expanduser().absolute()
    if target.is_symlink() or not target.is_file():
        raise ValueError("refusing to create, follow symlink or replace missing Hook")
    if not isinstance(expected_sha,str) or len(expected_sha)!=64 or any(
            ch not in "0123456789abcdef" for ch in expected_sha):
        raise ValueError("previous audit SHA-256 required")
    report=inspect(target,canonical)
    if report["current_sha256"]!=expected_sha:
        raise ValueError("Hook changed since audit; retry read-only audit")
    if report["status"]=="unmanaged":
        raise ValueError("refusing to overwrite unrecognized HATCH script")
    if report["status"]=="in_sync":
        return {**report,"updated":False,"backup_path":None}
    source=Path(canonical).read_bytes()
    # A copy of the exact old executable is saved for rollback. Never reset
    # the native Hook task or old Agent SQLite/Outbox identities.
    old=target.read_bytes()
    if digest(old)!=expected_sha:
        raise ValueError("Hook changed before snapshot")
    mode=stat.S_IMODE(target.stat().st_mode)
    if mode & 0o022:
        raise ValueError("Hook file writable by group/others; repair permissions first")
    backup=target.with_name(target.name+".backup-"+expected_sha[:12])
    if backup.is_symlink():
        raise ValueError("refusing symlink backup")
    if backup.exists():
        if not backup.is_file() or digest(backup.read_bytes())!=expected_sha:
            raise ValueError("existing backup does not match verified old content")
    else:
        fd=os.open(str(backup),os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
        with os.fdopen(fd,"wb") as f:
            f.write(old)
            f.flush()
            os.fsync(f.fileno())
    fd,tmp=tempfile.mkstemp(prefix="."+target.name+".",dir=str(target.parent))
    try:
        with os.fdopen(fd,"wb") as f:
            f.write(source)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp,mode|stat.S_IXUSR)
        if target.is_symlink() or digest(target.read_bytes())!=expected_sha:
            raise ValueError("Hook changed concurrently; no update performed")
        os.replace(tmp,target)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
    result=inspect(target,canonical)
    if result["status"]!="in_sync":
        raise RuntimeError("Hook replacement was not verified")
    return {**result,"updated":True,"backup_path":str(backup)}


def main(argv=None):
    parser=argparse.ArgumentParser(description="Check and align a single *existing* HATCH Hook script")
    cmds=parser.add_subparsers(dest="command",required=True)
    a=cmds.add_parser("audit",help="read-only local Hook script drift check")
    a.add_argument("--installed",required=True)
    a=cmds.add_parser("sync",help="atomic update of known existing Hook, not HATCH registration")
    a.add_argument("--installed",required=True)
    a.add_argument("--expected-sha",required=True)
    args=parser.parse_args(argv)
    try:
        if args.command=="audit":
            result=inspect(args.installed)
        else:
            result=sync(args.installed,args.expected_sha)
        print(json.dumps(result,ensure_ascii=False,sort_keys=True))
        return 0 if result["status"]=="in_sync" else 3
    except (ValueError,OSError,RuntimeError) as e:
        # Avoid echoing potentially private paths/content or injected errors.
        print(json.dumps({"status":"failed","error":type(e).__name__,
                          "native_registration_verified":False}))
        return 2


if __name__=="__main__":
    sys.exit(main())
