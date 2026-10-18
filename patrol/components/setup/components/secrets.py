"""Secret references resolve ONLY to pre-approved private host files.

No token is accepted on argv/stdin/manifest, persisted in SQLite, or logged.
A real host secret manager may provision these files securely outside Patrol.
"""
import os
import re
import stat
from pathlib import Path

_REF=re.compile(r"^secret://(crewquay|github)/([a-z][a-z0-9_-]{0,63})$")


class SecretUnavailable(Exception):
    def __init__(self,reason):
        self.reason=reason
        super().__init__(reason)


def private_dirs(state_dir,create=False):
    root=Path(state_dir).expanduser()
    if root.is_symlink():
        raise SecretUnavailable("STATE_DIR_SYMLINK")
    if create:
        root.mkdir(parents=True,exist_ok=True,mode=0o700)
    if not root.is_dir():
        raise SecretUnavailable("STATE_DIR_MISSING")
    if os.name=="posix":
        if stat.S_IMODE(root.stat().st_mode)&0o077:
            raise SecretUnavailable("STATE_DIR_NOT_PRIVATE")
    folder=root/"secrets"
    if folder.is_symlink():
        raise SecretUnavailable("SECRET_DIR_SYMLINK")
    if create:
        folder.mkdir(exist_ok=True,mode=0o700)
    if not folder.is_dir():
        raise SecretUnavailable("SECRET_IMPORT_UNAVAILABLE")
    if os.name=="posix" and stat.S_IMODE(folder.stat().st_mode)&0o077:
        raise SecretUnavailable("SECRET_DIR_NOT_PRIVATE")
    return folder


def safe_secret_file(state_dir,reference):
    match=_REF.fullmatch(reference) if isinstance(reference,str) else None
    if not match:
        raise SecretUnavailable("INVALID_SECRET_REFERENCE")
    folder=private_dirs(state_dir)
    # The logical namespace+key are fixed. Never allow a path from manifest
    # to open arbitrary ~/.ssh or the other Agent's private files.
    secret=folder/(match[1]+"_"+match[2])
    if secret.is_symlink() or not secret.is_file():
        raise SecretUnavailable("SECURE_SECRET_INPUT_REQUIRED")
    if os.name!="posix":
        # Cannot establish Windows ACL security through chmod/st_mode alone.
        raise SecretUnavailable("HOST_ACL_VERIFICATION_REQUIRED")
    info=secret.stat()
    if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode)&0o177:
        raise SecretUnavailable("SECRET_FILE_NOT_0600")
    if hasattr(os,"getuid") and info.st_uid!=os.getuid():
        raise SecretUnavailable("SECRET_OWNER_MISMATCH")
    return secret


def read_secret(state_dir,reference):
    path=safe_secret_file(state_dir,reference)
    # Never return file path, contents, or exception text to report callers.
    with path.open("rb") as f:
        value=f.read(16385)
    if not value or len(value)>16384 or b"\x00" in value or b"\n" in value or b"\r" in value:
        raise SecretUnavailable("INVALID_PRIVATE_SECRET")
    try:
        token=value.decode("utf-8").strip()
    except UnicodeError:
        raise SecretUnavailable("INVALID_PRIVATE_SECRET") from None
    if not token or any(x.isspace() for x in token):
        raise SecretUnavailable("INVALID_PRIVATE_SECRET")
    return token
