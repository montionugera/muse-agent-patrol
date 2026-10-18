"""#68: unified HATCH execution, exact wake filtering and safe installed-copy sync."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT=Path(__file__).resolve().parents[1]
TOOLS=ROOT/"tools"
spec=importlib.util.spec_from_file_location("hook_manager",TOOLS/"hatch_hook_manager.py")
hook_manager=importlib.util.module_from_spec(spec)
spec.loader.exec_module(hook_manager)


class HookManagerTests(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory()
        self.addCleanup(t.cleanup)
        self.tmp=Path(t.name)
        self.installed=self.tmp/"legacy-hook.sh"
        self.installed.write_text("#!/usr/bin/env bash\n# Native HATCH 60s Hook adapter:\n# old script\n")
        self.installed.chmod(0o700)

    def test_audit_then_atomic_sync_keeps_backup_and_rejects_stale_cas(self):
        prior=hook_manager.inspect(self.installed)
        self.assertEqual(prior["status"],"drift")
        self.assertFalse(prior["native_registration_verified"])
        result=hook_manager.sync(self.installed,prior["current_sha256"])
        self.assertEqual(result["status"],"in_sync")
        self.assertTrue(result["updated"])
        backup=Path(result["backup_path"])
        self.assertEqual(backup.read_bytes(),b"#!/usr/bin/env bash\n# Native HATCH 60s Hook adapter:\n# old script\n")
        self.assertEqual(self.installed.read_bytes(),(TOOLS/"crewquay_hatch_hook.sh").read_bytes())
        self.assertFalse(hook_manager.sync(
            self.installed,result["current_sha256"])["updated"])
        with self.assertRaises(ValueError):
            hook_manager.sync(self.installed,prior["current_sha256"])

    def test_unknown_symlink_and_missing_cannot_be_overwritten(self):
        self.installed.write_text("#!/bin/sh\nexec evil-helper\n")
        s=hook_manager.inspect(self.installed)
        self.assertEqual(s["status"],"unmanaged")
        with self.assertRaises(ValueError):
            hook_manager.sync(self.installed,s["current_sha256"])
        missing=self.tmp/"missing.sh"
        with self.assertRaises(ValueError):
            hook_manager.inspect(missing)
        self.installed.unlink()
        self.installed.symlink_to(TOOLS/"crewquay_hatch_hook.sh")
        with self.assertRaises(ValueError):
            hook_manager.inspect(self.installed)

    def test_shipped_legacy_filename_delegates_to_canonical(self):
        legacy=(TOOLS/"crewquay_phase3_hook.sh").read_text()
        self.assertIn("MUSE_HATCH_HOOK_COMPAT=1",legacy)
        self.assertIn("crewquay_hatch_hook.sh",legacy)
        self.assertNotIn("runtime-cycle",legacy)
        for name in ("crewquay_phase3_hook.sh","crewquay_hatch_hook.sh"):
            check=subprocess.run(["bash","-n",str(TOOLS/name)],
                                 capture_output=True,text=True)
            self.assertEqual(check.returncode,0,check.stderr)


class HookExecutionTests(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory()
        self.addCleanup(t.cleanup)
        self.tmp=Path(t.name)
        self.mockbin=self.tmp/"bin"
        self.mockbin.mkdir()
        py=self.mockbin/"python3"
        py.write_text("""#!/usr/bin/env bash
if [ "$1" = "-m" ]; then
    printf '%s\\n' "$MOCK_CYCLE_JSON"
    printf '%s\\n' "$*" >> "$MOCK_CYCLE_CALLS"
    exit 0
fi
exec "$REAL_PYTHON" "$@"
""")
        py.chmod(0o700)
        self.runtime=self.tmp/"runtime.sh"
        self.runtime.write_text("""log(){ printf 'log:%s\\n' "$1" >> "$MOCK_HATCH_LOG"; }
silent(){ printf 'silent:%s\\n' "$1" >> "$MOCK_HATCH_LOG"; }
wake(){ printf '%s\\n' "$2" >> "$MOCK_WAKE_LOG"; }
""")
        self.config=self.tmp/"runtime.json"
        self.config.write_text("{}")
        self.db=self.tmp/"agent.sqlite"
        self.db.touch()
        self.log=self.tmp/"hatch.log"
        self.wakes=self.tmp/"wakes.log"
        self.calls=self.tmp/"calls.log"
        self.env=dict(os.environ)
        self.env.update(
            PATH=str(self.mockbin)+os.pathsep+os.environ["PATH"],
            REAL_PYTHON=sys.executable,
            HATCH_HOOK_RUNTIME=str(self.runtime),
            CREWQUAY_REPO=str(ROOT),
            CREWQUAY_CONFIG=str(self.config),
            CREWQUAY_DB=str(self.db),
            CREWQUAY_HOST_CAPABILITY="best_effort",
            MOCK_CYCLE_CALLS=str(self.calls),
            MOCK_HATCH_LOG=str(self.log),
            MOCK_WAKE_LOG=str(self.wakes),
        )

    def handoff(self,**kw):
        h={"should_wake":True,"status":"notification_intent",
           "source_type":"work","source_id":"job_1","wakeup_id":"wake_1",
           "lease_version":1,"notification_generation":1,
           "host_capability":"best_effort","requires_host_receipt":False}
        h.update(kw)
        return json.dumps({"status":"ok","host":h})

    def run_hook(self,entry="crewquay_hatch_hook.sh"):
        r=subprocess.run(["bash",str(TOOLS/entry)],env=self.env,
                         cwd=self.tmp,capture_output=True,text=True,timeout=8)
        self.assertEqual(r.returncode,0,r.stderr)
        return r

    def test_canonical_and_legacy_use_one_runtime_cycle_and_exact_handoff(self):
        self.env["MOCK_CYCLE_JSON"]=self.handoff()
        self.run_hook()
        body=self.wakes.read_text().splitlines()
        self.assertEqual(len(body),1)
        wake=json.loads(body[0])
        self.assertEqual(wake["wakeup_id"],"wake_1")
        self.assertEqual(wake["notification_generation"],1)
        self.assertEqual(self.calls.read_text().count("runtime-cycle"),1)
        self.assertTrue(Path(str(self.db)+".hatch-hook.lock").exists())
        self.wakes.unlink()
        self.run_hook("crewquay_phase3_hook.sh")
        self.assertEqual(len(self.wakes.read_text().splitlines()),1)
        self.assertEqual(self.calls.read_text().count("runtime-cycle"),2)

    def test_invalid_generation_and_mode_are_not_woken(self):
        self.env["MOCK_CYCLE_JSON"]=self.handoff(notification_generation=2)
        self.run_hook()
        self.assertFalse(self.wakes.exists())
        self.env["MOCK_CYCLE_JSON"]=self.handoff(host_capability="reliable_ack")
        self.run_hook()
        self.assertFalse(self.wakes.exists())

    def test_no_native_wake_sets_unavailable_and_does_not_fake_ack(self):
        self.runtime.write_text("""log(){ :; }
silent(){ :; }
""")
        self.env["MOCK_CYCLE_JSON"]=self.handoff()
        self.run_hook()
        self.assertFalse(self.wakes.exists())
        self.assertIn("--host-capability unavailable",self.calls.read_text())

    def test_agents_on_same_home_use_different_sqlite_mutexes(self):
        self.env["MOCK_CYCLE_JSON"]=self.handoff(should_wake=False)
        self.run_hook()
        other=self.tmp/"worker-two.sqlite"
        other.touch()
        self.env["CREWQUAY_DB"]=str(other)
        self.run_hook()
        self.assertTrue(Path(str(self.db)+".hatch-hook.lock").exists())
        self.assertTrue(Path(str(other)+".hatch-hook.lock").exists())
        self.assertEqual(self.calls.read_text().count("runtime-cycle"),2)


if __name__=="__main__":
    unittest.main()
