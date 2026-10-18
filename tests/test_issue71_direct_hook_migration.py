"""#71: direct new canonical Hook, no legacy runtime, no native env injection."""
import importlib.util
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
TOOLS=ROOT/"tools"
spec=importlib.util.spec_from_file_location("hatch_direct",TOOLS/"hatch_direct_migrate.py")
migrate=importlib.util.module_from_spec(spec)
spec.loader.exec_module(migrate)

class DirectMigrationTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir=Path(tmp.name)
        self.installed=self.dir/"crewquay-wakeup.sh"
        self.original=b"#!/usr/bin/env bash\n# custom unmarked native hook before ABI\n"
        self.installed.write_bytes(self.original)
        self.installed.chmod(0o750)
        self.config=self.dir/"runtime.json"
        self.db=self.dir/"agent.sqlite"
        self.db.touch()
        self.runtime=self.dir/"hatch-native.sh"
        self.runtime.write_text("""log(){ printf 'log:%s\n' "$1" >> "$MOCK_HATCH_LOG"; }
silent(){ printf 'silent:%s\n' "$1" >> "$MOCK_HATCH_LOG"; }
wake(){ printf '%s\n' "$2" >> "$MOCK_WAKE_LOG"; }
""")
        self.actor="actor-a"
        self.config.write_text(json.dumps({"actor_id":self.actor,"database":str(self.db)}))
        self.spec=self.dir/"native-binding.json"
        self.data={"schema_version":1,"agent_id":self.actor,"host_id":"host-one",
                   "repo":str(ROOT),"db":str(self.db),"config":str(self.config),
                   "hatch_runtime":str(self.runtime)}
        self._write_spec()
        self.sha=migrate.sha(self.original)
        self.bin=self.dir/"bin"
        self.bin.mkdir()
        script=self.bin/"python3"
        script.write_text("""#!/usr/bin/env bash
if [ "$1" = "-m" ]; then
  printf '%s\n' "$*" >> "$MOCK_CYCLE_CALLS"
  printf '%s\n' "$MOCK_CYCLE_JSON"
  exit 0
fi
exec "$REAL_PYTHON" "$@"
""")
        script.chmod(0o700)
        self.env={k:v for k,v in os.environ.items()
                  if not k.startswith("CREWQUAY_") and k!="HATCH_HOOK_RUNTIME"}
        self.env.update(PATH=str(self.bin)+os.pathsep+os.environ["PATH"],
                        REAL_PYTHON=sys.executable,
                        MOCK_CYCLE_CALLS=str(self.dir/"calls.log"),
                        MOCK_WAKE_LOG=str(self.dir/"wakes.log"),
                        MOCK_HATCH_LOG=str(self.dir/"hatch.log"))
        self.env["MOCK_CYCLE_JSON"]=json.dumps({"status":"ok","host":{
            "should_wake":True,"status":"notification_intent",
            "host_capability":"best_effort","source_type":"work",
            "source_id":"job_1","wakeup_id":"wake_one",
            "lease_version":1,"notification_generation":1,
            "requires_host_receipt":False}})

    def _write_spec(self):
        self.spec.write_text(json.dumps(self.data))
        self.spec.chmod(0o600)

    def migration(self,apply=False,**extra):
        args=dict(installed=str(self.installed),expected_sha=self.sha,
            binding_spec=str(self.spec),native_task_id="real-hatch-task",
            native_command_path=str(self.installed),apply=apply)
        args.update(extra)
        return migrate.migrate(**args)

    def test_new_hook_directly_replaces_unmanaged_custom_script_and_boots_without_env(self):
        result=self.migration()
        self.assertEqual(result["status"],"ready_to_migrate")
        self.assertEqual(self.installed.read_bytes(),self.original)
        report=self.migration(apply=True)
        self.assertTrue(report["backup_verified"])
        self.assertTrue(report["new_executable_only"])
        self.assertFalse(report["legacy_hook_executed"])
        self.assertEqual(self.installed.read_bytes(),(TOOLS/"crewquay_hatch_hook.sh").read_bytes())
        private=Path(str(self.installed)+".binding.json")
        self.assertEqual(private.stat().st_mode & 0o777,0o600)
        self.assertEqual(json.loads(private.read_text()),self.data)
        run=subprocess.run(["bash",str(self.installed)],env=self.env,
                           capture_output=True,text=True,timeout=9)
        self.assertEqual(run.returncode,0,run.stderr)
        self.assertEqual((self.dir/"calls.log").read_text().count("runtime-cycle"),1)
        self.assertEqual(json.loads((self.dir/"wakes.log").read_text())["wakeup_id"],"wake_one")
        rolled=migrate.rollback(installed=str(self.installed),
                                expected_sha=report["canonical_sha256"])
        self.assertEqual(rolled["status"],"rolled_back")
        self.assertEqual(self.installed.read_bytes(),self.original)

    def test_noenv_without_binding_fails_before_runtime_call(self):
        self.installed.write_bytes((TOOLS/"crewquay_hatch_hook.sh").read_bytes())
        run=subprocess.run(["bash",str(self.installed)],env=self.env,
                           capture_output=True,text=True,timeout=9)
        self.assertEqual(run.returncode,2)
        self.assertIn("private Agent binding",run.stderr)
        self.assertFalse((self.dir/"calls.log").exists())

    def test_wrong_actor_and_permission_protection(self):
        self.data["agent_id"]="other-agent"
        self._write_spec()
        with self.assertRaises(ValueError):
            self.migration()
        self.data["agent_id"]=self.actor
        self._write_spec()
        self.installed.chmod(0o770)
        with self.assertRaises(ValueError):
            self.migration()
        self.installed.chmod(0o750)
        self.spec.chmod(0o640)
        with self.assertRaises(ValueError):
            self.migration()

    def test_bad_native_path_changed_sha_and_unrecognized_manager_fail_closed(self):
        with self.assertRaises(ValueError):
            self.migration(native_command_path="/tmp/a-different-hook")
        with self.assertRaises(ValueError):
            self.migration(expected_sha="0"*64)
        manager_spec=importlib.util.spec_from_file_location(
            "old_manager",TOOLS/"hatch_hook_manager.py")
        old=importlib.util.module_from_spec(manager_spec)
        manager_spec.loader.exec_module(old)
        self.assertEqual(old.inspect(self.installed)["status"],"unmanaged")
        with self.assertRaises(ValueError):
            old.sync(self.installed,self.sha)

    def test_two_agents_same_home_cannot_reuse_other_agent_binding(self):
        self.migration(apply=True)
        copy=self.dir/"other-agent-hook.sh"
        copy.write_bytes(self.installed.read_bytes())
        copy.chmod(0o750)
        # No corresponding private sidecar for another installation.
        run=subprocess.run(["bash",str(copy)],env=self.env,
                           capture_output=True,text=True,timeout=9)
        self.assertEqual(run.returncode,2)
        self.assertFalse((self.dir/"calls.log").exists())

if __name__=="__main__":
    unittest.main()
