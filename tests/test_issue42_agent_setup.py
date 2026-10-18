"""#42: setup v1 has no plaintext secret, false grants, or repeated prompts."""
import json
import os
import stat
import tempfile
import unittest
from pathlib import Path

from patrol.components.setup.components.manifest import Manifest
from patrol.components.setup.components.probes import Probes, observation
from patrol.components.setup.components.secrets import (
    read_secret, SecretUnavailable, private_dirs)
from patrol.components.setup.index import Setup
from patrol.components.runtime.index import main


def fixture(root, role="worker",github_required=False):
    state=Path(root)/"muse-worker"/"hatch-01"
    return {"schema_version":1,
     "agent":{"id":"muse-worker","role":role,"host_instance":"hatch-01"},
     "crewquay":{"base_url":"https://mxrlyy.asia","actor_id":"muse-worker",
         "credential_ref":"secret://crewquay/agent",
         "required_scopes":["authorized_project_tasks"],"required":True},
     "github":{"auth_type":"fine_grained_pat",
         "credential_ref":"secret://github/pat",
         "repositories":["fantasywinter207/muse-agent-patrol"],
         "permissions":{"contents":"write","pull_requests":"write","issues":"read"},
         "policy":{"allow_pr_create":True,"allow_pr_merge":False,
                   "allow_direct_main_push":False},
         "required":github_required},
     "runtime":{"state_dir":str(state),"config_path":str(state/"runtime.json"),
         "database":str(state/"patrol.sqlite3"),
         "hook":{"provider":"hatch","interval_seconds":60},"required":True}}


class FakeProbes:
    def __init__(self):
        self.calls=[]
        self.overrides={}
    def check(self,step):
        self.calls.append(step)
        return self.overrides.get(step,observation("verified","SAFE_UNIT_FIXTURE"))


class SetupTests(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory()
        self.addCleanup(t.cleanup)
        self.root=Path(t.name)
        self.manifest=Manifest(fixture(self.root))
        self.probes=FakeProbes()
        self.setup=Setup(self.manifest,probes=self.probes,clock=lambda:1000)

    def test_manifest_rejects_unknown_secrets_duplicate_and_shared_state(self):
        base=fixture(self.root)
        for mutate in (
            lambda d:d.update({"github_pat":"ghp_not_allowed"}),
            lambda d:d["github"].update({"token":"not a place for credentials"}),
            lambda d:d["github"].update({"credential_ref":"github_pat_value"}),
            lambda d:d["runtime"].update({"state_dir":str(self.root)}),
            lambda d:d["runtime"]["hook"].update({"interval_seconds":120}),
            lambda d:d["github"]["policy"].update({"allow_direct_main_push":True}),
            lambda d:d["agent"].update({"role":"owner"})):
            raw=json.loads(json.dumps(base))
            mutate(raw)
            with self.assertRaises(ValueError):
                Manifest(raw)
        self.assertEqual(self.manifest.data["github"]["auth_type"],"fine_grained_pat")
        path=self.root/"manifest.json"
        path.write_text('{"schema_version":1,"schema_version":1}',encoding="utf-8")
        with self.assertRaisesRegex(ValueError,"duplicate"):
            Manifest.load(path)

    def test_init_next_is_one_at_a_time_and_restart_idempotent(self):
        self.setup.init()
        self.probes.overrides["crewquay_identity"]=observation(
            "awaiting_user","SECURE_SECRET_INPUT_REQUIRED",
            "request_one_secure_crewquay_credential_import")
        first=self.setup.next()
        self.assertEqual(first["current"]["step"],"crewquay_identity")
        self.assertTrue(first["owner_contact_required"])
        self.assertEqual(self.probes.calls,["runtime_paths","crewquay_identity"])
        second=Setup(self.manifest,probes=self.probes,clock=lambda:1060).next()
        self.assertEqual(second["current"]["step"],"crewquay_identity")
        self.assertFalse(second["owner_contact_required"])
        self.probes.overrides["crewquay_identity"]=observation(
            "verified","ACTUAL_AUTHENTICATED_FIXTURE")
        third=self.setup.next()
        self.assertEqual(third["status"],"setup_steps_resolved")
        self.assertFalse(third["host_deployment_verified"])
        self.assertEqual(self.setup.status()["status"],"ready_for_external_e2e")

    def test_optional_skip_cannot_bypass_required(self):
        self.setup.init()
        with self.assertRaises(ValueError):
            self.setup.skip_optional("crewquay_identity")
        result=self.setup.skip_optional("github_credential")
        self.assertEqual(result["status"],"skipped_optional")
        self.assertEqual(result["grants_changed"],False)
        self.probes.overrides["native_hatch"]=observation(
            "unverified","NATIVE_HATCH_REGISTRATION_NOT_OBSERVED",
            "agent_check_real_native_hook_registration_and_3_ticks")
        self.assertEqual(self.setup.next()["current"]["step"],"native_hatch")
        self.assertFalse(self.setup.status()["required_verified"])

    def test_denial_sticky_but_is_not_remote_grant(self):
        self.setup.init()
        self.setup.deny("crewquay_scopes")
        state=self.setup.next()
        self.assertEqual(state["current"]["status"],"denied")
        self.assertFalse(state["owner_contact_required"])
        self.assertEqual(self.setup.status()["required_verified"],False)

    def test_private_secret_file_and_no_plaintext_in_sqlite(self):
        self.setup.init()
        folder=private_dirs(self.manifest.state_dir)
        if os.name!="posix":
            with self.assertRaises(SecretUnavailable):
                read_secret(self.manifest.state_dir,"secret://github/pat")
            return
        secret=folder/"github_pat"
        secret.write_text("token_for_unit_tests_ONLY",encoding="utf-8")
        secret.chmod(0o600)
        self.assertEqual(read_secret(self.manifest.state_dir,"secret://github/pat"),
                         "token_for_unit_tests_ONLY")
        self.probes.overrides["github_credential"]=observation(
            "awaiting_user","SECURE_SECRET_INPUT_REQUIRED",
            "request_one_secure_github_pat_import")
        self.setup.verify()
        db=(self.manifest.state_dir/"setup.sqlite3").read_bytes()
        self.assertNotIn(b"token_for_unit_tests_ONLY",db)
        secret.chmod(0o644)
        with self.assertRaises(SecretUnavailable):
            read_secret(self.manifest.state_dir,"secret://github/pat")
        with self.assertRaises(SecretUnavailable):
            read_secret(self.manifest.state_dir,"secret://github/../../ssh")

    def test_github_live_read_does_not_claim_write_permission(self):
        if os.name!="posix":
            self.skipTest("host-private file ACL verification required on Windows")
        self.setup.init()
        private_dirs(self.manifest.state_dir)
        path=self.manifest.state_dir/"secrets"/"github_pat"
        path.write_text("unit_fine_grained_token",encoding="utf-8")
        path.chmod(0o600)
        calls=[]
        def fake_remote(url,token):
            calls.append(url)
            self.assertEqual(token,"unit_fine_grained_token")
            if url=="/user":
                return 200,{"login":"muse-bot"}
            return 200,{"full_name":"fantasywinter207/muse-agent-patrol"}
        probe=Probes(self.manifest,github_reader=fake_remote)
        self.assertEqual(probe.check("github_credential")["status"],"verified")
        self.assertEqual(probe.check("github_repositories")["status"],"verified")
        result=probe.check("github_permissions")
        self.assertEqual(result["status"],"unverified")
        self.assertEqual(result["reason"],"GITHUB_WRITE_PERMISSIONS_AND_RULESETS_UNVERIFIED")
        self.assertTrue(all(c in ("/user","/repos/fantasywinter207/muse-agent-patrol")
                            for c in calls))
        self.assertEqual(probe.check("native_hatch")["status"],"unverified")

    def test_cli_setup_does_not_load_old_runtime_config(self):
        manifest_file=self.root/"setup.json"
        manifest_file.write_text(json.dumps(fixture(self.root)),encoding="utf-8")
        self.assertEqual(main(["setup","--manifest",str(manifest_file),"init"]),0)
        self.assertEqual(main(["setup","--manifest",str(manifest_file),"status"]),0)
        self.assertEqual(main(["setup","--manifest",str(manifest_file),"next"]),0)
        self.assertFalse((self.root/"muse-worker"/"hatch-01"/"patrol.sqlite3").exists())


if __name__=="__main__":
    unittest.main()
