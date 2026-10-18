"""#42: real typed GitHub REST adapter, no forged scopes or blind duplicate writes."""
import os
import tempfile
import unittest
from pathlib import Path

from patrol.components.github.index import GitHubOperations
from patrol.components.setup.components.manifest import Manifest
from patrol.components.setup.components.secrets import private_dirs
from patrol.components.setup.index import Setup


def manifest(root,role="worker",merge=False):
    state=Path(root)/"worker-agent"/"host-one"
    return Manifest({"schema_version":1,
      "agent":{"id":"worker-agent","role":role,"host_instance":"host-one"},
      "crewquay":{"base_url":"https://mxrlyy.asia","actor_id":"worker-agent",
        "credential_ref":"secret://crewquay/agent","required_scopes":["tasks"],
        "required":True},
      "github":{"auth_type":"fine_grained_pat","credential_ref":"secret://github/pat",
        "repositories":["fantasywinter207/muse-agent-patrol"],
        "permissions":{"contents":"write","issues":"write","pull_requests":"write"},
        "policy":{"allow_pr_create":True,"allow_pr_merge":merge,
                  "allow_direct_main_push":False},"required":True},
      "runtime":{"state_dir":str(state),"config_path":str(state/"runtime.json"),
        "database":str(state/"patrol.sqlite3"),
        "hook":{"provider":"hatch","interval_seconds":60},"required":True}})


class FakeGitHub:
    def __init__(self,fail_after=False):
        self.calls=[]
        self.fail_after=fail_after
    def request(self,method,path,token,payload=None):
        self.calls.append((method,path))
        if token!="unit_fine_grained_pat":
            raise AssertionError("wrong private fixture credential")
        if (method,path)==("GET","/user"):
            return 200,{"login":"single-github-account"}
        if (method,path)==("GET","/repos/fantasywinter207/muse-agent-patrol"):
            return 200,{"full_name":"fantasywinter207/muse-agent-patrol"}
        if self.fail_after:
            return 0,None
        if method=="POST" and path.endswith("/issues"):
            return 201,{"number":15,"id":15000}
        if method=="GET" and path.endswith("/issues/15"):
            return 200,{"id":15000,"title":"Unit issue"}
        if method=="POST" and path.endswith("/git/refs"):
            return 201,{"ref":"refs/heads/feat/demo","object":{"sha":"a"*40}}
        if method=="GET" and path.endswith("/git/ref/heads/feat/demo"):
            return 200,{"ref":"refs/heads/feat/demo","object":{"sha":"a"*40}}
        if method=="POST" and path.endswith("/pulls"):
            return 201,{"number":22,"id":22000}
        if method=="GET" and path.endswith("/pulls/22"):
            return 200,{"id":22000,"title":"Unit PR",
                        "head":{"ref":"feat/demo"},"base":{"ref":"main"}}
        if method=="PUT" and path.endswith("/pulls/22/merge"):
            return 200,{"merged":True,"sha":"b"*40}
        return 0,None


@unittest.skipUnless(os.name=="posix","Windows credential read requires verified native ACL adapter")
class GitHubOperationsTests(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory()
        self.addCleanup(t.cleanup)
        self.manifest=manifest(t.name)
        Setup(self.manifest).init()
        folder=private_dirs(self.manifest.state_dir)
        secret=folder/"github_pat"
        secret.write_text("unit_fine_grained_pat",encoding="utf-8")
        secret.chmod(0o600)
        self.api=FakeGitHub()
        self.ops=GitHubOperations(self.manifest,transport=self.api,clock=lambda:1010)
        self.repo="fantasywinter207/muse-agent-patrol"

    def test_issue_exact_readback_and_repeat_no_post(self):
        first=self.ops.action("logical.issue.0001",self.repo,"create_issue",
                              {"title":"Unit issue","body":"Safe test"})
        self.assertEqual(first["status"],"confirmed")
        self.assertEqual(first["remote_id"],"15")
        second=self.ops.action("logical.issue.0001",self.repo,"create_issue",
                               {"title":"Unit issue","body":"Safe test"})
        self.assertTrue(second["reused"])
        self.assertFalse(second["new_request_sent"])
        self.assertEqual(sum(1 for x in self.api.calls if x[0]=="POST"),1)
        self.assertNotIn(b"unit_fine_grained_pat",
                         (self.manifest.state_dir/"github-actions.sqlite3").read_bytes())

    def test_unknown_remote_response_must_not_post_twice(self):
        self.api.fail_after=True
        first=self.ops.action("logical.issue.0002",self.repo,"create_issue",
                              {"title":"Unit issue","body":"Safe test"})
        self.assertEqual(first["status"],"unknown")
        self.assertTrue(first["requires_remote_reconciliation"])
        self.api.fail_after=False
        again=self.ops.action("logical.issue.0002",self.repo,"create_issue",
                              {"title":"Unit issue","body":"Safe test"})
        self.assertTrue(again["reused"])
        self.assertEqual(again["status"],"unknown")
        self.assertEqual(sum(1 for x in self.api.calls if x[0]=="POST"),1)

    def test_branch_and_pr_policy_and_readback(self):
        created=self.ops.action("logical.branch.001",self.repo,"create_branch",
                                {"branch":"feat/demo","sha":"a"*40})
        self.assertEqual(created["status"],"confirmed")
        pr=self.ops.action("logical.pull.0001",self.repo,"create_pull",
                           {"head":"feat/demo","base":"main",
                            "title":"Unit PR","body":"Tested"})
        self.assertEqual(pr["status"],"confirmed")
        with self.assertRaises(ValueError):
            self.ops.action("logical.branch.002",self.repo,"create_branch",
                            {"branch":"main","sha":"a"*40})
        with self.assertRaises(ValueError):
            self.ops.action("logical.pull.0002",self.repo,"merge_pull",
                            {"number":22,"expected_head_sha":"a"*40})
        self.assertFalse(any(method=="PUT" for method,path in self.api.calls))

    def test_owner_deny_blocks_existing_token_without_remote_write(self):
        Setup(self.manifest).deny("github_credential")
        with self.assertRaises(ValueError):
            self.ops.action("logical.issue.0003",self.repo,"create_issue",
                            {"title":"Unit issue","body":"No rights"})
        self.assertEqual(self.api.calls,[])

    def test_planner_can_request_merge_only_when_remote_readback_proves_it(self):
        m=manifest(self.manifest.state_dir.parents[1],role="planner",merge=True)
        # The declared host path is the same only inside this isolated fixture.
        api=FakeGitHub()
        # Changing role is not changing GitHub account identity.
        ops=GitHubOperations(m,transport=api)
        result=ops.action("logical.merge.0001",self.repo,"merge_pull",
                          {"number":22,"expected_head_sha":"a"*40})
        self.assertEqual(result["status"],"unknown")
        self.assertTrue(result["requires_remote_reconciliation"])


if __name__=="__main__":
    unittest.main()
