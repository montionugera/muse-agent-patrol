"""Phase 6 host handoff: fake host only, real temporary SQLite.

No CrewQuay API requests, credentials, actual HATCH, or model invocation.
"""
import contextlib
import io
import json
import os
import shutil
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

from patrol.components.host.index import HostHandoff
from patrol.components.persistence.index import Database, StaleLease
from patrol.components.queue.index import Queue
from patrol.components.runtime.index import main
from patrol.components.wakeup.index import Wakeups
from patrol.components.scheduling.index import Scheduler


class HostHandoffTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.db=Database(Path(tmp.name)/"host.sqlite3")
        self.queue=Queue(self.db)
        self.wakeups=Wakeups(self.db,lease_seconds=120)
        self.host=HostHandoff(self.db,lease_seconds=120)

    def work(self,key="owner-request",priority=90,now=1000):
        return self.queue.enqueue(key,"human_action",
                                  {"reason":"test operator decision"},now,
                                  priority=priority)

    def test_reservation_is_leased_and_does_not_claim_muse_activity(self):
        ident=self.work()
        self.assertEqual(self.wakeups.stage(1000),1)
        first=self.host.prepare("hatch-a",1000)
        self.assertTrue(first["should_wake"])
        self.assertFalse(first["host_called"])
        self.assertFalse(first["activity_dispatched"])
        self.assertTrue(first["requires_host_receipt"])
        self.assertEqual(first["source_id"],ident)
        self.assertEqual(first["source_type"],"work")
        self.assertEqual(first["lease_version"],1)
        self.assertEqual(first["scheduler_advice"],"ready_to_dispatch")
        self.assertTrue(first["candidate_activity_id"])
        # Repeated or overlapping Hook turns cannot reserve a second wake.
        second=self.host.prepare("hatch-b",1001)
        self.assertFalse(second["should_wake"])
        self.assertEqual(second["status"],"idle")
        self.assertEqual(self.db.read(
            "SELECT count(*) AS n FROM wakeup_requests")[0]["n"],1)
        self.assertEqual(self.db.read(
            "SELECT count(*) AS n FROM schedule_activities "
            "WHERE status='running'")[0]["n"],0)
        # Actual Muse is responsible for claiming the local activity slice.
        granted=Scheduler(self.db).dispatch("muse-worker",1002)
        self.assertTrue(granted["granted"])
        self.assertEqual(granted["activity"]["id"],first["candidate_activity_id"])
        self.assertEqual(granted["activity"]["work_id"],ident)

    def test_verified_host_ack_does_not_mean_work_complete(self):
        ident=self.work()
        self.wakeups.stage(1000)
        first=self.host.prepare("hatch",1000)
        accepted=self.wakeups.acknowledge(first["wakeup_id"],"hatch",
                      first["lease_version"],1001,"accepted",
                      {"host_receipt":"test-host-returned-acceptance"})
        self.assertEqual(accepted["status"],"accepted")
        self.assertFalse(accepted["business_work_completed"])
        self.assertFalse(self.host.prepare("hatch",1002)["should_wake"])
        self.assertEqual(self.db.read("SELECT status FROM work_items WHERE id=?",
                                      (ident,))[0]["status"],"pending")

    def test_unacknowledged_wake_becomes_unknown_not_duplicate(self):
        self.work()
        self.wakeups.stage(1000)
        first=self.host.prepare("hatch",1000)
        self.assertEqual(self.wakeups.recover(1121,detail=True),
                         {"unknown":1,"obsolete":0})
        self.assertFalse(self.host.prepare("hatch",1122)["should_wake"])
        with self.assertRaises(StaleLease):
            self.wakeups.claim(first["wakeup_id"],"hatch",1122)
        self.wakeups.reconcile_unknown(first["wakeup_id"],1123,True,
                                       {"test_host_proven_absent":True})
        retried=self.host.prepare("hatch",1124)
        self.assertTrue(retried["should_wake"])
        self.assertEqual(retried["wakeup_id"],first["wakeup_id"])
        self.assertEqual(retried["lease_version"],2)

    def test_obsolete_source_retired_and_next_live_request_reserved(self):
        old=self.work("no-longer-needed",priority=100)
        live=self.work("still-needed",priority=90)
        self.wakeups.stage(1000)
        with self.db.transaction() as c:
            c.execute("UPDATE work_items SET status='done' WHERE id=?",(old,))
        picked=self.host.prepare("hatch",1001)
        self.assertEqual(picked["source_id"],live)
        self.assertEqual(self.db.read(
            "SELECT status FROM wakeup_requests WHERE source_id=?",
            (old,))[0]["status"],"obsolete")

    def test_future_retry_intent_is_not_reserved_early(self):
        ident=self.work()
        self.wakeups.stage(1000)
        with self.db.transaction() as c:
            c.execute("""UPDATE work_items SET status='retry_wait',
                         next_action_at=2000 WHERE id=?""",(ident,))
        self.assertFalse(self.host.prepare("hatch",1001)["should_wake"])
        resumed=self.host.prepare("hatch",2001)
        self.assertTrue(resumed["should_wake"])
        self.assertEqual(resumed["source_id"],ident)

    def test_hook_script_has_valid_shell_syntax_and_fenced_order(self):
        source=Path(__file__).resolve().parents[1]/"tools"/"crewquay_phase3_hook.sh"
        script=source.read_text(encoding="utf-8")
        self.assertIn("host-wakeup-prepare",script)
        self.assertIn("outbox-drain --once --lane any",script)
        self.assertLess(script.index("host-wakeup-prepare"),
                        script.index('wake "CrewQuay Muse work ready'))
        self.assertIn("requires_host_receipt",Path(__file__).resolve().parents[1].joinpath(
            "patrol/components/host/components/bridge.py").read_text(encoding="utf-8"))
        self.assertNotIn('wakeup-ack --result accepted',script)
        # Windows subprocess hands a native D:\\... path to Git Bash, which
        # is not a valid Bash pathname. Validate the POSIX-only Hook on POSIX.
        # Portable static invariants above still run on every platform.
        if os.name=="posix" and shutil.which("bash"):
            checked=subprocess.run(["bash","-n",str(source)],capture_output=True,text=True)
            self.assertEqual(checked.returncode,0,checked.stderr)

    def test_empty_host_prepare_remains_local_without_extra_activity(self):
        report=self.host.prepare("hatch",1000)
        self.assertEqual(report["status"],"idle")
        self.assertFalse(report["should_wake"])
        self.assertEqual(self.db.read(
            "SELECT count(*) AS n FROM schedule_activities")[0]["n"],0)

    def test_cli_host_prepare_prints_minimal_host_safe_json(self):
        now=time.time()
        self.work("cli-work",now=now)
        self.wakeups.stage(now)
        buf=io.StringIO()
        with contextlib.redirect_stdout(buf):
            result=main(["--db",str(self.db.path),"host-wakeup-prepare",
                         "--owner","cli-host"])
        self.assertEqual(result,0)
        data=json.loads(buf.getvalue())
        self.assertTrue(data["should_wake"])
        self.assertEqual(data["lease_owner"],"cli-host")
        self.assertNotIn("payload",data)
        self.assertNotIn("checkpoint",data)
        self.assertFalse(data["host_called"])
        with contextlib.redirect_stdout(io.StringIO()) as output:
            result=main(["--db",str(self.db.path),"host-wakeup-prepare",
                         "--owner","cli-host"])
        self.assertEqual(result,0)
        self.assertFalse(json.loads(output.getvalue())["should_wake"])


if __name__=="__main__":
    unittest.main()
