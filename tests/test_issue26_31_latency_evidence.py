"""#26/#31: local-only stalled Worker evidence and end-to-end latency accounting."""
import contextlib
import sqlite3
import tempfile
import unittest
from pathlib import Path

from patrol.components.host.index import HostHandoff, MuseConsumer
from patrol.components.outbox.index import Outbox
from patrol.components.persistence.index import Database
from patrol.components.persistence.storage.engine import SCHEMA
from patrol.components.queue.index import Queue
from patrol.components.runtime.composition.latency import LatencyReport
from patrol.components.wakeup.index import Wakeups


class WorkerEvidenceTests(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory()
        self.addCleanup(t.cleanup)
        self.db=Database(Path(t.name)/"evidence.sqlite3")
        with self.db.transaction() as c:
            c.execute("INSERT INTO meta(key,value) VALUES('actor','muse')")
        self.queue=Queue(self.db)
        self.wakes=Wakeups(self.db)
        self.host=HostHandoff(self.db)
        self.consumer=MuseConsumer(self.db)

    def begin(self,key="owner",at=1000):
        wid=self.queue.enqueue(key,"human_action",{"reason":"test"},at,priority=95)
        self.wakes.stage(at)
        notice=self.host.prepare("native-hatch",at,capability="best_effort")
        self.assertEqual(notice["source_id"],wid)
        started=self.consumer.consume(notice["wakeup_id"],
            notice["notification_generation"],"worker",at+2)
        self.assertEqual(started["status"],"started")
        return wid,notice,started

    def test_stalled_best_effort_alert_does_not_claim_host_ack_or_retry(self):
        wid,notice,started=self.begin()
        health=self.wakes.execution_health(1400)
        self.assertEqual(health["best_effort_started_stalled"],1)
        self.assertEqual(health["new_alerts"],1)
        self.assertEqual(self.wakes.execution_health(1401)["new_alerts"],0)
        self.assertEqual(self.db.read(
            "SELECT status FROM wakeup_requests WHERE id=?",
            (notice["wakeup_id"],))[0]["status"],"pending")
        self.assertEqual(self.db.read("SELECT count(*) n FROM outbox")[0]["n"],0)
        self.assertIsNone(self.db.read(
            "SELECT confirmed_at FROM wakeup_requests WHERE id=?",
            (notice["wakeup_id"],))[0]["confirmed_at"])

    def test_fenced_worker_heartbeat_suppresses_false_stalled_alarm(self):
        wid,notice,started=self.begin()
        renew=lambda at,seq: self.consumer.renew_liveness(
            notice["wakeup_id"],notice["notification_generation"],"worker",
            started["scheduler_activity_id"],started["scheduler_generation"],
            started["work_lease_version"],at,
            {"observation_seq":seq,"host_session":"native-hatch"})
        renew(1090,1)
        renew(1180,2)
        report=self.wakes.execution_health(1400)
        self.assertEqual(report["best_effort_started_stalled"],0)
        self.assertEqual(self.wakes.execution_health(1500)["best_effort_started_stalled"],1)

    def test_report_only_uses_durable_local_confirmation(self):
        wid,notice,started=self.begin()
        metrics=LatencyReport(self.db).snapshot(1003)
        self.assertEqual(metrics["metrics"]["work_to_worker"]["samples"],1)
        self.assertEqual(metrics["metrics"]["work_to_worker"]["p50_seconds"],2)
        self.assertIsNone(metrics["metrics"]["work_to_verified"]["p50_seconds"])
        out=Outbox(self.db).stage("reply-1","muse","send","chat-owner",
            {"content":"ack"},1010,schedule_context={
              "id":started["scheduler_activity_id"],"owner":"worker",
              "generation":started["scheduler_generation"]})
        with self.db.transaction() as c:
            c.execute("UPDATE outbox SET status='unknown' WHERE id=?",(out,))
        Outbox(self.db)._confirmed(out,{"id":"real-remote-fixture"},verified_at=1050)
        snapshot=LatencyReport(self.db).snapshot(1051)
        self.assertEqual(snapshot["metrics"]["outbox_to_verified"]["p50_seconds"],40)
        self.assertEqual(snapshot["metrics"]["work_to_verified"]["p95_seconds"],50)
        self.assertIsNone(snapshot["metrics"]["message_to_verified"]["p50_seconds"])
        self.assertEqual(snapshot["work_sample_size"],1)
        self.assertFalse(snapshot["side_effects_executed"])
        self.assertTrue(snapshot["samples"][0]["fully_verified_with_local_timestamp"])

    def test_historical_confirmed_without_verified_time_is_not_backfilled(self):
        t=tempfile.TemporaryDirectory()
        self.addCleanup(t.cleanup)
        path=Path(t.name)/"legacy.sqlite"
        old=SCHEMA.replace(" verify_cursor TEXT NOT NULL DEFAULT '0', verified_at REAL);",
                           " verify_cursor TEXT NOT NULL DEFAULT '0');")
        with contextlib.closing(sqlite3.connect(str(path))) as c:
            c.executescript(old)
            c.execute("""INSERT INTO outbox(id,actor,action,target,logical_key,idem_key,
                payload,payload_hash,status,created_at,next_attempt_at)
                VALUES('legacy-1','muse','send','c','once','idem','{}','hash','confirmed',1000,1000)""")
            c.commit()
        migrated=Database(path)
        cols=[x["name"] for x in migrated.read("PRAGMA table_info(outbox)")]
        self.assertIn("verified_at",cols)
        self.assertIsNone(migrated.read(
            "SELECT verified_at FROM outbox WHERE id='legacy-1'")[0]["verified_at"])
        self.assertEqual(migrated.read("SELECT status FROM outbox WHERE id='legacy-1'")[0]["status"],
                         "confirmed")


if __name__=="__main__":
    unittest.main()
