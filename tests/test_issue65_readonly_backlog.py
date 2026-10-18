"""#65: true read-only backlog census on a preexisting SQLite database."""
import json
import tempfile
import unittest
from pathlib import Path

from patrol.components.persistence.index import Database
from patrol.components.queue.index import Queue
from patrol.components.runtime.composition.event_backlog import (
    backlog_report,read_only_backlog_report
)


class BacklogReadOnlyTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.path=Path(temp.name)/"original.sqlite3"
        self.db=Database(self.path)
        self.queue=Queue(self.db)

    def test_separate_human_backlog_and_inspect_notifications_without_changes(self):
        with self.db.transaction() as c:
            c.execute("""INSERT INTO events(source,scope,stream,external_key,payload,observed_at)
                VALUES('agent_events','global','s','evt-one',?,1000)""",
                (json.dumps({"type":"task.ready","resourceId":"tsk_one"}),))
        ev=self.db.read("SELECT id FROM events WHERE external_key='evt-one'")[0]["id"]
        for n in range(37):
            self.queue.enqueue("human-"+str(n),"human_action",
                               {"event_id":ev},1000,priority=70,event_id=ev)
        for n in range(176):
            self.queue.enqueue("inspect-"+str(n),"inspect_event",
                               {"event_id":ev},999,priority=20,event_id=ev)
        before=self.db.read("SELECT id,status,lease_version FROM work_items ORDER BY id")
        report=read_only_backlog_report(str(self.path),1001,limit=100)
        self.assertEqual(report["total_work_by_type_status"]["human_action"]["pending"],37)
        self.assertEqual(report["total_work_by_type_status"]["inspect_event"]["pending"],176)
        self.assertEqual(report["active_rows_examined"],100)
        self.assertTrue(report["truncated"])
        self.assertFalse(report["safe_to_bulk_close"])
        self.assertEqual(report["changes_applied"],0)
        self.assertEqual(self.db.read("SELECT id,status,lease_version FROM work_items ORDER BY id"),before)
        self.assertFalse(Path(str(self.path)+".tick.lock").exists())
        self.assertNotIn("tsk_one",json.dumps(report))
        self.assertNotIn("evt-one",json.dumps(report))

    def test_in_memory_report_is_aggregated_and_never_marks_complete(self):
        self.queue.enqueue("p-one","human_action",{"note":"private"},1000)
        report=backlog_report(self.db,1100,limit=100)
        self.assertEqual(report["status"],"read_only")
        self.assertEqual(report["total_work_by_type_status"]["human_action"]["pending"],1)
        self.assertTrue(any(x["age"]=="under_15m" for x in report["active_age_buckets"]))
        self.assertFalse(report["safe_to_bulk_close"])
        self.assertEqual(self.db.read("SELECT status FROM work_items")[0]["status"],"pending")

    def test_missing_database_and_unbounded_limit_are_rejected(self):
        with self.assertRaises(FileNotFoundError):
            read_only_backlog_report(str(self.path.parent/"missing.db"),1000)
        with self.assertRaises(ValueError):
            read_only_backlog_report(str(self.path),1000,limit=5001)


if __name__=="__main__":
    unittest.main()
