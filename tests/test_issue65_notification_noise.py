"""Issue #65: deterministic Agent Event routing; never erase real decisions."""
import json
import tempfile
import unittest
from pathlib import Path

from patrol.components.collection.index import Sync
from patrol.components.config.index import Config
from patrol.components.persistence.index import Database
from patrol.components.runtime.index import Tick


class NotificationRoutingTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.db = Database(Path(directory.name) / "events.sqlite3")
        with self.db.transaction() as c:
            c.execute("INSERT INTO meta(key,value) VALUES('actor','muse')")
        self.now = 1000
        self.tick = Tick(self.db, Config(), clock=lambda: self.now,
                         monotonic=lambda: 0)
        self.tick.actor = "muse"
        self.tick.run_id = "test-issue65-tick"
        self.tick.request_deadline = 1
        self.sync = Sync(self.db)
        self.next_cursor = 0

    def feed(self, kinds):
        items = []
        for n, (kind, resource) in enumerate(kinds):
            self.next_cursor += 1
            items.append({
                "cursor": str(self.next_cursor),
                "eventId": "e-" + str(self.next_cursor),
                "type": kind,
                "resourceId": resource,
                "createdAt": "1970-01-01T00:16:40Z",
            })
        self.sync.page("agent_events", "muse", "stream-1", items,
                       str(self.next_cursor), self.now)

    def pending(self):
        return self.db.read(
            "SELECT id,payload,event_id,status FROM work_items "
            "WHERE type='human_action' ORDER BY created_at,id")

    def test_metadata_only_events_stay_audited_but_not_model_work(self):
        self.feed([
            ("chat.read", "chat_1"), ("room.read", "room_1"),
            ("community.read", "community_hub"),
            ("leaderboard.updated", "leaderboards:1"),
            ("grants.changed", "auth_1"),
            ("task.ready", "tsk_real"),
            ("bug.created", "bug_real"),
        ])
        self.assertEqual(self.tick.execute(25), 7)
        works = [json.loads(r["payload"])["event_type"] for r in self.pending()]
        self.assertCountEqual(works, ["task.ready", "bug.created"])
        self.assertEqual(self.db.read(
            "SELECT value FROM meta WHERE key='authorization_sync_pending'")[0]["value"], "1")
        self.assertEqual(self.db.read(
            "SELECT COUNT(*) AS n FROM events WHERE source='agent_events'")[0]["n"], 7)

    def test_only_same_recent_pending_ready_is_coalesced(self):
        self.feed([("task.ready", "tsk_1"), ("task.ready", "tsk_1"),
                   ("task.ready", "tsk_2"), ("plan.published", "plan_1")])
        self.assertEqual(self.tick.execute(25), 4)
        works = self.pending()
        self.assertEqual(len(works), 3)
        self.assertEqual(
            sorted(json.loads(r["payload"])["event_type"] for r in works),
            ["plan.published", "task.ready", "task.ready"])
        evidence = self.db.read(
            "SELECT evidence FROM work_items WHERE type='inspect_event'")
        self.assertTrue(any("agent_event_coalesced_pending_task_ready" in
                            (r["evidence"] or "") for r in evidence))

        # Once the original decision is running, another signal must not be
        # silently merged into its already-started, possibly stale execution.
        with self.db.transaction() as c:
            c.execute("UPDATE work_items SET status='running' WHERE id=?",
                      (next(r["id"] for r in works if
                            json.loads(r["payload"])["event_type"]=="task.ready"),))
        self.feed([("task.ready", "tsk_1")])
        self.assertEqual(self.tick.execute(25), 1)
        self.assertEqual(len(self.pending()), 4)

    def test_unknown_event_not_silently_discarded(self):
        self.feed([("custom.unknown", "unknown_1")])
        self.tick.execute(10)
        self.assertEqual(
            json.loads(self.pending()[0]["payload"])["event_type"], "custom.unknown")


if __name__ == "__main__":
    unittest.main()
