"""Extra independent safety checks; no credentials or real network."""
import json
import tempfile
import unittest
from pathlib import Path
from patrol.components.persistence.index import Database, StaleLease
from patrol.components.collection.index import Sync, Collector
from patrol.components.config.index import Config
from patrol.components.outbox.index import Outbox
from patrol.components.queue.index import Queue
from patrol.components.runtime.index import Tick
from patrol.components.transport.index import Response, BudgetExceeded
from patrol.components.migration.index import migrate_legacy
from patrol.components.promises.index import Promises

class SafetyChecks(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.root=Path(self.tmp.name)
        self.db=Database(self.root/'d.sqlite3'); self.cfg=Config(database=str(self.db.path),max_pages_per_source=1)
    def tearDown(self): self.tmp.cleanup()

    def test_failed_resync_does_not_rebuild_checkpoint(self):
        Sync(self.db).reset('old','8',90)
        class Client:
            secret=staticmethod(lambda: (_ for _ in ()).throw(AssertionError('secret forbidden')))
            def request(self,m,p,*a):
                if p=='/api/v1/agents/me':return Response(200,{'id':'actor'})
                if p=='/api/v1/events?limit=1':return Response(200,{'streamId':'new','waterline':'10'})
                if p.startswith('/api/v1/inbox?'):return Response(503,{})
                return Response(200,{'items':[],'nextCursor':'0','hasMore':False})
        r=Tick(self.db,self.cfg,Client(),clock=lambda:100).run()
        state=Sync(self.db).state('events','global')
        self.assertEqual((state['stream'],state['received']),('old','8'))
        self.assertEqual(r['resync'],'incomplete; prior cursor retained')
        self.assertEqual(r['status'],'degraded')

    def test_budget_cannot_advance_unfetched_event_page(self):
        sync=Sync(self.db);sync.reset('s','8',90)
        def get(*args):raise BudgetExceeded()
        self.assertEqual(Collector(sync,get,self.cfg).events('10','s',100),'incomplete')
        self.assertEqual(sync.state('events','global')['received'],'8')

    def test_one_page_budget_preserves_fixed_target_and_continuation(self):
        sync=Sync(self.db);sync.reset('s','8',90)
        def get(*args):return Response(200,{'items':[{'eventId':'e9','cursor':'9'}],'nextCursor':'9','hasMore':True})
        self.assertEqual(Collector(sync,get,self.cfg).events('10','s',100),'incomplete')
        state=sync.state('events','global')
        self.assertEqual((state['received'],state['target']),('9','10'))

    def test_expired_and_reclaimed_worker_cannot_finish(self):
        q=Queue(self.db,10); ident=q.enqueue('x','inspect_event',{},100)
        old=q.claim('old',100);q.recover(111);new=q.claim('new',111)
        with self.assertRaises(StaleLease):q.finish(old,112,evidence={'old':'x'})
        q.finish(new,112,evidence={'new':'x'})
        self.assertEqual(self.db.read('SELECT status FROM work_items WHERE id=?',(ident,))[0]['status'],'done')

    def test_unknown_recovery_never_sends_again_automatically(self):
        out=Outbox(self.db);ident=out.stage('x','actor','send','chat',{'content':'x'},100)
        with self.db.transaction() as c:c.execute("UPDATE outbox SET status='sending',dispatch_at=100,dispatch_owner='dead' WHERE id=?",(ident,))
        out.recover(300,120);sent=[]
        out.process(300,lambda r:(True,None),lambda r:sent.append(r),lambda r,s:None)
        self.assertEqual(sent,[])
        self.assertEqual(self.db.read('SELECT status FROM outbox WHERE id=?',(ident,))[0]['status'],'unknown')

    def test_ack_denied_until_dependent_chat_is_covered(self):
        cfg=Config(database=str(self.db.path),allow_writes=True,ack_single_enabled=True,ack_chat_map={'chat.message':['chat']})
        sync=Sync(self.db);sync.page('inbox','global','',[{'seq':1,'type':'chat.message'}],'1',100)
        tick=Tick(self.db,cfg);tick.actor='actor'
        row={'actor':'actor','action':'ack','target':'','payload':{'ids':[1]}}
        self.assertFalse(tick.can_send(row)[0])
        sync.page('messages','chat','',[],'10',101,target='incomplete')
        self.assertFalse(tick.can_send(row)[0])
        sync.page('messages','chat','',[],'11',102)
        self.assertTrue(tick.can_send(row)[0])

    def test_repeat_migration_and_due_watchdog_are_idempotent(self):
        old=self.root/'old';old.mkdir();p=old/'state.json'
        p.write_text(json.dumps({'lastProcessedCursor':'42','streamId':'s'}),encoding='utf-8')
        before=p.read_bytes();migrate_legacy(self.db,old,100,stopped=True)
        second=migrate_legacy(self.db,old,101,stopped=True)
        self.assertEqual(second['sources'][0]['status'],'already_imported')
        self.assertEqual(p.read_bytes(),before)
        self.assertEqual(Sync(self.db).state('events','global')['received'],'42')
        promises=Promises(self.db)
        ident=promises.create('w','o','m','x',100,110,120,due_at=130)
        self.assertEqual(len(promises.watchdog(131)),3)
        self.assertEqual(promises.watchdog(132),[])
        self.assertEqual(self.db.read('SELECT due_at FROM promises WHERE id=?',(ident,))[0]['due_at'],130)

if __name__=='__main__':unittest.main(verbosity=2)
