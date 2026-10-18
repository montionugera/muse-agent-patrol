"""Typed platform task actions use real identity gates and exact fake REST evidence."""
import tempfile
import base64
import hashlib
import unittest
from pathlib import Path

from patrol.components.config.index import Config
from patrol.components.executor.index import ActionSender
from patrol.components.outbox.index import Outbox
from patrol.components.persistence.index import Database
from patrol.components.transport.index import Response


class TaskAPI:
    def __init__(self):
        self.calls=[]
        self.task={"id":"task42","projectId":"proj","status":"ready",
                   "version":1,"assigneeId":None,"assignmentId":None,
                   "progress":""}
        self.submissions=[]
        self.reviews=[]
        self.decisions=[]
        self.artifacts=[]

    def request(self,method,path,payload=None,idem_key=None):
        self.calls.append((method,path,payload,idem_key))
        if method=="GET" and path=="/api/v1/agents/me":
            return Response(200,{"id":"actor"})
        if method=="GET" and path.startswith("/api/v1/agent/runtime-manifest"):
            return Response(404,{})  # Verified older platform only.
        if method=="GET" and path=="/api/v1/tasks/task42":
            return Response(200,self.task.copy())
        if method=="GET" and path=="/api/v1/tasks/task42/submissions":
            return Response(200,self.submissions[:])
        if method=="GET" and path=="/api/v1/tasks/task42/reviews":
            return Response(200,self.reviews[:])
        if method=="GET" and path=="/api/v1/artifacts/art1/metadata":
            return Response(200,self.artifacts[0] if self.artifacts else {})
        if method=="GET" and path=="/api/v1/projects/proj/decisions":
            return Response(200,self.decisions[:])
        if method=="POST" and path=="/api/v1/tasks/task42/claim":
            self.task.update(status="in_progress",assigneeId="actor",
                             assignmentId="asg1",version=self.task["version"]+1)
            return Response(200,self.task.copy())
        if method=="POST" and path=="/api/v1/tasks/task42/progress":
            self.task.update(progress=payload["progress"],version=self.task["version"]+1)
            return Response(200,self.task.copy())
        if method=="POST" and path=="/api/v1/tasks/task42/submissions":
            submitted={"id":"sub1","taskId":"task42","agentId":"actor",
                       "summary":payload["summary"],"notes":payload["notes"],
                       "artifacts":payload["artifacts"]}
            self.submissions.append(submitted)
            self.task["status"]="submitted"
            return Response(201,submitted)
        if method=="POST" and path=="/api/v1/submissions/sub1/reviews":
            review={"id":"rev1","submissionId":"sub1","reviewerId":"actor",**payload}
            self.reviews.append(review)
            self.task["status"]="completed"
            return Response(201,review)
        if method=="POST" and path=="/api/v1/projects/proj/artifacts":
            artifact={"id":"art1","projectId":"proj","actorId":"actor",
                "sha256":hashlib.sha256(base64.b64decode(payload["contentBase64"])).hexdigest()}
            self.artifacts.append(artifact)
            return Response(201,artifact)
        if method=="POST" and path=="/api/v1/projects/proj/decisions":
            decision={"id":"dec1","actorId":"actor","content":payload["content"]}
            self.decisions.append(decision)
            return Response(201,decision)
        raise AssertionError("unexpected API "+method+" "+path)


class PlatformTaskActions(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.db=Database(Path(temp.name)/"tasks.sqlite3")
        with self.db.transaction() as c:
            c.execute("INSERT INTO meta(key,value) VALUES('actor','actor')")
        self.cfg=Config(database=str(self.db.path),allow_writes=True,
            allowed_project_action_ids=["proj"],
            allowed_task_actions=["task_claim","task_progress","task_submit",
                                  "submission_review","project_decision","artifact_upload"])
        self.api=TaskAPI()
        self.outbox=Outbox(self.db)

    def stage(self,logical,action,target,payload):
        return self.outbox.stage(logical,"actor",action,target,payload,1000,
                                 producer_id="muse-planner")

    def drain(self,ident):
        return ActionSender(self.db,self.cfg,self.api,clock=lambda:1000).drain_once(
            limit=1,action_id=ident,lane="tasks")

    def assert_confirmed(self,report):
        self.assertEqual(report["item_status"],"confirmed",report)
        ident=report["outbox_attempts"][0]["id"]
        row=self.db.read("SELECT status,remote_id,result,attempts FROM outbox WHERE id=?",(ident,))[0]
        self.assertEqual(row["status"],"confirmed")
        self.assertIn("sha256",row["result"])
        self.assertEqual(row["attempts"],1)

    def test_claim_and_progress_are_version_readback_confirmed(self):
        a=self.stage("claim42","task_claim","task42",{"expectedVersion":1})
        self.assert_confirmed(self.drain(a))
        self.assertEqual(self.api.task["assignmentId"],"asg1")
        self.assertEqual(self.api.task["version"],2)
        b=self.stage("progress42","task_progress","task42",
                     {"expectedVersion":2,"progress":"half complete"})
        self.assert_confirmed(self.drain(b))
        self.assertEqual(self.api.task["version"],3)
        posts=[x for x in self.api.calls if x[0]=="POST"]
        self.assertEqual(len(posts),2)
        self.drain(b)
        self.assertEqual(len([x for x in self.api.calls if x[0]=="POST"]),2)

    def test_submission_and_review_require_exact_remote_id(self):
        self.api.task.update(status="in_progress",assigneeId="actor",
                             assignmentId="asg1",version=3)
        sub=self.stage("submit42","task_submit","task42",{
            "assignmentId":"asg1","baseTaskVersion":3,
            "summary":"verified test","notes":"test only","artifacts":[]})
        self.assert_confirmed(self.drain(sub))
        self.assertEqual(self.api.submissions[0]["id"],"sub1")
        review=self.stage("review42","submission_review","sub1",{
            "taskId":"task42","decision":"approve","feedback":"ready",
            "requiredChanges":[]})
        self.assert_confirmed(self.drain(review))
        post=[x for x in self.api.calls if x[0]=="POST" and
              x[1]=="/api/v1/submissions/sub1/reviews"][0]
        self.assertNotIn("taskId",post[2])
        self.assertEqual(self.api.reviews[0]["submissionId"],"sub1")

    def test_formal_project_decision_has_exact_readback(self):
        ident=self.stage("decision42","project_decision","proj",{
            "expectedPlannerEpoch":1,"content":"Approved scope"})
        self.assert_confirmed(self.drain(ident))
        self.assertEqual(len(self.api.decisions),1)

    def test_artifact_upload_has_exact_remote_digest_proof(self):
        ident=self.stage("artifact42","artifact_upload","proj",{
            "filename":"report.txt","mediaType":"text/plain",
            "contentBase64":base64.b64encode(b"Verified artifact").decode()})
        self.assert_confirmed(self.drain(ident))
        self.assertEqual(self.api.artifacts[0]["sha256"],
                         hashlib.sha256(b"Verified artifact").hexdigest())

    def test_mutations_default_deny_even_when_chat_writes_enabled(self):
        ident=self.stage("not-granted","task_claim","task42",{"expectedVersion":1})
        cfg=Config(database=str(self.db.path),allow_writes=True,
                   allowed_chat_ids=["owner"],project_ids=["proj"])
        report=ActionSender(self.db,cfg,self.api,clock=lambda:1000).drain_once(
            action_id=ident,lane="tasks")
        self.assertEqual(report["blocked"],1,report)
        self.assertFalse(any(x[0]=="POST" for x in self.api.calls))

    def test_unknown_review_without_remote_id_is_not_text_matched(self):
        self.api.submissions=[{"id":"sub1","taskId":"task42","agentId":"worker"}]
        ident=self.stage("unknown-review","submission_review","sub1",{
            "taskId":"task42","decision":"approve","feedback":"approved",
            "requiredChanges":[]})
        with self.db.transaction() as c:
            c.execute("UPDATE outbox SET status='unknown' WHERE id=?",(ident,))
        report=self.drain(ident)
        self.assertEqual(report["item_status"],"unknown",report)
        self.assertFalse(any(x[0]=="POST" for x in self.api.calls))

    def test_payload_shapes_are_restricted(self):
        for action,target,payload in [
            ("task_claim","task42",{"expectedVersion":True}),
            ("task_progress","task42",{"expectedVersion":1,"progress":""}),
            ("task_submit","task42",{"assignmentId":"asg1","baseTaskVersion":2,"summary":"x","notes":"","artifacts":[{"artifactId":"a","sha256":"oops"}]}),
            ("submission_review","sub1",{"taskId":"task42","decision":"approve","feedback":"","requiredChanges":[]}),
            ("project_decision","proj",{"expectedPlannerEpoch":0,"content":"x"}),
        ]:
            with self.subTest(action=action):
                with self.assertRaises(ValueError):
                    self.stage("invalid-"+action,action,target,payload)


if __name__=="__main__":
    unittest.main()
