"""#42: parent Runtime bounds phase budgets; fake subprocesses only."""
import json
import subprocess
import unittest
from patrol.components.runtime.composition.cycle import RuntimeCycle


class FakeClock:
    def __init__(self):
        self.t=0
    def __call__(self):
        return self.t


class RuntimeCycleTests(unittest.TestCase):
    def setUp(self):
        self.clock=FakeClock()
        self.calls=[]

    def runner(self,cmd,*,capture_output,text,timeout,check):
        self.assertLessEqual(timeout,50)
        self.assertGreater(timeout,0)
        self.calls.append((cmd,timeout))
        if "host-wakeup-prepare" in cmd:
            reply={"should_wake":True,"status":"notification_intent",
                   "wakeup_id":"existing-1","source_id":"work-1",
                   "lease_version":1,"host_capability":"best_effort"}
        else:
            reply={"status":"ok"}
        self.clock.t+=1
        return subprocess.CompletedProcess(cmd,0,stdout=json.dumps(reply))

    def test_owner_wake_is_advised_before_outbox_and_social_defers(self):
        obj=RuntimeCycle(None,"/tmp/unit.sqlite",runner=self.runner,
                         monotonic=self.clock,total_seconds=40)
        reply=obj.run("hatch",host_capability="best_effort")
        self.assertEqual(reply["status"],"ok")
        self.assertTrue(reply["host"]["should_wake"])
        self.assertFalse(reply["host_receipt_fabricated"])
        self.assertFalse(reply["deployment_started"])
        commands=[" ".join(x[0]) for x in self.calls]
        self.assertEqual(len(commands),3)
        self.assertIn("tick --light",commands[0])
        self.assertIn("host-wakeup-prepare",commands[1])
        self.assertIn("--host-capability best_effort",commands[1])
        self.assertIn("outbox-drain --once",commands[2])
        self.assertFalse(any("deployment-ensure" in x or
                             "platform-auth-bootstrap" in x for x in commands))
        self.assertEqual(reply["phases"][-1]["status"],"deferred_for_work")

    def test_idle_social_only_through_runtime(self):
        def idle(cmd,**kwargs):
            result=self.runner(cmd,**kwargs)
            if "host-wakeup-prepare" in cmd:
                result.stdout=json.dumps({"status":"idle","should_wake":False})
            return result
        obj=RuntimeCycle(None,"/tmp/unit.sqlite",runner=idle,
                         monotonic=self.clock,total_seconds=40)
        reply=obj.run("hatch")
        self.assertFalse(reply["host"]["should_wake"])
        self.assertIn("idle_social_discovery",[x["name"] for x in reply["phases"]])
        self.assertEqual(len(self.calls),4)

    def test_failed_host_advice_never_fakes_wake(self):
        def broken(cmd,**kwargs):
            if "host-wakeup-prepare" in cmd:
                raise subprocess.TimeoutExpired(cmd,kwargs["timeout"])
            return self.runner(cmd,**kwargs)
        obj=RuntimeCycle(None,"/tmp/unit.sqlite",runner=broken,
                         monotonic=self.clock,total_seconds=30)
        reply=obj.run("hatch")
        self.assertEqual(reply["status"],"degraded")
        self.assertFalse(reply["host"]["should_wake"])
        self.assertIn("host_advice:timeout_unknown",reply["failed_phases"])

    def test_deadline_can_skip_social_but_never_overrun(self):
        def slow(cmd,**kwargs):
            self.clock.t+=kwargs["timeout"]
            self.calls.append((cmd,kwargs["timeout"]))
            raise subprocess.TimeoutExpired(cmd,kwargs["timeout"])
        obj=RuntimeCycle(None,"/tmp/unit.sqlite",runner=slow,
                         monotonic=self.clock,total_seconds=20)
        reply=obj.run("hatch")
        self.assertFalse(reply["host"]["should_wake"])
        self.assertLessEqual(self.clock.t,20.001)
        self.assertTrue(any(p["status"]=="skipped_deadline" for p in reply["phases"]))


if __name__=="__main__":
    unittest.main()
