"""Bounded composition of the *existing* Patrol operations for one native Hook turn.

No second scheduler, socket, timer or host SDK. Subprocess boundaries release
CLI SQLite locks between phases, while a single monotonic deadline bounds the
entire turn. The model Worker owns its execution, not this coordinator.
"""
import json
import subprocess
import sys
import time


class RuntimeCycle:
    def __init__(self, config_path, db_path, *, runner=subprocess.run,
                 monotonic=time.monotonic, total_seconds=47):
        if not 20<=total_seconds<=55:
            raise ValueError("runtime-cycle total budget must be 20..55 seconds")
        self.config_path=config_path
        self.db_path=db_path
        self.runner=runner
        self.monotonic=monotonic
        self.total_seconds=total_seconds

    def run(self, owner, host_capability="best_effort"):
        if not isinstance(owner,str) or not 1<=len(owner)<=128:
            raise ValueError("stable Hook identity required")
        if host_capability not in ("unavailable","best_effort","reliable_ack"):
            raise ValueError("unsupported Host capability")
        deadline=self.monotonic()+self.total_seconds
        base=[sys.executable,"-m","patrol"]
        if self.config_path:
            base+=["--config",self.config_path]
        base+=["--db",self.db_path]
        phases=[]
        failures=[]
        def invoke(name, args, desired_seconds, reserve=0):
            remaining=deadline-self.monotonic()
            seconds=min(desired_seconds,remaining-reserve)
            if seconds<=0.5:
                phases.append({"name":name,"status":"skipped_deadline",
                               "elapsed_seconds":0})
                return None
            started=self.monotonic()
            status="failed"
            result=None
            try:
                completed=self.runner(base+args,capture_output=True,text=True,
                                      timeout=seconds,check=False)
                if completed.returncode==0:
                    obj=json.loads(completed.stdout)
                    if isinstance(obj,dict):
                        result=obj
                        status=obj.get("status","ok")
                    else:
                        status="invalid_result"
                else:
                    status="command_failed"
            except subprocess.TimeoutExpired:
                status="timeout_unknown"
            except (ValueError,TypeError,KeyError,OSError):
                status="invocation_failed"
            elapsed=max(0,self.monotonic()-started)
            phases.append({"name":name,"status":status,
                           "elapsed_seconds":round(elapsed,3),
                           "budget_seconds":round(seconds,3)})
            if result is None:
                failures.append(name+":"+status)
            return result

        # Collection is bounded first. Reserve time for genuine Wakeup advice,
        # then independent Outbox readback; optional social can never consume
        # the work/wake budget.
        tick=invoke("light_tick",["tick","--light"],18,reserve=16)
        advice=invoke("host_advice",
                      ["host-wakeup-prepare","--owner",owner,
                       "--host-capability",host_capability],6,reserve=10)
        if not isinstance(advice,dict):
            advice={"should_wake":False,"status":"advice_unavailable",
                    "host_called":False,"host_capability":host_capability}
        # Never map Wakeup advice to Host accepted. The native Hook alone
        # invokes wake() and any reliable receipt must come from its adapter.
        allowed=("notification_intent","reserved")
        if advice.get("status") not in allowed or advice.get("should_wake") is not True:
            advice["should_wake"]=False

        outbox=invoke("outbox_reconcile",
                      ["outbox-drain","--once","--lane","any",
                       "--limit","3"],10,reserve=0)
        social=None
        if not advice["should_wake"]:
            # Existing SQLite idle limiter inside social-sync remains the
            # source of truth. This phase is always optional and read-only.
            source="circles" if int(time.time()//1800)%2 else "ranked"
            args=["social-sync","--if-idle-due","--source",source,
                  "--pages","2" if source=="circles" else "1",
                  "--limit","10","--max-new","3"]
            social=invoke("idle_social_discovery",args,4,reserve=0)
        else:
            phases.append({"name":"idle_social_discovery","status":"deferred_for_work",
                           "elapsed_seconds":0})
        return {"status":"degraded" if failures else "ok",
                "host":advice,
                "phases":phases,
                "failed_phases":failures,
                "remaining_seconds":round(max(0,deadline-self.monotonic()),3),
                "deployment_started":False,
                "host_receipt_fabricated":False,
                "model_invoked":False,
                "remote_receipts_inferred":False}
