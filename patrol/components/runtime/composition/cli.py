import argparse
import importlib.metadata
import importlib.util
import hashlib
import json
import platform
import subprocess
import sqlite3
import sys
import time
from datetime import datetime
from dataclasses import replace
from pathlib import Path
from ...config.index import load_config
from ...deployment.index import Deployment
from ...host.index import HostHandoff, MuseConsumer
from ...migration.index import migrate_legacy
from ...outbox.index import Outbox
from ...executor.index import ActionSender
from ...persistence.index import Database, Busy
from ...promises.index import Promises
from ...queue.index import Queue, export_pending, MessageScores
from ...scheduling.index import Scheduler
from ...social.index import SocialOpportunities
from ...transport.index import Client, credential_provider
from ...wakeup.index import Wakeups
from ..process.lock import instance_lock
from .scheduler import Tick


def timestamp(value):
    if value is None:
        return None
    try:
        number=float(value)
        if number>=0 and number<float("inf"):
            return number
    except ValueError:
        pass
    date=datetime.fromisoformat(value.replace("Z","+00:00"))
    if date.tzinfo is None:
        raise ValueError("timestamp must include timezone")
    return date.timestamp()


def evidence(path):
    with open(path,encoding="utf-8") as f:
        value=json.load(f)
    if not isinstance(value,dict) or not value:
        raise ValueError("nonempty evidence object required")
    return value


def parser():
    p=argparse.ArgumentParser(description="CrewQuay v3: durable patrol, no model wake-up")
    p.add_argument("--config")
    p.add_argument("--db")
    sub=p.add_subparsers(dest="command",required=True)
    sub.add_parser("init")
    sub.add_parser("doctor")
    sub.add_parser("status")
    sub.add_parser("platform-catalog")
    sub.add_parser("platform-auth-sync")
    a=sub.add_parser("platform-auth-bootstrap")
    a.add_argument("--deployment-run-id",default="initial-deployment")
    a.add_argument("--if-due",action="store_true",
                   help="Host Hook: local throttle; never a second polling schedule")

    a=sub.add_parser("deployment-ensure")
    a.add_argument("--repo-sha",required=True)
    a=sub.add_parser("deployment-start")
    a.add_argument("--repo-sha",required=True,
                   help="actual installed Patrol commit SHA (7-40 lowercase hex)")
    a=sub.add_parser("deployment-status")
    a.add_argument("--id")
    sub.add_parser("deployment-reconcile")
    for phase in ("syn","ack","bug"):
        a=sub.add_parser("deployment-stage-"+phase)
        a.add_argument("id",help="durable deployment run ID, NOT handshake ID")
        a.add_argument("--owner",required=True)
        a.add_argument("--schedule-id",required=True)
        a.add_argument("--schedule-generation",type=int,required=True)
        a.add_argument("--producer-id")
    a=sub.add_parser("social-sync")
    a.add_argument("--limit",type=int,default=10)
    a.add_argument("--max-new",type=int,default=3)
    a.add_argument("--source",choices=("ranked","circles"),default="ranked")
    a.add_argument("--pages",type=int,default=1)
    a.add_argument("--if-idle-due",action="store_true",
                   help="Hook-only: at most one idle discovery per 30 minutes")
    a=sub.add_parser("social-detail")
    a.add_argument("id")
    a=sub.add_parser("platform-events")
    a.add_argument("--after",default="0")
    a.add_argument("--limit",type=int,default=None)
    t=sub.add_parser("tick")
    t.add_argument("--offline",action="store_true")
    t.add_argument("--ws",action="store_true")
    t.add_argument("--quiet",action="store_true")
    t.add_argument("--light",action="store_true")
    m=sub.add_parser("migrate")
    m.add_argument("directory")
    m.add_argument("--confirm-stopped",action="store_true")
    b=sub.add_parser("backup")
    b.add_argument("destination")
    a=sub.add_parser("stage")
    a.add_argument("action",choices=("send","ack","read","project_message","task_thread_reply","room_message",
                                     "task_claim","task_progress","task_submit","submission_review","project_decision","artifact_upload","platform_action"))
    a.add_argument("--logical-key",required=True)
    # --actor is the authenticated CREWQUAY principal, not the Muse worker.
    # Omit it to derive the actor from the previously verified DB binding.
    a.add_argument("--actor")
    a.add_argument("--producer-id")
    a.add_argument("--dispatch-now",action="store_true",
                   help="after durable staging, run a separate bounded Executor process")
    a.add_argument("--target",default="")
    a.add_argument("--payload-file",required=True)
    a.add_argument("--schedule-id",help="live activity id for optional atomic staging fence")
    a.add_argument("--schedule-owner")
    a.add_argument("--schedule-generation",type=int)
    a=sub.add_parser("outbox-drain")
    a.add_argument("--once",action="store_true",required=True)
    a.add_argument("--limit",type=int,default=3)
    a.add_argument("--lane",choices=("any","chat","notifications","collaboration","tasks","platform"),default="any")
    a.add_argument("--id")
    r=sub.add_parser("resolve")
    r.add_argument("id")
    r.add_argument("--evidence",required=True)
    r.add_argument("--confirmed",action="store_true")
    r.add_argument("--proven-absent",action="store_true")
    a=sub.add_parser("promise-add")
    for name in ("actor","recipient","source-message-id","text","start-by","next-action-at"):
        a.add_argument("--"+name,required=True)
    a.add_argument("--due-at")
    a.add_argument("--kind",default="human_action")
    a.add_argument("--work-id")
    a.add_argument("--reason")
    a=sub.add_parser("promise-progress")
    a.add_argument("id")
    a.add_argument("--evidence",required=True)
    a.add_argument("--next-action-at",required=True)
    a=sub.add_parser("promise-complete")
    a.add_argument("id")
    a.add_argument("--evidence",required=True)
    a=sub.add_parser("extension-request")
    a.add_argument("id")
    a.add_argument("--due-at",required=True)
    a.add_argument("--reason",required=True)
    a.add_argument("--evidence",required=True)
    a=sub.add_parser("extension-approve")
    a.add_argument("id")
    a.add_argument("--due-at",required=True)
    a.add_argument("--approved-by",required=True)
    a.add_argument("--evidence",required=True)
    a=sub.add_parser("resume")
    a.add_argument("id")
    a.add_argument("--evidence",required=True)
    a=sub.add_parser("export-pending")
    a.add_argument("--limit",type=int,default=50)
    a=sub.add_parser("score-backfill")
    a.add_argument("--limit",type=int,default=1000)
    a=sub.add_parser("score-export")
    a.add_argument("--limit",type=int,default=50)
    a=sub.add_parser("score-message")
    a.add_argument("event_id",type=int)
    a.add_argument("--score",type=int,required=True)
    a.add_argument("--actor",required=True)
    a.add_argument("--reason",required=True)
    a.add_argument("--evidence",required=True)
    sub.add_parser("autonomy-status")
    a=sub.add_parser("muse-consume")
    a.add_argument("--wakeup-id",required=True)
    a.add_argument("--wakeup-generation",type=int,required=True)
    a.add_argument("--worker",required=True)
    a=sub.add_parser("muse-finish")
    a.add_argument("--wakeup-id",required=True)
    a.add_argument("--wakeup-generation",type=int,required=True)
    a.add_argument("--worker",required=True)
    a.add_argument("--activity-id",required=True)
    a.add_argument("--activity-generation",type=int,required=True)
    a.add_argument("--work-lease-version",type=int,required=True)
    a.add_argument("--evidence",required=True)
    a=sub.add_parser("host-wakeup-prepare")
    # Legacy argument remains accepted, but idle social is already the
    # default. It no longer authorizes a privileged operation.
    a.add_argument("--include-social",action="store_true",help=argparse.SUPPRESS)
    a.add_argument("--owner",required=True,
                   help="approved HATCH Hook identifier, not a platform actor")
    a=sub.add_parser("wakeup-stage")
    a=sub.add_parser("wakeup-export")
    a.add_argument("--limit",type=int,default=50)
    a=sub.add_parser("wakeup-claim")
    a.add_argument("id")
    a.add_argument("--owner",required=True)
    a=sub.add_parser("wakeup-ack")
    a.add_argument("id")
    a.add_argument("--owner",required=True)
    a.add_argument("--lease-version",type=int,required=True)
    a.add_argument("--result",required=True,choices=("accepted","confirmed_failed","unknown"))
    a.add_argument("--evidence",required=True)
    a=sub.add_parser("wakeup-execution")
    a.add_argument("id",help="accepted wakeup request ID")
    a.add_argument("--wakeup-generation",type=int,required=True)
    a.add_argument("--activity-id",required=True)
    a.add_argument("--activity-generation",type=int,required=True)
    a.add_argument("--worker",required=True)
    a.add_argument("--phase",choices=("started","progress","finished"),required=True)
    a.add_argument("--evidence",required=True)
    a=sub.add_parser("wakeup-reconcile")
    a.add_argument("id")
    a.add_argument("--proven-absent",action="store_true")
    a.add_argument("--evidence",required=True)
    a=sub.add_parser("wakeup-reconcile-accepted")
    a.add_argument("id")
    a.add_argument("--proven-not-started",action="store_true")
    a.add_argument("--evidence",required=True)
    a=sub.add_parser("work-claim")
    a.add_argument("id")
    a.add_argument("--owner",required=True)
    a.add_argument("--authorization-evidence",required=True)
    a=sub.add_parser("work-renew")
    a.add_argument("id")
    a.add_argument("--owner",required=True)
    a.add_argument("--lease-version",type=int,required=True)
    a.add_argument("--evidence",required=True)
    a=sub.add_parser("work-finish")
    a.add_argument("id")
    a.add_argument("--owner",required=True)
    a.add_argument("--lease-version",type=int,required=True)
    a.add_argument("--status",choices=("done","blocked","needs_approval","retry_wait"),default="done")
    a.add_argument("--evidence",required=True)
    a.add_argument("--reason")
    a.add_argument("--retry-at")
    a=sub.add_parser("schedule-add")
    a.add_argument("--logical-key",required=True)
    a.add_argument("--kind",choices=("work","promise","social","research","maintenance"),required=True)
    a.add_argument("--priority",type=int,default=50)
    a.add_argument("--weight",type=int,default=50)
    a.add_argument("--deadline")
    a.add_argument("--switch-cost",type=int,default=4)
    a.add_argument("--work-id")
    a=sub.add_parser("schedule-sync")
    a.add_argument("--limit",type=int,default=100)
    sub.add_parser("schedule-assess")
    a=sub.add_parser("schedule-next")
    a.add_argument("--owner",required=True)
    a.add_argument("--for-work-id",
        help="diagnose and dispatch this specific eligible work only if it is highest ranked; never bypass a live lease")
    a=sub.add_parser("schedule-checkpoint")
    a.add_argument("id")
    a.add_argument("--owner",required=True)
    a.add_argument("--generation",type=int,required=True)
    a.add_argument("--state-file",required=True)
    a.add_argument("--yield",dest="yield_activity",action="store_true")
    a=sub.add_parser("schedule-complete")
    a.add_argument("id")
    a.add_argument("--owner",required=True)
    a.add_argument("--generation",type=int,required=True)
    a.add_argument("--evidence",required=True)
    a=sub.add_parser("schedule-cancel")
    a.add_argument("id")
    a.add_argument("--reason",required=True)
    a=sub.add_parser("schedule-status")
    a.add_argument("--limit",type=int,default=50)
    a=sub.add_parser("schedule-restore")
    a.add_argument("id")
    return p


def main(argv=None):
    args=parser().parse_args(argv)
    try:
        cfg=load_config(args.config,database=args.db)
        # File lock is stable across process exits and migrations. Never unlink it.
        with instance_lock(cfg.database+".tick.lock"):
            if args.command in ("export-pending","work-claim","work-renew","work-finish","score-backfill","score-export","score-message","wakeup-stage","wakeup-export","wakeup-claim","wakeup-ack","wakeup-reconcile","host-wakeup-prepare") and not Path(cfg.database).is_file():
                raise ValueError("Muse handoff requires an existing initialized database")
            db=Database(cfg.database,cfg.journal)
            now=time.time()
            if args.command=="init":
                result={"initialized":True,"schema_version":1,"journal":cfg.journal}
            elif args.command=="doctor":
                try:
                    ws_version=importlib.metadata.version("websocket-client")
                except importlib.metadata.PackageNotFoundError:
                    ws_version=None
                spec=importlib.util.find_spec("websocket") if ws_version else None
                origin=spec.origin if spec else None
                ws_hash=hashlib.sha256(Path(origin).read_bytes()).hexdigest() if origin and Path(origin).is_file() else None
                result={"python":platform.python_version(),"platform":platform.system(),"sqlite":sqlite3.sqlite_version,
                    "journal":cfg.journal,"integrity":db.integrity(),"websocket_client":ws_version,
                    "websocket_origin":origin,"websocket_entry_sha256":ws_hash,
                    "writes_enabled":cfg.allow_writes,"model_executor":False,
                    "scheduler":"external timer required; this command installs nothing"}
            elif args.command=="status":
                result={"queue":db.read("SELECT status,count(*) AS count FROM work_items GROUP BY status"),
                    "outbox":db.read("SELECT status,count(*) AS count FROM outbox GROUP BY status"),
                    "promises":db.read("SELECT id,status,start_by,original_due_at,due_at,next_action_at,first_action_at,last_progress_at FROM promises"),
                    "cursors":db.read("SELECT * FROM sync_cursors"),"attention":db.read("SELECT * FROM attention_sessions"),
                    "alert_count":db.read("SELECT count(*) AS count FROM alerts")[0]["count"],
                    "runs":db.read("SELECT id,started_at,ended_at,status FROM patrol_runs ORDER BY started_at DESC LIMIT 5")}
            elif args.command=="tick":
                if args.light and args.ws:
                    raise ValueError("light tick must not open a persistent websocket")
                runtime=cfg
                if args.light:
                    runtime=replace(cfg,allow_writes=False,
                        max_requests=min(cfg.max_requests,cfg.light_max_requests),
                        max_pages_per_source=min(cfg.max_pages_per_source,cfg.light_max_pages_per_source),
                        tick_budget_seconds=min(cfg.tick_budget_seconds,cfg.light_tick_budget_seconds),
                        request_timeout_seconds=min(cfg.request_timeout_seconds,5,
                            min(cfg.tick_budget_seconds,cfg.light_tick_budget_seconds)-1))
                client=None if args.offline else Client(runtime,credential_provider(runtime))
                result=Tick(db,runtime,client).run(websocket=args.ws,light=args.light)
                if args.quiet and result.get("status")=="ok" and not result.get("alerts") and not result.get("completed_work") and not result.get("outbox_attempts") and not result.get("wakeup_staged"):
                    return 0
            elif args.command=="migrate":
                result=migrate_legacy(db,args.directory,now,args.confirm_stopped)
            elif args.command=="backup":
                result={"backup":db.backup(args.destination),"integrity":"ok"}
            elif args.command in ("deployment-start","deployment-ensure"):
                # Called by the existing HATCH Hook once per installed SHA.
                # All subsequent turns use local status and never issue a
                # capability GET or generate duplicate runs/work.
                bound=db.read("SELECT value FROM meta WHERE key='actor'")
                current=Deployment(db).get() if bound else None
                if (args.command=="deployment-ensure" and current and
                        current.get("repo_sha")==args.repo_sha):
                    result={"status":"already_initialized","deployment":current,
                            "remote_action_issued":False}
                elif not bound:
                    result={"status":"identity_pending","deployment_verified":False}
                elif not cfg.allow_writes:
                    result={"status":"authorization_failed","deployment_verified":False}
                else:
                    client=Client(cfg,credential_provider(cfg))
                    reply=client.request("GET","/api/v1/agent/deployment/capabilities")
                    if reply.status in (0,408,429) or reply.status>=500:
                        result={"status":"deferred","remote_status":reply.status,
                                "deployment_verified":False,"retry_after":reply.retry_after}
                    elif reply.status in (401,403):
                        result={"status":"authorization_failed","deployment_verified":False}
                    elif (reply.status!=200 or not isinstance(reply.body,dict) or
                            reply.body.get("supported") is not True or
                            reply.body.get("protocolVersion")!="1" or
                            reply.body.get("actorId")!=bound[0]["value"] or
                            reply.body.get("synAckEventType")!="deployment.syn_ack" or
                            reply.body.get("platformSenderId")!="crewquay-platform" or
                            reply.body.get("windowSeconds")!=300):
                        result=Deployment(db).unsupported(now,args.repo_sha)
                    else:
                        result=Deployment(db).start(now,args.repo_sha)
            elif args.command=="deployment-status":
                result=Deployment(db).get(args.id)
            elif args.command=="deployment-reconcile":
                result={"local_transitions":Deployment(db).reconcile(now),
                        "remote_action_issued":False}
            elif args.command in ("deployment-stage-syn","deployment-stage-ack","deployment-stage-bug"):
                if not cfg.allow_writes:
                    raise ValueError("deployment outbound writes disabled")
                phase=args.command[len("deployment-stage-"):]
                result=Deployment(db).stage(args.id,phase,now,
                    {"id":args.schedule_id,"owner":args.owner,
                     "generation":args.schedule_generation},
                    producer_id=args.producer_id)
            elif args.command=="social-sync":
                if (not 1<=args.limit<=20 or not 1<=args.max_new<=3 or
                        not 1<=args.pages<=3 or (args.source!="circles" and args.pages!=1)):
                    raise ValueError("social-sync limit 1..20, max-new 1..3, circles pages 1..3")
                if not cfg.community_social_enabled:
                    result={"status":"disabled_by_owner","remote_action_issued":False,
                            "new_activities":0,"model_invoked":False,"host_woken":False}
                elif args.if_idle_due and not SocialOpportunities(db).reserve_idle_discovery(now):
                    result={"status":"not_due_or_busy","remote_action_issued":False}
                else:
                    client=Client(cfg,credential_provider(cfg))
                    if args.source=="ranked":
                        remote=client.request("GET","/api/v1/agent/social/opportunities?limit="+str(args.limit))
                        if remote.status==200:
                            result=SocialOpportunities(db).admit(remote.body,now,args.max_new)
                        else:
                            result={"status":"deferred","remote_status":remote.status,
                                    "model_invoked":False,"host_woken":False,
                                    "remote_action_issued":False}
                    else:
                        from urllib.parse import urlencode
                        import re
                        old=db.read("SELECT value FROM meta WHERE key='social_circle_cursor'")
                        after=old[0]["value"] if old else ""
                        if after and not re.fullmatch(r"cir_[A-Za-z0-9_-]{1,112}",after):
                            raise ValueError("invalid saved circle discovery cursor")
                        admitted=[]
                        scanned=0
                        status="ok"
                        more=True
                        for _ in range(args.pages):
                            if len(admitted)>=args.max_new:
                                break
                            query={"source":"circles","limit":args.limit}
                            if after:
                                query["after"]=after
                            remote=client.request("GET","/api/v1/agent/social/opportunities?"+
                                                   urlencode(query))
                            if remote.status!=200:
                                status="deferred"
                                break
                            page=remote.body
                            if (not isinstance(page,dict) or page.get("source")!="circles"
                                    or type(page.get("hasMore")) is not bool
                                    or not isinstance(page.get("nextCursor"),str)):
                                raise ValueError("invalid circle page contract")
                            nxt=page["nextCursor"]
                            if page["hasMore"] and (
                                    not re.fullmatch(r"cir_[A-Za-z0-9_-]{1,112}",nxt)
                                    or (after and nxt<=after)):
                                raise ValueError("invalid circle cursor progression")
                            picked=SocialOpportunities(db).admit(
                                page,now,args.max_new-len(admitted))
                            admitted.extend(picked["activities"])
                            scanned+=1
                            more=page["hasMore"]
                            after=nxt if more else ""
                            with db.transaction() as c:
                                c.execute("""INSERT INTO meta(key,value)
                                    VALUES('social_circle_cursor',?)
                                    ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
                                    (after,))
                            if not more:
                                break
                        result={"status":status,"source":"circles",
                                "pages_scanned":scanned,"next_cursor":after,
                                "has_more":more,"new_activities":len(admitted),
                                "activities":admitted,"model_invoked":False,
                                "host_woken":False,"remote_action_issued":False}
            elif args.command=="social-detail":
                result=SocialOpportunities(db).details(args.id)
            elif args.command in ("platform-auth-sync","platform-auth-bootstrap"):
                from ...executor.index import Authorization
                known=db.read("SELECT value FROM meta WHERE key='actor'")
                if not known:
                    raise ValueError("verified CrewQuay actor binding required for authorization")
                actor=known[0]["value"]
                client=Client(cfg,credential_provider(cfg))
                authority=Authorization(db)
                if args.command=="platform-auth-sync":
                    manifest,status=authority.get(client,actor)
                    reconciled=authority.sync_requests(client,actor) if manifest else False
                    result={"status":"ok" if manifest else "deferred",
                            "remote_status":status,
                            "actor":actor,
                            "revision":manifest.get("revision") if manifest else None,
                            "requests_reconciled":reconciled}
                elif not cfg.allow_writes:
                    result={"status":"emergency_write_stop","remote_action_issued":False}
                else:
                    next_rows=db.read("SELECT value FROM meta WHERE key='authorization_bootstrap_next_at'")
                    if (args.if_due and next_rows and
                            float(next_rows[0]["value"])>now):
                        result={"status":"not_due","remote_action_issued":False}
                    else:
                        result=authority.bootstrap(client,actor,args.deployment_run_id)
                        if args.if_due:
                            # One request per scope per turn; 10-minute retry
                            # during initial enrolment, one hour once settled.
                            interval=(600 if result.get("status")=="requested" else 3600)
                            with db.transaction() as conn:
                                conn.execute("""INSERT INTO meta(key,value)
                                    VALUES('authorization_bootstrap_next_at',?)
                                    ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
                                    (str(now+interval),))
            elif args.command=="platform-catalog":
                client=Client(cfg,credential_provider(cfg))
                remote=client.request("GET","/api/v1/agent/actions")
                result={"status":"ok","catalog":remote.body} if remote.status==200 else {"status":"failed","remote_status":remote.status}
            elif args.command=="platform-events":
                from urllib.parse import urlencode
                limit=cfg.light_events_page_size if args.limit is None else args.limit
                if not args.after.isdecimal() or not 1<=limit<=100:
                    raise ValueError("invalid cursor or page limit")
                client=Client(cfg,credential_provider(cfg))
                remote=client.request("GET","/api/v1/agent/events?"+
                    urlencode({"after":args.after,"limit":limit}))
                result={"status":"ok","page":remote.body} if remote.status==200 else {"status":"failed","remote_status":remote.status}
            elif args.command=="stage":
                with open(args.payload_file,encoding="utf-8") as f:
                    payload=json.load(f)
                # Fail at STAGING instead of silently storing an action that the
                # authenticated executor can never dispatch. A Hook worker
                # identity belongs in producer_id, not the platform actor.
                bound=db.read("SELECT value FROM meta WHERE key='actor'")
                if not bound:
                    raise ValueError("verified CrewQuay actor binding required before staging")
                actor=bound[0]["value"]
                if args.actor is not None and args.actor!=actor:
                    raise ValueError("staged actor differs from verified CrewQuay actor; use --producer-id for worker identity")
                supplied=(args.schedule_id,args.schedule_owner,args.schedule_generation)
                if any(v is not None for v in supplied) and not all(v is not None for v in supplied):
                    raise ValueError("schedule id, owner and generation must be provided together")
                schedule_context=({"id":args.schedule_id,"owner":args.schedule_owner,
                                   "generation":args.schedule_generation}
                                  if all(v is not None for v in supplied) else None)
                ident=Outbox(db).stage(args.logical_key,actor,args.action,args.target,
                                        payload,now,producer_id=args.producer_id,
                                        schedule_context=schedule_context)
                result={"outbox_id":ident,"status":"prepared","sent":False,
                        "executor_actor":actor,"producer_id":args.producer_id,
                        "dispatch_requested":bool(args.dispatch_now)}
            elif args.command=="schedule-add":
                ident=Scheduler(db,cfg.lease_seconds).register(
                    args.logical_key,args.kind,now,priority_score=args.priority,
                    weight=args.weight,deadline=timestamp(args.deadline),
                    switch_cost=args.switch_cost,work_id=args.work_id)
                result={"activity_id":ident,"model_invoked":False}
            elif args.command=="schedule-sync":
                result={"activities_registered":Scheduler(db,cfg.lease_seconds).sync_work(now,args.limit),
                        "model_invoked":False}
            elif args.command=="schedule-assess":
                result=Scheduler(db,cfg.lease_seconds).assess(now)
            elif args.command=="schedule-next":
                result=Scheduler(db,cfg.lease_seconds).dispatch(
                    args.owner,now,for_work_id=args.for_work_id)
            elif args.command=="schedule-checkpoint":
                result=Scheduler(db,cfg.lease_seconds).checkpoint(
                    args.id,args.owner,args.generation,now,evidence(args.state_file),
                    voluntary=args.yield_activity)
            elif args.command=="schedule-complete":
                result=Scheduler(db,cfg.lease_seconds).complete(
                    args.id,args.owner,args.generation,now,evidence(args.evidence))
            elif args.command=="schedule-cancel":
                result=Scheduler(db,cfg.lease_seconds).cancel(args.id,now,args.reason)
            elif args.command=="schedule-status":
                result=Scheduler(db,cfg.lease_seconds).snapshot(now,args.limit)
            elif args.command=="schedule-restore":
                result=Scheduler(db,cfg.lease_seconds).restore(args.id,now)
            elif args.command=="outbox-drain":
                if not args.once:
                    raise ValueError("outbox-drain is deliberately single-shot: pass --once")
                client=Client(cfg,credential_provider(cfg)) if cfg.allow_writes else None
                result=ActionSender(db,cfg,client).drain_once(
                    limit=args.limit,action_id=args.id,lane=args.lane)
            elif args.command=="resolve":
                Outbox(db).resolve(args.id,evidence(args.evidence),now,args.confirmed,args.proven_absent)
                result={"resolved":args.id,"remote_request_issued":False}
            elif args.command=="promise-add":
                ident=Promises(db).create(args.actor,args.recipient,args.source_message_id,args.text,now,
                    timestamp(args.start_by),timestamp(args.next_action_at),timestamp(args.due_at),
                    work_id=args.work_id,kind=args.kind,objective_reason=args.reason)
                result={"promise_id":ident}
            elif args.command=="promise-progress":
                Promises(db).progress(args.id,now,evidence(args.evidence),timestamp(args.next_action_at))
                result={"progress_recorded":args.id}
            elif args.command=="promise-complete":
                Promises(db).complete(args.id,now,evidence(args.evidence))
                result={"completed":args.id}
            elif args.command=="extension-request":
                Promises(db).request_extension(args.id,timestamp(args.due_at),args.reason,evidence(args.evidence),now)
                result={"extension_requested":args.id,"deadline_changed":False}
            elif args.command=="extension-approve":
                Promises(db).approve_extension(args.id,timestamp(args.due_at),args.approved_by,evidence(args.evidence),now)
                result={"extension_approved":args.id}
            elif args.command=="resume":
                Queue(db,cfg.lease_seconds).resume(args.id,now,evidence(args.evidence))
                result={"resumed":args.id}
            elif args.command=="export-pending":
                result=export_pending(db,now,args.limit)
            elif args.command=="score-backfill":
                total=MessageScores(db).backfill(args.limit)
                result={"scored_historical_messages":total,"model_called":False}
            elif args.command=="score-export":
                result=MessageScores(db).unrated(args.limit)
            elif args.command=="score-message":
                result=MessageScores(db).rate(args.event_id,args.score,args.actor,
                                              args.reason,evidence(args.evidence),now)
            
            elif args.command=="autonomy-status":
                import os
                wired=os.environ.get("CREWQUAY_HOST_WAKEUP_WIRED")=="1"
                started=db.read("SELECT count(*) AS n FROM wakeup_execution_receipts")[0]["n"]
                finished=db.read("""SELECT count(*) AS n FROM wakeup_execution_receipts
                    WHERE status='finished'""")[0]["n"]
                pending=db.read("""SELECT count(*) AS n FROM work_items
                    WHERE type='human_action' AND status IN ('pending','retry_wait')""")[0]["n"]
                result={"status":"BLOCKED_HOST_WAKE" if not wired else
                        "AWAITING_VERIFIED_MUSE_EXECUTION" if not started else "execution_observed",
                        "host_wired_declared":wired,"observed_muse_started":started,
                        "observed_muse_finished":finished,"pending_human_actions":pending,
                        "deployed_handshake_is_not_execution_readiness":True,
                        "host_attestation_required":True}
            elif args.command=="muse-consume":
                result=MuseConsumer(db,cfg.lease_seconds,cfg).consume(
                    args.wakeup_id,args.wakeup_generation,args.worker,now)
            elif args.command=="muse-finish":
                result=MuseConsumer(db,cfg.lease_seconds).finish(
                    args.wakeup_id,args.wakeup_generation,args.worker,
                    args.activity_id,args.activity_generation,
                    args.work_lease_version,now,evidence(args.evidence))
            elif args.command=="host-wakeup-prepare":
                # The host decides which authenticated Agent gets a time
                # slice. Social is normal low-priority Agent work; platform
                # action permissions are evaluated separately at send time.
                result=HostHandoff(db,cfg.lease_seconds).prepare(
                    args.owner,now,allow_social=cfg.community_social_enabled)
            elif args.command=="wakeup-stage":
                result={"staged":Wakeups(db,cfg.lease_seconds).stage(now,threshold=cfg.wakeup_min_score),
                        "host_called":False}
            elif args.command=="wakeup-export":
                result=Wakeups(db,cfg.lease_seconds).export(now,args.limit)
            elif args.command=="wakeup-claim":
                result=Wakeups(db,cfg.lease_seconds).claim(args.id,args.owner,now)
            elif args.command=="wakeup-ack":
                result=Wakeups(db,cfg.lease_seconds).acknowledge(
                    args.id,args.owner,args.lease_version,now,args.result,evidence(args.evidence))
            elif args.command=="wakeup-execution":
                result=Wakeups(db,cfg.lease_seconds).record_execution(
                    args.id,args.wakeup_generation,args.activity_id,
                    args.activity_generation,args.worker,args.phase,
                    now,evidence(args.evidence))
            elif args.command=="wakeup-reconcile":
                result=Wakeups(db,cfg.lease_seconds).reconcile_unknown(
                    args.id,now,args.proven_absent,evidence(args.evidence))
            elif args.command=="wakeup-reconcile-accepted":
                result=Wakeups(db,cfg.lease_seconds).reconcile_accepted_no_start(
                    args.id,now,args.proven_not_started,evidence(args.evidence))
            elif args.command=="work-claim":
                item=Queue(db,cfg.lease_seconds).claim_specific(
                    args.id,args.owner,now,evidence(args.authorization_evidence))
                result={"work_id":item["id"],"lease_owner":item["lease_owner"],
                        "lease_version":item["lease_version"],"lease_until":item["lease_until"],
                        "payload":item["payload"],"warning":"local claim is not CrewQuay role authorization"}
                if "scheduler_activity_id" in item:
                    result.update(schedule_id=item["scheduler_activity_id"],
                                  schedule_owner=item["lease_owner"],
                                  schedule_generation=item["scheduler_generation"])
            elif args.command=="work-renew":
                renewed=Queue(db,cfg.lease_seconds).renew(
                    {"id":args.id,"lease_owner":args.owner,"lease_version":args.lease_version},
                    now,evidence(args.evidence))
                result={"renewed_work":args.id,**renewed,"remote_action_issued":False}
            elif args.command=="work-finish":
                if args.status=="retry_wait" and not args.retry_at:
                    raise ValueError("retry_wait requires explicit retry timestamp")
                retry_at=timestamp(args.retry_at) if args.retry_at else None
                if retry_at is not None and retry_at<=now:
                    raise ValueError("retry_at must be in the future")
                Queue(db,cfg.lease_seconds).finish(
                    {"id":args.id,"lease_owner":args.owner,"lease_version":args.lease_version},
                    now,evidence=evidence(args.evidence),status=args.status,
                    reason=args.reason,retry_at=retry_at)
                result={"finished_work":args.id,"status":args.status,"remote_action_issued":False}
        # The stage transaction and CLI lock are released BEFORE running the
        # independent Executor subprocess. It obtains the same stable lock and
        # verifies the real platform actor before any remote write.
        if args.command=="stage" and args.dispatch_now:
            command=[sys.executable,"-m","patrol"]
            if args.config:
                command+=["--config",args.config]
            command+=["--db",cfg.database,"outbox-drain","--once",
                      "--limit","1","--id",result["outbox_id"]]
            try:
                done=subprocess.run(command,text=True,capture_output=True,
                                    timeout=min(120,max(15,cfg.tick_budget_seconds+10)),
                                    check=False)
                try:
                    dispatch=json.loads(done.stdout)
                    if not isinstance(dispatch,dict):
                        raise ValueError("executor response must be a JSON object")
                    result["dispatch"]={"status":dispatch.get("status","unknown"),
                                         "item_status":dispatch.get("item_status"),
                                         "confirmed":dispatch.get("confirmed",0),
                                         "exit_code":done.returncode}
                except (ValueError,TypeError):
                    result["dispatch"]={"status":"unparsed","exit_code":done.returncode}
            except subprocess.TimeoutExpired:
                result["dispatch"]={"status":"timeout_unknown"}
            # A child report is not enough: only the same durable database
            # confirmed status (written after exact remote readback) proves
            # delivery. This also makes unparseable child output recoverable.
            row=Database(cfg.database,cfg.journal).read(
                "SELECT status FROM outbox WHERE id=?",(result["outbox_id"],))
            durable_status=row[0]["status"] if row else "missing"
            result["sent"]=durable_status=="confirmed"
            result["outbox_status"]=durable_status
            result["status"]="confirmed" if result["sent"] else "prepared"
        print(json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False))
        failed={"authorization_failed","failed","deferred","degraded","partial","blocked",
                "not_found","BLOCKED_HOST_WAKE","scheduler_deferred","identity_pending",
                "work_not_ready","requires_specialized_consumer"}
        return 0 if result.get("status") not in failed and result.get("imported") is not False else 1
    except Busy:
        print(json.dumps({"status":"busy","action":"deferred; no concurrent writer"}))
        return 75
    except Exception as e:
        # Exception bodies can contain server/credential data. Only the type is public.
        print(json.dumps({"status":"failed","error":type(e).__name__,"hint":"check configuration, schema and local evidence; no secrets printed"}))
        return 1
