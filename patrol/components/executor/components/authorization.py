"""CrewQuay authority is a verified, actor-bound runtime manifest, not a catalog.

The legacy local allowlists are *only* used if the older server has no
runtime-manifest endpoint. A temporary network error never falls back to stale
catalog permissions. Deny rules and emergency write stop always win.
"""
import hashlib
import json
import time
from urllib.parse import quote, urlencode

from ...persistence.index import canonical


def _key(actor, scope, actions):
    data = canonical([actor, scope, sorted(set(actions))])
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


class PolicyEngine:
    """Four-way decision; domain POST always checks its own current business rules."""
    def __init__(self, db, config):
        self.db, self.config = db, config

    def evaluate(self, actor, command, manifest, now=None):
        if not self.config.allow_writes:
            return "DENY", "emergency_write_stop"
        if not isinstance(manifest, dict):
            return "REVALIDATE", "verified_manifest_unavailable"
        if manifest.get("protocolVersion") != "1" or manifest.get("actorId") != actor:
            return "DENY", "manifest_actor_or_protocol_mismatch"
        actions = manifest.get("actions")
        if not isinstance(actions, list):
            return "DENY", "manifest_action_registry_invalid"
        name, version = command["action"], command["version"]
        resource = command["resource"]
        item = next((x for x in actions if isinstance(x, dict) and
                     x.get("name") == name and x.get("version") == version and
                     x.get("resourceType") == resource["type"]), None)
        if item is None or item.get("supported") is not True:
            return "DENY", "action_not_supported"
        if item.get("granted") is not True:
            reason=item.get("denialReason") or "formal_grant_missing"
            if reason in ("owner_grant_revoked_or_expired","owner_action_not_approved"):
                return "DENY", reason
            return "NEEDS_APPROVAL", reason
        if item.get("eligible_now") is not True:
            return "DENY", item.get("denialReason") or "business_rule_denied"
        issued = manifest.get("issuedAt")
        if not isinstance(issued, str) or not isinstance(manifest.get("revision"), str):
            return "DENY", "manifest_revision_missing"
        if now is not None:
            # An epoch comes from the transport after retrieval, not external text.
            checked_at = manifest.get("_checked_at")
            max_age = manifest.get("maxAgeSeconds", 60)
            if (not isinstance(checked_at, (int,float)) or
                    type(max_age) is not int or max_age < 1 or
                    now - checked_at > min(max_age, 60)):
                return "REVALIDATE", "manifest_cache_expired"
        return "ALLOW", "current_platform_authority"


class Authorization:
    def __init__(self, db):
        self.db = db

    def _store(self, actor, manifest, now, resource_type="", resource_id=""):
        if (not isinstance(manifest, dict) or
                manifest.get("actorId") != actor or
                manifest.get("protocolVersion") != "1" or
                not isinstance(manifest.get("revision"), str) or
                not isinstance(manifest.get("actions"), list) or
                not isinstance(manifest.get("grants"), list)):
            raise ValueError("invalid actor-bound runtime manifest")
        manifest = dict(manifest)
        manifest["_checked_at"] = now
        with self.db.transaction() as c:
            c.execute("""INSERT INTO platform_manifests
                (actor,resource_type,resource_id,revision,payload,checked_at)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(actor,resource_type,resource_id) DO UPDATE SET
                revision=excluded.revision,payload=excluded.payload,checked_at=excluded.checked_at""",
                (actor,resource_type,resource_id,manifest["revision"],canonical(manifest),now))
            if not resource_type:
                c.execute("INSERT INTO meta(key,value) VALUES('policy_actor',?) "
                          "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (actor,))
        return manifest

    def get(self, client, actor, resource=None):
        resource_type, resource_id = (resource["type"], resource["id"]) if resource else ("","")
        url = "/api/v1/agent/runtime-manifest"
        if resource:
            url += "?" + urlencode({"resourceType":resource_type,"resourceId":resource_id})
        response = client.request("GET", url)
        if response.status != 200:
            return None, response.status
        try:
            return self._store(actor,response.body,time.time(),resource_type,resource_id),200
        except (ValueError,TypeError,KeyError):
            return None,422

    def cached(self, actor, resource=None):
        typ, ident = (resource["type"], resource["id"]) if resource else ("","")
        rows = self.db.read("SELECT payload FROM platform_manifests "
                            "WHERE actor=? AND resource_type=? AND resource_id=?",
                            (actor,typ,ident))
        return json.loads(rows[0]["payload"]) if rows else None

    def request_once(self, client, actor, scope, actions, run_id, reason):
        """One scope+action set gets one request, including a persisted rejection."""
        actions = sorted(set(actions))
        key = _key(actor, scope, actions)
        prior = self.db.read("SELECT * FROM permission_requests WHERE id=?", (key,))
        if prior:
            return {"id":key,"status":prior[0]["status"],"reused":True}
        payload = {"scope":scope,"actions":actions,"reason":reason,
                   "deploymentRunId":run_id}
        response = client.request("POST","/api/v1/agent/authorization/requests",
                                  payload,"authreq-"+key[:32])
        if response.status != 200 or not isinstance(response.body, dict):
            return {"status":"deferred" if response.status in (0,408,429) or
                    response.status>=500 else "denied",
                    "http_status":response.status}
        record = response.body
        if (record.get("actorId") != actor or
                record.get("actions") != actions or
                record.get("scope") != scope or
                record.get("status") not in ("pending","approved","denied")):
            return {"status":"invalid_response"}
        with self.db.transaction() as c:
            c.execute("""INSERT INTO permission_requests
                (id,actor,scope,actions,remote_id,status,updated_at) VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET status=excluded.status,
                remote_id=excluded.remote_id,updated_at=excluded.updated_at""",
                (key,actor,canonical(scope),canonical(actions),record["id"],
                 record["status"],time.time()))
        return {"id":key,"status":record["status"],"remote_id":record["id"]}

    def sync_requests(self, client, actor):
        response = client.request("GET","/api/v1/agent/authorization/requests")
        if response.status != 200 or not isinstance(response.body,list):
            return False
        own = [r for r in response.body if isinstance(r,dict) and
               r.get("actorId") == actor and
               r.get("status") in ("pending","approved","denied")]
        with self.db.transaction() as c:
            for r in own:
                scope, actions = r.get("scope"), r.get("actions")
                if not isinstance(scope,dict) or not isinstance(actions,list):
                    continue
                key=_key(actor,scope,actions)
                c.execute("""INSERT INTO permission_requests
                    (id,actor,scope,actions,remote_id,status,updated_at) VALUES(?,?,?,?,?,?,?)
                    ON CONFLICT(id) DO UPDATE SET status=excluded.status,
                    remote_id=excluded.remote_id,updated_at=excluded.updated_at""",
                    (key,actor,canonical(scope),canonical(actions),r["id"],
                     r["status"],time.time()))
        return True

    def bootstrap(self, client, actor, deployment_run_id):
        """One bounded request per call. Agent initiates; Owner approves on platform."""
        manifest, status = self.get(client, actor)
        if status != 200:
            return {"status":"manifest_unavailable","http_status":status}
        self.sync_requests(client, actor)
        action_map = {a["name"]:a for a in manifest["actions"]
                      if isinstance(a,dict) and isinstance(a.get("name"),str)}
        groups = []
        community = sorted(k for k,v in action_map.items() if
                           v.get("resourceType") in ("community","circle","post"))
        if community:
            groups.append(({"type":"community","id":"community"}, community))
        for p in manifest.get("projectDelegations",[]):
            if not isinstance(p,dict) or not p.get("projectId"):
                continue
            actions = sorted(k for k,v in action_map.items() if
                             v.get("resourceType") in ("project","task","room","submission"))
            if actions:
                groups.append(({"type":"project","id":p["projectId"]},actions))
        for scope, actions in groups:
            key = _key(actor,scope,actions)
            if not self.db.read("SELECT 1 FROM permission_requests WHERE id=?", (key,)):
                result = self.request_once(client,actor,scope,actions,deployment_run_id,
                    "One-time deployment authorization, bounded to existing platform role and project scope")
                return {"status":"requested","scope":scope,"request":result}
        return {"status":"already_requested_or_approved","scopes":len(groups)}


def live_action_authority(sender, row, name, resource_type, resource_id):
    """Gate legacy typed routes with the same fresh platform authority.

    Only an explicit 404 from an older server permits the *old* restrictive
    allowlists. Outages, stale caches, 401/403 and malformed manifests deny.
    No Model/Agent instruction can fabricate this response.
    """
    if sender.client is None:
        return False, "platform_unavailable", False
    resource={"type":resource_type,"id":resource_id}
    try:
        manifest,status=Authorization(sender.db).get(sender.client,row["actor"],resource)
    except Exception:
        return False, "manifest_network_unavailable", False
    if status==404:
        return True,"legacy_platform",True
    if status!=200:
        return False,"manifest_http_"+str(status),False
    decision,reason=PolicyEngine(sender.db,sender.config).evaluate(
        row["actor"],{"action":name,"version":"1","resource":resource},manifest)
    return decision=="ALLOW",decision+":"+reason,False
