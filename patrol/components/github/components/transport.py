"""GitHub official REST transport. No redirects, proxies or secret-bearing logs."""
import json
import urllib.error
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,request,fp,code,msg,headers,newurl):
        return None


class GitHubAPI:
    def __init__(self,*,opener=None):
        self.opener=opener or urllib.request.build_opener(
            urllib.request.ProxyHandler({}),NoRedirect)

    def request(self,method,path,token,body=None):
        if method not in ("GET","POST","PUT") or not isinstance(path,str) or (
                not path.startswith("/") or "//" in path or "?" in path or "#" in path):
            raise ValueError("unsupported GitHub request target")
        if body is not None and (method=="GET" or not isinstance(body,dict)):
            raise ValueError("invalid GitHub mutation payload")
        raw=(json.dumps(body,ensure_ascii=False,separators=(",",":")).encode("utf-8")
             if body is not None else None)
        if raw is not None and len(raw)>32000:
            raise ValueError("bounded GitHub action payload required")
        request=urllib.request.Request(
            "https://api.github.com"+path,data=raw,method=method,
            headers={"Authorization":"Bearer "+token,
                     "Accept":"application/vnd.github+json",
                     "X-GitHub-Api-Version":"2022-11-28",
                     "User-Agent":"muse-agent-github/1",
                     **({"Content-Type":"application/json"} if raw is not None else {})})
        try:
            with self.opener.open(request,timeout=8) as r:
                data=r.read(1024*1024+1)
                if len(data)>1024*1024:
                    return 0,None
                return r.status,(json.loads(data) if data else {})
        except urllib.error.HTTPError as e:
            # Status is enough to distinguish denied vs ambiguous. NEVER
            # expose response headers/body/exception (may contain secrets).
            return e.code,None
        except (urllib.error.URLError,OSError,TimeoutError,ValueError,TypeError):
            return 0,None
