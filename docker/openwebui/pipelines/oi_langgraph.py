"""
title: OI LangGraph (Proxy)
author: You
description: OpenWebUI ↔ LangGraph Server thin pipeline (with Cognito/Okta ID token forwarding)
required_open_webui_version: 0.4.3
version: 1.2.0
license: MIT
"""

import os
import json
import time
from typing import Iterator, List, Dict, Any, Union
from pydantic import BaseModel, Field
import requests
from urllib.parse import parse_qsl

# --- event keys (unchanged) ---
NODE_START_KEYS = {"node_started", "node_start", "graph:nodes:start", "graph:node:start"}
NODE_END_KEYS   = {"node_finished", "node_end", "graph:nodes:end", "graph:node:end"}
TOOL_START_KEYS = {"tool_called", "tool_start", "tools:start", "mcp:tool:start"}
TOOL_END_KEYS   = {"tool_result", "tool_end", "tools:end", "mcp:tool:end"}

def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default

def _now_ms() -> int:
    return int(time.time() * 1000)

def _json_short(x: Any, n: int = 200) -> str:
    try:
        s = json.dumps(x, ensure_ascii=False)
    except Exception:
        s = str(x)
    return (s[:n] + "…") if len(s) > n else s

def _flatten_events(payload: Any):
    if payload is None:
        return
    if isinstance(payload, (str, bytes)):
        try:
            payload = json.loads(payload)
        except Exception:
            return
    if isinstance(payload, list):
        for it in payload:
            yield from _flatten_events(it)
        return
    if isinstance(payload, dict):
        yield payload
        for k in ("data", "event", "payload"):
            if k in payload and isinstance(payload[k], (dict, list, str)):
                yield from _flatten_events(payload[k])
        return

def _event_name_lower(ev: dict) -> str:
    e = ev.get("event") or ev.get("type") or ev.get("name") or ""
    return str(e).lower().strip()

def _starts_with_any(s: str, prefixes: list[str]) -> bool:
    s = s.lower()
    return any(s.startswith(p) for p in prefixes)

def _classify(ev: dict) -> tuple[str, dict]:
    et = _event_name_lower(ev)
    if et in ("run_status", "status"):
        return "status", {"status": ev.get("status") or ev.get("state")}
    if et in NODE_START_KEYS or _starts_with_any(et, ["graph:node:start", "graph:nodes:start"]):
        node = ev.get("node_name") or ev.get("node") or ev.get("name") \
            or (ev.get("data") or {}).get("node_name") \
            or (ev.get("payload") or {}).get("node")
        return "node_start", {"node": node or "?"}
    if et in NODE_END_KEYS or _starts_with_any(et, ["graph:node:end", "graph:nodes:end"]):
        node = ev.get("node_name") or ev.get("node") or ev.get("name") \
            or (ev.get("data") or {}).get("node_name") \
            or (ev.get("payload") or {}).get("node")
        return "node_end", {"node": node or "?"}
    if et in TOOL_START_KEYS or _starts_with_any(et, ["tools:start", "mcp:tool:start"]):
        tool = ev.get("tool_name") or ev.get("name") or ev.get("tool")
        d = ev.get("data") or ev.get("payload") or {}
        args = ev.get("args") or ev.get("input") or ev.get("parameters") or d.get("args") or d.get("input")
        return "tool_start", {"tool": tool or "tool", "args": args, "ts": ev.get("ts") or _now_ms()}
    if et in TOOL_END_KEYS or _starts_with_any(et, ["tools:end", "mcp:tool:end"]):
        tool = ev.get("tool_name") or ev.get("name") or ev.get("tool")
        d = ev.get("data") or ev.get("payload") or {}
        result = ev.get("result") or ev.get("output") or d.get("result") or d.get("output")
        return "tool_end", {"tool": tool or "tool", "result": result, "ts": ev.get("ts") or _now_ms()}
    if et in ("ai", "assistant", "message", "delta", "chunk") or any(k in ev for k in ("messages", "content")):
        content = ev.get("content") or (ev.get("messages") if "messages" in ev else None)
        return "message", {"content": content}
    return "other", {"raw": ev}

# ------------------ auth helpers ------------------

def _parse_cookies(raw_cookie: str) -> Dict[str, str]:
    """
    Tiny cookie parser; OWUI may pass headers into body['headers'].
    """
    out: Dict[str, str] = {}
    if not raw_cookie:
        return out
    # Split on ; and then on =
    for part in raw_cookie.split(";"):
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.strip()] = v.strip()
    return out

def _extract_id_token_from_body(body: Dict[str, Any], auth_cookie_name: str) -> str | None:
    # From explicit header?
    auth = (body.get("headers") or {}).get("authorization") or body.get("authorization")
    if isinstance(auth, str) and auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()
    # From cookie header?
    cookie_hdr = (body.get("headers") or {}).get("cookie") or body.get("cookie")
    if isinstance(cookie_hdr, str):
        ck = _parse_cookies(cookie_hdr)
        tok = ck.get(auth_cookie_name)
        if tok:
            return tok
    return None

# ------------------ pipeline ------------------

class Pipeline:
    class Valves(BaseModel):
        LANGGRAPH_URL: str = Field(default_factory=lambda: _env("LANGGRAPH_URL", "http://langgraph-backend:2024"))
        LANGGRAPH_API_KEY: str = Field(default_factory=lambda: _env("LANGGRAPH_API_KEY", ""))

        # Auth wiring
        AUTH_REQUIRED: bool = Field(default=_env("AUTH_REQUIRED", "true").lower() not in ("0","false","no"))
        AUTH_LOGIN_URL: str = Field(default_factory=lambda: _env("AUTH_LOGIN_URL", "http://localhost:9099/login"))
        AUTH_COOKIE_NAME: str = Field(default_factory=lambda: _env("AUTH_COOKIE_NAME", "id_token"))
        STATIC_ID_TOKEN: str = Field(default_factory=lambda: _env("STATIC_ID_TOKEN", ""))  # dev-only escape hatch

        # LangGraph assistant
        ASSISTANT_ID: str = Field(default_factory=lambda: _env("LANGGRAPH_ASSISTANT_ID", "34bc4aef-88cb-5105-8fc7-fdd174e78f32"))

        RUN_MAX_SECONDS: int = Field(default=int(_env("RUN_MAX_SECONDS", "500")))
        POLL_INTERVAL_SECONDS: float = Field(default=float(_env("POLL_INTERVAL_SECONDS", "1.5")))
        USE_SSE: bool = Field(default=_env("USE_SSE", "true").lower() not in ("0","false","no"))
        VERSION: str = Field(default="1.2.0")

    def __init__(self):
        self.valves = self.Valves()
        self.name = "OI LangGraph Pipeline (Auth)"

        self.base_headers = {"Content-Type": "application/json"}
        if self.valves.LANGGRAPH_API_KEY:
            self.base_headers["Authorization"] = f"Bearer {self.valves.LANGGRAPH_API_KEY}"

        self.conversations: Dict[str, Dict[str, Any]] = {}

    # ---------- headers w/ token ----------
    def _auth_headers(self, id_token: str | None) -> Dict[str, str]:
        h = dict(self.base_headers)
        if id_token:
            # Forward to LangGraph/MCP: they verify and assume role
            h["X-User-Auth"] = "cognito"  # optional hint
            h["Authorization"] = f"Bearer {id_token}"
        return h

    # ---------- thread/run helpers ----------
    def _ensure_thread(self, headers: Dict[str, str], conv_id: str) -> str:
        data = self.conversations.get(conv_id)
        if data and data.get("thread_id"):
            return data["thread_id"]
        url = f"{self.valves.LANGGRAPH_URL.rstrip('/')}/threads"
        r = requests.post(url, headers=headers, data=json.dumps({}), timeout=15)
        r.raise_for_status()
        thread_id = r.json()["thread_id"]
        self.conversations[conv_id] = {"thread_id": thread_id}
        return thread_id

    def _start_run(self, headers: Dict[str, str], thread_id: str, latest_text: str, username: str) -> str:
        url = f"{self.valves.LANGGRAPH_URL.rstrip('/')}/threads/{thread_id}/runs"
        payload = {
            "assistant_id": self.valves.ASSISTANT_ID,
            "input": {
                "messages": [{"type": "human", "content": latest_text}],
                "openwebui_username": username,
            },
            "metadata": {},
            "stream_mode": ["updates", "messages"],
        }
        r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
        r.raise_for_status()
        run_id = r.json().get("run_id")
        if not run_id:
            raise RuntimeError("LangGraph run start failed: missing run_id")
        return run_id

    def _stream_run(self, headers: Dict[str, str], thread_id: str, run_id: str) -> Iterator[str]:
        def _emit(kind: str, info: dict) -> str | None:
            if kind == "status":      return f"↻ status: {info.get('status','')}"
            if kind == "node_start":  return f"➡️ enter **{info.get('node','?')}**"
            if kind == "node_end":    return f"⬅️ exit **{info.get('node','?')}**"
            if kind == "tool_start":  return f"🛠️ **{info.get('tool','tool')}** args: `{_json_short(info.get('args'))}`"
            if kind == "tool_end":    return f"✅ **{info.get('tool','tool')}** done"
            if kind == "message":     return None
            return None

        # SSE first
        if self.valves.USE_SSE:
            stream_url = f"{self.valves.LANGGRAPH_URL.rstrip('/')}/threads/{thread_id}/runs/{run_id}/stream"
            h = dict(headers); h["Accept"] = "text/event-stream"
            try:
                with requests.get(stream_url, headers=h, stream=True, timeout=(5, 300)) as resp:
                    resp.raise_for_status()
                    for raw in resp.iter_lines(decode_unicode=True):
                        if not raw or not raw.startswith("data:"):
                            continue
                        data = raw[5:].strip()
                        if not data:
                            continue
                        for ev in _flatten_events(data):
                            kind, info = _classify(ev)
                            line = _emit(kind, info)
                            if line:
                                yield line + "\n"
                            if kind == "status" and (info.get("status") in ("success","error","timeout","interrupted")):
                                return
                return
            except Exception as e:
                yield f"… stream unavailable ({e.__class__.__name__}); switching to polling\n"

        # Polling fallback
        check_url = f"{self.valves.LANGGRAPH_URL.rstrip('/')}/threads/{thread_id}/runs/{run_id}"
        start = time.time()
        seen_status = None
        while time.time() - start <= self.valves.RUN_MAX_SECONDS:
            r = requests.get(check_url, headers=headers, timeout=10)
            r.raise_for_status()
            info = r.json()
            status = info.get("status", "unknown")
            if status != seen_status:
                yield f"↻ status: {status}\n"
                seen_status = status
            if status == "success":
                return
            if status in ("error", "timeout", "interrupted"):
                raise RuntimeError(f"Run failed with status: {status}")
            yield "… working\n"
            time.sleep(self.valves.POLL_INTERVAL_SECONDS)
        raise TimeoutError(f"Run timed out after {self.valves.RUN_MAX_SECONDS}s")

    def _fetch_join(self, headers: Dict[str, str], thread_id: str, run_id: str) -> List[Dict[str, Any]]:
        url = f"{self.valves.LANGGRAPH_URL.rstrip('/')}/threads/{thread_id}/runs/{run_id}/join"
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
        data = r.json()
        return data.get("messages", [])

    # ---------- OpenWebUI entrypoint ----------
    def pipe(self, user_message: str, model_id: str, messages: List[Dict[str, Any]], body: Dict[str, Any]) -> Union[str, Iterator[str]]:
        yield "<thinking>"

        try:
            if not messages:
                yield "</thinking>\n\nError: No messages provided"
                return

            # 1) Extract per-user ID token
            id_token = _extract_id_token_from_body(body, self.valves.AUTH_COOKIE_NAME)
            if not id_token and self.valves.STATIC_ID_TOKEN:
                id_token = self.valves.STATIC_ID_TOKEN  # DEV ONLY

            if self.valves.AUTH_REQUIRED and not id_token:
                yield f'</thinking>\n\nYou are not signed in. Please [Sign in]({self.valves.AUTH_LOGIN_URL}).'
                return

            headers = self._auth_headers(id_token)

            # 2) Conversation -> thread
            conv_id = body.get("conversation_id") or body.get("session_id") or f"owui-{abs(hash(str(messages[0].get('content',''))) )% (10**12)}"
            thread_id = self._ensure_thread(headers, f"owui-{conv_id}")

            # 3) Start run
            username = (body.get("user") or {}).get("name") or "unknown"
            latest_text = messages[-1].get("content", "")
            if not isinstance(latest_text, str):
                latest_text = str(latest_text or "")
            run_id = self._start_run(headers, thread_id, latest_text, username)

            # 4) Stream graph traversal
            yield "\n\n### graph traversal\n"
            for line in self._stream_run(headers, thread_id, run_id):
                yield f"{line}\n"

            # 5) Join + final answer
            yield "\n\n📥 Retrieving response..."
            msgs = self._fetch_join(headers, thread_id, run_id)
            if isinstance(msgs, dict):
                msgs = [msgs]

            yield "</thinking>"

            # Extract final assistant text (same logic as before)
            def _parts_to_text(parts) -> str:
                if isinstance(parts, str): return parts
                if isinstance(parts, list):
                    out = []
                    for p in parts:
                        if isinstance(p, dict):
                            t = p.get("text") or p.get("content") or ""
                            if t: out.append(str(t))
                        elif isinstance(p, str):
                            out.append(p)
                    return "\n".join(out)
                if isinstance(parts, dict):
                    return parts.get("text") or parts.get("content") or ""
                return ""

            content = ""
            for m in reversed(list(msgs)):
                if not isinstance(m, dict): continue
                mt = (m.get("type") or m.get("role") or "").lower()
                if mt in ("ai", "assistant"):
                    content = _parts_to_text(m.get("content"))
                    if content: break

            if not content:
                content = "No response content."

            # Try to unwrap JSON {answer, citations}
            try:
                obj = json.loads(content)
                if isinstance(obj, dict):
                    yield f"\n\n{obj.get('answer', content)}"
                    for c in obj.get("citations") or []:
                        quote = (c.get("quote") or "").strip()
                        source = c.get("source", "Source")
                        file_url = c.get("file_url", "#")
                        if quote:
                            yield {
                                "event": {
                                    "type": "citation",
                                    "data": {
                                        "document": [quote],
                                        "metadata": [{"source": source}],
                                        "source": {"name": source, "url": file_url},
                                    },
                                }
                            }
                else:
                    yield f"\n\n{content}"
            except Exception:
                yield f"\n\n{content}"

        except requests.exceptions.RequestException as e:
            yield f"</thinking>\n\n🌐 Network error: {e}"
        except TimeoutError as e:
            yield f"</thinking>\n\n⏰ {e}"
        except Exception as e:
            yield f"</thinking>\n\n⚠️ Unexpected error: {e}"
