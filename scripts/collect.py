#!/usr/bin/env python3
"""
BP Compass 360 — self-driving collector.

Chạy trên GitHub Actions runner (internet đầy đủ), gọi BP MCP Worker qua
JSON-RPC streamable HTTP, tự giữ ledger và tự đẩy tiến độ mỗi run.

Vì sao tự lái: GitHub App của Claude connector chỉ có quyền ĐỌC repo, không ghi
được. Nên Claude không ra lệnh cho collector được. Collector phải tự quyết
"lần này lấy gì" theo một quy tắc tất định, không cần suy luận.

Quy tắc tất định mỗi run:
  1. snapshot + rpc_health
  2. index signature mới nhất của BYJu  → sig chưa thấy thì đưa vào pending
  3. nếu pending còn ít và chưa quét hết lịch sử → index lùi theo cursor
  4. parse tối đa PARSE_BUDGET signature trong pending, cũ nhất trước
  5. ghi ledger + runlog, commit

Script này CỐ TÌNH NGU. Không phân loại BUY/SELL, không tính VWAP, không suy
luận gì. Nó lưu nguyên văn response. Toàn bộ guardrail của handover —
evidence hierarchy, UNKNOWN không thành fact, balance delta không phải
BUY/SELL — nằm ở phía Claude khi đọc repo.

Không cần secret: Helius key nằm trong Cloudflare Worker runtime (handover §3B).
"""

import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LEDGER_PATH = ROOT / "state" / "ledger.json"
OUT_LATEST = ROOT / "out" / "latest"
OUT_PARSED = ROOT / "out" / "parsed"
RUNLOG_PATH = ROOT / "out" / "runlog.json"

MCP_URL = os.environ.get(
    "BP_MCP_URL", "https://bp-market-mcp.playerpress.workers.dev/mcp"
)
MCP_TOKEN = os.environ.get("BP_MCP_TOKEN", "").strip()

# Cloudflare Browser Integrity Check chặn User-Agent mặc định của urllib
# (Python-urllib/3.x) bằng lỗi 1010 browser_signature_banned. Worker này là của
# mình, nên đây là chuyện cấu hình chứ không phải rào bảo mật của bên thứ ba.
# Sửa bền vững nằm ở phía Cloudflare (WAF skip rule cho header dưới đây);
# UA chỉ là để đi tiếp ngay.
USER_AGENT = os.environ.get(
    "BP_MCP_UA",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
)
# Dấu hiệu riêng để WAF nhận ra client hợp lệ. Đặt BP_MCP_CLIENT_KEY trong
# repo secrets và cho Worker/WAF cho qua khi header này khớp.
CLIENT_KEY = os.environ.get("BP_MCP_CLIENT_KEY", "").strip()

BYJU = "BYJuT6vQwqxdjF8yZbV4zbTc6RV4dPgRQTGBCr3ctmV9"
DQJQ = "DqjqYnihbh64EXpSxtdwpDbYEybzdReXqtvSMhwK6Kke"

# Handover §3: batch 10–20, không vượt 20 khi RPC nóng.
PARSE_BUDGET = int(os.environ.get("BP_PARSE_BUDGET", "20"))
# Còn ít hơn ngần này thì đi lấy thêm trang cũ hơn.
PENDING_LOW_WATER = 40

THROTTLE_NORMAL = 1.4      # §3: bình thường tối thiểu 1.2–1.5s
THROTTLE_AFTER_429 = 4.5   # §3: 4–5s sau khi gặp 429
TIMEOUT = 90

SIG_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{80,90}$")

_session_id = None
_seen_429 = False
_req_id = 0


def utcnow():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg):
    print(f"[{utcnow()}] {msg}", flush=True)


# --------------------------------------------------------------------------
# MCP transport
# --------------------------------------------------------------------------

def _parse_body(raw, content_type):
    text = raw.decode("utf-8", errors="replace")
    if "text/event-stream" in (content_type or ""):
        for line in text.splitlines():
            line = line.strip()
            if line.startswith("data:"):
                payload = line[5:].strip()
                if payload and payload != "[DONE]":
                    try:
                        return json.loads(payload)
                    except json.JSONDecodeError:
                        continue
        raise ValueError(f"không parse được SSE body: {text[:400]}")
    return json.loads(text)


def rpc(method, params=None, notify=False):
    global _session_id, _seen_429, _req_id

    body = {"jsonrpc": "2.0", "method": method}
    if params is not None:
        body["params"] = params
    if not notify:
        _req_id += 1
        body["id"] = _req_id

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "User-Agent": USER_AGENT,
        "Accept-Language": "en-US,en;q=0.9",
        "X-BP-Client": "bp-compass-collector/2.1",
    }
    if CLIENT_KEY:
        headers["X-BP-Client-Key"] = CLIENT_KEY
    if _session_id:
        headers["Mcp-Session-Id"] = _session_id
    if MCP_TOKEN:
        headers["Authorization"] = f"Bearer {MCP_TOKEN}"

    req = urllib.request.Request(
        MCP_URL, data=json.dumps(body).encode("utf-8"), headers=headers, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            sid = resp.headers.get("Mcp-Session-Id")
            if sid:
                _session_id = sid
            if notify:
                return True, None
            payload = _parse_body(resp.read(), resp.headers.get("Content-Type"))
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")[:600]
        if e.code == 429:
            _seen_429 = True
            log("  gặp 429 — chuyển sang throttle chậm, KHÔNG retry cascade")
        return False, {"transport_error": f"HTTP {e.code}", "detail": detail}
    except Exception as e:  # noqa: BLE001
        return False, {"transport_error": type(e).__name__, "detail": str(e)[:600]}

    if isinstance(payload, dict) and "error" in payload:
        return False, {"rpc_error": payload["error"]}
    return True, payload.get("result") if isinstance(payload, dict) else payload


def call_tool(name, args):
    time.sleep(THROTTLE_AFTER_429 if _seen_429 else THROTTLE_NORMAL)
    return rpc("tools/call", {"name": name, "arguments": args})


def unwrap(result):
    """MCP bọc kết quả trong content[].text. Cố mở ra JSON; không được thì trả nguyên."""
    if isinstance(result, dict) and isinstance(result.get("content"), list):
        for item in result["content"]:
            if isinstance(item, dict) and item.get("type") == "text":
                try:
                    return json.loads(item.get("text", ""))
                except (json.JSONDecodeError, TypeError):
                    return item.get("text")
    return result


# --------------------------------------------------------------------------
# Trích signature — schema-agnostic
# --------------------------------------------------------------------------

def extract_signatures(obj):
    """Đi khắp cây JSON gom signature. Không giả định schema của Worker.

    Ưu tiên key tên signature/sig/txSignature. Nếu không có key nào khớp thì
    quét chuỗi trông giống base58 signature. Giữ nguyên thứ tự xuất hiện.
    """
    found, seen = [], set()

    def add(s):
        if isinstance(s, str) and SIG_RE.match(s) and s not in seen:
            seen.add(s)
            found.append(s)

    def walk(node, keyed_only):
        if isinstance(node, dict):
            for k, v in node.items():
                if keyed_only and isinstance(v, str) and k.lower() in (
                    "signature", "sig", "txsignature", "txhash",
                ):
                    add(v)
                walk(v, keyed_only)
        elif isinstance(node, list):
            for v in node:
                walk(v, keyed_only)
        elif not keyed_only:
            add(node)

    walk(obj, keyed_only=True)
    if not found:
        walk(obj, keyed_only=False)
    return found


# --------------------------------------------------------------------------
# Ledger
# --------------------------------------------------------------------------

def load_ledger():
    if LEDGER_PATH.exists():
        return json.loads(LEDGER_PATH.read_text())
    # Seed từ BP_RE_FULL_HANDOVER v2.
    return {
        "_source": "seed từ BP_RE_FULL_HANDOVER_2026-09-14 v2",
        "cursor_older": (
            "jDNWF7XvzcjUbA6U8sQmemiR1mpDJaUJt1199F56vgCCgKAfEuZX1NgyCc6hoU4z5"
            "MzzGSkXrcHnFAX58bKvBdQ"
        ),
        "older_exhausted": False,
        "pending": [],
        "parsed": [],
        "newest_seen": None,
        "runs": 0,
    }


def save_ledger(led):
    LEDGER_PATH.parent.mkdir(parents=True, exist_ok=True)
    LEDGER_PATH.write_text(json.dumps(led, indent=2, ensure_ascii=False))


# --------------------------------------------------------------------------

def main():
    OUT_LATEST.mkdir(parents=True, exist_ok=True)
    OUT_PARSED.mkdir(parents=True, exist_ok=True)

    led = load_ledger()
    known = set(led["pending"]) | set(led["parsed"])

    run = {
        "started_utc": utcnow(),
        "mcp_url": MCP_URL,
        "user_agent": USER_AGENT,
        "client_key_set": bool(CLIENT_KEY),
        "handshake": None,
        "tools_available": None,
        "steps": [],
        "new_signatures": 0,
        "parsed_this_run": 0,
        "saw_429": False,
    }

    def step(name, ok, note=""):
        run["steps"].append({"step": name, "ok": ok, "note": note})

    # 1. handshake -----------------------------------------------------------
    log(f"handshake → {MCP_URL}")
    ok, res = rpc(
        "initialize",
        {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "bp-compass-collector", "version": "2.0"},
        },
    )
    run["handshake"] = {"ok": ok, "result": res}
    if not ok:
        log(f"HANDSHAKE FAILED: {res}")
        # Ghi cả ledger lẫn runlog trước khi thoát: workflow commit theo thư mục,
        # và ledger vắng mặt từng làm hỏng bước commit.
        led["last_run_utc"] = utcnow()
        led["last_error"] = "handshake failed"
        save_ledger(led)
        run["finished_utc"] = utcnow()
        RUNLOG_PATH.write_text(json.dumps(run, indent=2, ensure_ascii=False))
        return 0  # commit runlog để Claude thấy lỗi thật, không im lặng
    rpc("notifications/initialized", {}, notify=True)

    # 2. tool list — giải open edge get_bp_health / get_bp_self_test ---------
    ok, res = rpc("tools/list", {})
    if ok and isinstance(res, dict):
        names = sorted(t.get("name", "?") for t in res.get("tools", []))
        run["tools_available"] = names
        log(f"tools: {names}")
        (OUT_LATEST / "_tools.json").write_text(
            json.dumps(res, indent=2, ensure_ascii=False)
        )
    step("tools/list", ok)

    # 3. market + rpc health ------------------------------------------------
    for jid, tool in (("snapshot", "get_bp_snapshot"), ("rpc_health", "get_bp_rpc_health")):
        ok, res = call_tool(tool, {})
        (OUT_LATEST / f"{jid}.json").write_text(
            json.dumps(
                {"tool": tool, "fetched_utc": utcnow(), "ok": ok, "result": res},
                indent=2,
                ensure_ascii=False,
            )
        )
        step(jid, ok, "" if ok else str(res)[:200])

    # 4. Dqjq — invariant I1: balance phải == tổng sweep --------------------
    ok, res = call_tool(
        "get_bp_wallet_signature_index",
        {
            "address": DQJQ,
            "fromTime": "2026-09-08T00:00:00Z",
            "pageSize": 100,
            "maxPages": 1,
            "maxReturned": 50,
            "outputMode": "full",
            "includeFailed": False,
        },
    )
    (OUT_LATEST / "dqjq_index.json").write_text(
        json.dumps(
            {"fetched_utc": utcnow(), "ok": ok, "result": res}, indent=2, ensure_ascii=False
        )
    )
    step("dqjq_index", ok, "" if ok else str(res)[:200])

    # 5. BYJu — index mới nhất ---------------------------------------------
    ok, res = call_tool(
        "get_bp_wallet_signature_index",
        {
            "address": BYJU,
            "pageSize": 100,
            "maxPages": 1,
            "maxReturned": 80,
            "outputMode": "full",
            "includeFailed": False,
        },
    )
    (OUT_LATEST / "byju_index_new.json").write_text(
        json.dumps(
            {"fetched_utc": utcnow(), "ok": ok, "result": res}, indent=2, ensure_ascii=False
        )
    )
    new_sigs = []
    if ok:
        sigs = extract_signatures(unwrap(res))
        if sigs:
            led["newest_seen"] = sigs[0]
        new_sigs = [s for s in sigs if s not in known]
        # sig mới nhất xử lý trước — hoạt động mới quan trọng hơn lịch sử
        led["pending"] = new_sigs + led["pending"]
        known.update(new_sigs)
        log(f"index mới: {len(sigs)} sig, {len(new_sigs)} chưa thấy")
    step("byju_index_new", ok, f"{len(new_sigs)} mới")

    # 6. BYJu — đi lùi lịch sử khi pending cạn ------------------------------
    if (
        ok
        and not led.get("older_exhausted")
        and len(led["pending"]) < PENDING_LOW_WATER
        and led.get("cursor_older")
    ):
        ok2, res2 = call_tool(
            "get_bp_wallet_signature_index",
            {
                "address": BYJU,
                "before": led["cursor_older"],
                "pageSize": 100,
                "maxPages": 1,
                "maxReturned": 80,
                "outputMode": "full",
                "includeFailed": False,
            },
        )
        (OUT_LATEST / "byju_index_older.json").write_text(
            json.dumps(
                {
                    "fetched_utc": utcnow(),
                    "cursor_used": led["cursor_older"],
                    "ok": ok2,
                    "result": res2,
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        older_new = []
        if ok2:
            sigs2 = extract_signatures(unwrap(res2))
            older_new = [s for s in sigs2 if s not in known]
            led["pending"].extend(older_new)
            known.update(older_new)
            if sigs2:
                led["cursor_older"] = sigs2[-1]
            if not older_new:
                led["older_exhausted"] = True
                log("hết lịch sử — older_exhausted=true")
            log(f"index cũ: {len(sigs2)} sig, {len(older_new)} chưa thấy")
        step("byju_index_older", ok2, f"{len(older_new)} mới")

    # 7. parse batch — cũ nhất trước ----------------------------------------
    batch = led["pending"][-PARSE_BUDGET:] if led["pending"] else []
    if batch:
        ok3, res3 = call_tool(
            "get_bp_transactions_by_signatures",
            {"address": BYJU, "signatures": batch},
        )
        stamp = utcnow().replace(":", "").replace("-", "")
        (OUT_PARSED / f"{stamp}.json").write_text(
            json.dumps(
                {
                    "fetched_utc": utcnow(),
                    "signatures": batch,
                    "ok": ok3,
                    "result": res3,
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        if ok3:
            # chỉ chuyển sang parsed khi call thành công; fail thì giữ pending
            led["pending"] = [s for s in led["pending"] if s not in set(batch)]
            led["parsed"].extend(batch)
            run["parsed_this_run"] = len(batch)
            log(f"parse {len(batch)} sig → out/parsed/{stamp}.json")
        else:
            log(f"parse FAILED, giữ nguyên pending: {str(res3)[:200]}")
        step("parse", ok3, f"{len(batch)} sig")
    else:
        step("parse", True, "pending rỗng")

    # 8. ghi ledger ----------------------------------------------------------
    led["runs"] = led.get("runs", 0) + 1
    led["last_run_utc"] = utcnow()
    led["pending_count"] = len(led["pending"])
    led["parsed_count"] = len(led["parsed"])
    save_ledger(led)

    run["new_signatures"] = len(new_sigs)
    run["saw_429"] = _seen_429
    run["pending_count"] = len(led["pending"])
    run["parsed_count"] = len(led["parsed"])
    run["older_exhausted"] = led.get("older_exhausted", False)
    run["finished_utc"] = utcnow()
    RUNLOG_PATH.write_text(json.dumps(run, indent=2, ensure_ascii=False))

    log(
        f"xong — pending={len(led['pending'])} parsed={len(led['parsed'])} "
        f"older_exhausted={led.get('older_exhausted')}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
