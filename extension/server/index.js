#!/usr/bin/env node
/**
 * BP Market MCP — cầu nối stdio ↔ HTTP.
 *
 * Claude Desktop nói stdio; Worker nói JSON-RPC qua HTTP. File này chuyển tiếp
 * giữa hai bên, không phụ thuộc gói ngoài (dùng fetch có sẵn của Node 18+).
 *
 * Lý do không dùng mcp-remote: nó gửi User-Agent của Node và dính Cloudflare
 * 1010 browser_signature_banned, đúng lỗi mà Python-urllib gặp. Ở đây UA được
 * đặt tường minh và sửa được qua env mà không phải đụng vào manifest.
 */

const MCP_URL =
  process.env.BP_MCP_URL || "https://bp-market-mcp.playerpress.workers.dev/mcp";
const UA =
  process.env.BP_MCP_UA ||
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) " +
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36";
const CLIENT_KEY = (process.env.BP_MCP_CLIENT_KEY || "").trim();

let sessionId = null;
let pending = 0;      // so request dang bay
let stdinClosed = false;

function maybeExit() {
  // Khong thoat khi con request chua tra loi, neu khong se nuot ket qua.
  if (stdinClosed && pending === 0) process.exit(0);
}

function send(obj) {
  process.stdout.write(JSON.stringify(obj) + "\n");
}

function fail(id, code, message, data) {
  if (id === undefined || id === null) return; // notification: không trả lời
  send({ jsonrpc: "2.0", id, error: { code, message, data } });
}

function parseBody(text, ctype) {
  if ((ctype || "").includes("text/event-stream")) {
    for (const line of text.split(/\r?\n/)) {
      const l = line.trim();
      if (!l.startsWith("data:")) continue;
      const p = l.slice(5).trim();
      if (!p || p === "[DONE]") continue;
      try {
        return JSON.parse(p);
      } catch {
        /* dòng data không phải JSON — bỏ qua, thử dòng kế */
      }
    }
    return null;
  }
  return text.trim() ? JSON.parse(text) : null;
}

async function forward(msg) {
  pending++;
  try {
    await forwardInner(msg);
  } finally {
    pending--;
    maybeExit();
  }
}

async function forwardInner(msg) {
  const headers = {
    "Content-Type": "application/json",
    Accept: "application/json, text/event-stream",
    "User-Agent": UA,
    "Accept-Language": "en-US,en;q=0.9",
    "X-BP-Client": "bp-market-mcpb/1.0",
  };
  if (sessionId) headers["Mcp-Session-Id"] = sessionId;
  if (CLIENT_KEY) headers["X-BP-Client-Key"] = CLIENT_KEY;

  let res;
  try {
    res = await fetch(MCP_URL, {
      method: "POST",
      headers,
      body: JSON.stringify(msg),
    });
  } catch (e) {
    return fail(msg.id, -32001, `khong ket noi duoc Worker: ${e.message}`);
  }

  const sid = res.headers.get("mcp-session-id");
  if (sid) sessionId = sid;

  const text = await res.text();

  if (!res.ok) {
    // 403 kem body noi ve browser signature => Worker/Cloudflare chan UA.
    return fail(msg.id, -32002, `Worker tra HTTP ${res.status}`, text.slice(0, 600));
  }

  let payload;
  try {
    payload = parseBody(text, res.headers.get("content-type"));
  } catch (e) {
    return fail(msg.id, -32700, `khong doc duoc response: ${e.message}`, text.slice(0, 300));
  }
  if (payload) send(payload);
}

let buf = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  buf += chunk;
  let idx;
  while ((idx = buf.indexOf("\n")) >= 0) {
    const line = buf.slice(0, idx).trim();
    buf = buf.slice(idx + 1);
    if (!line) continue;
    let msg;
    try {
      msg = JSON.parse(line);
    } catch {
      continue; // dòng rác — bỏ, không làm chết cầu nối
    }
    forward(msg);
  }
});
process.stdin.on("end", () => {
  stdinClosed = true;
  maybeExit();
});
