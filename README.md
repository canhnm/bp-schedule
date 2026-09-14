# BP Compass 360 — data plane

Repo này tồn tại vì hai ràng buộc hạ tầng, không phải vì thích microservice:

1. Sandbox của Claude Cowork bị egress allowlist của org chặn — không ra được
   `bp-market-mcp.playerpress.workers.dev`, `bp.pix.im`, Helius, hay bất kỳ
   Solana RPC nào. **Chỉ GitHub đi ra được.**
2. GitHub App của Claude connector chỉ có quyền **đọc** repo. Claude không ghi
   được, nên không ra lệnh cho collector được.

Từ hai ràng buộc đó ra kiến trúc:

```
GitHub Actions (data plane)         Claude (analysis plane)
────────────────────────────        ───────────────────────
cron :25 mỗi giờ                    scheduled task :37 mỗi giờ
  ↓                                   ↓
đọc state/ledger.json               đọc out/ + state/ qua GitHub connector
  ↓                                   ↓
tự quyết lấy gì (quy tắc tất định)  phân loại, reconcile, viết report tiếng Việt
  ↓                                   ↓
gọi BP MCP Worker (JSON-RPC)        → report + push notification
  ↓
ghi out/, cập nhật ledger, commit
```

Một chiều dữ liệu, một chiều ghi. Claude chỉ đọc.

## Collector tự lái thế nào

`scripts/collect.py` chạy đúng một quy tắc tất định mỗi lần, không suy luận:

1. `get_bp_snapshot` + `get_bp_rpc_health`
2. index signature mới nhất của BYJu → sig chưa thấy vào `pending`
3. nếu `pending` < 40 và chưa quét hết lịch sử → index lùi theo `cursor_older`
4. parse tối đa 20 signature trong `pending`, **cũ nhất trước**
5. ghi `state/ledger.json` + `out/runlog.json`, commit

Throttle theo handover §3: 1.4s giữa call, 4.5s sau khi gặp 429, và **không
retry cascade** — gặp 429 thì batch đó giữ nguyên trong `pending`, run sau làm
lại, không nhảy sang public RPC.

## Vì sao collector cố tình ngu

Nó không phân loại BUY/SELL, không tính VWAP, không suy luận. Nó lưu nguyên văn
response. Toàn bộ guardrail của handover — evidence hierarchy, `UNKNOWN` không
thành fact, balance delta không phải BUY/SELL, không bịa tổng cho signature
failed — nằm ở phía Claude. Nhét logic phân loại vào script là cách nhanh nhất
để mất chúng.

Hệ quả phụ đáng giá: git history thành audit trail. Mỗi run một commit, mỗi thay
đổi kết luận một diff. Một `UNKNOWN` bị nâng thành fact sẽ hiện ra trong diff.

Trích signature dùng cách schema-agnostic (tìm key `signature`/`sig`, fallback
quét chuỗi base58 80–90 ký tự) vì schema response của Worker chưa được xác minh.
Xem `out/runlog.json` → `steps[].note` để biết mỗi run bắt được bao nhiêu sig.

## Layout

```
scripts/collect.py        collector
state/ledger.json         collector ghi — cursor, pending, parsed
state/baseline.json       hằng số từ handover — người sửa tay, collector không đụng
out/latest/*.json         snapshot, rpc_health, index mới nhất
out/parsed/*.json         raw transaction đã parse, tích luỹ theo thời gian
out/runlog.json           kết quả run gần nhất, kể cả khi fail
```

## Secret

**Không có.** Helius API key nằm trong Cloudflare Worker runtime
(`wrangler secret put SOLANA_RPC_URL`) — đúng handover §3B, không đi qua repo này.
`BP_MCP_TOKEN` chỉ cần nếu sau này Worker bật auth.

## Setup

1. Settings → Actions → General → Workflow permissions → **Read and write**.
   (Không có bước này thì commit ở cuối workflow sẽ fail.)
2. Actions → BP collect → **Run workflow** để chạy thử ngay, không đợi cron.
3. Mở `out/latest/_tools.json` — lần đầu biết chắc tool list thật của Worker,
   giải quyết open edge `get_bp_health` / `get_bp_self_test`.
4. Mở `out/runlog.json` — nếu `handshake.ok = false` thì Worker hoặc URL sai,
   sửa trước khi tin bất cứ số nào.

## Chi phí

Private repo free tier: 2.000 phút Actions/tháng. 720 run/tháng × ~1 phút ≈ 720
phút. Nếu parse nặng hơn thì hạ tần suất xuống 2h/lần trước khi nghĩ tới repo public.

## Giới hạn cần biết

- Cron GitHub Actions **không đúng giờ tuyệt đối**, trễ vài phút khi hệ thống bận.
  Đệm 12 phút trước lượt Claude là để chịu độ trễ đó.
- Repo im 60 ngày thì Actions tự tắt schedule — cron hourly không dính.
- Claude chỉ đọc. Muốn Claude điều khiển collector thì phải cấp quyền ghi cho
  GitHub App của connector; hiện chưa cần.
