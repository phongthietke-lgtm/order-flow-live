# order-flow-live

Bản order flow **cập nhật liên tục trong phiên** — dự án riêng, không dùng chung gì với `order-flow` (bản sau phiên).
Kế hoạch đầy đủ: `C:\Users\IT\.claude\plans\h-y-nghi-n-c-u-3-keen-wirth.md`.

## Hiện có: giai đoạn −1 — bộ gom kho (chi phí 0)

GitHub Actions `collect` chạy 15:10 thứ Hai–Sáu (bù 15:30, 17:00, 20:00), lấy danh mục từ KingStock, tải toàn bộ
lệnh khớp phiên hôm nay từ VNDirect và lưu:

| Đường dẫn | Nội dung |
|---|---|
| `data/ticks/<MÃ>/<ngày>.json.gz` | lệnh khớp gốc `[giờ, giá thô, KL, side, KL luỹ kế]` — giữ mãi |
| `data/store/<MÃ>/<ngày>.json` | nến 5' có footprint, cùng định dạng kho `order-flow/data/store` |
| `data/state.json` | kết quả lượt gom có phiên mới gần nhất, số phiên trong kho |
| `data/watchlist.json` | bản chụp danh mục KingStock (dự phòng khi Fly chết) |

Khi dựng máy chủ trong phiên (giai đoạn 1), kho này nạp thẳng làm nền 20 phiên.

## Chạy trên máy

```
venv\Scripts\python -m pytest -q
venv\Scripts\python -m job.collect            # sau 15:00; trong giờ phiên job tự bỏ qua phiên dở dang
venv\Scripts\python -m job.collect --force    # gom lại cả mã đã có
```
