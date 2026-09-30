# order-flow-live

Bản order flow **cập nhật liên tục trong phiên** — dự án riêng, không dùng chung gì với `order-flow` (bản sau phiên).
Kế hoạch đầy đủ: `C:\Users\IT\.claude\plans\h-y-nghi-n-c-u-3-keen-wirth.md`.

## Hiện có: giai đoạn −1 — bộ gom kho toàn thị trường (chi phí 0)

GitHub Actions `collect` chạy 15:10 thứ Hai–Sáu (bù 15:30, 17:00, 20:00). Danh sách mã lấy thẳng từ VNDirect —
**toàn bộ cổ phiếu đang niêm yết HOSE + HNX** (704 mã, 30/09/2026; UPCOM bỏ theo yêu cầu), không phụ thuộc KingStock. Mỗi mã tải
toàn bộ lệnh khớp phiên hôm nay; mã không khớp lệnh hôm nay ghi vào danh sách "không GD".

| Nơi lưu | Nội dung |
|---|---|
| GitHub **Release** `t<ngày>` → `ticks-<ngày>.tar` | mỗi mã một `<MÃ>.json.gz`: `[giờ, giá thô, KL, side, KL luỹ kế]` — giữ mãi |
| `data/days/<ngày>.json` | mục lục ngày: mỗi mã số tick, KL, mua/bán chủ động, cp hụt; mã không GD; mã lỗi |
| `data/state.json` | kết quả lượt gom gần nhất, số phiên trong kho (`sessions`) |
| `data/symbols.json` | bản chụp danh sách mã (dự phòng khi endpoint danh sách lỗi) |

Tick thô không vào git (vài MB/ngày × hàng trăm phiên sẽ làm repo quá nặng). Nến 5'/footprint dựng lại từ tick bằng
`flow.ticks.session_record` — test phiên vàng FPT 25/09 chứng minh trùng tuyệt đối kho order-flow.

## Chạy trên máy

```
venv\Scripts\python -m pytest -q
venv\Scripts\python -m job.collect                  # sau 14:50; trước giờ đó job tự dừng
venv\Scripts\python -m job.collect --only FPT,VCB   # thử vài mã
venv\Scripts\python -m job.collect --force          # gom lại cả mã đã có
```
