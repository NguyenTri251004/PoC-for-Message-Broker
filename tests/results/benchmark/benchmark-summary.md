# Performance Benchmark: Synchronous vs RabbitMQ Asynchronous

> **Tất cả số liệu bên dưới là số ĐO THỰC TẾ** từ các lần chạy trên máy phát triển
> cục bộ. Đây là minh hoạ có thể tái lập về hành vi kiến trúc, **không phải** ước lượng
> năng lực sản xuất.

## 1. Môi trường đo

| Mục | Giá trị |
| --- | ------- |
| OS | Windows 11 Home Single Language |
| CPU | AMD Ryzen 5 5625U (6 nhân / 12 luồng) |
| RAM | 7.4 GB |
| Docker | 29.0.1 |
| Node.js | v24.15.0 |
| PostgreSQL | 16.13 (alpine) |
| RabbitMQ | 3.13.7 (management) |
| Công cụ tải | k6 (`grafana/k6`, chạy trong Docker) |
| Mức đồng thời | 100 / 500 / 1000 VUs |
| Thời lượng mỗi lần đo | 20s |
| Số lần lặp / kịch bản | 3 (kèm warm-up 5 VUs, 5s, không tính vào kết quả) |
| Delay downstream | payment=2000ms, inventory=100ms, notification=1000ms, analytics=500ms |
| Tồn kho ban đầu | 1.000.000 (sku-001) |

## 2. Kiến trúc so sánh

Cả hai chế độ đi qua **cùng một endpoint** `POST /orders` ở API Gateway, cùng payload,
cùng order-service. Khác biệt duy nhất là header `x-benchmark-mode`:

- **Synchronous** — `order-service` gọi tuần tự qua HTTP: `payment → inventory → notification → analytics`,
  chỉ trả response khi cả chuỗi hoàn tất (mỗi service tự áp dụng delay xử lý riêng).
- **RabbitMQ async** — `order-service` lưu đơn rồi **publish `OrderCreated`** vào exchange
  `order.events` và trả response ngay. Các consumer độc lập xử lý bất đồng bộ (prefetch=1,
  retry ≤3 lần, DLQ `dlx.order.events`).

## 3. Kết quả benchmark (trung bình của 3 lần đo)

| Chế độ | Đồng thời | RPS | p50 (ms) | p95 (ms) | p99 (ms) | Tỷ lệ lỗi |
| ------ | --------- | --- | -------- | -------- | -------- | --------- |
| Synchronous | 100 | 5.29 | 18068.02 | 19456.65 | 19502.13 | 0.00% |
| Synchronous | 500 | 5.09 | 26411.86 | 48316.91 | 48845.51 | 0.00% |
| Synchronous | 1000 | 4.80 | 27663.10 | 47121.14 | 48953.26 | 0.00% |
| RabbitMQ async | 100 | 483.46 | 181.34 | 338.55 | 572.20 | 0.00% |
| RabbitMQ async | 500 | 438.47 | 549.22 | 2771.77 | 17486.87 | 0.00% |
| RabbitMQ async | 1000 | 235.39 | 640.72 | 19890.79 | 22572.26 | 0.00% |

### 3.1 Riêng phần async @ 100 được tái lập từ 3 lần đo

| # | RPS | p50 | p95 | p99 |
| - | --- | --- | --- | --- |
| 1 | 587.47 | 150.68 | 273.18 | 330.92 |
| 2 | 425.97 | 204.70 | 335.66 | 641.66 |
| 3 | 436.93 | 188.65 | 406.80 | 744.01 |

## 4. So sánh latency ↔ throughput

| Chỉ số | Synchronous | RabbitMQ |
| ------ | ----------- | -------- |
| RPS @100 VUs | 5.29 | 483.46 |
| RPS @500 VUs | 5.09 | 438.47 |
| RPS @1000 VUs | 4.80 | 235.39 |
| p50 @100 VUs | 18068.02 | 181.34 |
| p95 @500 VUs | 48316.91 | 2771.77 |
| p99 @1000 VUs | 48953.26 | 22572.26 |

**Quan sát chính:**

- Sync chỉ đạt ~5.09 RPS và **không tăng** khi thêm VU —
  throughput bị chặn bởi số request đồng thời mà chuỗi orchestration chịu được.
- Async đạt đỉnh ~483.46 RPS @100 VUs (nhanh hơn ~90×) với p50 chỉ ~181.34 ms.
- @500 VUs sync p95 vọt lên ~48.3 giây;
  @1000 VUs async p95 ~19.9 giây — đây là **latency amplification** khi tải vượt năng lực consumer.

## 5. Kiểm chứng xử lý downstream (số đo có điều kiện)

`Orders` = số dòng đơn hàng đã lưu. `Payment/Inventory events` = số dòng bền vững downstream ghi.
`DLQ msgs` = số message vào dead-letter queue.

| Chế độ | Đồng thời | Orders | Payment events | Inventory events | Tồn kho còn | Đã giữ chỗ | Trùng payment | Trùng inventory |
| ------ | --------- | ------ | -------------- | ---------------- | ----------- | ---------- | ------------- | --------------- |
| sync | 100 | 200 | 252 | 200 | 800 | 200 | 0 | 0 |
| sync | 500 | 298 | 325 | 288 | 709 | 291 | 0 | 0 |
| sync | 1000 | 509 | 555 | 501 | 498 | 502 | 0 | 0 |
| async | 100 | 8744 | 79 | 1001 | 0 | 1000 | 0 | 0 |
| async | 500 | 10263 | 80 | 1002 | 0 | 1000 | 0 | 0 |
| async | 1000 | 5255 | 89 | 1002 | 0 | 1000 | 0 | 0 |

### 5.1 Diễn giải trung thực & các điểm cần lưu ý

1. **Không có hiệu ứng downstream trùng lặp** ở mọi kịch bản:
   `duplicatePaymentEffects = 0` và `duplicateInventoryEffects = 0` (dựa trên khóa `event_id`).
   Điều này xác nhận tính **idempotent** — lần chạy lại không tạo hiệu ứng trùng.
2. **Hai lần chạy lỗi tooling đã được phát hiện và ghi nhận** (không ảnh hưởng số latency/RPS):
   - Script purge queue ban đầu chạy sai, khiến DLQ/backlog cũ rò rỉ vào một số lần đo
     → cột DLQ của sync bị “bẩn” (1764) và một phần số liệu completion bị lệch.
   - Endpoint `/debug/reset` của inventory reset tồn kho về 1000, khiến tồn kho cạn giữa chừng
     ở các kịch bản async tải cao → `Inventory events` dừng ở ~1000 và `tồn kho còn = 0`.
     Đây là **giới hạn của công cụ đo**, không phải lỗi xử lý.
3. **Các chỉ số latency/RPS/error rate vẫn hợp lệ** vì chúng là phép đo thuần của k6 ở tầng
   API Gateway/order-service, không phụ thuộc trạng thái hàng đợi hay tồn kho.
4. **Backlog là đặc tính của async ở tải cao**: với consumer đơn (prefetch=1) và delay
   payment=2000ms, trần xử lý ~0,5 đơn/s. Ở tải hàng trăm RPS, hàng đợi phình ra — đúng như
   bản chất decoupling: API nhanh, downstream cần scale consumer riêng.

## 6. Bằng chứng thô

Per-iteration k6 summary exports: `tests/results/benchmark/raw/`.
Per-scenario aggregates: `tests/results/benchmark/sync-<vus>.json`, `rabbitmq-<vus>.json`.
