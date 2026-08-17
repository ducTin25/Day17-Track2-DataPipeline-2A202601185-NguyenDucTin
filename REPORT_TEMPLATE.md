# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** …  **Lớp:** AICB-P2T2  **Ngày:** …

---

## 0 · Kết quả `make verify`

<details>
<summary>Kết quả verify sau khi hoàn thành nhiệm vụ 1 và 2</summary>

```
gold_training_set     ✓ ok              12,480      12,480   ✓
gold_feature_daily    ✓ ok               9,100       9,100   ✓
gold_doc_chunks        ✓ ok              31,200      31,200   ✓

gold_training_set     8622572a97  8622572a97  8622572a97  ✓
gold_feature_daily    3db448685c  3db448685c  3db448685c  ✓
gold_doc_chunks       92d8e50131  92d8e50131  92d8e50131  ✓
```

</details>

Tại thời điểm này, 3/4 tiêu chí chính đạt: nhiệm vụ 1, nhiệm vụ 2 và bảng
đối chứng `gold_doc_chunks`. Tiêu chí chất lượng dữ liệu còn chờ nhiệm vụ 3.
`make verify` dừng ở phần kiểm tra dashboard tùy chọn vì chưa chạy
`make seed-extra`; điều này không ảnh hưởng đến hai nhiệm vụ đã hoàn thành.

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | `gold_training_set` tăng từ 12.480 lên 38.750 hàng và checksum thay đổi sau mỗi lượt chạy. |
| **Nguyên nhân** | Bảng có grain 1 hàng / `ticket_id` nhưng model incremental không có `unique_key`, nên dbt dùng cách append. Ticket có `op='u'` xuất hiện ở cả ngày tạo và ngày cập nhật; retry cũng thêm lại cùng ticket thay vì cập nhật hàng cũ. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'`. |
| **Bằng chứng** | trước: 38.750 hàng · sau: 12.480 hàng · checksum sau 3 lượt: `8622572a97`, `8622572a97`, `8622572a97` |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` chỉ có 8.645/9.100 hàng; thiếu tập trung ở các ngày cũ vì event xảy ra trước nhưng tới kho muộn. |
| **P99 độ trễ đo được** | **2.7258 ngày** *(≈ 2.73 ngày)* |
| **Lookback đã chọn** | **3 ngày** — làm tròn lên từ P99 để bao phủ dữ liệu đến muộn |
| **Nguyên nhân** | Điều kiện `event_date > max(event_date)` chỉ nhận ngày lớn hơn watermark hiện tại. Một event ngày 08-12 tới kho ngày 08-15 không còn thỏa điều kiện khi bảng đã có dữ liệu đến ngày 08-14, nên bị bỏ qua vĩnh viễn. |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql`: mở lookback 3 ngày bằng `event_date >= max(event_date) - interval 3 day`; thêm `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'merge'` để lần tính lại thay thế hàng cũ. |
| **Bằng chứng** | trước: 8.645 hàng · sau: 9.100 hàng · checksum sau 3 lượt: `3db448685c`, `3db448685c`, `3db448685c` |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> Truy vấn đo được `max = 2.9447 ngày` và tỷ lệ bản ghi đến muộn hơn 1 ngày là
> `0.050509`, tương đương khoảng **5.05%**. Chọn P99 thay vì max vì P99 bao phủ
> 99% dữ liệu nhưng không làm lookback bị chi phối bởi một vài giá trị cực đại.
> P99 = 2.7258 ngày nên chọn lookback 3 ngày; chi phí là mỗi lượt chạy phải
> tính lại thêm 3 ngày dữ liệu.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | |
| **Nguyên nhân** | |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | |
| **Cách khắc phục** | |
| **Bằng chứng** | `quarantine_tickets` = … hàng · `dbt test` … pass |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> …

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | A / B / không làm |
| **Nguyên nhân** | |
| **Cách khắc phục** | |
| **Bằng chứng** | |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Kiểm tra grain, khóa tự nhiên và tính idempotent của model incremental khi retry. |
| 2 | Đo độ trễ dữ liệu đến kho trước khi chọn watermark/lookback; sau đó kiểm tra cơ chế upsert khi tính lại window. |
| 3 | |
