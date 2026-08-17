# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** …  **Lớp:** AICB-P2T2  **Ngày:** …

---

## 0 · Kết quả `make verify`

<details>
<summary>Kết quả verify sau khi hoàn thành ba nhiệm vụ chính</summary>

```
gold_training_set     ✓ ok              12,480      12,480   ✓
gold_feature_daily    ✓ ok               9,100       9,100   ✓
gold_doc_chunks        ✓ ok              31,200      31,200   ✓
quarantine_tickets     ✓ ok                 312         312   ✓

gold_training_set     8dd7c98653  8dd7c98653  8dd7c98653  ✓
gold_feature_daily    3db448685c  3db448685c  3db448685c  ✓
gold_doc_chunks       92d8e50131  92d8e50131  92d8e50131  ✓
quarantine_tickets    ebb89036fb  ebb89036fb  ebb89036fb  ✓

dbt test                                    ✓ 11/11 pass
silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
dashboard rows scanned                      ✗ 5,000,000 → 5,000,000 (1.0×)
DAG: catchup / max_active_runs              ✗ True / None
```

</details>

Các tiêu chí chính của rubric đạt **4/4**. Hai bài mở rộng chưa làm: dashboard
chưa compact dữ liệu Parquet và DAG chưa cấu hình `catchup/max_active_runs`.

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | `gold_training_set` tăng từ 12.480 lên 38.750 hàng và checksum thay đổi sau mỗi lượt chạy. |
| **Nguyên nhân** | Bảng có grain 1 hàng / `ticket_id` nhưng model incremental không có `unique_key`, nên dbt dùng cách append. Ticket có `op='u'` xuất hiện ở cả ngày tạo và ngày cập nhật; retry cũng thêm lại cùng ticket thay vì cập nhật hàng cũ. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'`. |
| **Bằng chứng** | trước: 38.750 hàng · sau: 12.480 hàng · checksum sau 3 lượt: `8dd7c98653`, `8dd7c98653`, `8dd7c98653` |

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
| **Triệu chứng** | Sau ngày 08-10, các nhãn chữ hợp lệ bị `NULL`, còn các giá trị `0`, `5`, `-1` vẫn lọt vào Silver; model phân loại nhận dữ liệu ngoài contract. |
| **Nguyên nhân** | Backend đổi cách biểu diễn từ số sang nhãn chữ nhưng macro chỉ dùng `try_cast`: macro không map được `urgent/high/medium/low` và lại chấp nhận các số ngoài miền `1..4`. Đồng thời Silver xếp hạng trước khi lọc, nên một bản ghi mới nhất bị lỗi có thể làm mất cả ticket; contract và test miền giá trị cũng đang tắt. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | `1/2/3/4` giữ nguyên; `urgent/high/medium/low` map lần lượt thành `1/2/3/4`; `P1/unknown/0/5/-1/''/NULL` trả về `NULL` và đưa vào quarantine. |
| **Cách khắc phục** | `normalize_priority.sql`: dùng `CASE` chung cho Silver và quarantine. `silver_tickets.sql`: chuẩn hóa và lọc bản ghi lỗi trước `row_number`. `quarantine_tickets.sql`: lọc các giá trị macro trả về `NULL`. `schema.yml`: bật `contract.enforced` và thêm `not_null`/`accepted_values` cho priority. |
| **Bằng chứng** | `quarantine_tickets` = **312** hàng · `dbt test` **11/11 pass** · priority sạch · Silver giữ đủ **12.480** ticket |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> Không chặn ở Bronze vì bản ghi lỗi cần được giữ lại để truy vết và xử lý sau;
> Bronze vẫn giữ payload gốc. Silver là nơi chuẩn hóa dữ liệu, loại bản ghi
> không hợp lệ khỏi dữ liệu phục vụ model và định tuyến chúng sang quarantine.
> Pipeline không nên dừng vì 312 bản ghi lỗi không được phép chặn toàn bộ dữ
> liệu hợp lệ; contract và test vẫn bảo vệ downstream, còn quarantine giữ lại
> bằng chứng để người trực xử lý.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | Không làm |
| **Nguyên nhân** | — |
| **Cách khắc phục** | — |
| **Bằng chứng** | `make verify`: dashboard chưa tối ưu, 5.000 file Parquet vẫn còn |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Kiểm tra grain, khóa tự nhiên và tính idempotent của model incremental khi retry. |
| 2 | Đo độ trễ dữ liệu đến kho trước khi chọn watermark/lookback; sau đó kiểm tra cơ chế upsert khi tính lại window. |
| 3 | Đối chiếu schema evolution với contract: phân biệt đổi cách biểu diễn hợp lệ với dữ liệu lỗi, rồi kiểm tra quarantine và miền giá trị. |
