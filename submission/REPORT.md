# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Bùi Lê Gia Huy / 2A202602607
**Repo:** https://github.com/blgihuy/K4-Track02-Day17-BuiLeGiaHuy-2A202602607-DataPipelineEngineering
**Commit bài nộp:** fbf7454
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Antigravity IDE (Gemini) - hỗ trợ phân tích triệu chứng lỗi, refactor pipeline và thực thi kiểm tra verify/pytest/rerun/parity.
**Nguồn tham khảo khác (nếu có):** Slide bài giảng K4-Track02-Day17 (Data Pipeline Engineering, Debezium CDC, dbt microbatch).

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `silver_tickets` có 24 hàng cho 12 `ticket_id` ($n \ne n_{ids}$); T-91 có 3 trạng thái cùng xuất hiện; `test_silver_tickets_one_row_per_ticket` và `test_silver_tickets_latest_state_wins` FAIL. | `gold_feature_daily` lệch checksum so với full recompute (`c50b8851...` $\ne$ `8630e04a...`); user u05 ngày 2026-08-12 chỉ có 2 events (thiếu 3 clicks, 1 feedback down); 3 tests `feature_daily/late_events/lookback` FAIL. | T-97 đã xoá nhưng vẫn còn ở Silver (`is_deleted=False`, còn PII) và Gold; 3 tests `cdc_delete_becomes_tombstone`, `deleted_ticket_leaves_training_and_rag`, `doc_chunks_unique` FAIL (9 chunks thay vì 8). |
| **Nguyên nhân gốc** | `upsert_silver_tickets` chỉ dedup nội bộ từng batch rồi dùng `INSERT INTO` append vào `silver_tickets`, không `MERGE` theo khoá `ticket_id` giữa các batch và không so sánh `_lsn` nên replay batch cũ hoặc chạy nhiều batch gây trùng lặp và ghi đè trạng thái mới hơn. | `LOOKBACK_DAYS = 0` do giả định sai rằng event đến tức thì. Pipeline chỉ ghi đè partition ngày chạy (`day`), khiến các event xảy ra ngày 12 nhưng ingest ngày 15 không được tính bù vào partition ngày 12 của `gold_feature_daily`. | `ticket_changes_sql` chỉ trích `ticket_id` từ `after`, nhưng CDC delete (`_op='d'`) có `after=null` làm `ticket_id` bị null và bị điều kiện `WHERE ticket_id IS NOT NULL` lọc mất, khiến sự kiện xoá không thể đến Silver và Gold. |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: thay `INSERT INTO` bằng `MERGE INTO silver_tickets AS t USING _latest_changes AS s ON t.ticket_id = s.ticket_id WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT VALUES (...)`. | `pipeline/config.py`: đổi `LOOKBACK_DAYS = 3` (bằng $\lceil \text{P99} \rceil = 3$ ngày đo từ Bronze qua `event_lateness_sql`), giúp hàm `build_feature_daily` quét và recompute lại các partition trong cửa sổ `[day - 3, day]`. | `pipeline/staging.py`: trích `ticket_id` qua `coalesce(after.ticket_id, before.ticket_id, key.ticket_id)` để lấy khoá khi `after` null; giữ `_op IS NOT NULL` để phân biệt CDC delete với Kafka tombstone. |
| **Khái niệm trên slide** | Keyed Upsert / MERGE; Idempotent Pipeline; Log Sequence Number (LSN) Guard; Dedup nội bộ batch vs Cross-batch merge. | Event time vs Ingest time; Late-arriving Data; Lookback window ($\lceil \text{P99} \rceil$); Overwrite-partition recompute. | CDC Delete (`after = null`) vs Kafka Tombstone (`value = null`); Tombstone record (`is_deleted = true`); Xoá phải lan truyền (propagation). |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: `silver_tickets` là bảng thực thể có khoá định danh `ticket_id` cần duy trì trạng thái mới nhất dựa trên `_lsn` guard, còn `gold_feature_daily` là bảng tổng hợp theo ngày nên ghi đè toàn bộ partition `event_date` khi có dữ liệu mới/đến muộn để đảm bảo tính idempotent.
- Tombstone thay vì xoá hẳn hàng trong Silver: Lưu tombstone (`is_deleted = true`, PII null, giữ `_lsn`) giúp hệ thống giữ lại thứ tự transaction CDC để khi replay batch cũ không bị ghi đè/hồi sinh thực thể (resurrection), đồng thời phân biệt được giữa việc bản ghi đã bị xoá với chưa từng tồn tại.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: Đảm bảo tính tái lập (reproducibility) trong MLOps để kiểm thử hoặc huấn luyện lại đúng nguyên trạng mô hình quá khứ mà không làm sai lệch lịch sử thực nghiệm.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: Dữ liệu cỡ gigabytes vừa vặn bộ nhớ một máy tính đơn; DuckDB embedded loại bỏ chi phí vận hành cluster Spark phức tạp, khởi động tức thì và chi phí hạ tầng bằng 0.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày 08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
- **Giải pháp Crypto-shredding kết hợp Audited Snapshot Compaction:** Mã hoá các trường PII tự do bằng khoá riêng cho từng người dùng/ticket thông qua KMS; khi có yêu cầu xoá (GDPR), chỉ cần tiêu huỷ khoá giải mã thì dữ liệu trong snapshot cũ vĩnh viễn không thể đọc được mà không làm hỏng tính toàn vẹn của snapshot. Nếu luật định bắt buộc xoá vật lý, thực hiện quy trình bảo trì rewrite snapshot có lưu audit log giải trình và cập nhật lineage cho các model đã huấn luyện.

2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ đặt chốt PII nào, ở tầng nào, và đo nó ra sao?
- **Chốt PII & Vị trí:** Đặt chốt chính tại **tầng Silver** (ngay trước khi vào các bảng phân tích/Gold) bằng mô hình **NER (Named Entity Recognition)** tiếng Việt (như PhoBERT-NER / Presidio) kết hợp từ điển danh từ riêng để gán nhãn và mask thực thể `[PERSON]`. Tại Gold, bổ sung automated data contract test kiểm tra leak PII trước khi đưa vào training hay embedding.
- **Cách đo lường:** Đo **Precision & Recall** của bộ phát hiện PII trên tập benchmark gán nhãn thủ công (ưu tiên Recall $\ge 99.9\%$ để triệt tiêu False Negative). Định kỳ lấy mẫu ngẫu nhiên 1% dữ liệu văn bản ở Silver/Gold để chạy audit scanner (hoặc LLM judge) đo **PII Leakage Rate** với SLO mục tiêu là $0\%$.

## 5. Output (dán nguyên văn)

```text
$ .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ .\.venv\Scripts\python.exe -m pytest
..................................                                       [100%]
34 passed in 3.21s

$ .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ Push-Location dbt_project; try { ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17 } finally { Pop-Location }
05:48:10  Running with dbt=1.12.5
05:48:11  Registered adapter: duckdb=1.11.0
05:48:15  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
05:48:15  Concurrency: 1 threads (target='dev')
05:48:17  1 of 19 START sql view model main.stg_events ................................... [RUN]
05:48:18  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.14s]
05:48:18  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
05:48:18  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.05s]
05:48:18  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
05:48:18  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.11s]
05:48:18  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
05:48:18  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.16s]
05:48:18  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
05:48:18  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.20s]
05:48:18  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
05:48:18  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.05s]
05:48:18  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
05:48:18  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.03s]
05:48:18  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
05:48:18  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.03s]
05:48:18  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
05:48:18  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.03s]
05:48:18  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
05:48:18  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.03s]
05:48:18  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
05:48:18  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.03s]
05:48:18  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
05:48:18  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.02s]
05:48:18  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
05:48:18  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.02s]
05:48:18  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
05:48:18  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.02s]
05:48:18  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
05:48:18  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.03s]
05:48:18  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
05:48:18  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
05:48:18  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.04s]
05:48:18  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
05:48:19  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.09s]
05:48:19  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
05:48:19  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.05s]
05:48:19  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
05:48:19  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.04s]
05:48:19  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
05:48:19  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.04s]
05:48:19  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
05:48:19  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.05s]
05:48:19  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
05:48:19  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.04s]
05:48:19  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.39s]
05:48:19  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
05:48:19  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.03s]
05:48:19  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
05:48:19  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.03s]
05:48:19  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
05:48:19  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.02s]
05:48:19  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 4.40 seconds (4.40s).
05:48:19  Completed successfully
05:48:19  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree

$ .\.venv\Scripts\python.exe -m scripts.bonus_llm
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS
```

### Bằng chứng Bonus
- **B1 (LLM Transform & Cache):** Triển khai trong `pipeline/llm_label.py`, kiểm thử đạt `BONUS PASS` qua `scripts.bonus_llm`.
- **B2 (System Architecture Brainstorm):** Tài liệu thiết kế chi tiết tại [`bonus/DESIGN.md`](../bonus/DESIGN.md).

