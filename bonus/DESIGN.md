# B2 — Brainstorm Thiết Kế Hệ Thống: Feature Pipeline & Flywheel AI Cho CSKH Đa Kênh & Chống Gian Lận TMĐT

- **Tác giả:** Bùi Lê Gia Huy — MSSV: 2A202602607
- **Môn học:** K4-Track02 — Data Pipeline Engineering
- **Mục tiêu:** Thiết kế kiến trúc data pipeline giải quyết bài toán thực tế cho sản phẩm AI Customer Support & Fraud Detection trên sàn Thương mại Điện tử tại Việt Nam.

---

## 1. Bài toán & Ràng buộc Thực tế

### 1.1. Bối cảnh Nghiệp vụ
Hệ thống tiếp nhận trung bình 50.000 yêu cầu hỗ trợ (tickets) mỗi ngày qua đa kênh: Chatbot In-app (Mobile iOS/Android), Web Portal, Zalo Official Account, và Hotline. Dữ liệu bao gồm:
1. **CDC Database (Postgres):** Đơn hàng, thanh toán (MoMo, ZaloPay, COD), thông tin giao nhận, trạng thái khiếu nại hoàn tiền.
2. **Clickstream & Interaction Logs (Kafka):** Lịch sử duyệt sản phẩm, thao tác nhấn nút khiếu nại, log hội thoại bot-khách hàng.
3. **External Logistics Webhooks:** Tín hiệu giao hàng thành công / giao thất bại từ các đơn vị vận chuyển (GHN, GHTK, Viettel Post, NinjaVan).

### 1.2. Thách thức & Ràng buộc Cốt lõi
- **Dữ liệu đến muộn (Late-arriving Data):** Tín hiệu đối soát COD và cập nhật trạng thái đơn từ shipper thường bị trễ từ 1 đến 3 ngày (do vùng sâu vùng xa mất sóng, đối soát định kỳ cuối tuần). Nếu pipeline gán nhầm sự kiện muộn vào ngày ingest, các mô hình chấm điểm rủi ro người dùng sẽ bị sai lệch hoàn toàn.
- **Tuân thủ Quyền riêng tư (Nghị định 13/2023/NĐ-CP):** Dữ liệu giao nhận chứa PII mức độ cao (Họ tên thật người nhận, SĐT, địa chỉ nhà, số CCCD khi mua hàng giá trị cao). Pipeline bắt buộc phải che PII trước khi đưa vào RAG index và tập train, đồng thời phải hỗ trợ "Quyền được xoá dữ liệu" khi người dùng yêu cầu đóng tài khoản.
- **Rò rỉ Dữ liệu Tương lai (Data Leakage & Point-in-time Parity):** Mô hình AI phân loại ticket và phát hiện gian lận hoàn tiền đòi hỏi feature lúc huấn luyện (train) phải khớp chính xác trạng thái lúc phục vụ (inference). Tuyệt đối không được để lộ trạng thái "đã hoàn tiền" vào thời điểm khách hàng vừa mới tạo yêu cầu khiếu nại.
- **Flywheel Tự động:** Các cuộc hội thoại khách hàng không hài lòng (negative feedback) cần được tự động phân luồng để cải tiến mô hình LLM mà không làm "nhiễm độc" (data poisoning) tập huấn luyện.

---

## 2. Sơ đồ Kiến trúc Tổng thể

```
[Postgres CDC]      [Kafka Clickstream]      [3PL Webhooks]
       │                    │                      │
       ▼                    ▼                      ▼
┌─────────────────────────────────────────────────────────────┐
│ BRONZE LAYER: S3/GCS Object Storage (Raw Immutable Parquet) │
│ - Append-only, lưu nguyên trạng payload (JSON/Debezium)    │
│ - Giữ lại Debezium delete (op='d') và Kafka tombstones      │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ SILVER LAYER: DuckDB / dbt (Entity Tables, Cleaned & Masked)│
│ - Keyed Upsert với LSN Guard (chống hồi sinh bản ghi)       │
│ - Xử lý CDC Delete thành Tombstone (is_deleted=true)        │
│ - Dual PII Masking: Regex (SĐT/Email) + PhoBERT-NER (Tên)   │
│ - Crypto-shredding: Mã hoá PII bằng User KMS Key             │
└──────────────┬───────────────────────────────┬──────────────┘
               │                               │
               ▼                               ▼
┌──────────────────────────────┐ ┌─────────────────────────────┐
│ GOLD: Feature Store          │ │ GOLD: AI & MLOps Assets     │
│ - Microbatch (lookback=3d)   │ │ - Versioned Training Sets   │
│ - Overwrite-partition        │ │ - Point-in-time As-of Join  │
│ - Event-time Aggregations    │ │ - RAG Embeddings Cache      │
└──────────────┬───────────────┘ └─────────────┬───────────────┘
               │                               │
               ▼                               ▼
      [ML Routing Model]               [LLM Triage Agent]
               │                               │
               └───────────────┬───────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ FLYWHEEL: Trace Evaluation & Human-in-the-loop Filter       │
│ - Lọc Downvote/Escalated chats -> Review -> SFT Dataset     │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. Các Quyết định Kiến trúc & Đánh đổi Cốt lõi

### Quyết định 1: Ingestion qua Debezium CDC + Kafka thay vì Định kỳ Query Database (Polling REST/SQL)
- **Phương án chọn:** Sử dụng Debezium CDC bắt trực tiếp transaction log của PostgreSQL, đẩy vào Kafka topic theo định dạng append-only Parquet ở tầng Bronze.
- **Đánh đổi:** 
  - *Chi phí & Phức tạp:* Cần duy trì cụm Kafka Connect, quản lý schema registry và đối mặt với rủi ro schema drift khi bảng nguồn đổi cấu trúc.
  - *Lợi ích vượt trội:* Giảm tải 100% cho database vận hành chính (OLTP). Thu nhận được đầy đủ lịch sử thay đổi kèm theo số thứ tự log (`_lsn`), thời điểm commit (`ts_ms`), và đặc biệt là bắt được sự kiện xoá vật lý (`op = 'd'`). Điều này là bất khả thi nếu chỉ query polling định kỳ (vốn sẽ bỏ sót các bản ghi được tạo và xoá giữa hai lần quét).

### Quyết định 2: Xử lý Late Data bằng Microbatching có Lookback Window thay vì Full-Streaming (Kappa Architecture)
- **Phương án chọn:** Pipeline phân tích chạy microbatching hằng ngày với `batch_size = 'day'` và cửa sổ quét lùi `lookback = 3` ngày (bằng $\lceil P99 \rceil$ đo từ độ trễ vận chuyển của Bronze), áp dụng cơ chế overwrite-partition trên bảng `gold_feature_daily`.
- **Đánh đổi:**
  - *Mất độ trễ dưới giây (sub-second):* Tính năng tổng hợp theo ngày chỉ sẵn sàng sau mỗi batch chạy (SLA T+1 hoặc microbatch 1 giờ).
  - *Lợi ích vượt trội:* Bài toán phân tích hành vi và huấn luyện AI không đòi hỏi sub-second latency. So với việc duy trì cụm Apache Flink 24/7 cực kỳ tốn kém và phức tạp khi quản lý state, kiến trúc microbatch với lookback giúp tái tạo lại (recompute) dữ liệu lịch sử một cách **tuyệt đối idempotent**. Khi các tín hiệu vận chuyển của 3 ngày trước cập bến, toàn bộ partition ngày tương ứng được viết đè chính xác mà không lo trùng lặp dữ liệu hay memory leak.

### Quyết định 3: Bảo vệ Quyền Riêng Tư (PII) Bằng Cơ Chế Crypto-Shredding Kết Hợp PhoBERT-NER Tại Silver
- **Phương án chọn:** 
  1. Thay vì chỉ dùng Regex đơn giản (chỉ bắt được định dạng tĩnh như email, SĐT), tại tầng Silver tích hợp model NLP tiếng Việt (PhoBERT-NER) để phát hiện và thay thế tên người, địa chỉ xã/huyện thành nhãn `[PERSON]`, `[ADDRESS]`.
  2. Áp dụng **Crypto-shredding**: Mỗi khách hàng sở hữu một khoá mã hoá đối xứng riêng trong AWS KMS. Văn bản tự do (chat, nội dung khiếu nại) trước khi lưu vào Silver/Gold được mã hoá với khoá này.
- **Đánh đổi:**
  - *Tăng thời gian xử lý:* Tốc độ enrich ở tầng Silver giảm đi do phải chạy inference model NER và gọi KMS API.
  - *Giải quyết triệt để mâu thuẫn GDPR/Nghị định 13 vs Snapshot bất biến:* Khi người dùng kích hoạt "Quyền được xoá dữ liệu", hệ thống không cần tìm và viết lại toàn bộ hàng nghìn file Parquet/DuckDB snapshot lịch sử (việc này làm sai lệch hash và checksum kiểm thử). Ta chỉ cần **xoá khoá KMS của người dùng đó**. Ngay lập tức, mọi dữ liệu quá khứ của người dùng trong các snapshot lịch sử trở thành chuỗi nhị phân vô nghĩa vĩnh viễn không thể khôi phục, đáp ứng trọn vẹn yêu cầu pháp lý mà vẫn bảo toàn tính bất biến của data lake.

### Quyết định 4: Đảm Bảo Train/Serve Parity Bằng Point-in-time As-of Joins
- **Phương án chọn:** Tạo bảng SCD Type 2 (`silver_ticket_history`) lưu trữ mọi phiên bản của ticket với khoảng thời gian hiệu lực `[valid_from, valid_to)`. Khi tạo tập huấn luyện `gold_training_set`, các đặc trưng (features) và nhãn (labels) chỉ được join tại thời điểm $T_{\text{event}}$ (as-of join).
- **Đánh đổi:**
  - *Dung lượng lưu trữ:* Tăng dung lượng lưu trữ do phải giữ lại toàn bộ lịch sử biến động trạng thái thay vì chỉ lưu một snapshot cuối cùng.
  - *Lợi ích:* Loại bỏ hoàn toàn **Look-ahead Bias / Data Leakage**. Ví dụ: Mô hình phân loại rủi ro lúc khách hàng mở ticket không được biết trước việc 2 ngày sau khách có được hoàn tiền hay không. Nhờ đó, độ chính xác khi đưa ra production phản ánh trung thực kết quả thử nghiệm offline.

### Quyết định 5: Flywheel LLM Khép Kín Với Cơ Chế Kiểm Duyệt Ngăn Chặn Ngộ Độc Dữ Liệu (Data Poisoning)
- **Phương án chọn:** Log hội thoại giữa khách hàng và chatbot được tự động ghi nhận điểm đánh giá (Rating Up/Down). Những hội thoại bị Downvote hoặc bị Escalate sang tổng đài viên người (Agent) sẽ đi vào nhánh kiểm soát chất lượng (Quarantine & Review Queue). Chỉ sau khi chuyên viên CSKH gắn nhãn chuẩn và chỉnh sửa câu trả lời mẫu, dữ liệu mới được đẩy vào kho dữ liệu Fine-tuning (SFT) và Eval Benchmark.
- **Đánh đổi:**
  - *Tạo điểm nghẽn con người (Human bottleneck):* Dữ liệu fine-tune không tăng trưởng tự động 100% theo thời gian thực.
  - *Lợi ích:* Ngăn chặn hiện tượng LLM học lại chính các câu trả lời ảo giác (hallucination) của nó hoặc bị kẻ xấu cố tình thao túng phản hồi để đầu độc mô hình (Adversarial Prompt Injection).

---

## 4. Phương Án Bị Loại (Rejected Alternative)

### Bãi bỏ Kiến trúc Lambda Truyền thống (Apache Spark + Flink + Hadoop HDFS)
- **Mô tả:** Ban đầu, đội ngũ cân nhắc xây dựng một hệ thống theo mô hình Lambda Architecture kinh điển: Apache Flink xử lý streaming thời gian thực cho dashboard giám sát, song song với Apache Spark chạy batch ban đêm để tính toán lại feature trên Hadoop/S3.
- **Lý do loại bỏ dứt khoát:**
  1. **Dual-codebase Mismatch:** Việc phải duy trì 2 bộ mã nguồn độc lập (Java/Scala trên Flink cho real-time và PySpark/SQL cho batch) dẫn đến sự sai lệch không thể tránh khỏi trong logic tính toán feature giữa lúc serve và lúc train (vi phạm Train/Serve Parity).
  2. **Chi phí Hạ tầng & Vận hành Quá Lớn:** Cần cả một đội ngũ DataOps chuyên trách để vận hành cụm Spark/Flink phân tán, quản lý ZooKeeper/YARN/Kubernetes, tốn kém hàng ngàn USD mỗi tháng cho máy chủ nhàn rỗi.
  3. **Giải pháp thay thế hiện đại (Modern Data Stack):** Với khối lượng 50.000 tickets/ngày (tương đương ~5-10 GB dữ liệu nén/ngày), một kiến trúc tinh gọn sử dụng **Object Storage (Parquet) + DuckDB + dbt Core (Microbatching)** có thể hoàn thành toàn bộ công việc trong vòng dưới 3 phút trên một instance ảo duy nhất, đạt độ tin cậy $100\%$ về tính nhất quán, checksum có thể tái lập và chi phí hạ tầng giảm tới $90\%$.

---

## 5. Tổng kết

Bản thiết kế trên chứng minh rằng trong kỹ nghệ dữ liệu hiện đại, năng lực cốt lõi không nằm ở việc áp dụng các công cụ phân tán đắt đỏ nhất, mà ở **khả năng phân tích sâu sắc các ràng buộc nghiệp vụ**:
- Chọn đúng ranh giới **Event time vs Ingest time** để xử lý late data bằng lookback window.
- Áp dụng **LSN Guard & Tombstone** để bảo đảm tính bất biến và idempotent khi chạy lại.
- Dùng **Crypto-shredding** để giải quyết thấu đáo mâu thuẫn giữa quy định pháp luật và tính bất biến của MLOps.
- Thiết kế **Flywheel có kiểm soát** để sản phẩm AI ngày càng thông minh mà không bị tự đầu độc.
