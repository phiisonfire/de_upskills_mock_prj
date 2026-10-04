# MOCK PROJECT — XÂY DỰNG MOVIE DATA PLATFORM

 

- **Dataset:** [MovieLens 20M — Kaggle](https://www.kaggle.com/datasets/grouplens/movielens-20m-dataset) (nguồn gốc: GroupLens Research, University of Minnesota)
- **Phạm vi đánh giá:** Lakehouse architecture · Incremental processing · Common Data Model · Slowly Changing Dimension · Data Quality Framework · Dimensional Modeling

 

---

 

## 1. Bối cảnh nghiệp vụ

 

Bạn là Data Engineer của **CineInsight**, một nền tảng streaming phim đang xây dựng hệ thống phân tích tập trung. Ban lãnh đạo đặt ra ba yêu cầu:

 

> - Thứ nhất, hệ thống phải chạy hằng ngày và chỉ nạp phần dữ liệu mới — làm lại từ đầu mỗi ngày thì chi phí không kham nổi.
> - Thứ hai, sắp tới chúng ta tích hợp thêm nhiều nguồn khác. Chuẩn hoá về một mô hình chung ngay từ bây giờ, đừng để sau này phải làm lại.
> - Thứ ba, tôi cần biết một bộ phim **tại thời điểm tháng trước** được phân loại thể loại gì — không phải chỉ biết hiện tại nó là gì.

 

Đó chính là ba trụ cột của bài toán: 
- **incremental ingestion với CDC/watermark**
- **chuẩn hoá theo Common Data Model**
- **quản trị lịch sử thay đổi bằng Slowly Changing Dimension**

 

---

 

## 2. Đặc tả dataset

 

### 2.1. Thông tin tổng quan

 

| Thuộc tính | Số bản ghi |
|---|---|
| Rating | **20.000.263** |
| Tag | **465.564** |
| Movie | **27.278** |
| Link | **27.278**|
| Genome Tag | **1128** |
| Genome Score | **11.709.768** |

 

 

> **Nguyên tắc của đề bài:** phần dưới đây mô tả **cấu trúc và đặc tính đã biết** của dữ liệu. Việc profiling, phát hiện vấn đề chất lượng và **quyết định phương án xử lý** là trách nhiệm của bạn — đề bài không chỉ định sẵn cách làm, và bạn phải bảo vệ được lựa chọn của mình.

 

### 2.2. `rating.csv`

 

| Cột | Kiểu dữ liệu | Đặc tả |
|---|---|---|
| `userId` | `int64` | Định danh người dùng |
| `movieId` | `int64` | Khoá ngoại tới `movie.csv` |
| `rating` | `float64` | Thang đánh giá tối đa 5 sao, **bước nhảy 0.5**, miền hợp lệ [0.5, 5.0] → 10 giá trị rời rạc |
| `timestamp` | `datetime` | YYYY-MM-DD HH:mm:ss |

 

**Đặc tính kỹ thuật cần lưu ý:**
- Đây là bảng lớn nhất về số dòng (~20 triệu) → là đối tượng chính của **partitioning strategy** và là nơi bộc lộ **data skew**.
- Khoá nghiệp vụ tự nhiên là cặp `(userId, movieId)`. Bạn cần **tự kiểm chứng** tính duy nhất của cặp khoá này thay vì giả định.

 

### 2.3. `movie.csv`

 

| Cột | Kiểu dữ liệu | Đặc tả |
|---|---|---|
| `movieId` | `int64` | Khoá chính |
| `title` | `string` | Tên hiển thị |
| `genres` | `string` | Danh sách thể loại, **phân tách bởi ký tự `\|`** |

 

**Các vấn đề chất lượng đã biết trong cột `title`:**
- Năm phát hành được **nhúng trong chuỗi tên**, đặt trong ngoặc đơn ở cuối: `Toy Story (1995)`. Không tồn tại cột `year` riêng — bạn phải **trích xuất bằng regex** và xử lý các dòng không khớp mẫu.
- Phim nước ngoài thường mang **tên gốc trong ngoặc đơn** đứng trước năm: `Amelie (Fabuleux destin d'Amélie Poulain, Le) (2001)` → chuỗi có **nhiều cặp ngoặc đơn**
- Có ký tự Unicode có dấu và một số bản ghi bị **lỗi encoding**; một số ít dòng thiếu hẳn năm phát hành.

 

**Đặc tả cột `genres`:**
- Tập thể loại theo tài liệu gốc: Action, Adventure, Animation, Children, Comedy, Crime, Documentary, Drama, Fantasy, Film-Noir, Horror, Musical, Mystery, Romance, Sci-Fi, Thriller, War, Western, IMAX.
- Phim không có thể loại được đánh dấu bằng chuỗi literal **`(no genres listed)`** — đây là sentinel value, **không phải null**, và phải được xử lý tường minh.

 

### 2.4. `tag.csv`

 

| Cột | Kiểu dữ liệu | Đặc tả |
|---|---|---|
| `userId` | `int64` | Người gắn tag |
| `movieId` | `int64` | Phim được gắn tag |
| `tag` | `string` | **Free-text do người dùng tự nhập, hoàn toàn không kiểm soát** |
| `timestamp` | `datetime` | YYYY-MM-DD HH:mm:ss |

 

**Đặc tính cần lưu ý:**
- Khối lượng chỉ ~465 nghìn dòng — **nhỏ hơn rating khoảng 43 lần**. Chỉ một tỉ lệ nhỏ người dùng có hành vi gắn tag → tập người dùng của `tag.csv` là **tập con** của `rating.csv`.
- Dữ liệu bẩn điển hình: khác biệt hoa/thường, khoảng trắng thừa đầu–cuối, dấu câu tuỳ tiện, tag rỗng hoặc chỉ chứa khoảng trắng, nhiều biến thể cùng nghĩa (`sci-fi` / `Sci-Fi` / `scifi`), tag chứa dấu phẩy hoặc ký tự đặc biệt gây rủi ro khi parse CSV.
- Một người dùng có thể gắn **nhiều tag cho cùng một phim**, và gắn **cùng một tag ở nhiều thời điểm**.

 

### 2.5. `link.csv`

 

| Cột | Kiểu dữ liệu | Đặc tả |
|---|---|---|
| `movieId` | `int64` | Khoá phim trong MovieLens |
| `imdbId` | `int64` | Mã phim trên IMDb **không có tiền tố `tt` và không có số 0 đệm** |
| `tmdbId` | `int64` | Mã phim trên TMDb |

 

**Cạm bẫy kỹ thuật:**
- `imdbId` lưu dạng số nguyên nên **mọi số 0 ở đầu đều bị mất**. Mã IMDb thật có định dạng `tt` + **7 chữ số** (tài liệu gốc mô tả mã 7 ký tự), nên để dựng lại URL đúng bạn phải **zero-pad** rồi mới ghép: `https://www.imdb.com/title/tt<padded_imdbId>/`.
- `tmdbId` lưu dạng số nguyên. URL đúng: `https://www.themoviedb.org/movie/<tmdbId>`.
- Số dòng đúng bằng `movie.csv` (27.278) → quan hệ **1–1**, là cơ sở để kiểm tra **referential integrity** hai chiều.

 

### 2.6. `genome_scores.csv`

 

| Cột | Kiểu dữ liệu | Đặc tả |
|---|---|---|
| `movieId` | `int64` | Khoá phim trong MovieLens |
| `tagId` | `int64` | Khoá ngoại tới `genome_tags.csv` |
| `relevance` | `float64` | Điểm liên quan phim–tag, miền giá trị **[0, 1]** |

 

**Đặc tính kỹ thuật:**
- **11.709.768 dòng** — bảng lớn thứ hai, chiếm phần lớn dung lượng dataset.
- `relevance` là **điểm tính toán bằng thuật toán machine learning** từ tag, rating và review của người dùng (Vig et al., 2012), **không phải** dữ liệu người dùng nhập trực tiếp. Đây là dữ liệu *derived*, cần phân biệt rõ với `tag.csv`

 

### 2.7. `genome_tags.csv`

 

| Cột | Kiểu dữ liệu | Đặc tả |
|---|---|---|
| `tagId` | `int64` | Khoá chính |
| `tag` | `string` | Mô tả đặc trưng, đã được chuẩn hoá sẵn |

 

### 2.8. Sơ đồ quan hệ nguồn

 

```
        genome_tags
              │         
              │ tagId
              │
              ▼
      genome_scores ── movieId ──┐
                                 │
                                 ▼
   rating ─────── movieId ───► movie ◄──movieId ── link
     │                           ▲
     │                           │
     │ userId, movieId           │ movieId
     │                           │ 
     └────────── tag ────────────┘
```

 

---

 

## 3. Ba yêu cầu kỹ thuật cốt lõi

 

### 3.1. Incremental Data Processing

 

MovieLens 20M là một **snapshot tĩnh**. Bạn phải tự thiết kế kịch bản mô phỏng luồng dữ liệu mới phát sinh khi vận hành:

 

**a) Phân tách theo trục thời gian sự kiện:**

 

Dữ liệu trong **rating.csv** và **tag.csv** đều có **timestamp** — tức là bạn biết chính xác mỗi sự kiện xảy ra lúc nào. Hãy dùng nó để cắt dữ liệu thành các đợt nạp:
- Đợt đầu — nạp lịch sử: chọn một mốc thời gian T, lấy toàn bộ dữ liệu phát sinh trước mốc đó. Đây là khối dữ liệu nền, nạp một lần duy nhất.
- Các đợt sau — nạp bổ sung: phần dữ liệu sau mốc T được chia thành nhiều đợt nối tiếp, mỗi đợt giả lập một lần pipeline chạy định kỳ.

 

Nhờ vậy, khối dữ liệu tĩnh ban đầu biến thành một dòng chảy có dữ liệu mới phát sinh — đúng với cách hệ thống thật hoạt động, và là điều kiện để bạn làm incremental.

 

**b) Tạo dữ liệu thay đổi cho danh mục phim:**

 

**movie.csv** là danh mục phim của công ty. Ngoài đời danh mục này luôn biến động: phim mới được thêm, thông tin phim cũ được sửa, phim hết bản quyền bị gỡ xuống. Nhưng file bạn tải về chỉ là ảnh chụp một thời điểm, hoàn toàn tĩnh.

 

Nên bạn phải tự dựng ra những thay đổi đó, giả lập như hệ thống nguồn gửi cho bạn "danh sách những gì vừa thay đổi" sau mỗi chu kỳ. Gộp lại cần có đủ ba loại:

 

| Thao tác | Ví dụ |
|---|---|
| INSERT| Thêm phim mới với movieId chưa từng có |
| UPDATE | Đổi tên phim, sửa lỗi chính tả, thêm hoặc bớt thể loại, đính chính năm phát hành |
| DELETE (soft delete) | Đánh dấu phim hết bản quyền — bật cờ is_deleted, không nên xóa hẳn |

 

**c) Yêu cầu bắt buộc về mặt kỹ thuật:**
- Quản lý **high-water mark**, đảm bảo mỗi lần chạy chỉ đọc phần dữ liệu mới.
- Xử lý **late-arriving data**: batch sau có thể chứa bản ghi thuộc về cửa sổ thời gian của batch trước.
- Đảm bảo **idempotency**: chạy lại cùng một batch không sinh dữ liệu trùng lặp.
- Áp dụng các kỹ thuật xử lý **MERGE/UPSERT**.
- Ghi nhận **audit column** đầy đủ trên mọi bảng: nguồn, thời điểm nạp, `batch_id`, record hash.

 

### 3.2. Common Data Model

 

Mỗi nguồn dữ liệu có cách tổ chức riêng. Nếu xây warehouse bám sát cấu trúc của một nguồn, thì đến lúc tích hợp nguồn thứ hai — vốn gọi tên khác, chia bảng khác, định dạng khác — bạn sẽ phải sửa lại mô hình, sửa lại pipeline, sửa lại cả báo cáo đã chạy ổn định.

 

**Common Data Model** giải quyết việc đó bằng cách đặt ra một bộ khái niệm nghiệp vụ dùng chung, độc lập với mọi nguồn. Nguồn nào vào cũng được dịch về bộ khái niệm ấy.

 

**Ví dụ:** CineInsight muốn biết "một người dùng đã tương tác với một bộ phim như thế nào". Ba nguồn trả lời câu hỏi đó theo ba cách hoàn toàn khác nhau:

 

| Nguồn | Cách nguồn lưu |
|---|---|
| MovieLens `rating.csv` | `userId`, `movieId`, `rating` (0.5–5.0), `timestamp` (epoch) |
| IMDb | `user_ref`, `title_id`, `score` (1–10), `rated_on` (ISO date) |
| Log xem phim nội bộ | `account_id`, `film_code`, `watch_percent`, `event_time` (giờ VN) |

 

Cùng một nghiệp vụ, nhưng tên trường khác, thang điểm khác, định dạng thời gian khác. Dịch về CDM:

 

| Trường CDM | MovieLens | IMDb | Log nội bộ |
|---|---|---|---|
| `party_id` | `userId` | `user_ref` | `account_id` |
| `content_id` | `movieId` | `title_id` | `film_code` |
| `event_type` | `RATING` | `RATING` | `WATCH` |
| `event_value` | `rating × 20` | `score × 10` | `watch_percent` |
| `event_time_utc` | epoch → UTC | ISO → UTC | giờ VN → UTC |

 

Sau khi dịch, cả ba nguồn nằm chung một bảng `InteractionEvent`. Báo cáo chỉ cần viết một lần và chạy đúng trên mọi nguồn. Mai mốt có thêm hành vi "like" hay "share", chỉ cần bổ sung giá trị mới cho `event_type` — **không phải đổi cấu trúc bảng, không phải sửa báo cáo cũ**.

 

Cái lợi:
- **Thêm nguồn mới chỉ cần viết thêm lớp mapping**, phần lõi của warehouse giữ nguyên.
- **Dữ liệu từ nhiều nguồn so sánh và gộp được với nhau**, vì đã nói chung một ngôn ngữ.
- **Báo cáo và dashboard không vỡ** khi hệ thống nguồn đổi schema — thay đổi được chặn lại ở lớp mapping.
- **Cả công ty hiểu giống nhau** về một khái niệm, không còn mỗi đội một định nghĩa.

 

### 3.3. Slowly Changing Dimension

 

| Loại | Cơ chế | Tình huống áp dụng |
|---|---|---|
| **Type 1** | Ghi đè giá trị, không lưu lịch sử | Đính chính lỗi kỹ thuật: typo, chuẩn hoá định dạng, bổ sung định danh ngoài |
| **Type 2** | Lưu trọn lịch sử qua `effective_from`, `effective_to`, `is_current`, `version`; mỗi phiên bản mang một **surrogate key** riêng | Thay đổi có ý nghĩa phân tích: phân loại thể loại, phân khúc người dùng |
| **Type 3** | Lưu song song giá trị hiện tại và giá trị liền trước (`current_x` / `previous_x` / `changed_date`) | Nhu cầu so sánh trước–sau mà không cần toàn bộ chuỗi lịch sử |

 

**Yêu cầu bắt buộc:**

 

Hãy tự phân tích dữ liệu, xác định những thuộc tính nào có khả năng thay đổi, và **lựa chọn loại SCD phù hợp cho từng trường hợp**. Với mỗi lựa chọn, bạn cần lập luận được vì sao chọn loại đó mà không phải loại khác — đây là phần được đánh giá cao nhất, quan trọng hơn việc code chạy được.

 

---

 

## 4. Kiến trúc dữ liệu — Medallion Architect

 

```
SOURCE  →  LANDING  →  BRONZE  →  SILVER  →  GOLD
───────────────────────────────────────────────────────────
     Metadata · Control · Data Quality · Lineage
```

 

### 4.1. Layer contract

 

| Tiêu chí | **LANDING** | **BRONZE** | **SILVER** | **GOLD** | 
|---|---|---|---|---| 
| **Vai trò** | Vùng chứa dữ liệu đầu vào, giữ nguyên trạng | Kho lịch sử có cấu trúc | Single source of truth | Phục vụ tiêu dùng | 
| **Nội dung** | File gốc, bất biến | Có kiểu + audit column | Sạch, chuẩn CDM, hợp nhất | Star schema + SCD + marts | 
| **Định dạng** | Giữ định dạng nguồn | Delta / Iceberg | Delta / Iceberg | Delta / Iceberg | 
| **Schema** | Không ép kiểu | Ép kiểu tối thiểu, giữ tên gốc | Chặt, đặt tên theo CDM | Theo mô hình chiều | 
| **Biến đổi** | Không | Ép kiểu + audit | Cleansing + business rule | Aggregate + SCD + KPI | 
| **Cách ghi** | Write-once | **Append-only** | **MERGE / UPSERT** | MERGE / refresh | 
| **Bản ghi lỗi** | Không kiểm tra | Nhận hết, đánh dấu | **Quarantine** | Không còn |

 

### 4.2. Quy định chi tiết từng tầng

 

1. **Landing.** Sao chép file nguồn nguyên trạng, không parse, không ép kiểu, không đổi tên cột. Immutable — file đã có thì không được ghi đè hay xoá; mỗi lần nạp lại sinh `batch_id` mới. Lưu **checksum và row count** phục vụ đối soát. Yêu cầu then chốt: toàn bộ lakehouse phải **replay được** — xoá sạch ba tầng sau và dựng lại chỉ từ Landing vẫn cho ra kết quả đồng nhất.

 

2. **Bronze.** Append-only tuyệt đối, không update, không delete — mọi thay đổi từ nguồn là một bản ghi mới. Bắt buộc có audit column: `batch_id`, `ingested_at`, `source_file`, `source_system`, `record_hash`, số thứ tự dòng. Giữ nguyên giá trị nghiệp vụ gốc **kể cả bản ghi lỗi** — đây không phải tầng cleansing. Hỗ trợ **schema evolution** khi nguồn bổ sung cột. Có bước **reconciliation** đối chiếu row count và checksum với Landing, ghi kết quả vào control table.

 

3. **Silver.** Tầng **duy nhất** được phép áp dụng cleansing và business rule, đồng thời thực hiện chuyển đổi sang CDM. Ghi bằng MERGE/UPSERT trên business key kết hợp record hash. Khai báo tường minh **quy tắc chọn bản ghi thắng** khi trùng khoá. Bản ghi vi phạm blocking rule phải đi vào **quarantine table** kèm mã lỗi và lý do — không được loại bỏ ngầm. Silver chỉ được coi là sẵn sàng khi đạt trạng thái **deduplicated, conformed, validated**.

 

4. **Gold.** Mô hình hoá theo **Star Schema/Snowflake Schema**: fact và dimension, surrogate key, grain khai báo tường minh. Là nơi SCD và point-in-time join được hiện thực hoá. Tách rõ **core star schema/ snowflake schema** (dùng chung) và **data mart / aggregate table** (phục vụ từng dashboard hoặc feature set cho ML). Không phát sinh business logic mới tại tầng này. Tối ưu truy vấn bằng partitioning, Z-ordering/clustering, OPTIMIZE và VACUUM định kỳ.

 

**Cross-cutting.** 
- **Metadata, Control:** Phải có khả năng lưu trạng thái từng lần chạy (watermark, row count đọc/ghi/lỗi, thời gian, trạng thái).
- **Data Quality:** Phải có khả năng lưu kết quả từng rule theo batch.
-  **Lineage:** phải cho phép truy vết một bản ghi Gold ngược về đúng file Landing đã sinh ra nó.

 

### 4.3. Tech stack

 

| Hạng mục | Hướng A — Local | Hướng B — Cloud |
|---|---|---|
| Storage | MinIO / local filesystem | Amazon S3 |
| Processing | PySpark (Spark local) | AWS Glue / Databricks |
| Table format | Delta Lake / Iceberg | Delta Lake / Iceberg |
| Catalog | Hive Metastore | Glue Data Catalog / Unity Catalog |
| Query | Spark SQL | Athena / Databricks SQL |
| Orchestration | Apache Airflow (Local) | Airflow MWAA / Glue Workflow |
| Visualization | Streamlit / Matplotlib | QuickSight / Databricks Dashboard |

 

---

 

## 5. Phạm vi công việc theo giai đoạn

 

### Giai đoạn 1 — Ingestion & Data Profiling
- Nạp dataset file vào Landing rồi Bronze theo đúng layer contract, kèm checksum và bước reconciliation ghi vào control table. 
- Thực hiện profiling: khối lượng, kiểu dữ liệu thực tế so với kiểu suy luận, tỉ lệ null, giá trị bất thường, mức độ trùng lặp, phân bố khoá, mức độ **data skew**. 
- Phân tích các **rủi ro dữ liệu** phát hiện được và ảnh hưởng tới thiết kế pipeline. 
- Đưa ra các chiến lượcpartitioning strategy cho từng nguồn dữ liệu.

 

### Giai đoạn 2 — Data Quality Framework
- Định nghĩa và chuẩn hoá **rule**, theo các điều kiện: completeness, validity, uniqueness, consistency, referential integrity, timeliness. 
- Phân cấp rule thành **blocking** (dừng pipeline) và **warning** (ghi log, tiếp tục chạy). 
- Triển khai cơ chế **quarantine** kèm quy trình reprocessing. 
- Xuất DQ report theo từng batch, phân tích xu hướng chất lượng giữa các batch.

 

### Giai đoạn 3 — CDM & Silver Layer
- Hiện thực hoá CDM chuẩn hóa dữ liệu, tạo mapping document. 
- Triển khai MERGE incremental Bronze → Silver, xử lý late-arriving data và deduplication. 
- **Kiểm tra idempotency** bằng cách thực thi lặp cùng một batch và đối chiếu kết quả.

 

### Giai đoạn 4 — Dimensional Model & SCD
- Thiết kế **ERD**, khai báo grain của từng fact table, lập luận cho việc chọn surrogate key, xử lý các loại quan hệ có thể xảy ra. 
- Hiện thực hoá đầy đủ các kỹ thuật SCD Type 1, 2, 3. 
- Thực hiện point-in-time join và **kiểm tra sự khác biệt** so với join thông thường. 
- Thiết kế phương án xử lý **late-arriving dimension** (gợi ý: inferred member).

 

### Giai đoạn 5 — Business Analytics
Hãy trả lời được các câu hỏi sau:
1. Xếp hạng phim theo điểm đánh giá — **tự đề xuất và bảo vệ ngưỡng số lượt đánh giá tối thiểu**; chỉ ra hiện tượng xảy ra khi bỏ ngưỡng.
2. So sánh các thể loại về chất lượng và độ phổ biến; xác định thể loại có **phương sai ý kiến cao nhất**.
3. Xu hướng điểm số theo **năm phát hành** và theo **thời điểm đánh giá**.
4. Khai thác `tag.csv`: tag phổ biến và tương quan giữa tag với điểm đánh giá — **sau khi đã chuẩn hoá text**.
7. Khai thác `genome_scores` + `genome_tags`: mô tả đặc trưng nội dung của một nhóm phim tự chọn; đánh giá độ phủ genome trên toàn catalog.
8. Xác định nhóm **"hidden gems"** (chất lượng cao, độ phủ thấp), xuất kèm URL IMDb/TMDb **đã dựng đúng định dạng** từ `link.csv`.

 

### Giai đoạn 6 — Orchestration
**Airflow DAG** điều phối toàn bộ pipeline incremental: dependency tường minh, retry, SLA, **fail-fast** khi blocking rule bị vi phạm, hỗ trợ **backfill** cho một ngày bất kỳ.

 

---

 

## 6. Sản phẩm bàn giao

 

- Version Control (code ETL, DQ, SCD, model, DAG)
- Notebook analytic `.ipynb`, kèm theo bản export `.html` để có thể preview nhanh

 

---

 

## 7. Ràng buộc

 

1. **Full reload tại tầng Silver hoặc Gold** — vi phạm mục tiêu cốt lõi của đề bài.
2. **Gộp tầng hoặc để một tầng thực hiện sai vai trò** — trừ điểm nặng và kéo theo điểm của mọi giai đoạn phía sau.
3. **Không giải thích được code đã nộp** — bạn được phép dùng công cụ AI hỗ trợ, nhưng phần nào không giải thích được thì phần đó không được tính điểm.

 

---