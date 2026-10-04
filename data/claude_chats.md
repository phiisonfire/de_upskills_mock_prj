Mình đọc file requirements trước nhé.Cần đọc phần bị cắt để nắm đủ yêu cầu incremental.Đừng bắt đầu bằng code pipeline hay Airflow. Ba việc đầu tiên nên là **hiểu dữ liệu, chốt quyết định thiết kế, và dựng công cụ mô phỏng dữ liệu đến theo batch**. Phần lớn điểm của đề nằm ở lập luận thiết kế (chọn SCD loại nào, vì sao chọn watermark đó...), nên các quyết định này cần làm sớm.

## Bước 0: Chốt các quyết định nền (1-2 ngày, chủ yếu là viết)

Tạo một file `DESIGN.md` và ghi lại các câu trả lời sau. Các bước sau đều dựa vào nó, và nó cũng là tài liệu bạn dùng để bảo vệ bài.

- **Chọn Hướng A (local) hay B (cloud).** Mình khuyên Hướng A: PySpark + Delta Lake + Airflow chạy bằng Docker Compose, đỡ tốn tiền và dễ debug.
- **Mốc T và kế hoạch chia batch.** Ví dụ: T = đầu năm 2014 cho lịch sử, phần sau chia theo tuần hoặc tháng thành N batch. Cần xem phân bố timestamp trước khi chọn (xem bước 2).
- **Kịch bản thay đổi cho `movie`.** Liệt kê cụ thể phim nào bị INSERT, UPDATE, DELETE ở batch nào.
- **Quy ước chung:** định dạng `batch_id`, tên các control table, schema bảng DQ result và bảng quarantine. Đây là phần cross-cutting nên phải thống nhất từ đầu.

## Bước 1: Dựng môi trường và cấu trúc repo

Dựng Spark + Delta chạy được, đọc và ghi thử một file nhỏ. Cấu trúc repo gợi ý:

```
/landing  /bronze  /silver  /gold  /control
/src (ingest, dq, silver, gold, scd)
/dags  /notebooks  /docs  /tests
```

Nên commit lên Git ngay từ đầu vì đề yêu cầu version control.

## Bước 2: Profiling nhanh trước khi thiết kế

Tải dataset về, đọc thử từng file bằng pandas hoặc Spark rồi trả lời các câu hỏi sau:

- `(userId, movieId)` trong rating có duy nhất không?
- Phân bố timestamp theo tháng ra sao? Cái này quyết định cách chọn T và chia batch.
- Có `movieId` nào trong rating, tag hoặc link mà movie không có (orphan)?
- Regex trích năm từ `title` bỏ sót bao nhiêu dòng? Có lỗi encoding không?
- Có bao nhiêu phim mang `(no genres listed)`?
- Phân bố số rating trên mỗi phim và mỗi user lệch đến mức nào (data skew)?

Kết quả bước này đưa thẳng vào báo cáo Giai đoạn 1 và quyết định partitioning (rating gần như chắc chắn nên partition theo thời gian).

## Bước 3: Viết "simulator" (bước quan trọng nhất và dễ bị bỏ qua)

Vì dataset là snapshot tĩnh, bạn phải tự sinh dữ liệu đến theo batch:

- **rating và tag:** script tách theo timestamp thành `batch_0001` (lịch sử trước T), rồi `batch_0002`, `batch_0003`... đặt vào `landing/`.
- **movie:** script sinh các file CDC gồm `movieId`, các cột dữ liệu, `op_type` (I/U/D) và `changed_at`.
- **Cố ý cài tình huống khó vào dữ liệu:** một vài bản ghi late-arriving (thuộc cửa sổ của batch trước nhưng nằm ở batch sau), một vài bản ghi trùng, và vài lần đổi genres có ý nghĩa phân tích để sau này SCD Type 2 có dữ liệu mà chứng minh.

Nếu bỏ qua bước này thì bạn không có gì để test incremental, idempotency hay SCD.

## Bước 4: Làm một lát cắt mỏng chạy hết các tầng

Đừng làm hết Landing cho mọi bảng rồi mới sang Bronze. Hãy chọn **`movie`** (nhỏ, nhưng có đủ CDC và SCD) và đi qua Landing → Bronze → Silver → Gold với một batch. Sau đó mở rộng sang `rating` và các bảng còn lại. Cách này giúp bạn phát hiện sớm lỗi thiết kế layer contract.

## Thứ tự các giai đoạn sau đó

1. **Giai đoạn 1** (Landing/Bronze, control table, reconciliation, profiling).
2. **Giai đoạn 2** (DQ framework). Nên làm sớm vì Silver phụ thuộc vào quarantine.
3. **Giai đoạn 3** (CDM + Silver MERGE, kiểm tra idempotency).
4. **Giai đoạn 4** (Star schema + SCD 1/2/3 + point-in-time join).
5. **Giai đoạn 5** (analytics) và **Giai đoạn 6** (Airflow) cuối cùng. Airflow chỉ là lớp bọc quanh các job đã chạy tốt.

## Lưu ý

- Landing phải **immutable và replay được**: xóa Bronze, Silver, Gold rồi dựng lại chỉ từ Landing phải cho kết quả giống nhau. Hãy thiết kế theo yêu cầu này ngay từ đầu.
- Không được full reload ở Silver hoặc Gold (mục 7 ràng buộc). Chỉ cần lỡ tay dùng `overwrite` là mất điểm nặng.
- Đề có nhảy số câu hỏi ở Giai đoạn 5 (từ câu 4 sang câu 7). Có thể chỉ là lỗi đánh số, nhưng bạn nên xác nhận với giảng viên xem có câu nào bị thiếu không.

Bạn định dùng Hướng A (local) hay B (cloud), và máy bạn RAM bao nhiêu? Với 20 triệu dòng rating thì cấu hình Spark local cũng cần tính trước.