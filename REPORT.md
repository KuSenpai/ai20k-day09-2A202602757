# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Ngô Gia Quốc |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602757 |
| GitHub | KuSenpai |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-07304 |

> MSSV có chữ `A` nên không `int()` được, mình lấy phần số: `202602757 % 26 = 9` → `list-trips[9]` = trip-07304.

## Kết quả cuối

- `dataset_version` của bạn: `bd9952ba7d92`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: 717 / 162 / 140 (18 / 4 / 4 trip)
- `report/verify.txt`: 27/27 checks passed.
- Ảnh trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, `crash_task_log.png`, `crash_graph_red.png`, `crash_clear_dialog.png`, `crash_graph_after_clear.png`.

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Khác biệt A và B:
- Tên cột: `ts_utc, speed_mps, heading_deg, gas...` so với `timestamp_ms, spd_kmh, hdg, throttle_pct...`
- Tốc độ: A là m/s (1.213), B là km/h (55.11)
- Thời gian: A là chuỗi ISO UTC, B là epoch ms (1759933801509)
- Bàn đạp: A là 0..1, B là phần trăm
- Đồng hồ B chạy nhanh hơn video 1500 ms (A = 0)

Tách bảng vì hai schema khác nhau, gộp sớm dễ mất hoặc hiểu sai cột. Để dạng chữ vì bronze phải giữ nguyên bản: ép kiểu sớm thì gặp ô lỗi sẽ hỏng cả lần nạp hoặc thành NULL im lặng, và mất bản gốc để xử lý lại. Nạp lần 2 báo `already present` (theo sha256), `raw_telemetry_a` chỉ có 1 commit (3021 dòng).

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

`stg_telemetry_b` đổi tên cột theo A, `km/h / 3.6`, `% / 100`, và chuyển `(timestamp_ms - 1500) / 1000` thành timestamp UTC. Offset lấy từ `sources.json` qua `bronze.video_files`, không gõ cứng.

Bằng chứng: B bắt đầu lúc `14:30:00.009`, khớp giờ bắt đầu video (chưa trừ sẽ là 14:30:01.5). Tốc độ max 71 và 100 km/h là hợp lý.

| drive | duplicate | speed_missing | speed_out_of_range | sạch + cách ly = nhận |
|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 2968 + 53 = 3021 |
| drive_b | 20 | 30 | 3 | 3181 + 53 = 3234 |

Dòng lỗi vào `rejects` thay vì bị xoá để còn đếm được (không mất dòng nào), truy được về file/dòng gốc, và lấy lại được nếu sau này phát hiện luật lọc sai.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` chỉ báo cảm biến mất tín hiệu (hầm, nhiễu). Đó là chuyện thật ngoài đường, dữ liệu vẫn đúng, chỉ thưa hơn, nên `warn`. `in_range` bắt tốc độ vô lý, là dữ liệu sai sẽ đi thẳng vào gold, nên `error` để dừng pipeline.

Break-gate: nới ngưỡng 70 lên 1000 m/s, `dbt test` ra `PASS=5 WARN=1 ERROR=1`. Test `in_range_silver_telemetry_speed_mps__70__0` bắt được 6 dòng (3 dòng vô lý × 2 drive). Khôi phục lại thì `ERROR=0`. Như vậy cổng chất lượng là lớp chặn độc lập: kể cả khi luật làm sạch bị sửa sai, test vẫn chặn được dữ liệu xấu trước khi tới gold.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Chạy lại trip-00003 (60 khung) hai lần: bảng vẫn 1246 dòng, fingerprint giữ nguyên `6c7480dfde7e`. Ghi kiểu `append` hai lần thì trip đó thành 120 dòng, tức là nhân đôi.

Delta `overwrite` theo `trip_id` là một commit nguyên tử: chỉ thay đúng trip đó, ghi xong mới commit. Lỗi giữa chừng thì bảng vẫn ở version cũ, đầy đủ. Còn "xoá rồi ghi" là hai bước rời nhau: lỗi sau khi xoá sẽ làm mất hoặc thiếu partition, người đọc lúc đó có thể thấy bảng rỗng.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

Khung đen (độ sáng < 12) là lỗi, ảnh không có thông tin, nên bị loại. Khung tối (12–40) là cảnh thật như hầm hay bóng râm, nên chỉ gắn cờ `is_dark` và vẫn giữ. Drive A: 6 đen, 4 tối; drive B: 4 đen, 61 tối. Mình đồng ý, vì bỏ khung tối thì mô hình sẽ yếu đúng ở điều kiện khó.

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Mình chọn ngưỡng 2. Ngưỡng 0 bỏ sót camera đứng hình có nhiễu nén; ngưỡng 6 loại tới 60% drive A, tức là loại cả khung khác nhau thật. Ngưỡng phụ thuộc tốc độ xe: xe chậm (A) thì hai khung cách nhau 0.5 s gần giống nhau, nên ra nhiều khung trùng; xe nhanh (B) thì ít hơn. Tốt nhất nên chỉnh ngưỡng theo tốc độ.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Có 1019 khung, 26 trip. Chia theo khung hình thì cả 26/26 trip bị cắt và có khung nằm ở cả train lẫn test. Chia theo trip thì 0 trip bị cắt.

Các khung liền nhau trong một trip gần như giống hệt nhau. Khung ở train và khung kế bên ở test thì mô hình chỉ cần nhớ cảnh là đoán đúng, nên điểm test cao nhưng không phản ánh được khả năng xử lý cảnh mới.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

Version là sha256 của danh sách `frame_id|split`: cùng nội dung thì cùng version, khác một khung thì version đổi. Dùng ngày giờ hay số đếm thì chạy lại sẽ đổi dù dữ liệu y hệt, không tái lập được. Các lần chạy của mình (`step 4`, `export-dataset`, `step all --reset`) đều ra `bd9952ba7d92`.

MSSV được trộn vào thứ tự trip: `md5(trip_id || MSSV)`, rồi cắt 70/15/15. MSSV khác thì trip rơi vào split khác, nên version khác. Mình thử với `2024000` thì ra `8fc131884584` (703/182/134); đặt lại MSSV của mình thì quay về `bd9952ba7d92`.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

```
bronze.raw_telemetry_a: 0, raw_telemetry_b: 0, trips_raw: 0, video_files: 1,
silver.frames: 25, telemetry: 0, frames_flagged: 0, frame_telemetry: 0,
input_video_sha256: drive_a 636ba1a9…, drive_b bfa62703…
```

Dataset được tạo từ 2 video có sha256 như trên, telemetry bronze version 0, `silver.frames` version 25 (26 trip là 26 commit, từ 0 đến 25). Luật lọc là bỏ khung đen, mờ, trùng và lệch đồng bộ; split theo trip, sắp theo md5.

3 tháng sau muốn tái tạo cần: sha256 video gốc, version từng bảng Delta (để time travel), luật trong manifest cùng `dbt_project.yml`, và MSSV. Làm lại xong, băm phải ra đúng `bd9952ba7d92`.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Với `LAB_FAIL_ONCE=trip-07304`:
- 1 task đỏ: `silver_frames` map_index 19, log ghi `simulated crash while extracting trip-07304`
- 7 task `upstream_failed`: từ `dbt_run_silver` tới `publish_warehouse`
- 25 trip còn lại vẫn success, không phải chạy lại

Mình chỉ clear task đỏ đó (kèm downstream), Airflow báo Affected Tasks: 8. Chạy lại thì DAG xanh 37/37, chỉ task đó có `try_number = 2`. Không có dòng nào bị nhân đôi: `silver_frames` có 1246 dòng = 1246 `frame_id`. Verify 27/27, version vẫn `bd9952ba7d92`. Ảnh: `crash_task_log.png`, `crash_graph_red.png`, `crash_clear_dialog.png`, `crash_graph_after_clear.png`.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

1. **Decode video bằng FFmpeg trong một container Airflow**: nút cổ chai CPU, chạy không kịp trong ngày. Thay bằng cụm worker phân tán (Spark/Ray/Kubernetes), decode bằng GPU, Airflow chỉ lo điều phối.
2. **DuckDB + dbt trên một máy**: không đủ RAM cho join ASOF hàng tỷ dòng. Thay bằng Spark SQL/Trino đọc thẳng Delta, partition theo ngày và xe, model incremental.
3. **MinIO 1 node, Postgres làm warehouse, ảnh JPG lẻ**: quá nhiều file nhỏ, Postgres không chịu nổi tải. Thay bằng object storage phân tán, gom ảnh thành shard (WebDataset/Parquet), warehouse dạng cột (BigQuery/ClickHouse).

## Câu hỏi riêng của bạn trên Metabase

Hai thẻ `[2A202602757]` trên dashboard:

```sql
-- 1. Tỷ lệ khung bị loại theo trip
select drive_id, trip_id, split, frames, frames - frames_kept as dropped,
       round(100.0 * (frames - frames_kept) / frames, 1) as drop_pct, avg_speed_kmh
from marts.mart_trip_summary
order by drop_pct desc;

-- 2. Tỷ lệ khung bị loại theo dải tốc độ
select case when avg_speed_kmh < 20 then '1: <20 km/h' when avg_speed_kmh < 50 then '2: 20-50 km/h' else '3: >=50 km/h' end as speed_band,
       count(*) as trips, round(100.0 * sum(frames - frames_kept) / sum(frames), 1) as drop_pct
from marts.mart_trip_summary group by 1 order by 1;
```

Bị loại nhiều nhất là trip-04622 (56.7%), rồi trip-03904 (50%), trip-02086 (46.7%), đều ở drive A và gần như toàn bộ là khung trùng. Theo tốc độ: dưới 20 km/h mất 32.4%, 20–50 km/h mất 17.1%, từ 50 km/h trở lên mất 14.4%. Vậy nguyên nhân chính là xe chạy chậm, không phải lỗi cảm biến.

## Thí nghiệm riêng: đổi `sync_tol_ms`

| sync_tol_ms | khung lệch đồng bộ | khung trong gold | train / val / test |
|---|---|---|---|
| 50 | 600 | 538 | 377 / 89 / 72 |
| 100 | 324 | 762 | 532 / 126 / 104 |
| **150 (gốc)** | **14** | **1019** | **717 / 162 / 140** |
| 300 | 14 | 1019 | 717 / 162 / 140 |

Telemetry lấy mẫu khoảng 80 ms một lần, nên độ lệch 40–80 ms là bình thường. Đặt 50 ms thì mất 47% dữ liệu gold. Từ 150 ms trở lên chỉ còn 14 khung lệch, đúng là các đoạn mất tín hiệu thật, nên 150 là ngưỡng hợp lý. Bằng chứng: `report/evidence/own_sync_tol.json`.

## Nhìn lại

- Bất ngờ nhất: trip-04622 chạy 67 km/h mà vẫn có 34/60 khung bị coi là trùng. Có thể dHash quá thô với cảnh cao tốc đơn điệu, tức là false positive mà pipeline chưa phân biệt được.
- Pipeline chưa bắt được dữ liệu "hợp lệ nhưng sai", ví dụ GPS nhảy vài trăm mét hoặc tốc độ không khớp với quãng đường GPS. Nên thêm kiểm tra chéo giữa các cột.
- Trên Windows, Git đổi script sang CRLF khiến Airflow không khởi động được; phải đưa về LF.
- Chưa rõ: vì sao trip-07304 là map_index 19 trong DAG nhưng lại nằm ở vị trí 9 trong `list-trips`.
