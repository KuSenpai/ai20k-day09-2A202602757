# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Ngô Gia Quốc |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602757 |
| GitHub | KuSenpai |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-07304 |

> Ghi chú về trip: MSSV có chữ `A` nên `int(MSSV)` không chạy; tôi dùng phần số `202602757`: `202602757 % 26 = 9` → `list-trips[9]` = **trip-07304**.

## Kết quả cuối

- `dataset_version` của bạn: `bd9952ba7d92`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: 717 / 162 / 140 (tổng 1019; số trip 18 / 4 / 4)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (`crash_task_log.png`, `crash_graph_red.png`, `crash_clear_dialog.png`, `crash_graph_after_clear.png`).

## Bước 1 · Bronze

### Câu 1
Khác biệt giữa A và B ở bronze (`step 1`, 2 dòng đầu mỗi bảng):
1. **Tên cột**: A dùng `ts_utc, speed_mps, heading_deg, lat, lon, alt_m, acc_x, gas, brake, steer`; B dùng `timestamp_ms, spd_kmh, hdg, latitude, longitude, altitude, ax, throttle_pct, brake_pct, steering`.
2. **Đơn vị tốc độ**: A là m/s (`1.213`), B là km/h (`55.11`).
3. **Kiểu thời gian**: A là chuỗi ISO UTC (`2025-10-07T08:00:00.014Z`), B là epoch milli giây (`1759933801509`).
4. **Bàn đạp**: A là tỉ lệ 0..1 (`gas 0.028`), B là phần trăm (`throttle_pct 2.0`).
5. **Đồng hồ**: thiết bị của B chạy nhanh hơn video `telemetry_clock_offset_ms = 1500` (A = 0), xem `bronze_video_files`.
6. Số dòng: A 3021, B 3234.

Giữ **mỗi nguồn một bảng** vì schema khác nhau: gộp ngay thì phải chọn một schema và sẽ đánh mất/hiểu sai cột của nguồn kia. Để **mọi cột là chữ** vì bronze là bản sao nguyên trạng, chỉ thêm (slide 14): ép kiểu ngay thì một ô hỏng (ô trống, `abc`) làm hỏng cả lần nạp hoặc bị biến thành NULL im lặng, và không còn nguyên bản để xử lý lại khi luật làm sạch đổi. Việc ép kiểu và đổi đơn vị dời sang silver, nơi có thể chạy lại bất cứ lúc nào từ bronze. Nạp lần hai báo `already present` (nhận diện bằng sha256), và `history bronze.raw_telemetry_a` chỉ có **1 commit** (version 0, `rows_added 3021`).

## Bước 2 · Silver telemetry

### Câu 2
Trong `stg_telemetry_b.sql`: đổi tên cột về schema của A; `spd_kmh / 3.6` → m/s; `throttle_pct/100`, `brake_pct/100` → 0..1; thời gian `to_timestamp((timestamp_ms - offset_ms)/1000)` tức là **đổi epoch ms sang timestamp UTC và trừ 1500 ms** lệch đồng hồ, lấy độ lệch từ `bronze.video_files` (khai báo trong `sources.json`) chứ không gõ cứng. Sau đó hai nguồn được `union` vào `silver_telemetry`, dòng lỗi tách sang `silver_telemetry_rejects`.

Bằng chứng: `bat_dau` của drive B là `2025-10-08 14:30:00.009` — trùng giờ bắt đầu video `14:30:00` (nếu chưa trừ sẽ là `14:30:01.5`); tốc độ tối đa sau chuẩn hoá là 71 km/h (A) và 100 km/h (B), hợp lý; verify module 2 đạt 7/7.

| drive | duplicate | speed_missing | speed_out_of_range | sạch + cách ly = nhận |
|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 2968 + 53 = 3021 |
| drive_b | 20 | 30 | 3 | 3181 + 53 = 3234 |

Dòng lỗi vào `rejects` kèm `reject_reason` thay vì bị xoá vì: (1) **đếm được** — phương trình sạch + cách ly = nhận chứng minh không mất dòng nào; (2) **điều tra được** — có thể truy về file/batch/dòng gốc (`_batch_id`, `_row_no`) để báo lại cho đội phần cứng; (3) **phục hồi được** — nếu luật sai (ví dụ ngưỡng 70 m/s quá chặt), sửa luật và chạy lại là dòng quay về silver. Xoá im lặng thì mất cả ba.

### Câu 3
Trong `dbt/models/schema.yml`: `max_gap_ms` (khoảng hở giữa hai mẫu > 1000 ms) có `severity: warn` vì cảm biến **mất tín hiệu là chuyện thật** ngoài đường (hầm, nhiễu): dữ liệu vẫn đúng, chỉ thưa hơn, nên cần báo để theo dõi chứ không đáng dừng cả pipeline. `in_range` (0..70 m/s) là `error` vì một tốc độ vô lý lọt vào silver là **dữ liệu sai**, sẽ đi tiếp vào gold, vào nhãn huấn luyện; phải chặn lại.

Thí nghiệm `break-gate` (`break_gate.json`): nới `max_speed_mps` từ 70 lên 1000 → bước làm sạch không loại nữa, `dbt test` cho `PASS=5 WARN=1 ERROR=1`: test `in_range_silver_telemetry_speed_mps__70__0` **bắt được 6 dòng** (đúng 3 dòng vô lý × 2 drive mà bình thường nằm trong quarantine). Khôi phục luật → `PASS=6 WARN=1 ERROR=0`. Ý nghĩa: cổng chất lượng là **lớp phòng thủ độc lập** với logic làm sạch — khi ai đó nới/sửa nhầm luật biến đổi, test vẫn chặn được, và trong Airflow test `error` làm các task sau không chạy, dữ liệu xấu không tới gold.

## Bước 3 · Silver frames

### Câu 4
`idempotency.json` (trip-00003, 60 khung): trước khi chạy lại bảng `silver.frames` có **1246 dòng**, dấu vân tay `6c7480dfde7e`; sau **2 lần chạy lại** partition đó vẫn **1246 dòng**, vân tay `6c7480dfde7e` → không đổi. Kiểu ghi `append` sai: chạy 2 lần cho cùng trip ra **120 dòng** cho một trip 60 khung → **nhân đôi**.

Delta `overwrite` theo predicate `trip_id = ...` là **một commit nguyên tử**: thay đúng partition đó, các trip khác không đụng tới, và commit chỉ hiện ra khi đã ghi xong toàn bộ file. Nếu giữa chừng lỗi (node chết, đầy đĩa) thì commit không xảy ra → bảng vẫn ở version cũ, người đọc thấy dữ liệu cũ đầy đủ. Còn "xoá rồi ghi lại" là hai thao tác tách rời: lỗi xảy ra sau khi xoá và trước khi ghi xong thì partition **mất** hoặc **thiếu một nửa**, và người đọc ở giữa có thể thấy trạng thái rỗng. Ngoài ra Delta còn giữ lịch sử (time travel) để quay lại version trước.

### Câu 5
**Khung đen** (độ sáng trung bình < 12, `dark_luma`) là lỗi cảm biến/đường truyền: ảnh không có thông tin → **loại**. **Khung tối** (12 ≤ độ sáng < 40) là điều kiện thật (hầm, bóng râm, chiều tối): ảnh vẫn có nội dung → **gắn cờ `is_dark` nhưng giữ lại**. Kết quả `step 3`: A có 6 đen / 4 tối giữ lại, B có 4 đen / **61** tối giữ lại. Tôi đồng ý: mô hình xe tự lái **phải** thấy cảnh thiếu sáng, bỏ chúng đi thì dataset lệch về trời sáng và mô hình hỏng đúng lúc khó nhất; cờ `is_dark` vẫn cho phép lọc/cân bằng sau này.

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

(Tổng khung: A 602, B 644; ngưỡng 1/4/8: A 109/269/438, B 23/137/348.) Tôi giữ ngưỡng **2**: ngưỡng 0 chỉ bắt khung giống hệt từng bit, bỏ sót camera đứng hình có nhiễu nén (verify bắt 11/12 khung đứng hình ở A với ngưỡng 2); ngưỡng 6 loại tới 60% drive A và 38% drive B — rõ ràng đã loại cả khung khác nhau thật. Ngưỡng phụ thuộc tốc độ: ở 2 khung/giây, xe chạy chậm (A, 40 km/h trung bình ở train, nhiều trip < 20 km/h) cho hai khung liền nhau gần như giống nhau → nhiều "trùng"; xe nhanh (B, ~74 km/h) cảnh đổi nhiều → ít trùng. Lý tưởng là ngưỡng (hoặc tốc độ lấy mẫu) theo tốc độ xe, ví dụ đứng yên thì chỉ giữ 1 khung, chạy nhanh thì ngưỡng chặt hơn.

## Bước 4 · Gold

### Câu 6
`leakage.json`: 1019 khung dùng được, 26 trip. Chia **theo khung hình** (ngẫu nhiên 70/15/15): **26/26 trip** nằm ở nhiều hơn một split, và **26 trip có khung ở cả train lẫn test**. Chia **theo trip** (gold của pipeline): **0** trip bị chia cắt.

Điểm kiểm thử đẹp giả tạo vì các khung liền nhau trong một trip (cách 0,5 giây) gần như giống hệt: cùng đường, cùng xe phía trước, cùng ánh sáng. Khi một khung ở train và khung kế bên ở test, mô hình chỉ cần **nhớ** cảnh đó là đoán đúng — nó đã "thấy đề thi". Điểm test cao nhưng không đo khả năng tổng quát hoá sang **cảnh mới**, thứ duy nhất quan trọng khi xe chạy trên đường thật.

### Câu 7
`dataset_version = sha256(danh sách "frame_id|split" đã sắp xếp)[:12]`. Vì là **băm nội dung**: cùng nội dung → cùng version, khác một khung hay một nhãn split → khác version. Ngày giờ hay số đếm thì đổi mỗi lần chạy dù dữ liệu y hệt (không so sánh được, không tái lập được), hoặc giữ nguyên dù dữ liệu đã đổi. Hai lần chạy liên tiếp của tôi (`step 4` rồi `cli export-dataset`, và cả lần `step all --reset` cuối) đều ra **`bd9952ba7d92`**, cùng 717/162/140.

MSSV được trộn vào thứ tự xếp trip: `row_number() over (partition by drive_id order by md5(trip_id || LAB_STUDENT_ID))` rồi cắt 70/15/15 → MSSV khác thì trip rơi vào split khác → cặp `frame_id|split` khác → version khác. Tôi đã thử chạy `step 4` với `LAB_STUDENT_ID=2024000`: version thành **`8fc131884584`** (train/val/test = 703/182/134), đặt lại MSSV của tôi thì quay về `bd9952ba7d92`. Dữ liệu và khung hình giữ lại y hệt (1019), chỉ phép chia khác.

### Câu 8
`lineage` trong `gold/datasets/bd9952ba7d92/manifest.json`:

```
bronze.raw_telemetry_a: 0, bronze.raw_telemetry_b: 0, bronze.trips_raw: 0, bronze.video_files: 1,
silver.frames: 25, silver.telemetry: 0, silver.frames_flagged: 0, silver.frame_telemetry: 0,
input_video_sha256: drive_a 636ba1a98dfc5ba6…e7e9aa, drive_b bfa62703f645ff77…bee9fdc
```

Trả lời "dataset này được tạo từ dữ liệu nào?": từ hai video có sha256 trên, telemetry bronze A/B version 0, và bảng `silver.frames` ở **version 25** (lần chạy cuối `step all --reset` từ lake trống: 26 commit ghi từng trip, version 0..25; trong lúc làm bài, sau thí nghiệm idempotent, manifest từng ghi version 27 vì 2 lần chạy lại trip-00003 thêm 2 commit — nội dung y hệt nên `dataset_version` không đổi), qua luật `rules.drop = [is_black, is_blurry, is_near_dup, not sync_ok]` và `split = per trip, stratified by drive, md5 order`. Ba tháng sau, để tái tạo đúng: sha256 của video đầu vào (chứng minh đúng file gốc), version của từng bảng Delta (time travel về đúng trạng thái), luật lọc/chia trong manifest + tham số `dbt_project.yml` (commit git của mã), và MSSV dùng để chia; tái tạo xong thì băm lại phải ra `bd9952ba7d92`, kèm `manifest_sha256 = 20bd55cc…68fa5a2` để kiểm tra manifest không bị sửa.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Đặt `LAB_FAIL_ONCE=trip-07304`, chạy `day09_reset` rồi `day09_video_pipeline` (run `run_crash`). Kết quả đọc từ bảng `task_instance` của Airflow:
- **1 task đỏ**: `silver_frames` map_index 19, log: `RuntimeError: simulated crash while extracting trip-07304; run it again` (`crash_task_log.png`).
- **7 task `upstream_failed`**: `dbt_run_silver, dbt_test_silver, dbt_run_gold, dbt_test_gold, publish_lake, export_dataset, publish_warehouse` (`crash_graph_red.png`).
- 29 task thành công vẫn giữ nguyên: 4 task bronze/trip_commands và **25 trip còn lại không chạy lại**.

Tôi chỉ **Clear task** đúng `silver_frames [19]` (mặc định kèm Downstream): hộp thoại báo **Affected Tasks: 8** = 1 task đỏ + 7 task phía sau (`crash_clear_dialog.png`). Sau đó DAG xanh, **37/37 task success**; chỉ `silver_frames[19]` có `try_number = 2` (`crash_graph_after_clear.png`). Dữ liệu **không nhân đôi**: `silver_frames` có 1246 dòng = 1246 `frame_id` khác nhau; `scripts/lab.sh verify` **27/27**; `dataset_version` **vẫn `bd9952ba7d92`**. Lý do: mỗi trip là một partition ghi bằng Delta overwrite (Câu 4), nên chạy lại một task là an toàn, và chia mỗi trip một task giúp chỉ phải chạy lại 1/26 phần việc nặng nhất (decode video).

### Câu 10
Nâng lên 500 xe/ngày (≈ 250× hiện tại, mỗi xe nhiều giờ video, 7 camera):
1. **Decode video bằng FFmpeg trong một container Airflow** (slide 24): đây là nút cổ chai CPU; 26 task chạy tuần tự/ít song song trên một máy sẽ không kịp trong ngày. Thay bằng xử lý phân tán trên cụm worker (Spark/Ray hoặc Kubernetes job, Airflow chỉ điều phối với `KubernetesPodOperator`/Celery), dùng GPU decode (NVDEC), và ingest theo luồng (Kafka) thay vì nạp lô cả file.
2. **DuckDB + dbt chạy trong một tiến trình, một file `.duckdb`** (slide 22): một máy, một người ghi, bộ nhớ có hạn; join ASOF hàng tỷ dòng telemetry sẽ vỡ. Thay bằng engine phân tán đọc thẳng Delta (Spark SQL / Trino / Databricks) với dbt adapter tương ứng, partition theo ngày + xe, và chạy incremental (`is_incremental`) thay vì build lại toàn bộ.
3. **Lưu trữ và phục vụ: một node MinIO + Postgres làm warehouse + ảnh JPG nhỏ lẻ** (slide 12, 23): hàng trăm triệu file ảnh nhỏ làm chậm list/đọc, Postgres một node không chịu nổi bảng gallery/mart cỡ đó. Thay bằng S3/MinIO phân tán với ảnh gom lại (WebDataset/tar shard, Parquet), một warehouse cột (BigQuery/Snowflake/ClickHouse) cho Metabase, và quản lý dataset version bằng công cụ chuyên dụng (lakeFS/DVC) thay vì thư mục theo version.

## Câu hỏi riêng của bạn trên Metabase

Hai thẻ có tiền tố `[2A202602757]` trong dashboard (`metabase_dashboard.png`):

```sql
-- 1. Tỷ lệ khung bị loại theo từng trip
select drive_id, trip_id, split, frames, frames - frames_kept as dropped,
       round(100.0 * (frames - frames_kept) / frames, 1) as drop_pct, avg_speed_kmh
from marts.mart_trip_summary
order by drop_pct desc;

-- 2. Tỷ lệ khung bị loại theo dải tốc độ trung bình của trip (câu hỏi vận hành, điểm cộng)
select case when avg_speed_kmh < 20 then '1: <20 km/h' when avg_speed_kmh < 50 then '2: 20-50 km/h' else '3: >=50 km/h' end as speed_band,
       count(*) as trips, round(100.0 * sum(frames - frames_kept) / sum(frames), 1) as drop_pct
from marts.mart_trip_summary group by 1 order by 1;
```

Diễn giải: trip bị loại nhiều nhất là **trip-04622 (56,7%, 34/60)**, rồi trip-03904 (50%), trip-02086 (46,7%), trip-08888 (40%) — đều ở drive A. Tra lại silver cho thấy gần như toàn bộ là **near-duplicate** (trip-04622: 34 khung trùng, 0 đen/mờ/mất đồng bộ; riêng trip-06570 có 9 khung mờ). Theo dải tốc độ: trip < 20 km/h (5 trip) mất **32,4%**, 20–50 km/h (7 trip) mất 17,1%, ≥ 50 km/h (14 trip) mất 14,4%. Tức là nguyên nhân loại chính là xe chạy chậm/đứng yên chứ không phải lỗi cảm biến; hai trip test (04622, 08888) bị mất ~40–57% khung nên test set nghiêng về trip có nhiều khung khác biệt.

## Thí nghiệm riêng (điểm cộng): đổi `sync_tol_ms`

`scripts/lab.sh dbt run --select silver_frame_telemetry+ --vars '{sync_tol_ms: N}'` (bằng chứng: `report/evidence/own_sync_tol.json`; sau đó chạy lại `step all --reset` với giá trị gốc 150):

| sync_tol_ms | khung mất đồng bộ | khung trong gold | train / val / test |
|---|---|---|---|
| 50 | 600 | 538 | 377 / 89 / 72 |
| 100 | 324 | 762 | 532 / 126 / 104 |
| **150 (gốc)** | **14** | **1019** | **717 / 162 / 140** |
| 300 | 14 | 1019 | 717 / 162 / 140 |

Telemetry lấy mẫu khoảng 80 ms/lần, nên khung hình cách mẫu gần nhất tới ~40–80 ms là bình thường (trung bình 66–73 ms ở `step 3`). Đặt dung sai 50 ms làm mất **47%** gold vì những khoảng lệch tự nhiên đó; từ 150 ms trở lên chỉ còn đúng 14 khung nằm trong các đoạn cảm biến mất tín hiệu thật (max 2884–3112 ms), và nới tới 300 cũng không đổi gì. Vậy 150 ms là điểm "đầu gối" hợp lý: đủ rộng cho nhịp lấy mẫu, vẫn chặn được đoạn mất tín hiệu.

## Nhìn lại

- Bất ngờ nhất: trip-04622 chạy nhanh (67 km/h) mà vẫn bị coi là trùng 34/60 khung. dHash 64 bit trên ảnh nhỏ có thể quá thô với cảnh đường cao tốc đơn điệu (trời, mặt đường chiếm phần lớn khung) → đây có thể là **false positive** mà pipeline chưa phân biệt được với camera đứng hình thật; nên kết hợp thêm tín hiệu tốc độ (xe đang chạy > 30 km/h thì khung trùng là đáng ngờ).
- Lỗi dữ liệu pipeline chưa bắt: telemetry **giá trị hợp lệ nhưng sai** (ví dụ GPS nhảy vài trăm mét giữa hai mẫu, tốc độ 30 m/s khi gia tốc ~0 và vị trí đứng yên) vẫn qua `in_range`; cần kiểm tra nhất quán chéo (tốc độ so với quãng đường GPS).
- Sự cố môi trường: trên Windows, Git `core.autocrlf=true` đổi script sang CRLF nên container Airflow không khởi động được (`exec /airflow-entrypoint.sh: no such file or directory`); phải đưa file về LF. `check_submission.py --save-verify` cũng không gọi được `scripts/lab.sh` trên Windows nên tôi chạy `scripts/lab.sh verify > report/verify.txt` bằng Git Bash.
- Chưa rõ: vì sao task của trip-07304 là map_index 19 trong khi trip này ở vị trí 9 của `list-trips` — thứ tự mapping trong DAG khác thứ tự của lệnh liệt kê.
