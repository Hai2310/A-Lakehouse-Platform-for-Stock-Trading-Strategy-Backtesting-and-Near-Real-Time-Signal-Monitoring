# Thiết kế tầng Silver

Silver chuẩn hóa Market, VSDC và BCTC về cùng định danh chứng khoán, quy ước thời gian và đơn vị. Dữ liệu đầu ra phải truy về bản ghi Bronze và cho biết thời điểm có thể sử dụng trong backtest. Giai đoạn đầu chạy trên Parquet local trong `data/`.

## Hiện trạng đầu vào

| Nguồn | Bronze hiện có | Điều cần kiểm tra trước khi dùng |
|---|---|---|
| Market | Nến `1D`, `5m`, `1m` của 1.000 cổ phiếu. Có thêm nến của bốn chỉ số, danh mục, ảnh chụp khớp lệnh và một số kỳ thành phần rổ. | `1m`/`5m` cổ phiếu có khoảng ba tháng. Kiểm tra thực tế theo mã và phiên. Thành phần rổ và ảnh chụp chỉ có giá trị tại các mốc được xác minh. |
| VSDC | `corporate_actions` có 21.240 dòng lịch quyền. `notice_details` có HTML của 9.602 thông báo được chọn theo tiêu đề. Nối hai đầu vào bằng `notice_id`. | Ngày trên lịch là ngày đăng ký cuối cùng. Dòng “Cập nhật ngày” của bài viết cần giữ dưới dạng thời điểm nguồn ghi nhận. Muốn dùng làm ngày công bố ban đầu phải có bằng chứng. |
| BCTC | Script FHSC ghi `financial_statements` theo loại báo cáo và kỳ quý/năm. | Tại lúc lập thiết kế, chưa thấy Parquet BCTC trong vị trí mặc định. Cần lấy mẫu thực để xác nhận tên chỉ tiêu, đơn vị, phạm vi hợp nhất và ngày công bố trước khi chốt mapping. |

## Bảng và khóa

Thiết kế gồm **16 bảng Silver**, trong đó có **13 bảng nghiệp vụ** và **3 bảng hỗ trợ**. Khóa dưới đây xác định một dòng trong bảng. Mỗi bảng lưu thêm `source` hoặc thông tin nguồn tương ứng để truy vết.

### Market · 9 bảng

| Bảng | Khóa | Cột chính và ý nghĩa |
|---|---|---|
| `daily_ohlc` | `security_id, trading_date, source, bar_origin` | Một nến ngày. Lưu OHLC, `volume_shares`, `bar_end_utc`, `available_at_utc`, `price_basis`. |
| `intraday_ohlc` | `security_id, interval, bar_start_utc, source, bar_origin` | Một nến `1m` hoặc `5m`. Lưu thêm `bar_end_utc`, `trading_date`, `session_segment`, `available_at_utc`. |
| `daily_index` | `index_id, trading_date, source, bar_origin` | Nến ngày của chỉ số. Giá tính theo điểm chỉ số. |
| `intraday_index` | `index_id, interval, bar_start_utc, source, bar_origin` | Nến trong phiên của chỉ số. Giữ quy ước khối lượng có xác nhận của nguồn. |
| `daily_stock_price` | `security_id, exchange, source, snapshot_kind, board, observed_at_utc` | Ảnh chụp theo loại, như `LATEST_TRADE` hoặc `QUOTE_BOARD`. `last_trade_at_utc` là thời điểm khớp cuối cùng. |
| `securities` | `security_id` | Danh mục định danh ổn định, loại chứng khoán, tên, tiền tệ và mã hiện tại nếu đã xác minh. |
| `securities_details` | `security_id, source, snapshot_kind, snapshot_id` | Ảnh chụp hồ sơ hoặc định nghĩa giao dịch. `snapshot_id` phân biệt bản theo sàn/bảng giao dịch nếu nguồn trả nhiều bản cùng lúc. |
| `index_list` | `index_id` | Định danh chỉ số, mã, tên và sàn. |
| `index_components` | `index_id, security_id, source, snapshot_id` | Một mã thành viên trong một kỳ công bố. Giữ kỳ công bố, ngày hiệu lực nếu xác minh được, ngày quan sát và các tỷ lệ do nguồn cung cấp. |

### VSDC · 1 bảng

| Bảng | Khóa | Cột chính và ý nghĩa |
|---|---|---|
| `corporate_actions` | `event_id, revision_id` | Một phiên bản của một quyền. Có `security_id`, `notice_id`, `event_type`, `status`, `announced_at_utc`, `source_updated_at_utc`, `available_at_utc`, `ex_date`, `record_date`, `payment_date`, `cash_per_share_vnd`, tỷ lệ chuyển đổi và `subscription_price_vnd` nếu phù hợp. |

Một thông báo có thể tạo nhiều dòng, mỗi dòng ứng với một quyền. Ví dụ công bố BLI `36052` tạo một sự kiện cổ tức cổ phiếu và một sự kiện quyền mua. Các bản sửa hoặc hủy được giữ với nguồn dẫn riêng. Chỉ nối chúng với `event_id` cũ khi xác minh đúng sự kiện, nếu còn mơ hồ thì đưa vào danh sách kiểm tra. Lưu tỷ lệ dạng số cổ phiếu cũ/mới hoặc số tiền trên mỗi cổ phiếu với đơn vị rõ ràng.

### BCTC · 3 bảng

| Bảng | Chỉ tiêu ưu tiên |
|---|---|
| `income_statement` | Doanh thu thuần, lợi nhuận sau thuế, lợi nhuận thuộc cổ đông công ty mẹ, EPS. |
| `balance_sheet` | Tổng tài sản, nợ phải trả, vốn chủ sở hữu, tiền và tương đương tiền. |
| `cash_flow` | Dòng tiền từ kinh doanh, đầu tư và tài chính. |

Khóa chung là `security_id, fiscal_period_end, period_type, statement_scope, source, revision_id`. Quý và năm dùng chung mỗi bảng. Giữ `published_at_utc`, `available_at_utc`, `observed_at_utc`, tiền tệ và đơn vị gốc. Báo cáo hợp nhất và riêng lẻ có khóa riêng. Báo cáo chưa rõ kỳ hoặc phạm vi đi vào quarantine vì thiếu khóa. Báo cáo thiếu ngày công bố được gắn `WARN` và loại khỏi truy vấn point-in-time. Chỉ dùng kỳ lũy kế để tính số quý riêng khi có đủ kỳ đối chiếu và cùng phạm vi báo cáo. Các yếu tố như ROE và P/E được tính ở Gold.

### Bảng hỗ trợ · 3 bảng

| Bảng | Khóa | Vai trò |
|---|---|---|
| `security_symbol_history` | `security_id, symbol, exchange, valid_from` | Ánh xạ mã và ISIN theo khoảng hiệu lực. Giữ `observed_at_utc` và bằng chứng cho ngày bắt đầu/kết thúc. |
| `trading_calendar` | `exchange, trading_date, session_segment, calendar_version` | Lịch theo sàn, đoạn phiên sáng/chiều/đấu giá, giờ bắt đầu/kết thúc và ngoại lệ theo thời kỳ. |
| `data_coverage` | `instrument_type, instrument_id, interval, source, bar_origin, trading_date, calendar_version` | Số nến kỳ vọng nếu có đủ căn cứ từ lịch và chính sách nguồn, số nến nhận, mốc đầu/cuối, khoảng thiếu và lý do đã xác minh. |

## Quy ước xử lý

1. **Định danh.** Dùng ISIN và lịch sử mã/sàn có bằng chứng để lập `security_id`. Với nguồn chỉ có ticker, ánh xạ theo mã, sàn và ngày giao dịch. Trường hợp nhiều ứng viên hoặc chưa có bằng chứng được ghi nhận để xử lý. Việc lưu ảnh chụp hiện tại chỉ xác nhận định danh tại mốc quan sát đó.
2. **Thời gian.** Lưu timestamp có múi giờ UTC. `trading_date` là ngày phiên Việt Nam. Giữ riêng thời gian dữ liệu mô tả, thời điểm công bố hoặc khả dụng và thời điểm hệ thống thu thập. Giá trị chưa xác minh để `null`. Backtest chỉ sử dụng bản ghi đủ điều kiện `available_at_utc <= decision_time_utc`.
3. **Giá và đơn vị.** Giá cổ phiếu dùng VND/cổ phiếu, khối lượng dùng số cổ phiếu và tiền BCTC dùng VND sau khi xác minh hệ số quy đổi. Giữ đơn vị và cơ sở giá của nguồn. Dữ liệu có `price_basis=UNKNOWN` cần được xác minh trước khi điều chỉnh quyền. Nếu có nến tổng hợp từ `1m`, lưu `bar_origin=DERIVED_1M`, lịch phiên và độ phủ. Giữ nến tải trực tiếp với `bar_origin=DIRECT` để đối soát. Khung `1D` tải trực tiếp vẫn có thể phục vụ backtest dài hạn trong phạm vi xác nhận.
4. **Truy vết và phiên bản.** Lưu `source`, khóa gốc, `bronze_file`, `observed_at_utc`, `silver_run_id`, `schema_version` và trạng thái chất lượng. Ghi đè bản ghi cùng khóa chỉ khi quy tắc chọn bản ghi xác định được. Giữ các phiên bản nghiệp vụ khác nhau của BCTC và VSDC. Báo xung đột OHLCV giữa nguồn để Gold có thể áp dụng chính sách chọn nguồn rõ ràng.

## Data Quality và ghi dữ liệu

```text
data/bronze/{market,vsdc,bctc}
    → đọc và chuẩn hóa theo từng lô
    → ghi Parquet Silver tạm và dòng lỗi
    → kiểm tra chất lượng và ghi độ phủ
    → publish các phân vùng đạt yêu cầu cho Gold
```

- **Chặn phân vùng** khi lô đầu ra thiếu cột bắt buộc, còn khóa trùng, còn OHLCV không hợp lệ sau bước lọc, hoặc ghi Parquet thất bại. Dòng không hợp lệ đi vào `data/quarantine/silver/<domain>/<table>/` kèm mã lỗi và đường dẫn Bronze. Kiểm tra tỷ lệ dòng bị loại theo ngưỡng được chốt cho từng nguồn và khung sau khi chạy mẫu. Giữ đầu ra đã publish trước đó nếu lần mới thất bại.
- **Báo cảnh báo và giới hạn phạm vi sử dụng** khi thiếu lịch phiên xác thực, lịch sử nguồn ngắn, thành phần rổ chỉ có snapshot hoặc dữ liệu chưa rõ ngày công bố. Độ phủ cần phân biệt trạng thái xác minh được như chưa niêm yết, tạm ngừng, không có giao dịch, lỗi nguồn và chưa xác định. Tránh suy ra thiếu dữ liệu chỉ từ việc một phút không có nến.
- **Great Expectations** kiểm tra cột bắt buộc, số dòng, khóa và quy tắc nghiệp vụ trên Parquet tạm của 13 bảng bằng Spark. Lỗi validation dừng publish và lưu kết quả vào thư mục quarantine của lượt chạy. **Soda** được dành cho bước theo dõi độ mới và độ phủ theo phân vùng.
- Dữ liệu xuất dưới `data/silver/{market,vsdc,bctc,reference,quality}/<table>/`. Nến trong phiên ưu tiên phân vùng theo `interval` và tháng phiên, nến ngày theo năm, BCTC theo năm tài chính và VSDC theo năm chốt quyền. Đọc lại file tạm trước khi publish. Chạy lại cùng đầu vào phải cho cùng khóa và nội dung nghiệp vụ.

## Thứ tự triển khai và nghiệm thu

| Bước | Phần triển khai | Cách kiểm chứng |
|---|---|---|
| 1 | Mở rộng `transform/silver/market.py` và chuẩn hóa định danh, lịch phiên, độ phủ. | Với một mã mỗi sàn, đọc được nến `1D`, `5m`, `1m`, kiểm tra giờ phiên, đơn vị nguồn và báo cáo khoảng thiếu thực tế. |
| 2 | Thêm `transform/silver/vsdc.py` đọc lịch và nội dung công bố. | BLI `36052` tạo hai sự kiện có nguồn dẫn. CLC `35991` có giá trị tiền/cổ phiếu và ngày thanh toán đối chiếu được với bài gốc. |
| 3 | Thêm `transform/silver/bctc.py` sau khi có mẫu thật từ FHSC. | Một mã có ba báo cáo, quý và năm. Join theo thời gian chọn đúng phiên bản có thể sử dụng tại ngày quyết định. |
| 4 | Chạy kiểm tra tổng thể, sau đó nối Silver sang Gold. | Chạy lại không tăng số khóa, các phân vùng lỗi có báo cáo, truy vấn trước ngày công bố cho kết quả đúng và không có dữ liệu intraday bị điền giả. |

## Chạy Silver Market và VSDC bằng Spark

Cần Java 17 trở lên và PySpark 4.0.1. Đặt `JAVA_HOME` nếu Java nằm ngoài `PATH`.

```bash
source .venv/bin/activate
python -m pip install pyspark==4.0.1 great-expectations==1.25.0
python -m transform.silver.pipeline
```

Lệnh tạo 13 bảng từ Bronze Market và VSDC. Mỗi lượt publish một snapshot trong `data/silver/_runs/` và cập nhật con trỏ `data/silver/current/` sau khi đã ghi đủ các bảng. Spark xử lý nến, độ phủ và lịch quan sát. Các bảng danh mục nhỏ và bước trích xuất nội dung VSDC dùng pandas/PyArrow. Đọc từng thư mục bảng trong snapshot như một Parquet dataset. Bảng giá chia theo `timeframe` và `period` (năm cho 1D, tháng cho 1m và 5m). Dùng `--master`, `--driver-memory` và `--shuffle-partitions` để điều chỉnh tài nguyên. Báo cáo tại `data/silver/current/quality_report.csv`, các dòng lỗi được ghi tại `data/quarantine/silver/`.

Lịch và độ phủ trong bản local dựa trên nến quan sát được. Phiên chính thức, số nến kỳ vọng, điều kiện `available_at_utc` và lịch sử hiệu lực chưa xác minh được giữ trống hoặc có nhãn kiểm tra. Mốc cập nhật của VSDC được lưu riêng với ngày công bố. Báo cáo Great Expectations nằm ở `data/silver/current/gx_validation.json`. Có thể chạy lại kiểm tra bằng `python -m transform.silver.quality`.
