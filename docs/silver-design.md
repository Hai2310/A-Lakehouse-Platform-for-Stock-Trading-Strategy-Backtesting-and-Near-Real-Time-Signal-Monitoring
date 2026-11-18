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

