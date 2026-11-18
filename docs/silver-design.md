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

