# Thiết kế tầng Silver

Silver chuẩn hóa Market, VSDC và BCTC về cùng định danh chứng khoán, quy ước thời gian và đơn vị. Dữ liệu đầu ra phải truy về bản ghi Bronze và cho biết thời điểm có thể sử dụng trong backtest. Giai đoạn đầu chạy trên Parquet local trong `data/`.

## Hiện trạng đầu vào

| Nguồn | Bronze hiện có | Điều cần kiểm tra trước khi dùng |
|---|---|---|
| Market | Nến `1D`, `5m`, `1m` của 1.000 cổ phiếu. Có thêm nến của bốn chỉ số, danh mục, ảnh chụp khớp lệnh và một số kỳ thành phần rổ. | `1m`/`5m` cổ phiếu có khoảng ba tháng. Kiểm tra thực tế theo mã và phiên. Thành phần rổ và ảnh chụp chỉ có giá trị tại các mốc được xác minh. |
| VSDC | `corporate_actions` có 21.240 dòng lịch quyền. `notice_details` có HTML của 9.602 thông báo được chọn theo tiêu đề. Nối hai đầu vào bằng `notice_id`. | Ngày trên lịch là ngày đăng ký cuối cùng. Dòng “Cập nhật ngày” của bài viết cần giữ dưới dạng thời điểm nguồn ghi nhận. Muốn dùng làm ngày công bố ban đầu phải có bằng chứng. |
| BCTC | Script FHSC ghi `financial_statements` theo loại báo cáo và kỳ quý/năm. | Tại lúc lập thiết kế, chưa thấy Parquet BCTC trong vị trí mặc định. Cần lấy mẫu thực để xác nhận tên chỉ tiêu, đơn vị, phạm vi hợp nhất và ngày công bố trước khi chốt mapping. |

