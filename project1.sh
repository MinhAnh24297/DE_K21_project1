#!/bin/bash

# Tên file dữ liệu
DATA_FILE="tmdb-movies.csv"
CLEAN_DATE_FILE="movies_clean.csv"
SORTED_DATE_FILE="movies_sorted_by_date.csv"
HIGH_RATING_FILE="movies_high_rating.csv"

echo "1. Sắp xếp các bộ phim theo ngày phát hành giảm dần rồi lưu ra một file mới"
echo "Loading..."

# --- Bước 1: Chuẩn hóa Ngày bằng AWK ---
# Sử dụng gawk với FPAT để xử lý CSV chính xác.
# FPAT giúp đọc các trường có dấu phẩy và dấu nháy kép bên trong.
# OFS="," đảm bảo các cột được in ra lại đúng định dạng CSV.
awk '
    BEGIN {
        # Sử dụng FPAT để xác định trường CSV chính xác (cho các trường có dấu phẩy bên trong)
        FPAT = "([^,]+)|(\"([^\"]|\"\")*\")";
        OFS=","
    }
    # Dòng 1 (Header): Chỉ in ra và chuyển sang dòng tiếp theo
    NR==1 {print; next}
    {
        # 1. TIỀN XỬ LÝ (Thay thế / thành - chỉ trong cột 16
        # $16 là release_date, có định dạng mm/dd/yy hoặc mm-dd-yy.
        gsub("/", "-", $16); 

        # 2. CHUẨN HÓA NĂM (YYYY)
        # Tách ngày (hiện là mm-dd-yy) dựa trên ký tự -
        split($16, d, "-"); 

        # Kiểm tra nếu năm (d[3]) chỉ có 2 chữ số
        if (length(d[3]) == 2) {
            # Logic quy ước: YY < 25 => 20YY, còn lại => 19YY
            if (d[3] < 25) {
                year = "20" d[3];
            } else {
                year = "19" d[3];
            }
            
            # Định dạng lại ngày thành YYYY-MM-DD và gán lại vào cột 16
            $16 = year "-" d[1] "-" d[2];
        }
        # In ra toàn bộ dòng đã xử lý
        print
    }' "$DATA_FILE" > "$CLEAN_DATE_FILE"

# --- Bước 2: Sắp xếp theo ngày phát hành giảm dần ---
# Sắp xếp dựa trên cột release_date ($16) đã được chuẩn hóa (YYYY-MM-DD)
csvsort -r -c release_date "$CLEAN_DATE_FILE" > "$SORTED_DATE_FILE"

echo "Hoàn thành Task 1. Kết quả lưu tại file $SORTED_DATE_FILE"
echo "================================================="
echo "2. Lọc ra các bộ phim có đánh giá trung bình > 7.5 rồi lưu ra một file mới"
echo "Loading..."

# Lọc các bộ phim có đánh giá trung bình (vote_average) từ 7.6 trở lên.
# Cú pháp Regex:
# ^(7\.[6-9]) : Bắt đầu bằng 7, sau đó là dấu chấm (.), và tiếp theo là 6, 7, 8, hoặc 9. (Ví dụ: 7.6, 7.9)
# |           : HOẶC
# [8-9]\.     : Bắt đầu bằng 8 hoặc 9, sau đó là dấu chấm (.). (Ví dụ: 8.0, 9.1)

csvgrep -c vote_average -r '^(7\.[6-9]|[8-9]\.)' "$DATA_FILE" > "$HIGH_RATING_FILE"

echo "Hoàn thành Task 2. Kết quả lưu tại file $HIGH_RATING_FILE"
echo "================================================="
echo "3. Tìm ra phim nào có doanh thu cao nhất và doanh thu thấp nhất"
echo "Doanh thu các bộ phim được đánh giá dựa trên revenue adjustment"

echo "a. Phim có Doanh thu cao nhất"

# Pipeline:
# 1. csvgrep: Lọc các dòng có 'revenue_adj' không rỗng, > 0 (bắt đầu bằng ít nhất 1 chữ số, tránh lỗi khi sort).
# 2. csvsort: Sắp xếp giảm dần (-r) theo cột 'revenue_adj'.
# 3. csvcut: Chỉ giữ lại 'original_title' và 'revenue_adj'.
# 4. csvformat -T: original_title và revenue_adj cách nhau bởi khoảng trắng tab.
# 5. head -n 2: Lấy header + dòng đầu tiên.
csvgrep -c revenue_adj -r '^[1-9]' "$DATA_FILE" | \
csvsort -c revenue_adj -r | \
csvcut -c original_title,revenue_adj | \
csvformat -T | head -n 2

echo "b. Phim có Doanh thu thấp nhất"

# Pipeline:
# 1. csvgrep: Lọc các dòng có 'revenue_adj' không rỗng, > 0  (bắt đầu bằng ít nhất 1 chữ số, tránh lỗi khi sort).
# 2. csvsort: Sắp xếp tăng dần theo cột 'revenue_adj'
# 3. csvcut: Chỉ giữ lại 'original_title' và 'revenue_adj'.
# 4. csvformat -T: original_title và revenue_adj cách nhau bởi khoảng trắng tab.
# 5. head -n 2: Lấy header + dòng đầu tiên.
csvgrep -c revenue_adj -r '^[1-9]' "$DATA_FILE" | \
csvsort -c revenue_adj | \
csvcut -c original_title,revenue_adj | \
csvformat -T | head -n 2

echo "================================================="
echo "4. Tính tổng doanh thu tất cả các bộ phim"

# Pipeline:
# 1. csvcut: Trích xuất chính xác cột 'revenue_adj'.
# 2. tail -n +2: Bỏ qua dòng header.
# 3. awk: Tính tổng (sum) các giá trị trong cột.
#    - awk '{x+=$1}': Cộng dồn giá trị của cột đầu tiên (sau khi csvcut) vào biến x.
#    - END{print x}: In ra tổng cuối cùng.

TOTAL_REVENUE=$(csvcut -c revenue_adj "$DATA_FILE" | \
tail -n +2 | \
awk '{x+=$1}END{printf "%.3f\n", x}') # Dùng printf để định dạng số thập phân

echo "Tổng Doanh thu Điều chỉnh của tất cả các bộ phim = $TOTAL_REVENUE"
echo "================================================="
echo "5. Top 10 bộ phim đem về lợi nhuận cao nhất"
echo "lợi nhuận được tính dựa trên revenue_adj - budget_adj"

# Pipeline:
# 1. csvcut: Trích xuất chính xác 3 cột cần thiết: original_title ($1), budget_adj ($2), revenue_adj ($3).
# 2. tail -n +2: Bỏ qua dòng header.
# 3. awk: Tính lợi nhuận và định dạng đầu ra.
#    - -F ',': Đặt delimiter là dấu phẩy (dữ liệu sau csvcut đã chuẩn).
#    - ($3 - $2): Tính Lợi nhuận (Revenue_adj - Budget_adj).
#    - printf "%.0f,%s\n": In Lợi nhuận (số nguyên) và Tên phim, phân cách bằng dấu phẩy.
# 4. sort: Sắp xếp giảm dần (-r) theo giá trị số (-n) của cột 1 (Lợi nhuận).
# 5. head -n 10: Lấy 10 dòng đầu tiên.

csvcut -c original_title,budget_adj,revenue_adj "$DATA_FILE" | \
tail -n +2 | \
awk -F ',' '{printf "%.0f,%s\n", ($3 - $2), $1}' | \
sort -t ',' -k 1,1 -n -r | \
head -n 10

echo "================================================="
echo "6. Đạo diễn nào có nhiều bộ phim nhất và diễn viên nào đóng nhiều phim nhất"

echo "1. Đạo diễn có nhiều phim nhất"
# Pipeline:
# 1. Trích xuất cột 'director' bằng csvcut.
# 2. tail -n +2: Bỏ qua header.
# 3. sed 's/"//g': Xóa dấu ngoặc kép.
# 4. tr '|' '\n': Tách các đạo diễn ra mỗi dòng.
# 5. grep -v '^$': Xóa dòng trống.
# 6. sort | uniq -c: Đếm số lần xuất hiện.
# 7. sort -nr | head -n 1: Lấy dòng có số lượng lớn nhất.
# 8. awk: Lấy kết quả cuối cùng (số lượng và tên) và định dạng thành câu.
#    - $1 là số lượng (Count), $2 là Tên (Name).
csvcut -c director "$DATA_FILE" | \
tail -n +2 | \
tr -d '"' | \
tr '|' '\n' | \
grep -v '^$' | \
sort | \
uniq -c | \
sort -nr | \
head -n 1

echo "2. Diễn viên đóng nhiều phim nhất"
# Pipeline: (Tương tự cho cột 'cast')
csvcut -c cast "$DATA_FILE" | \
tail -n +2 | \
tr -d '"' | \
tr '|' '\n' | \
grep -v '^$' | \
sort | \
uniq -c | \
sort -nr | \
head -n 1

echo "================================================="
echo "8. Idea của bạn để có thêm những phân tích cho dữ liệu?"

# Pipeline:
# 1. csvcut: Trích xuất cột thứ 9 ('director').
# 2. tail -n +2: Bỏ qua dòng header.
# 3. grep -c: Đếm số dòng (-c) có chứa ký tự '|' (đại diện cho nhiều hơn 1 đạo diễn).

NUM_MULTIPLE_DIRECTORS=$(csvcut -c director "$DATA_FILE" | \
tail -n +2 | \
grep -c '|')

echo "Số lượng phim có từ 2 đạo diễn trở lên là: $NUM_MULTIPLE_DIRECTORS"
