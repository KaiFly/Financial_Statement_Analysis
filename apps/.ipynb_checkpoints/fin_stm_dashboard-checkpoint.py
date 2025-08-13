import streamlit as st
import pandas as pd
import plotly.express as px
from io import BytesIO

# --------------------------------------------------------------------------
# Cấu hình trang (Page Configuration)
# --------------------------------------------------------------------------
# Thiết lập cấu hình cho trang Streamlit, sử dụng layout rộng để có không gian hiển thị tốt hơn.
st.set_page_config(
    page_title="Bảng điều khiển Phân tích Tài chính",
    page_icon="📊",
    layout="wide"
)

# --------------------------------------------------------------------------
# Hàm tải và xử lý dữ liệu (Data Loading and Processing Function)
# --------------------------------------------------------------------------
@st.cache_data # Sử dụng cache để tăng tốc độ tải lại ứng dụng
def load_data(file_path):
    """
    Tải dữ liệu từ file CSV, xử lý các kiểu dữ liệu và loại bỏ các dòng không cần thiết.
    
    Args:
        file_path (str): Đường dẫn đến file CSV.

    Returns:
        pandas.DataFrame: DataFrame đã được xử lý.
    """
    try:
        # Đọc dữ liệu từ file, sử dụng tab làm dấu phân cách
        df = pd.read_csv(file_path, sep='\t')
        
        # Xử lý cột 'value': chuyển đổi sang kiểu số, các giá trị không hợp lệ sẽ thành NaN
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        
        # Xử lý cột 'report_date': chuyển đổi sang kiểu số nguyên, các giá trị không hợp lệ sẽ thành NaN
        df['report_date'] = pd.to_numeric(df['report_date'], errors='coerce')

        # Chỉ loại bỏ NaN ở các cột quan trọng cho việc lọc, giữ lại NaN ở cột 'account' để tính toán
        df.dropna(subset=['report_date', 'exchange', 'industry', 'report_type'], inplace=True)

        # Chuyển đổi 'report_date' sang kiểu integer
        df['report_date'] = df['report_date'].astype(int)
        
        return df
    except FileNotFoundError:
        st.error(f"Lỗi: Không tìm thấy tệp tại đường dẫn '{file_path}'. Vui lòng kiểm tra lại.")
        return pd.DataFrame() # Trả về DataFrame rỗng nếu có lỗi

# --------------------------------------------------------------------------
# Hàm chuyển đổi DataFrame sang Excel (DataFrame to Excel Conversion Function)
# --------------------------------------------------------------------------
def to_excel(df):
    """
    Chuyển đổi một DataFrame sang định dạng file Excel trong bộ nhớ.

    Args:
        df (pandas.DataFrame): DataFrame cần chuyển đổi.

    Returns:
        bytes: Dữ liệu file Excel dưới dạng bytes.
    """
    output = BytesIO()
    # Sử dụng 'with' để đảm bảo writer được đóng đúng cách
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='FilteredData')
    processed_data = output.getvalue()
    return processed_data

# --------------------------------------------------------------------------
# Tải dữ liệu chính
# --------------------------------------------------------------------------
# Sử dụng file dữ liệu lớn hơn theo yêu cầu
file_path = 'data/Financial_Statement__Full_Company_sample.csv' 
file_format_path = 'data/account_mapping.xlsx'
file_company_path = 'data/Vietcap__Company_list.xlsx'

# Tải dữ liệu chính từ file parquet
try:
    df = pd.read_parquet('data/Financial_Statement__Full_Company_L10Y.parquet')
    df['report_date'] = df['report_date'].astype(int)
except FileNotFoundError:
    df = pd.read_parquet('Financial_Statement__Full_Company_L10Y.parquet')
    df['report_date'] = df['report_date'].astype(int)


# Nếu không tải được dữ liệu, dừng ứng dụng
if df.empty:
    st.stop()

# --------------------------------------------------------------------------
# Giao diện thanh bên (Sidebar Interface) - Đã sắp xếp lại
# --------------------------------------------------------------------------
st.sidebar.header("Bộ lọc Dữ liệu ⚙️")

# --- Filter 1: Sàn giao dịch (Exchange) ---
sorted_exchanges = sorted(df['exchange'].unique())
selected_exchanges = st.sidebar.multiselect(
    'Sàn giao dịch (Exchange)',
    options=sorted_exchanges,
    default=sorted_exchanges
)

# --- Filter 2: Loại báo cáo (Report Type) ---
report_types = ['Tất cả'] + sorted(df['report_type'].unique())
selected_report_type = st.sidebar.selectbox(
    'Loại báo cáo (Report Type)',
    options=report_types
)

# --- Filter 3: Năm báo cáo (Report Date) ---
min_year, max_year = int(df['report_date'].min()), int(df['report_date'].max())
selected_year_range = st.sidebar.slider(
    'Năm báo cáo (Report Year)',
    min_value=min_year,
    max_value=max_year,
    value=(min_year, max_year)
)

# --- Filter 4: Ngành (Industry) ---
sorted_industries = sorted(df['industry'].unique())
selected_industries = st.sidebar.multiselect(
    'Ngành (Industry)',
    options=sorted_industries,
    default=sorted_industries
)

# --------------------------------------------------------------------------
# Lọc dữ liệu dựa trên lựa chọn của người dùng (Data Filtering)
# --------------------------------------------------------------------------
# Bắt đầu với một bản sao của DataFrame gốc để tránh thay đổi dữ liệu gốc
df_filtered = df.copy()

# Áp dụng các bộ lọc
if selected_exchanges:
    df_filtered = df_filtered[df_filtered['exchange'].isin(selected_exchanges)]
if selected_report_type != 'Tất cả':
    df_filtered = df_filtered[df_filtered['report_type'] == selected_report_type]
df_filtered = df_filtered[
    (df_filtered['report_date'] >= selected_year_range[0]) & 
    (df_filtered['report_date'] <= selected_year_range[1])
]
if selected_industries:
    df_filtered = df_filtered[df_filtered['industry'].isin(selected_industries)]


# --------------------------------------------------------------------------
# Giao diện chính (Main Interface)
# --------------------------------------------------------------------------
st.title("📊 Bảng điều khiển Báo cáo Tài chính - ValuX Team")
st.markdown("---")

# Tải và hiển thị nút download cho các file mapping
try:
    df_account = pd.read_excel(file_format_path)
    mapping_excel_data = to_excel(df_account)
    st.download_button(
        label="📥 Tải xuống file Format trường account",
        data=mapping_excel_data,
        file_name="ValuX_account_formatting.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
except FileNotFoundError:
    st.warning(f"Không tìm thấy file '{file_format_path}'. Nút tải xuống file format sẽ bị vô hiệu hóa.")

try:
    df_company = pd.read_excel(file_company_path)
    company_excel_data = to_excel(df_company)
    st.download_button(
        label="📥 Tải xuống thông tin mã CK",
        data=company_excel_data,
        file_name="ValuX_company_list.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
except FileNotFoundError:
    st.warning(f"Không tìm thấy file '{file_company_path}'. Nút tải xuống thông tin công ty sẽ bị vô hiệu hóa.")


# --- Hiển thị các chỉ số chính (Key Metrics) - Đã cập nhật ---
if not df_filtered.empty:
    # Tính toán các chỉ số mới
    num_records = len(df_filtered)
    num_companies = df_filtered['company_code'].nunique()
    num_accounts = df_filtered['account'].nunique()
    null_accounts = df_filtered['account'].isnull().sum()
    null_ratio = (null_accounts / num_records) * 100 if num_records > 0 else 0

    # Hiển thị 5 chỉ số trên 5 cột
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Số lượng bản ghi", f"{num_records:,}")
    col2.metric("Số lượng công ty", f"{num_companies:,}")
    col3.metric("Số lượng account", f"{num_accounts:,}")
    col4.metric("Account bị null", f"{null_accounts:,}")
    col5.metric("Tỉ lệ null (%)", f"{null_ratio:.2f}%")
else:
    st.warning("Không có dữ liệu phù hợp với bộ lọc đã chọn.")
    st.stop() # Dừng nếu không có dữ liệu để hiển thị

st.markdown("---")

# --- Hiển thị các biểu đồ (Charts) - Đã cập nhật ---
col_chart1, col_chart2 = st.columns(2)

with col_chart1:
    st.subheader("Phân bổ bản ghi theo Ngành")
    # Nhóm dữ liệu theo ngành và đếm số lượng bản ghi
    industry_counts = df_filtered['industry'].value_counts().sort_values(ascending=False)
    # Tạo biểu đồ cột
    fig_industry = px.bar(
        industry_counts,
        x=industry_counts.index,
        y=industry_counts.values,
        title="Số lượng bản ghi theo từng Ngành",
        labels={'y': 'Số lượng bản ghi', 'x': 'Ngành'},
        color=industry_counts.index,
        template='plotly_white'
    )
    fig_industry.update_layout(showlegend=False)
    st.plotly_chart(fig_industry, use_container_width=True)

with col_chart2:
    st.subheader("Tỉ lệ phân bổ các Loại báo cáo")
    # Đếm số lượng bản ghi cho mỗi loại báo cáo
    report_type_counts = df_filtered['report_type'].value_counts()
    # Tạo biểu đồ tròn (pie chart)
    fig_report_type = px.pie(
        report_type_counts,
        names=report_type_counts.index,
        values=report_type_counts.values,
        title="Tỉ lệ các loại báo cáo trong dữ liệu đã lọc",
        template='plotly_white'
    )
    fig_report_type.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig_report_type, use_container_width=True)


# --- Hiển thị mẫu dữ liệu (Sample Data) ---
st.subheader("Dữ liệu chi tiết sau khi lọc")
st.dataframe(df_filtered.head(100)) # Chỉ hiển thị 100 dòng đầu tiên để giao diện gọn gàng

# --- Nút tải xuống (Download Button) ---
st.markdown("---")
st.subheader("Tải xuống dữ liệu")
st.markdown("Tải về toàn bộ dữ liệu đã được lọc. Nếu dữ liệu lớn (>500,000 dòng), tệp sẽ được tải về dưới dạng CSV để tối ưu hiệu suất.")


# Logic để chọn định dạng tải xuống dựa trên kích thước DataFrame
if df_filtered.shape[0] <= 500000:
    excel_data = to_excel(df_filtered)
    st.download_button(
        label="📥 Tải xuống file Excel",
        data=excel_data,
        file_name="ValuX_financial_statement_filtered.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
else:
    # Chuyển đổi DataFrame sang CSV trực tiếp cho các tệp lớn
    csv_data = df_filtered.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Tải xuống file CSV (Dữ liệu lớn)",
        data=csv_data,
        file_name="ValuX_financial_statement_data_filtered.csv",
        mime="text/csv"
    )
