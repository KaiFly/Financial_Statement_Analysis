import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from io import BytesIO
import os

# --------------------------------------------------------------------------
# Cấu hình trang (Page Configuration)
# --------------------------------------------------------------------------
# Thiết lập cấu hình cho trang Streamlit, sử dụng layout rộng để có không gian hiển thị tốt hơn.
st.set_page_config(
    page_title="ValuX Team | Financial Statement Data",
    page_icon="📊",
    layout="wide"
)

# --------------------------------------------------------------------------
# CSS Tùy chỉnh (Custom CSS Injection)
# --------------------------------------------------------------------------
# Thêm CSS để thu nhỏ giao diện và cải thiện thẩm mỹ
st.markdown("""
    <style>
        /* Đặt kích thước phông chữ cơ bản cho toàn bộ ứng dụng là 12px */
        html, body, [class*="st-"] {
            font-size: 12px;
        }
        .st-emotion-cache-16txtl3 { /* Sidebar */
             background-color: #f8f9fa;
        }
        h1 {
            color: #FFFFFF; /* Dark Blue */
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            font-weight: 700;
            font-size: 2.5rem; /* Giữ kích thước tiêu đề chính lớn để dễ đọc */
        }
        h2 {
            font-size: 2rem;
        }
        h3 {
            font-size: 1.5rem;
        }
    </style>
    """,
    unsafe_allow_html=True,
)
# --------------------------------------------------------------------------
# Hàm tiện ích (Utility Functions)
# --------------------------------------------------------------------------

@st.cache_data # Sử dụng cache để tăng tốc độ tải lại ứng dụng
def load_parquet_data(file_path):
    """Tải dữ liệu từ file Parquet."""
    if not os.path.exists(file_path):
        st.error(f"Lỗi: Không tìm thấy tệp tại đường dẫn '{file_path}'. Vui lòng kiểm tra lại.")
        return pd.DataFrame()
    try:
        return pd.read_parquet(file_path)
    except Exception as e:
        st.error(f"Lỗi khi đọc file Parquet: {e}")
        return pd.DataFrame()

def to_excel(df):
    """Chuyển đổi DataFrame sang định dạng file Excel trong bộ nhớ."""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='FilteredData')
    return output.getvalue()

# --------------------------------------------------------------------------
# Tải dữ liệu chính (Data Loading)
# --------------------------------------------------------------------------
# Xác định đường dẫn tương đối để ứng dụng linh hoạt hơn
current_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else '.'
data_dir = os.path.join(current_dir, 'data')

# Đường dẫn đến các tệp dữ liệu
file_path = os.path.join(data_dir, 'Financial_Statement__Full_Company_L10Y.parquet')
file_format_path = os.path.join(data_dir, 'account_mapping.parquet')
file_company_path = os.path.join(data_dir, 'Vietcap__Company_List.parquet')

# Tải dữ liệu chính
df = load_parquet_data(file_path)
if df.empty:
    # Fallback to root directory if 'data' folder not found
    file_path = os.path.join(current_dir, 'Financial_Statement__Full_Company_L10Y.parquet')
    df = load_parquet_data(file_path)
    if df.empty:
        st.stop()

df['report_date'] = df['report_date'].astype(int)

# --------------------------------------------------------------------------
# Giao diện thanh bên (Sidebar Interface)
# --------------------------------------------------------------------------
with st.sidebar:
    st.header("Bộ lọc Dữ liệu ⚙️")

    # --- Filter 1: Sàn giao dịch (Exchange) ---
    sorted_exchanges = sorted(df['exchange'].unique())
    selected_exchanges = st.multiselect(
        'Sàn giao dịch (Exchange)',
        options=sorted_exchanges,
        default=sorted_exchanges
    )

    # --- Filter 2: Loại báo cáo (Report Type) ---
    report_types = ['Tất cả'] + sorted(df['report_type'].unique())
    selected_report_type = st.selectbox(
        'Loại báo cáo (Report Type)',
        options=report_types
    )

    # --- Filter 3: Năm báo cáo (Report Date) ---
    min_year, max_year = int(df['report_date'].min()), int(df['report_date'].max() - 1)
    selected_year_range = st.slider(
        'Năm báo cáo (Report Year)',
        min_value=min_year,
        max_value=max_year,
        value=(min_year, max_year)
    )

    # --- Filter 4: Ngành (Industry) ---
    sorted_industries = sorted(df['industry'].unique())
    selected_industries = st.multiselect(
        'Ngành (Industry)',
        options=sorted_industries,
        default=sorted_industries
    )
    # --- Filter 5: Stock (Company_code) ---
    sorted_company = sorted(df['company_code'].unique())
    selected_company = st.multiselect(
        'Mã Chứng Khoán (Company Code) ',
        options=sorted_company,
        default=[] # Mặc định không chọn mã nào
    )
    
    st.divider()
    
    # --- Tùy chỉnh màu sắc cho biểu đồ ---
    st.header("Tùy chỉnh Biểu đồ 🎨")
    # Đặt màu mặc định ở định dạng HEX để tương thích với st.color_picker
    default_color1 = "#66c5cc" # Tương đương màu pastel của Plotly
    default_color2 = "#f68e66" # Tương đương màu pastel của Plotly
    color1 = st.color_picker('Màu cho "Số lượng công ty"', default_color1)
    color2 = st.color_picker('Màu cho "Số lượng chỉ số"', default_color2)

# --------------------------------------------------------------------------
# Lọc dữ liệu (Data Filtering)
# --------------------------------------------------------------------------
# Sử dụng .query() để lọc dữ liệu một cách gọn gàng
query_parts = []
if selected_exchanges:
    query_parts.append('exchange in @selected_exchanges')
if selected_report_type != 'Tất cả':
    query_parts.append('report_type == @selected_report_type')
if selected_industries:
    query_parts.append('industry in @selected_industries')
if selected_company:
    query_parts.append('company_code in @selected_company')

query_parts.append('report_date >= @selected_year_range[0]')
query_parts.append('report_date <= @selected_year_range[1]')

df_filtered = df.query(' and '.join(query_parts))

# --------------------------------------------------------------------------
# Giao diện chính (Main Interface)
# --------------------------------------------------------------------------
st.title("📊 Dữ liệu Báo cáo Tài chính - ValuX Team")
st.markdown("---")

# --- Khu vực tải file phụ trợ ---
with st.container(border=True):
    st.subheader("I. Tài liệu Crawl & Format")
    col1, col2 = st.columns(2)
    with col1:
        # Tải file mapping trong thư mục data hoặc thư mục gốc
        df_account = load_parquet_data(file_format_path)
        if df_account.empty:
            df_account = load_parquet_data(os.path.join(current_dir, 'account_mapping.parquet'))
        
        if not df_account.empty:
            st.download_button(
                label="📥 Tải file Format trường account",
                data=to_excel(df_account),
                file_name="ValuX_account_formatting.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
    with col2:
        df_company = load_parquet_data(file_company_path)
        if df_company.empty:
             df_company = load_parquet_data(os.path.join(current_dir, 'Vietcap__Company_List.parquet'))

        if not df_company.empty:
            st.download_button(
                label="📥 Tải file thông tin mã CK",
                data=to_excel(df_company),
                file_name="ValuX_company_list.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

st.subheader("II. Chỉ số về Dữ liệu 📇")
if not df_filtered.empty:
    with st.container(border=True):
        # Tính toán các chỉ số
        num_records = len(df_filtered)
        num_companies = df_filtered['company_code'].nunique()
        num_accounts = df_filtered['account'].nunique()
        
        # Số liệu null cho 'account'
        null_accounts = df_filtered[df_filtered['account'] == 'nan'].shape[0]
        null_account_ratio = (null_accounts / num_records) * 100 if num_records > 0 else 0
        
        # Số liệu null cho 'value'
        null_values = df_filtered['value'].isnull().sum()
        null_value_ratio = (null_values / num_records) * 100 if num_records > 0 else 0

        # Hiển thị các chỉ số
        cols = st.columns(5)
        cols[0].metric("Số lượng Record", f"{num_records:,}", )
        cols[1].metric("Số lượng Company", f"{num_companies:,}")
        cols[2].metric("Số lượng Account", f"{num_accounts:,}", help = 'Trường chỉ số tài chính trong BCTC đã chuẩn hóa (format)')
        
        with cols[3]:
            st.metric("Account Null", f"{null_accounts:,}")
        with cols[4]:
            st.metric("Account % Null", f"{null_account_ratio:.2f}%")
else:
    st.warning("Không có dữ liệu phù hợp với bộ lọc đã chọn.")
    st.stop()

# --- Hiển thị các biểu đồ (Charts) ---
st.subheader("III. Trực quan hóa Dữ liệu 📈")

# Chart 1: Time Series (Full Width)
with st.container(border=True):
    st.markdown("#### **Số lượng Công ty & Chỉ số BCTC theo Thời gian**")
    # Chuẩn bị dữ liệu cho biểu đồ time series
    df_time_series = df_filtered.groupby('report_date').agg(
        company_count=('company_code', 'nunique'),
        record_count=('account','nunique') 
    ).reset_index()

    # Tạo biểu đồ với trục y thứ hai
    fig_time_series = make_subplots(specs=[[{"secondary_y": True}]])

    # Thêm đường cho số lượng công ty
    fig_time_series.add_trace(
        go.Scatter(
            x=df_time_series['report_date'], 
            y=df_time_series['company_count'], 
            name="Số lượng công ty", 
            mode='lines+markers',
            line=dict(color=color1) # <--- ÁP DỤNG MÀU TÙY CHỈNH
        ),
        secondary_y=False,
    )

    # Thêm đường cho số lượng bản ghi
    fig_time_series.add_trace(
        go.Scatter(
            x=df_time_series['report_date'], 
            y=df_time_series['record_count'], 
            name="Số lượng chỉ số", 
            mode='lines+markers',
            line=dict(color=color2) # <--- ÁP DỤNG MÀU TÙY CHỈNH
        ),
        secondary_y=True,
    )

    # Cập nhật layout
    fig_time_series.update_layout(
        height=400, 
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        paper_bgcolor='rgba(0,0,0,0)', 
        plot_bgcolor='rgba(0,0,0,0)'
    )
    fig_time_series.update_xaxes(
        title_text="Năm báo cáo",
        dtick=1 
    )
    fig_time_series.update_yaxes(title_text="Số lượng Công ty", secondary_y=False)
    fig_time_series.update_yaxes(title_text="Số lượng Chỉ số", secondary_y=True)
    
    st.plotly_chart(fig_time_series, use_container_width=True)


# Chart 2 & 3: Side-by-side
col_chart1, col_chart2 = st.columns(2)

with col_chart1:
    with st.container(border=True):
        st.markdown("#### **Phân bổ bản ghi theo Ngành**")
        industry_counts = df_filtered['industry'].value_counts().nlargest(15)
        fig_industry = px.bar(
            industry_counts,
            x=industry_counts.index, y=industry_counts.values,
            labels={'y': 'Số lượng bản ghi', 'x': 'Ngành'},
            color=industry_counts.index, color_discrete_sequence=px.colors.qualitative.Pastel1,
            text_auto=True
        )
        fig_industry.update_layout(
            height=400, 
            showlegend=False, 
            title_x=0.5, 
            paper_bgcolor='rgba(0,0,0,0)', 
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_industry, use_container_width=True)

with col_chart2:
    with st.container(border=True):
        st.markdown("#### **Tỉ lệ phân bổ các Loại báo cáo**")
        report_type_counts = df_filtered['report_type'].value_counts()
        fig_report_type = px.pie(
            report_type_counts, names=report_type_counts.index, values=report_type_counts.values,
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        fig_report_type.update_traces(textposition='inside', textinfo='percent+label')
        fig_report_type.update_layout(
            height=400, 
            showlegend=True, 
            title_x=0.5, 
            paper_bgcolor='rgba(0,0,0,0)', 
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_report_type, use_container_width=True)

st.subheader("IV. Preview và Tải về Data 🗃️")
with st.container(border=True):
    
    # Sắp xếp dữ liệu
    df_to_display = df_filtered.copy().sort_values(by=['report_date'], ascending=False)

    # Thêm ô tìm kiếm
    col_search1, col_search2 = st.columns(2)
    with col_search1:
        search_company = st.text_input('Tìm kiếm theo Mã Công ty (Company Code)', placeholder='Nhập mã, ví dụ: FPT, VNM...')
    with col_search2:
        search_account = st.text_input('Tìm kiếm theo Tên Chỉ tiêu (Account)', placeholder='Nhập từ khóa, ví dụ: net_profit_to_parent_shareholders, net_operating_profit...')

    # Lọc dữ liệu dựa trên ô tìm kiếm (nếu có nhập)
    if search_company:
        df_to_display = df_to_display[df_to_display['company_code'].str.contains(search_company, case=False, na=False)]
    if search_account:
        df_to_display = df_to_display[df_to_display['account'].str.contains(search_account, case=False, na=False)]

    # Hiển thị dataframe
    st.dataframe(df_to_display.head(5000))
    st.markdown("---")
    st.markdown(f"Tải về **{len(df_to_display):,}** dòng dữ liệu đã được lọc. Nếu dữ liệu lớn (>500,000 dòng), tệp sẽ được tải về dưới dạng CSV để tối ưu hiệu suất.")
    
    if df_to_display.shape[0] <= 500000:
        st.download_button(
            label="📥 Tải xuống file Excel",
            data=to_excel(df_to_display),
            file_name="ValuX_financial_statement_filtered.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.download_button(
            label="📥 Tải xuống file CSV (Dữ liệu lớn)",
            data=df_to_display.to_csv(index=False).encode('utf-8'),
            file_name="ValuX_financial_statement_data_filtered.csv",
            mime="text/csv"
        )
