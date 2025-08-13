import streamlit as st
import pandas as pd
import io
import os
import shutil
from datetime import datetime

# --------------------------------------------------------------------------
# Page Configuration
# --------------------------------------------------------------------------
st.set_page_config(
    page_title="Financial Statement Term Formatting",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --------------------------------------------------------------------------
# App Configuration
# --------------------------------------------------------------------------
current_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else '.'
data_dir = os.path.join(current_dir, 'data').replace('\\pages', '').replace('/pages', '')
CONFIG = {
    "data_filename": "account_mapping_adjust.parquet", # <-- Changed to .parquet
    "output_filename": "formatted_account_mapping.xlsx"
}

# --------------------------------------------------------------------------
# Utility Functions
# --------------------------------------------------------------------------
@st.cache_data
def load_parquet_data(file_path):
    """
    Tải dữ liệu từ tệp Parquet được chỉ định.
    Sử dụng cache của Streamlit để tránh tải lại dữ liệu sau mỗi tương tác.
    """
    if not os.path.exists(file_path):
        st.error(f"Lỗi: Không tìm thấy tệp '{file_path}'.")
        return None
    try:
        df = pd.read_parquet(file_path)
        st.write(file_path)
        return df
    except Exception as e:
        st.error(f"Đã xảy ra lỗi khi đọc tệp Parquet: {e}")
        st.warning("Vui lòng đảm bảo tệp có định dạng .parquet hợp lệ.")
        return None

def to_excel(df):
    """Chuyển đổi DataFrame thành file Excel trong bộ nhớ để tải xuống."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Formatted Accounts')
    return output.getvalue()

# --------------------------------------------------------------------------
# Main Application Logic
# --------------------------------------------------------------------------
def main():
    st.title("📊 Chuẩn hóa Tên Chỉ tiêu Báo cáo Tài chính")

    # --- Load Data ---
    # Tải dữ liệu ban đầu. Dữ liệu này sẽ là cơ sở để so sánh và cập nhật.
    file_path = os.path.join(data_dir, CONFIG["data_filename"])
    
    # Sử dụng session state để lưu trữ dataframe, giúp duy trì các thay đổi của người dùng
    if 'df' not in st.session_state:
        df_loaded = load_parquet_data(file_path)
        if df_loaded is None:
            st.stop() # Dừng thực thi nếu không tải được dữ liệu
        st.session_state.df = df_loaded

    # --- Sidebar Filters ---
    st.sidebar.header("Tùy chọn Lọc ⚙️")
    report_types = ["Tất cả"] + st.session_state.df['report_type'].unique().tolist()
    selected_report_type = st.sidebar.selectbox(
        "Lọc theo Loại báo cáo",
        options=report_types
    )

    # Lọc dataframe dựa trên lựa chọn.
    if selected_report_type == "Tất cả":
        df_filtered = st.session_state.df
    else:
        df_filtered = st.session_state.df[st.session_state.df['report_type'] == selected_report_type]

    # --- Display Metrics ---
    st.subheader("I. Tiến độ format các chỉ tiêu (account)", divider="rainbow")
    col1, col2, _ = st.columns([1, 1, 2])
    
    total_count = len(st.session_state.df)
    filled_count = st.session_state.df['account'].notna().sum()
    fill_percentage = (filled_count / total_count) * 100 if total_count > 0 else 0

    with col1:
        st.metric(label="Tổng thể Hoàn thành", value=f"{fill_percentage:.2f} %")
    with col2:
        st.metric(label="Tổng số tài khoản đã điền", value=f"{filled_count} / {total_count}")

    # --- Data Editor ---
    st.subheader("II. Chỉnh sửa format trường Account", divider="rainbow")
    st.info(f"💡 Chỉnh sửa trực tiếp trên cột **'account ✏️'** bên dưới. Dữ liệu đang hiển thị cho Loại báo cáo: **{selected_report_type}**")

    sort_nulls_first = st.checkbox("✔️ Sắp xếp các trường 'account' trống lên đầu để dễ fill")

    df_to_display = df_filtered.copy()
    if sort_nulls_first:
        df_to_display.sort_values(by='account', ascending=True, na_position='first', inplace=True)

    edited_df = st.data_editor(
        df_to_display,
        column_config={
            "account": st.column_config.TextColumn(
                "account ✏️",
                help="Đây là cột để bạn điền format chuẩn. Hãy chỉnh sửa trực tiếp vào ô này.",
                width="large",
            )
        },
        disabled=["report_type", "account_vi", "account_en"],
        use_container_width=True,
        height=400
    )

    # --- Update Session State with Edits ---
    if not edited_df.equals(df_to_display):
        # Cập nhật dataframe trong session state với các thay đổi từ editor
        st.session_state.df.update(edited_df)
        st.rerun()

    # --- Bố cục cho Trạng thái và Lưu trữ ---
    col_status, col_save = st.columns([1, 1])

    with col_status:
        st.subheader("III. Trạng thái dữ liệu", divider="rainbow")
        unfilled_mask = df_filtered['account'].isna()
        filled_accounts = df_filtered[~unfilled_mask]['account_vi'].tolist()
        unfilled_accounts = df_filtered[unfilled_mask]['account_vi'].tolist()

        with st.expander(f"✅ Đã điền ({len(filled_accounts)})", expanded=False):
            st.dataframe(pd.DataFrame({"Tên tài khoản": filled_accounts}), use_container_width=True, hide_index=True)

        with st.expander(f"❌ Chưa điền ({len(unfilled_accounts)})", expanded=True):
            st.dataframe(pd.DataFrame({"Tên tài khoản": unfilled_accounts}), use_container_width=True, hide_index=True)

    with col_save:
        st.subheader("Lưu DL đã chỉnh sửa", divider="rainbow")
        with st.container(border=True):
            if st.button("💾 Lưu thay đổi vào File", use_container_width=True):
                source_path = os.path.join(data_dir, CONFIG["data_filename"])
                try:
                    # Tạo tên file backup với timestamp
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    backup_filename = CONFIG["data_filename"].replace('.parquet', f'_bk_{timestamp}.parquet')
                    backup_path = os.path.join(data_dir, backup_filename)

                    # 1. Tạo backup
                    # if os.path.exists(source_path):
                    #     shutil.copy(source_path, backup_path)
                    
                    # 2. Ghi đè file gốc bằng dữ liệu đã chỉnh sửa từ session_state
                    st.session_state.df.to_parquet(source_path, index=False)
                    
                    # 3. Xóa cache và thông báo thành công
                    st.cache_data.clear()
                    st.success(f"Đã lưu thành công! Bản sao lưu đã được tạo: '{backup_filename}'")
                
                except Exception as e:
                    st.error(f"Lưu file thất bại: {e}")

            # --- Chức năng Tải xuống (vẫn là Excel để tiện sử dụng) ---
            excel_data = to_excel(st.session_state.df)
            st.download_button(
                label="📥 Tải xuống bản sao Excel",
                data=excel_data,
                file_name=CONFIG["output_filename"],
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Tải xuống một bản sao của tất cả các thay đổi của bạn vào một tệp .xlsx mới.",
                use_container_width=True
            )

if __name__ == "__main__":
    main()
