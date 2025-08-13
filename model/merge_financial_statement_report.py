import os
import argparse
import logging
import pandas as pd
import glob
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def merge_files(input_dir: str, output_file: str, file_type: str = 'parquet'):
    """
    Finds all files of a specific type in a directory, merges them into a
    single pandas DataFrame, and saves the result to a new file.

    Args:
        input_dir (str): The path to the directory containing the files to merge.
        output_file (str): The path where the merged file will be saved.
        file_type (str): The type of files to merge ('parquet' or 'csv').
    """
    if not os.path.isdir(input_dir):
        logging.error(f"Thư mục đầu vào không tồn tại: '{input_dir}'")
        return

    search_pattern = os.path.join(input_dir, f'*.{file_type}')
    file_list = glob.glob(search_pattern)

    if not file_list:
        logging.warning(f"Không tìm thấy file nào có định dạng '.{file_type}' trong thư mục '{input_dir}'")
        return
    logging.info(f"Tìm thấy {len(file_list)} file '.{file_type}' để hợp nhất.")

    list_of_dfs = []
    for f in tqdm(file_list, desc=f"Đang đọc file .{file_type}"):
        try:
            if file_type == 'parquet':
                df = pd.read_parquet(f)
            elif file_type == 'csv':
                # Giả sử file CSV được phân cách bằng tab, dựa trên script trước
                df = pd.read_csv(f, sep='\t')
            else:
                logging.error(f"Loại file không được hỗ trợ: {file_type}")
                return
            list_of_dfs.append(df)
        except Exception as e:
            logging.error(f"Lỗi khi đọc file '{f}': {e}")
            continue # Bỏ qua file lỗi và tiếp tục

    if not list_of_dfs:
        logging.error("Không đọc được file nào thành công. Dừng quá trình.")
        return

    logging.info("Đang hợp nhất dữ liệu...")
    merged_df = pd.concat(list_of_dfs, ignore_index=True)
    logging.info(f"Hợp nhất hoàn tất. DataFrame cuối cùng có {merged_df.shape[0]} dòng và {merged_df.shape[1]} cột.")

    output_format = os.path.splitext(output_file)[1].lower()
    
    output_dir_path = os.path.dirname(output_file)
    if output_dir_path:
        os.makedirs(output_dir_path, exist_ok=True)
        
    logging.info(f"Đang lưu file đã hợp nhất vào '{output_file}'...")
    try:
        if output_format == '.parquet':
            merged_df.to_parquet(output_file, index=False)
        elif output_format == '.csv':
            merged_df.to_csv(output_file, index=False, sep='\t')
        elif output_format == '.xlsx':
             merged_df.to_excel(output_file, index=False)
        else:
            logging.error("Định dạng file đầu ra không được hỗ trợ. Vui lòng sử dụng '.parquet', '.csv', hoặc '.xlsx'.")
            default_output_path = os.path.splitext(output_file)[0] + '.parquet'
            logging.warning(f"Lưu file dưới dạng Parquet mặc định tại: {default_output_path}")
            merged_df.to_parquet(default_output_path, index=False)
        logging.info(f"Đã lưu thành công file tại '{output_file}'")
    except Exception as e:
        logging.error(f"Lỗi khi lưu file đầu ra: {e}")


def main():
    """Hàm chính để phân tích các tham số dòng lệnh."""
    parser = argparse.ArgumentParser(
        description="Hợp nhất nhiều file báo cáo tài chính thành một file duy nhất.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '--input-dir',
        type=str,
        default='output_data',
        help='Thư mục chứa các file dữ liệu cần hợp nhất.'
    )
    parser.add_argument(
        '--output-file',
        type=str,
        default='merged_data/all_financial_statements.parquet',
        help="Đường dẫn file đầu ra. Đuôi file (.parquet, .csv, .xlsx) sẽ quyết định định dạng lưu."
    )
    parser.add_argument(
        '--file-type',
        type=str,
        default='parquet',
        choices=['parquet', 'csv'],
        help="Loại file cần tìm và hợp nhất trong thư mục đầu vào."
    )

    args = parser.parse_args()
    merge_files(args.input_dir, args.output_file, args.file_type)

if __name__ == '__main__':
    main()