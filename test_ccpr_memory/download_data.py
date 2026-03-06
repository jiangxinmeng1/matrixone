#!/usr/bin/env python3
"""
下载CCPR内存测试所需的数据文件
从腾讯云COS下载ca_comprehensive_dataset数据集的前N行
"""

import os
import sys

try:
    from qcloud_cos import CosConfig
    from qcloud_cos import CosS3Client
except ImportError:
    print("Error: qcloud_cos package not installed.")
    print("Please install it with: pip install cos-python-sdk-v5")
    sys.exit(1)

# COS配置
SECRET_ID = "AKIDUtG3skpK1hK7BSoClmsDVegirATitKiD"
SECRET_KEY = "pXGubPAxolknvyzsqEoRBteLzmbSH3pb"
REGION = "ap-guangzhou"
BUCKET = "mo-load-guangzhou-1308875761"
KEY = "mo-big-data/ca_comprehensive_dataset_5000w.csv"

# 默认输出配置
DEFAULT_OUTPUT_FILE = "sample_1m.csv"
DEFAULT_MAX_LINES = 1000000


def download_data(output_file: str = DEFAULT_OUTPUT_FILE, max_lines: int = DEFAULT_MAX_LINES):
    """
    从COS下载数据文件
    
    Args:
        output_file: 输出文件路径
        max_lines: 最大下载行数
    """
    print(f"Downloading data from COS...")
    print(f"  Bucket: {BUCKET}")
    print(f"  Key: {KEY}")
    print(f"  Output: {output_file}")
    print(f"  Max lines: {max_lines}")
    
    config = CosConfig(
        Region=REGION,
        SecretId=SECRET_ID,
        SecretKey=SECRET_KEY
    )
    
    client = CosS3Client(config)
    
    try:
        response = client.get_object(
            Bucket=BUCKET,
            Key=KEY
        )
    except Exception as e:
        print(f"Error: Failed to get object from COS: {e}")
        sys.exit(1)
    
    body = response['Body']
    line_count = 0
    
    # 确保输出目录存在
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    print("Downloading...")
    
    with open(output_file, "wb") as f:
        buffer = b""
        for chunk in body.get_stream(1024 * 1024):  # 1MB chunks
            buffer += chunk
            lines = buffer.split(b'\n')
            
            # 保留最后一个可能不完整的行
            buffer = lines[-1]
            lines = lines[:-1]
            
            for line in lines:
                if line_count >= max_lines:
                    break
                
                f.write(line + b'\n')
                line_count += 1
                
                # 打印进度
                if line_count % 100000 == 0:
                    print(f"  Downloaded {line_count:,} lines...")
            
            if line_count >= max_lines:
                break
        
        # 处理最后一行（如果还没达到限制）
        if line_count < max_lines and buffer:
            f.write(buffer + b'\n')
            line_count += 1
    
    print(f"Done! Downloaded {line_count:,} lines to {output_file}")
    
    # 显示文件大小
    file_size = os.path.getsize(output_file)
    if file_size >= 1024 * 1024 * 1024:
        size_str = f"{file_size / (1024 * 1024 * 1024):.2f} GB"
    elif file_size >= 1024 * 1024:
        size_str = f"{file_size / (1024 * 1024):.2f} MB"
    else:
        size_str = f"{file_size / 1024:.2f} KB"
    print(f"File size: {size_str}")
    
    return output_file


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Download CCPR memory test data from COS"
    )
    parser.add_argument(
        "-o", "--output",
        default=DEFAULT_OUTPUT_FILE,
        help=f"Output file path (default: {DEFAULT_OUTPUT_FILE})"
    )
    parser.add_argument(
        "-n", "--lines",
        type=int,
        default=DEFAULT_MAX_LINES,
        help=f"Maximum number of lines to download (default: {DEFAULT_MAX_LINES})"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing file without prompting"
    )
    
    args = parser.parse_args()
    
    # 检查文件是否已存在
    if os.path.exists(args.output) and not args.force:
        print(f"File {args.output} already exists.")
        response = input("Overwrite? [y/N]: ").strip().lower()
        if response != 'y':
            print("Aborted.")
            sys.exit(0)
    
    download_data(args.output, args.lines)


if __name__ == "__main__":
    main()
