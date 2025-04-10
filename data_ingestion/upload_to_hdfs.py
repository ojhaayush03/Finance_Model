import subprocess
import os

HADOOP_BIN = "C:/hadoop/bin/hdfs.cmd"  # Use full path with .cmd for Windows

def upload_to_hdfs(local_path, hdfs_dir):
    file_name = os.path.basename(local_path)
    hdfs_path = os.path.join(hdfs_dir, file_name)
    
    subprocess.run([HADOOP_BIN, "dfs", "-mkdir", "-p", hdfs_dir])
    subprocess.run([HADOOP_BIN, "dfs", "-rm", "-f", hdfs_path])
    result = subprocess.run([HADOOP_BIN, "dfs", "-put", local_path, hdfs_path])
    
    if result.returncode == 0:
        print(f"✅ Uploaded to HDFS: {hdfs_path}")
    else:
        print("❌ Failed to upload to HDFS")

# ========= CONFIGURE BELOW =========
ticker = input("Enter the stock ticker to upload: ").strip().upper()
local_csv_path = f"../processed_data/{ticker}_indicators.csv"
hdfs_target_dir = f"/user/ayush/financial_project/processed_data/"

# ====================================
if os.path.exists(local_csv_path):
    upload_to_hdfs(local_csv_path, hdfs_target_dir)
else:
    print(f"⚠️ File not found: {local_csv_path}")
