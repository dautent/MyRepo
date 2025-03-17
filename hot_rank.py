import os
import pandas as pd
import akshare as ak
from datetime import datetime
import subprocess  # 用于 Git 操作

# 数据存储路径
CSV_FILE = os.path.join(os.getenv("GITHUB_WORKSPACE", ""), "hot_rank.csv.gz")
LOG_FILE = os.path.join(os.getenv("GITHUB_WORKSPACE", ""), "hot_rank_log.txt")

def fetch_hot_rank():
    """获取当天数据"""
    try:
        df = ak.stock_hot_rank_em()
        if df.empty:
            print("未获取到数据，可能是接口异常")
            return None
        df["date"] = datetime.now().strftime("%Y-%m-%d")  # 添加日期字段
        return df
    except Exception as e:
        print(f"数据获取失败: {e}")
        return None

def load_existing_data():
    """加载本地存储的 CSV 数据"""
    if os.path.exists(CSV_FILE):
        try:
            return pd.read_csv(CSV_FILE, compression="gzip")
        except Exception as e:
            print(f"读取历史数据失败: {e}")
            return None
    return None

def save_to_csv(new_data):
    """合并新数据并存储到 CSV，并推送到 GitHub"""
    existing_data = load_existing_data()

    if existing_data is not None:
        combined_data = pd.concat([existing_data, new_data], ignore_index=True)
        combined_data.drop_duplicates(subset=["代码", "date"], keep="last", inplace=True)  # 去重
    else:
        combined_data = new_data

    try:
        combined_data.to_csv(CSV_FILE, index=False, compression="gzip")
        print(f"数据已保存至 {CSV_FILE}")
    except Exception as e:
        print(f"保存失败: {e}")
        return

    # **Git 操作**
    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "add", CSV_FILE], check=True)
        subprocess.run(["git", "commit", "-m", f"更新数据 {datetime.now().strftime('%Y-%m-%d')}"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("数据推送成功 ✅")
    except Exception as e:
        print(f"Git 推送失败: {e}")

def main():
    # 获取数据
    new_data = fetch_hot_rank()
    if new_data is not None:
        save_to_csv(new_data)

    #保存日志
    today = new_data["date"].iloc[-1]
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{today}\n")

if __name__ == "__main__":
    main()
