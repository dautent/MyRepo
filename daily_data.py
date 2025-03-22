import os
import pandas as pd
import akshare as ak
import subprocess  # 用于 Git 操作
from datetime import datetime, timezone
from zoneinfo import ZoneInfo  # Python 3.9+

# 数据存储路径
RANK_FILE = os.path.join(os.getenv("GITHUB_WORKSPACE", ""), "hot_rank.csv.gz")
SPOT_FILE = os.path.join(os.getenv("GITHUB_WORKSPACE", ""), "a_spot.csv.gz")
LOG_FILE = os.path.join(os.getenv("GITHUB_WORKSPACE", ""), "log.txt")
NOW = datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Shanghai"))

def fetch_hot_rank():
    """获取当天人气"""
    df = ak.stock_hot_rank_em()
    df["日期"] = NOW.strftime("%Y-%m-%d")  # 添加日期字段
    return df

def fetch_spot():
    '''获取当天行情'''
    df = ak.stock_zh_a_spot_em()
    df = df.dropna(subset=["今开", "最新价", "最高", "最低"], how="all")
    df = df[df["代码"].str.startswith(("60", "00", "30"))]
    df = df[~df["名称"].str.contains("ST")]

    df["日期"] = NOW.strftime("%Y-%m-%d")  # 添加日期字段
    cols = {
        "日期": "日期","名称": "股票名称","代码": "代码",
        "今开": "开盘","最新价": "收盘","最高": "最高","最低": "最低",
        "成交量": "成交量","成交额": "成交额","振幅": "振幅",
        "涨跌幅": "涨跌幅","涨跌额": "涨跌额","换手率": "换手率",}

    return df[list(cols.keys())].rename(columns=cols)

def git_run():
    # 推送到 GitHub
    subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
    subprocess.run(["git", "config", "--global", "user.email", "github-actions@github.com"], check=True)
    subprocess.run(["git", "add", RANK_FILE], check=True)
    subprocess.run(["git", "commit", "-m", f"更新数据 {NOW.strftime('%Y-%m-%d')}"], check=True)
    subprocess.run(["git", "push"], check=True)
    print("数据推送成功 ✅")

def save_to_csv(new_data, file):
    """合并新数据并存储到 CSV"""
    if os.path.exists(file):
        existing_data = pd.read_csv(file, encoding="utf-8", compression="gzip", dtype={"代码": str})
    else:
        existing_data = None

    if existing_data is not None:
        # 排除空列或全为 NaN 的列
        existing_data = existing_data.dropna(axis=1, how="all")
        new_data = new_data.dropna(axis=1, how="all")

        combined_data = pd.concat([existing_data, new_data], ignore_index=True)
        combined_data.drop_duplicates(subset=["代码", "日期"], keep="last", inplace=True)  # 去重
    else:
        combined_data = new_data

    combined_data.to_csv(file, index=False, encoding="utf-8", compression="gzip")
    print(f"数据已保存至 {file}")


def log(rank_data, spot_data):
    '''
    保存日志
    '''
    status = "✅" if (rank_data is not None and spot_data is not None) else "❌"
    rank_info = f"人气榜：{len(rank_data)}条" if rank_data is not None else "人气榜：没有获取到新数据，跳过日志写入。"
    spot_info = f"行情数据：{len(spot_data)}条" if spot_data is not None else "行情数据：没有获取到新数据，跳过日志写入。"

    # 生成日志内容
    log_message = f"{NOW.strftime('%Y-%m-%d %H:%M:%S')} {status} {rank_info}。{spot_info}"
    # 写入日志文件
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{log_message}\n")

def main():
    rank_data = fetch_hot_rank()
    if rank_data is not None: save_to_csv(rank_data, RANK_FILE)

    spot_data = fetch_spot()
    if spot_data is not None: save_to_csv(spot_data, SPOT_FILE)

    git_run()
    log(rank_data, spot_data)

if __name__ == "__main__":
    main()
