import os
import json
import time
import argparse
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import requests
from FinMind.data import DataLoader

# ========================
# 環境變數（用 GitHub Secrets 注入）
# ========================
API_TOKEN = os.getenv("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNS0wOC0wMiAwOTo1ODoyNiIsInVzZXJfaWQiOiJNYXJrTGFpIiwiaXAiOiIxLjE3NC44LjIzMCJ9.g3Igq0QuLzPN_KtqW5Shl1dJP2nqikV5IcUN-6sR1Xs", "").strip()
LINE_TOKEN = os.getenv("LINE_TOKEN", "").strip()
LINE_USER_ID = os.getenv("UdB3LRavB4/bduwyPF2tCV6pzd74FXEKHqarNyPfdP9za7eq24wmciiqtCGpm2RmMERxf7XWFyOSPNU+YVDrdSV32EbFn9pQh+ZUodt2NdX0GGrnf5EZF4xHviXO8dcVxxp+UMTqG53ySZjr30oMZ5AdB04t89/1O/w1cDnyilFU", "").strip()

# ===== 使用者設定區 =====
WINDOW = int(os.getenv("WINDOW", "12"))
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "30"))
TOTAL_STOCKS_EXPECTED = int(os.getenv("TOTAL_STOCKS_EXPECTED", "3850"))
NUM_BATCHES = int(os.getenv("NUM_BATCHES", "7"))
SLEEP_SEC = float(os.getenv("SLEEP_SEC", "0.5"))  # 每檔延遲，避免 API 壓力
# ========================


def get_default_offset_by_local_hour():
    """依台灣時間(UTC+8)的小時決定 offset 的預設值。"""
    now_hour = (datetime.utcnow() + timedelta(hours=8)).hour
    hour_to_batch_index = {16: 0, 17: 1, 18: 2, 19: 3, 20: 4, 21: 5, 22: 6}
    batch_idx = hour_to_batch_index.get(now_hour, 0)
    return batch_idx * (TOTAL_STOCKS_EXPECTED // NUM_BATCHES)


def get_latest_trade_date(dl: DataLoader):
    """往回找近 7 天有交易的日期（用台積電 2330 檢測）。"""
    date = (datetime.utcnow() + timedelta(hours=8)).date()
    for _ in range(7):
        df = dl.taiwan_stock_daily(stock_id="2330", start_date=str(date), end_date=str(date))
        if not df.empty:
            return date
        date -= timedelta(days=1)
    raise RuntimeError("❌ 找不到近一週的交易日")


def send_line_message(user_id: str, message: str):
    print("📤 準備發送 LINE 訊息：", message.replace("\n", " | ")[:180] + "...")
    if not LINE_TOKEN or not user_id:
        print("⚠️ 找不到 LINE_TOKEN 或 LINE_USER_ID，略過發送")
        return

    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Authorization": f"Bearer {LINE_TOKEN}",
        "Content-Type": "application/json",
    }
    data = {"to": user_id, "messages": [{"type": "text", "text": message}]}
    resp = requests.post(url, headers=headers, json=data)
    print(f"🔧 LINE 回應: {resp.status_code} - {resp.text}")
    if resp.status_code != 200:
        print(f"⚠️ LINE 發送失敗：{resp.status_code} - {resp.text}")
    else:
        print("✅ 已發送 LINE 通知")


# ===== 狀態檔：當日已通知清單 =====
def load_seen_set(state_file: Path) -> set:
    if state_file.exists():
        with open(state_file, "r", encoding="utf-8") as f:
            return set(json.load(f))  # list of "stock_id|date"
    return set()


def save_seen_set(state_file: Path, seen: set):
    state_file.parent.mkdir(parents=True, exist_ok=True)
    with open(state_file, "w", encoding="utf-8") as f:
        json.dump(sorted(list(seen)), f, ensure_ascii=False, indent=2)


def main():
    # ===== 參數處理 =====
    parser = argparse.ArgumentParser()
    parser.add_argument("--offset", type=int, default=get_default_offset_by_local_hour())
    parser.add_argument("--limit", type=int, default=TOTAL_STOCKS_EXPECTED // NUM_BATCHES)
    args = parser.parse_args()

    if not API_TOKEN:
        raise RuntimeError("❌ 缺少 API_TOKEN（請在 GitHub Secrets 設定 API_TOKEN）")

    # ===== 登入 FinMind =====
    print("🔐 登入 FinMind API...")
    dl = DataLoader()
    dl.login_by_token(api_token=API_TOKEN)

    # ===== 計算日期區間 =====
    latest_trade_date = get_latest_trade_date(dl)
    start_date = (latest_trade_date - timedelta(days=LOOKBACK_DAYS)).isoformat()
    end_date = latest_trade_date.isoformat()
    print(f"\n📅 偵測日期：{end_date}，Offset: {args.offset} Limit: {args.limit}")

    # ===== 讀取股票清單（固定排序，確保分段一致）=====
    stock_list = dl.taiwan_stock_info()
    stock_list = stock_list.sort_values("stock_id").reset_index(drop=True)
    all_stocks = stock_list["stock_id"].astype(str).tolist()
    selected_stocks = all_stocks[args.offset : args.offset + args.limit]
    print(f"📦 本批股票數：{len(selected_stocks)}")

    # ===== 狀態檔（當日去重）=====
    state_dir = Path("state")
    state_file = state_dir / f"seen_{end_date}.json"
    seen = load_seen_set(state_file)

    def already_seen(stock_id: str, date_str: str) -> bool:
        return f"{stock_id}|{date_str}" in seen

    def mark_seen(stock_id: str, date_str: str):
        seen.add(f"{stock_id}|{date_str}")

    result_msgs = []

    # ===== 開始掃描 =====
    for stock_id in selected_stocks:
        try:
            print(f"▶ {stock_id}")
            df = dl.taiwan_stock_daily(stock_id=stock_id, start_date=start_date, end_date=end_date)
            if df.empty or len(df) < WINDOW + 1:
                time.sleep(SLEEP_SEC)
                continue

            df = df.sort_values("date").reset_index(drop=True)

            # ===== 篩選條件 =====
            latest = df.iloc[-1]
            volume_today = latest.get("Trading_Volume", 0)
            volume_ma20 = df["Trading_Volume"].tail(20).mean() if "Trading_Volume" in df.columns else 0
            close_today = float(latest["close"])

            # 成交量 > 20MA 且 > 200,000；股價 > 30
            if volume_today <= volume_ma20 or volume_today <= 200_000:
                time.sleep(SLEEP_SEC)
                continue
            if close_today <= 30:
                time.sleep(SLEEP_SEC)
                continue

            # ===== 計算 高控 =====
            df["close_max"] = df["close"].rolling(WINDOW).max()
            df["close_min"] = df["close"].rolling(WINDOW).min()
            df["高控"] = (df["close_max"] * 2 + df["close_min"]) / 3

            # 僅第一天突破（前一日未過、今日收盤突破）
            cond_first_break = (
                df.iloc[-2]["close"] <= df.iloc[-2]["高控"]
                and df.iloc[-1]["close"] > df.iloc[-1]["高控"]
            )
            if not cond_first_break:
                time.sleep(SLEEP_SEC)
                continue

            date_str = str(df.iloc[-1]["date"])
            if already_seen(stock_id, date_str):
                # 同日其他批次已通知過 → 跳過
                # print(f"⏩ 跳過已通知: {stock_id} {date_str}")
                time.sleep(SLEEP_SEC)
                continue

            gap = df.iloc[-1]["close"] - df.iloc[-1]["高控"]
            ratio = gap / df.iloc[-1]["高控"] * 100
            row = stock_list[stock_list["stock_id"] == stock_id]
            stock_name = row["stock_name"].values[0] if not row.empty else ""

            msg = (
                f"📈【{stock_id} {stock_name}】\n"
                f"收盤價突破高控！\n收盤價: {df.iloc[-1]['close']}\n"
                f"高控: {round(df.iloc[-1]['高控'], 2)}\n突破幅度: {round(ratio, 2)}%\n"
                f"日期: {date_str}"
            )

            result_msgs.append(msg)
            mark_seen(stock_id, date_str)

        except Exception as e:
            print(f"⚠️ {stock_id} 發生錯誤：{e}")

        time.sleep(SLEEP_SEC)  # 每檔固定延遲

    # ===== 發送與保存狀態 =====
    if result_msgs:
        send_line_message(LINE_USER_ID, "\n\n".join(result_msgs))
    else:
        send_line_message(LINE_USER_ID, "😴 此批無突破高控")

    save_seen_set(state_file, seen)
    print(f"💾 已更新狀態檔: {state_file.resolve()}")


if __name__ == "__main__":
    main()
