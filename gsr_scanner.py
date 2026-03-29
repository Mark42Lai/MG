import pandas as pd
from FinMind.data import DataLoader
from datetime import datetime, timedelta
import argparse
import requests
import time

# ===== 使用者設定區 =====
api_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNS0wOC0wMiAwOTo1ODoyNiIsInVzZXJfaWQiOiJNYXJrTGFpIiwiaXAiOiIxLjE3NC44LjIzMCJ9.g3Igq0QuLzPN_KtqW5Shl1dJP2nqikV5IcUN-6sR1Xs"
window = 12
lookback_days = 30
line_user_id = "U26e8775cea7db4d35acfcdd9bd30c9b9"  # Uxxxxxxxxxxxx 開頭
line_token = "dB3LRavB4/bduwyPF2tCV6pzd74FXEKHqarNyPfdP9za7eq24wmciiqtCGpm2RmMERxf7XWFyOSPNU+YVDrdSV32EbFn9pQh+ZUodt2NdX0GGrnf5EZF4xHviXO8dcVxxp+UMTqG53ySZjr30oMZ5AdB04t89/1O/w1cDnyilFU="  # LINE Messaging API 的 token
total_stocks_expected = 3850  # 可依照實際股票數量微調
num_batches = 7  # 分成幾段處理（與排程數一致）
# ========================

now_hour = datetime.now().hour
hour_to_batch_index = {16: 0, 17: 1, 18: 2, 19: 3, 20: 4, 21: 5, 22: 6}
default_batch = hour_to_batch_index.get(now_hour, 0)

parser = argparse.ArgumentParser()
parser.add_argument("--offset", type=int, default=default_batch * (total_stocks_expected // num_batches))
parser.add_argument("--limit", type=int, default=total_stocks_expected // num_batches)
args = parser.parse_args()

def get_latest_trade_date(dl):
    date = datetime.today().date()
    for _ in range(7):
        df = dl.taiwan_stock_daily(stock_id='2330', start_date=str(date), end_date=str(date))
        if not df.empty:
            return date
        date -= timedelta(days=1)
    raise Exception("❌ 找不到近一週的交易日")

def send_line_message(user_id, message):
    print("📤 準備發送 LINE 訊息\n", message)
    if not line_token:
        print("❌ 找不到 LINE_TOKEN，略過發送")
        return

    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Authorization": f"Bearer {line_token}",
        "Content-Type": "application/json"
    }
    data = {
        "to": user_id,
        "messages": [{"type": "text", "text": message}]
    }

    response = requests.post(url, headers=headers, json=data)
    if response.status_code != 200:
        print(f"⚠️ LINE 發送失敗：{response.status_code} - {response.text}")
    else:
        print("✅ 已發送 LINE 通知")

# ✅ 登入與初始化
print("🔐 登入 FinMind API...")
dl = DataLoader()
dl.login_by_token(api_token=api_token)

latest_trade_date = get_latest_trade_date(dl)
start_date = (latest_trade_date - timedelta(days=lookback_days)).isoformat()
end_date = latest_trade_date.isoformat()
print(f"\n📅 偵測日期：{end_date}，Offset: {args.offset} Limit: {args.limit}")

# ✅ 關鍵修復：抓取股票清單並強制「去重複」
stock_list = dl.taiwan_stock_info()
stock_list = stock_list.drop_duplicates(subset=['stock_id']) # 確保沒有重複的代碼
stock_list = stock_list.sort_values("stock_id").reset_index(drop=True)
all_stocks = stock_list["stock_id"].tolist()
selected_stocks = all_stocks[args.offset: args.offset + args.limit]

result = []

for stock_id in selected_stocks:
    try:
        # print(f"▶ {stock_id}") # 測試時可打開，上線建議關閉以保持 log 乾淨
        df = dl.taiwan_stock_daily(stock_id=stock_id, start_date=start_date, end_date=end_date)
        if df.empty or len(df) < window + 1:
            continue
        df = df.sort_values("date").reset_index(drop=True)

        # ===== 條件區 =====
        latest = df.iloc[-1]
        volume_today = latest["Trading_Volume"]
        volume_ma20 = df["Trading_Volume"].tail(20).mean()
        close_today = latest["close"]

        if volume_today <= volume_ma20 or volume_today <= 200_000:
            continue
        if close_today <= 30:
            continue
        # ======================

        df["close_max"] = df["close"].rolling(window).max()
        df["close_min"] = df["close"].rolling(window).min()
        df["高控"] = (df["close_max"] * 2 + df["close_min"]) / 3

        yesterday = df.iloc[-2]
        today = df.iloc[-1]

        if (yesterday["close"] <= yesterday["高控"] and today["close"] > today["高控"]):
            gap = today["close"] - today["高控"]
            ratio = gap / today["高控"] * 100
            stock_name = stock_list[stock_list["stock_id"] == stock_id]["stock_name"].values[0]
            
            msg = f"📈【{stock_id} {stock_name}】\n收盤價突破高控！\n收盤價: {today['close']}\n高控: {round(today['高控'], 2)}\n突破幅度: {round(ratio, 2)}%\n日期: {today['date']}"
            
            # ✅ 雙重保險：如果這則訊息已經在清單裡了，就不要再加進去
            if msg not in result:
                result.append(msg)

    except Exception as e:
        print(f"⚠️ {stock_id} 發生錯誤：{e}")
        continue
    
    time.sleep(0.3)  # 稍微縮短延遲，加快整體掃描速度

# ✅ 傳送結果 (加入分段發送保護，避免字數超過 LINE 限制)
if result:
    # 每 15 檔股票合併成一個訊息發送
    for i in range(0, len(result), 15):
        batch_msg = "\n\n".join(result[i:i+15])
        send_line_message(line_user_id, batch_msg)
else:
    # 只有在第一批次才發送「無突破」，減少整天收到 😴 的情況
    if args.offset == 0:
        send_line_message(line_user_id, "😴 此批無突破高控")
