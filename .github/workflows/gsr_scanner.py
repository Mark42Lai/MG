import pandas as pd
from FinMind.data import DataLoader
from datetime import datetime, timedelta
import argparse
import requests

# ===== 使用者設定區 =====
api_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNS0wOC0wMiAwOTo1ODoyNiIsInVzZXJfaWQiOiJNYXJrTGFpIiwiaXAiOiIxLjE3NC44LjIzMCJ9.g3Igq0QuLzPN_KtqW5Shl1dJP2nqikV5IcUN-6sR1Xs"
window = 12
lookback_days = 30
line_user_id = "U26e8775cea7db4d35acfcdd9bd30c9b9"  # Uxxxxxxxxxxxx 開頭
line_token = "dB3LRavB4/bduwyPF2tCV6pzd74FXEKHqarNyPfdP9za7eq24wmciiqtCGpm2RmMERxf7XWFyOSPNU+YVDrdSV32EbFn9pQh+ZUodt2NdX0GGrnf5EZF4xHviXO8dcVxxp+UMTqG53ySZjr30oMZ5AdB04t89/1O/w1cDnyilFU="  # LINE Messaging API 的 token
total_stocks_expected = 2000  # 可依照實際股票數量微調
num_batches = 4  # 分成幾段處理（與排程數一致）
# ========================

now_hour = datetime.now().hour
hour_to_batch_index = {19: 0, 20: 1, 21: 2, 22: 3}
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
    print("📤 準備發送 LINE 訊息：", message)
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
    print(f"🔧 LINE 回應: {response.status_code} - {response.text}")
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

stock_list = dl.taiwan_stock_info()
stock_list = stock_list.sort_values("stock_id").reset_index(drop=True)
all_stocks = stock_list["stock_id"].tolist()
selected_stocks = all_stocks[args.offset: args.offset + args.limit]

# 取得股本資料（一次抓取）
profile_df = dl.taiwan_stock_info()

result = []

for stock_id in selected_stocks:
    try:
        print(f"▶ {stock_id}")
        df = dl.taiwan_stock_daily(stock_id=stock_id, start_date=start_date, end_date=end_date)
        if df.empty or len(df) < window + 1:
            continue
        df = df.sort_values("date").reset_index(drop=True)

        # ===== 新增條件區 =====
        latest = df.iloc[-1]
        volume_today = latest["Trading_Volume"]
        volume_ma20 = df["Trading_Volume"].tail(20).mean()
        close_today = latest["close"]

        # 成交量 > 20MA 且 > 200張
        if volume_today <= volume_ma20 or volume_today <= 200_000:
            continue

        # 股價 > 30
        if close_today <= 30:
            continue

        # 股本 > 10億
        try:
            capital = profile_df[profile_df["stock_id"] == stock_id]["capital"].values[0]
            if capital < 10_000_000_000:
                continue
        except:
            continue
        # ======================

        df["close_max"] = df["close"].rolling(window).max()
        df["close_min"] = df["close"].rolling(window).min()
        df["高控"] = (df["close_max"] * 2 + df["close_min"]) / 3

        if (
            df.iloc[-2]["close"] <= df.iloc[-2]["高控"]
            and df.iloc[-1]["close"] > df.iloc[-1]["高控"]
        ):
            gap = df.iloc[-1]["close"] - df.iloc[-1]["高控"]
            ratio = gap / df.iloc[-1]["高控"] * 100
            stock_name = stock_list[stock_list["stock_id"] == stock_id]["stock_name"].values[0]
            msg = f"📈【{stock_id} {stock_name}】\n收盤價突破高控！\n收盤價: {df.iloc[-1]['close']}\n高控: {round(df.iloc[-1]['高控'], 2)}\n突破幅度: {round(ratio, 2)}%\n日期: {df.iloc[-1]['date']}"
            result.append(msg)

    except Exception as e:
        print(f"⚠️ {stock_id} 發生錯誤：{e}")
        continue

# ✅ 傳送結果
if result:
    send_line_message(line_user_id, "\n\n".join(result))
else:
    send_line_message(line_user_id, "😴 此批無突破高控")
