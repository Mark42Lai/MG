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
# ========================

parser = argparse.ArgumentParser()
parser.add_argument("--offset", type=int, default=0)
parser.add_argument("--limit", type=int, default=100) # 股票數量上限
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
        "messages": [
            {
                "type": "text",
                "text": message
            }
        ]
    }

    response = requests.post(url, headers=headers, json=data)
    print(f"🔧 LINE 回應: {response.status_code} - {response.text}")

    if response.status_code != 200:
        print(f"⚠️ LINE 發送失敗：{response.status_code} - {response.text}")
    else:
        print("✅ 已發送 LINE 通知")


# ✅ 登入與日期初始化
print("🔐 登入 FinMind API...")
dl = DataLoader()
dl.login_by_token(api_token=api_token)

latest_trade_date = get_latest_trade_date(dl)
start_date = (latest_trade_date - timedelta(days=lookback_days)).isoformat()
end_date = latest_trade_date.isoformat()
print(f"\n📅 偵測日期：{end_date}，Offset: {args.offset} Limit: {args.limit}")

# ✅ 股票清單（先排序再分段）
stock_list = dl.taiwan_stock_info()
stock_list = stock_list.sort_values("stock_id").reset_index(drop=True)  # 依 stock_id 排序，確保穩定分段
all_stocks = stock_list["stock_id"].tolist()
selected_stocks = all_stocks[args.offset: args.offset + args.limit]


result = []

for stock_id in selected_stocks:
    try:
        print(f"▶ {stock_id}")
        df = dl.taiwan_stock_daily(stock_id=stock_id, start_date=start_date, end_date=end_date)
        if df.empty or len(df) < window + 1:
            continue
        df = df.sort_values("date").reset_index(drop=True)
        df["high_12"] = df["max"].rolling(window).max()
        df["low_12"]  = df["min"].rolling(window).min()
        df["HC"] = (df["high_12"] * 2 + df["low_12"]) / 3

        if df.iloc[-1]["close"] > df.iloc[-1]["HC"]:
            gap = df.iloc[-1]["close"] - df.iloc[-1]["HC"]
            ratio = gap / df.iloc[-1]["HC"] * 100
            stock_name = stock_list[stock_list["stock_id"] == stock_id]["stock_name"].values[0]
            msg = f"📈【{stock_id} {stock_name}】\n收盤價突破HC！\n收盤價: {df.iloc[-1]['close']}\nHC: {round(df.iloc[-1]['HC'], 2)}\n突破幅度: {round(ratio, 2)}%\n日期: {df.iloc[-1]['date']}"
            result.append(msg)

    except Exception as e:
        print(f"⚠️ {stock_id} 發生錯誤：{e}")
        continue

# ✅ 結果通知
if result:
    full_message = "\n\n".join(result)
    send_line_message(line_user_id, full_message)
else:
    send_line_message(line_user_id, "😴 此批無突破高控")
