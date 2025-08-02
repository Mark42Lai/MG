import pandas as pd
from FinMind.data import DataLoader
from datetime import datetime, timedelta
import argparse

# ===== 使用者設定區 =====
api_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNS0wOC0wMiAwOTo1ODoyNiIsInVzZXJfaWQiOiJNYXJrTGFpIiwiaXAiOiIxLjE3NC44LjIzMCJ9.g3Igq0QuLzPN_KtqW5Shl1dJP2nqikV5IcUN-6sR1Xs"
window = 12
lookback_days = 30
# ========================

parser = argparse.ArgumentParser()
parser.add_argument("--offset", type=int, default=0)
parser.add_argument("--limit", type=int, default=600)
args = parser.parse_args()

def get_latest_trade_date(dl):
    date = datetime.today().date()
    for _ in range(7):
        df = dl.taiwan_stock_daily(stock_id='2330', start_date=str(date), end_date=str(date))
        if not df.empty:
            return date
        date -= timedelta(days=1)
    raise Exception("❌ 找不到近一週的交易日")

# ✅ 登入與日期初始化
print("🔐 登入 FinMind API...")
dl = DataLoader()
dl.login_by_token(api_token=api_token)

latest_trade_date = get_latest_trade_date(dl)
start_date = (latest_trade_date - timedelta(days=lookback_days)).isoformat()
end_date = latest_trade_date.isoformat()
print(f"\n📅 偵測日期：{end_date}，Offset: {args.offset} Limit: {args.limit}")

# ✅ 股票清單
stock_list = dl.taiwan_stock_info()
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
        df["close_max"] = df["close"].rolling(window).max()
        df["close_min"] = df["close"].rolling(window).min()
        df["高控"] = (df["close_max"] * 2 + df["close_min"]) / 3

        if (
            df.iloc[-2]["close"] <= df.iloc[-2]["高控"]
            and df.iloc[-1]["close"] > df.iloc[-1]["高控"]
        ):
            gap = df.iloc[-1]["close"] - df.iloc[-1]["高控"]
            ratio = gap / df.iloc[-1]["高控"] * 100
            result.append({
                "股票代號": stock_id,
                "股票名稱": stock_list[stock_list["stock_id"] == stock_id]["stock_name"].values[0],
                "收盤價": df.iloc[-1]["close"],
                "高控": round(df.iloc[-1]["高控"], 2),
                "突破幅度%": round(ratio, 2),
                "日期": df.iloc[-1]["date"]
            })

    except Exception as e:
        print(f"⚠️ {stock_id} 發生錯誤：{e}")
        continue

df_result = pd.DataFrame(result)
if not df_result.empty:
    print(df_result.sort_values("突破幅度%", ascending=False).to_string(index=False))
else:
    print("😴 此批無突破高控")
