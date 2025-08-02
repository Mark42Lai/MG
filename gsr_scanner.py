import pandas as pd
from FinMind.data import DataLoader
from datetime import datetime, timedelta

# ===== 使用者設定區 =====
api_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNS0wOC0wMiAwOTo1ODoyNiIsInVzZXJfaWQiOiJNYXJrTGFpIiwiaXAiOiIxLjE3NC44LjIzMCJ9.g3Igq0QuLzPN_KtqW5Shl1dJP2nqikV5IcUN-6sR1Xs"
window = 12
lookback_days = 30
# ========================

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
print(f"\n📅 當前偵測日期：{end_date}\n")

# ✅ 抓取股票清單（只取前 600 檔）
stock_list = dl.taiwan_stock_info().head(600)
print("🚩 抓到前 600 檔股票")
print("🔍 類型欄位有哪些？", stock_list["type"].unique())
print(stock_list.head(3))

result = []

# ✅ 掃描上市與上櫃股票
for stock_type, market_label in [("twse", "上市"), ("tpex", "上櫃")]:
    print(f"\n📂 正在處理：{market_label} 股票...")

    stocks = stock_list[stock_list["type"] == stock_type]["stock_id"].tolist()

    for stock_id in stocks:
        try:
            print(f"    ▶ 正在處理 {stock_id}")
            df = dl.taiwan_stock_daily(
                stock_id=stock_id,
                start_date=start_date,
                end_date=end_date
            )
            if df.empty or len(df) < window + 1:
                continue

            df = df.sort_values("date").reset_index(drop=True)
            df["close_max"] = df["close"].rolling(window).max()
            df["close_min"] = df["close"].rolling(window).min()
            df["高控"] = (df["close_max"] * 2 + df["close_min"]) / 3

            # ✅ 判斷是否今天是第一天突破高控
            if (
                df.iloc[-2]["close"] <= df.iloc[-2]["高控"]
                and df.iloc[-1]["close"] > df.iloc[-1]["高控"]
            ):
                result.append({
                    "股票代號": stock_id,
                    "股票名稱": stock_list[stock_list["stock_id"] == stock_id]["stock_name"].values[0],
                    "市場": market_label,
                    "收盤價": df.iloc[-1]["close"],
                    "高控": round(df.iloc[-1]["高控"], 2),
                    "日期": df.iloc[-1]["date"]
                })

        except Exception as e:
            print(f"⚠️ {stock_id} 發生錯誤：{e}")
            continue

# ✅ 輸出結果
df_result = pd.DataFrame(result)
print("\n✅ 掃描完成！")

if df_result.empty:
    print("😴 今天沒有任何股票是『第一天』突破高控")
else:
    print("\n📈 以下是『今天首次突破高控』的股票：\n")
    print(df_result.sort_values("收盤價", ascending=False).to_string(index=False))
