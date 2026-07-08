import pandas as pd
from FinMind.data import DataLoader
from datetime import datetime, timedelta
import argparse
import requests
import warnings
warnings.filterwarnings('ignore') # 隱藏 pandas 運算時的一些警告

# ===== 使用者設定區 =====
api_token = "你的_FinMind_Token_請記得保護好"
line_user_id = "U26e8775cea7db4d35acfcdd9bd30c9b9"  
line_token = "你的_LINE_Token_請記得保護好"
window = 12
lookback_days = 150  # ❗必須拉長到 150 天，才夠計算 55 日均線 (MA55)
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
    print("📤 準備發送 LINE 訊息:\n", message)
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


# ✅ 登入與日期初始化
print("🔐 登入 FinMind API...")
dl = DataLoader()
# dl.login_by_token(api_token=api_token) # 請替換回你的 Token

latest_trade_date = get_latest_trade_date(dl)
start_date = (latest_trade_date - timedelta(days=lookback_days)).isoformat()
end_date = latest_trade_date.isoformat()
print(f"\n📅 偵測日期區間：{start_date} ~ {end_date}，Offset: {args.offset} Limit: {args.limit}")

# ✅ 股票清單（先排序再分段）
stock_list = dl.taiwan_stock_info()
stock_list = stock_list.sort_values("stock_id").reset_index(drop=True)
all_stocks = stock_list["stock_id"].tolist()
selected_stocks = all_stocks[args.offset: args.offset + args.limit]

result = []

for stock_id in selected_stocks:
    try:
        df = dl.taiwan_stock_daily(stock_id=stock_id, start_date=start_date, end_date=end_date)
        if df.empty or len(df) < 60: # 確保至少有超過 55 天的資料能算 MA55
            continue
            
        df = df.sort_values("date").reset_index(drop=True)
        
        # 1. 計算指標
        df["MA55"] = df["close"].rolling(55).mean()
        df["high_12"] = df["max"].rolling(window).max()
        df["low_12"]  = df["min"].rolling(window).min()
        
        # 高控 = (12日高*2 + 12日低)/3
        df["HC"] = (df["high_12"] * 2 + df["low_12"]) / 3
        # 低控 = (12日高 + 12日低*2)/3
        df["LC"] = (df["high_12"] + df["low_12"] * 2) / 3

        # 剔除還沒產生 MA55 的舊資料列
        df = df.dropna(subset=["MA55", "HC", "LC"]).reset_index(drop=True)
        if df.empty:
            continue

        # 2. 預先計算「黃金交叉」與「死亡交叉」的布林值 (True/False)
        # 突破高控：昨天收盤 <= 昨天高控，且 今天收盤 > 今天高控
        df["cross_up_HC"] = (df["close"] > df["HC"]) & (df["close"].shift(1) <= df["HC"].shift(1))
        # 跌破55日線：昨天收盤 >= 昨天55日線，且 今天收盤 < 今天55日線
        df["cross_down_MA55"] = (df["close"] < df["MA55"]) & (df["close"].shift(1) >= df["MA55"].shift(1))
        # 跌破低控：昨天收盤 >= 昨天低控，且 今天收盤 < 今天低控
        df["cross_down_LC"] = (df["close"] < df["LC"]) & (df["close"].shift(1) >= df["LC"].shift(1))

        # 3. 模擬交易狀態機 (State Machine)
        is_holding = False
        today_is_new_breakout = False

        # 從過去推演到今天
        for i in range(len(df)):
            curr_close = df.at[i, "close"]
            curr_ma55 = df.at[i, "MA55"]
            
            if not is_holding:
                # 空手狀態下，如果發生「突破高控」，則轉為持股
                if df.at[i, "cross_up_HC"]:
                    is_holding = True
                    # 如果這一天剛好是「最後一天(今天)」，記錄為最新觸發訊號
                    if i == len(df) - 1:
                        today_is_new_breakout = True
            else:
                # 持股狀態下，檢查是否滿足「出場訊號」
                # 出場1: 股價 > 55日線 卻跌破 55日線
                exit_1 = df.at[i, "cross_down_MA55"] 
                
                # 出場2: 股價 < 55日線 且 跌破低控
                exit_2 = (curr_close < curr_ma55) and df.at[i, "cross_down_LC"]
                
                if exit_1 or exit_2:
                    is_holding = False # 觸發出場，轉回空手狀態 (重新等待下一次突破)

        # 4. 如果今天剛好是「全新突破」，且不是前幾天遺留的重複訊號，才發布通知
        if today_is_new_breakout:
            last_row = df.iloc[-1]
            gap = last_row["close"] - last_row["HC"]
            ratio = gap / last_row["HC"] * 100
            
            stock_name_matches = stock_list[stock_list["stock_id"] == stock_id]["stock_name"].values
            stock_name = stock_name_matches[0] if len(stock_name_matches) > 0 else "未知名稱"
            
            msg = (f"📈【{stock_id} {stock_name}】\n"
                   f"🔥 最新突破高控！\n"
                   f"收盤價: {last_row['close']}\n"
                   f"高控(HC): {round(last_row['HC'], 2)}\n"
                   f"MA55: {round(last_row['MA55'], 2)}\n"
                   f"突破幅度: {round(ratio, 2)}%\n"
                   f"日期: {last_row['date']}")
            result.append(msg)
            print(f"✅ {stock_id} 找到全新突破訊號！")

    except Exception as e:
        print(f"⚠️ {stock_id} 發生錯誤：{e}")
        continue

# ✅ 結果通知
if result:
    full_message = "\n\n".join(result)
    send_line_message(line_user_id, full_message)
else:
    send_line_message(line_user_id, "😴 此批無全新突破高控的股票")
