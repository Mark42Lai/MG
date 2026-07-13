import os
import argparse
import warnings
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from FinMind.data import DataLoader

warnings.filterwarnings("ignore")


# =====================================================
# ① GitHub Repository Secrets
#
# GitHub Secrets 名稱：
# API_TOKEN
# LINE_TOKEN
# LINE_USER_ID
# =====================================================

def get_required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()

    if not value:
        raise RuntimeError(
            f"找不到環境變數 {name}。\n"
            "請確認 GitHub Actions workflow 已傳入對應的 Repository Secret。"
        )

    return value


API_TOKEN = get_required_env("API_TOKEN")
LINE_TOKEN = get_required_env("LINE_TOKEN")
LINE_USER_ID = get_required_env("LINE_USER_ID")


# LINE 的 Authorization Header 不能包含中文
try:
    LINE_TOKEN.encode("ascii")
except UnicodeEncodeError as error:
    raise RuntimeError(
        "LINE_TOKEN 含有中文字元。\n"
        "請確認程式已刪除「你的_LINE_Token」之類的提示文字，"
        "並確認 GitHub Secret LINE_TOKEN 儲存的是真正 Channel access token。"
    ) from error


# =====================================================
# ② 策略設定
# =====================================================

WINDOW = 12
LOOKBACK_DAYS = 201


# =====================================================
# ③ 執行參數
# =====================================================

parser = argparse.ArgumentParser()

parser.add_argument(
    "--offset",
    type=int,
    default=0,
    help="從排序後股票清單的第幾檔開始掃描",
)

parser.add_argument(
    "--limit",
    type=int,
    default=100,
    help="本次掃描股票數量",
)

args = parser.parse_args()


# =====================================================
# ④ 顯示環境變數讀取結果
# =====================================================

def show_environment_status() -> None:
    print("✅ 已成功讀取 GitHub Actions Secrets")
    print(f"✅ API_TOKEN 長度：{len(API_TOKEN)}")
    print(f"✅ LINE_TOKEN 長度：{len(LINE_TOKEN)}")
    print(f"✅ LINE_USER_ID：{LINE_USER_ID[:6]}***")


# =====================================================
# ⑤ 取得台灣日期
# =====================================================

def get_taiwan_today():
    taiwan_timezone = timezone(timedelta(hours=8))
    return datetime.now(taiwan_timezone).date()


# =====================================================
# ⑥ 尋找最近交易日
# =====================================================

def get_latest_trade_date(data_loader: DataLoader):
    check_date = get_taiwan_today()

    for _ in range(10):
        next_date = check_date + timedelta(days=1)

        df = data_loader.taiwan_stock_daily(
            stock_id="2330",
            start_date=check_date.isoformat(),
            end_date=next_date.isoformat(),
        )

        if not df.empty:
            return check_date

        check_date -= timedelta(days=1)

    raise RuntimeError("找不到最近十天的台股交易日")


# =====================================================
# ⑦ LINE 訊息切割
# =====================================================

def split_line_messages(messages, max_length=4800):
    batches = []
    current_batch = ""

    for message in messages:
        if not current_batch:
            current_batch = message
            continue

        combined = current_batch + "\n\n" + message

        if len(combined) <= max_length:
            current_batch = combined
        else:
            batches.append(current_batch)
            current_batch = message

    if current_batch:
        batches.append(current_batch)

    return batches


# =====================================================
# ⑧ 發送 LINE 訊息
# =====================================================

def send_line_message(user_id: str, message: str) -> bool:
    print("\n📤 準備發送 LINE 訊息：")
    print(message)

    url = "https://api.line.me/v2/bot/message/push"

    headers = {
        "Authorization": f"Bearer {LINE_TOKEN}",
        "Content-Type": "application/json; charset=UTF-8",
    }

    payload = {
        "to": user_id,
        "messages": [
            {
                "type": "text",
                "text": message,
            }
        ],
    }

    try:
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=30,
        )

    except UnicodeEncodeError as error:
        raise RuntimeError(
            "LINE_TOKEN 含有中文或其他不合法字元，"
            "無法放入 Authorization Header。"
        ) from error

    except requests.RequestException as error:
        print(f"❌ LINE API 連線失敗：{error}")
        return False

    if response.status_code == 200:
        print("✅ LINE 訊息發送成功")
        return True

    print(f"❌ LINE 訊息發送失敗：{response.status_code}")
    print(response.text)

    return False


# =====================================================
# ⑨ 取得股票名稱
# =====================================================

def get_stock_name(stock_list: pd.DataFrame, stock_id: str) -> str:
    matches = stock_list.loc[
        stock_list["stock_id"] == stock_id,
        "stock_name",
    ]

    if matches.empty:
        return "未知名稱"

    return str(matches.iloc[0])


# =====================================================
# ⑩ 掃描單一股票
# =====================================================

def scan_stock(
    data_loader: DataLoader,
    stock_list: pd.DataFrame,
    stock_id: str,
    start_date: str,
    end_date: str,
):
    df = data_loader.taiwan_stock_daily(
        stock_id=stock_id,
        start_date=start_date,
        end_date=end_date,
    )

    if df.empty or len(df) < 60:
        return None

    df = (
        df.sort_values("date")
        .drop_duplicates(
            subset=["date"],
            keep="last",
        )
        .reset_index(drop=True)
    )

    required_columns = [
        "date",
        "close",
        "max",
        "min",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise RuntimeError(
            f"缺少必要欄位：{missing_columns}"
        )

    for column in ["close", "max", "min"]:
        df[column] = pd.to_numeric(
            df[column],
            errors="coerce",
        )

    df = (
        df.dropna(
            subset=["close", "max", "min"]
        )
        .reset_index(drop=True)
    )

    if len(df) < 60:
        return None

    # ================================================
    # 計算 MA55、12日高低、HC、LC
    # ================================================

    df["MA55"] = (
        df["close"]
        .rolling(55)
        .mean()
    )

    df["high_12"] = (
        df["max"]
        .rolling(WINDOW)
        .max()
    )

    df["low_12"] = (
        df["min"]
        .rolling(WINDOW)
        .min()
    )

    # 高控
    df["HC"] = (
        df["high_12"] * 2
        + df["low_12"]
    ) / 3

    # 低控
    df["LC"] = (
        df["high_12"]
        + df["low_12"] * 2
    ) / 3

    df = (
        df.dropna(
            subset=["MA55", "HC", "LC"]
        )
        .reset_index(drop=True)
    )

    if len(df) < 2:
        return None

    # ================================================
    # 計算交叉訊號
    # ================================================

    # 昨日收盤 <= 昨日高控
    # 今日收盤 > 今日高控
    df["cross_up_HC"] = (
        (df["close"] > df["HC"])
        & (
            df["close"].shift(1)
            <= df["HC"].shift(1)
        )
    )

    # 昨日收盤 >= 昨日 MA55
    # 今日收盤 < 今日 MA55
    df["cross_down_MA55"] = (
        (df["close"] < df["MA55"])
        & (
            df["close"].shift(1)
            >= df["MA55"].shift(1)
        )
    )

    # 昨日收盤 >= 昨日低控
    # 今日收盤 < 今日低控
    df["cross_down_LC"] = (
        (df["close"] < df["LC"])
        & (
            df["close"].shift(1)
            >= df["LC"].shift(1)
        )
    )

    # ================================================
    # 模擬持股狀態
    # ================================================

    is_holding = False
    today_is_new_breakout = False

    for i in range(len(df)):
        current_close = float(df.at[i, "close"])
        current_ma55 = float(df.at[i, "MA55"])

        if not is_holding:
            if bool(df.at[i, "cross_up_HC"]):
                is_holding = True

                if i == len(df) - 1:
                    today_is_new_breakout = True

        else:
            exit_by_ma55 = bool(
                df.at[i, "cross_down_MA55"]
            )

            exit_by_lc = (
                current_close < current_ma55
                and bool(
                    df.at[i, "cross_down_LC"]
                )
            )

            if exit_by_ma55 or exit_by_lc:
                is_holding = False

    if not today_is_new_breakout:
        return None

    # ================================================
    # 產生通知內容
    # ================================================

    last_row = df.iloc[-1]

    close_price = float(last_row["close"])
    hc_value = float(last_row["HC"])
    ma55_value = float(last_row["MA55"])

    breakout_ratio = (
        (close_price - hc_value)
        / hc_value
        * 100
    )

    stock_name = get_stock_name(
        stock_list,
        stock_id,
    )

    signal_date = str(
        last_row["date"]
    )[:10]

    message = (
        f"📈【{stock_id} {stock_name}】\n"
        f"🔥 最新突破高控！\n"
        f"收盤價: {close_price:.2f}\n"
        f"高控(HC): {hc_value:.2f}\n"
        f"MA55: {ma55_value:.2f}\n"
        f"突破幅度: {breakout_ratio:.2f}%\n"
        f"日期: {signal_date}"
    )

    return message


# =====================================================
# ⑪ 主程式
# =====================================================

def main():
    show_environment_status()

    print("\n🔐 登入 FinMind API...")

    data_loader = DataLoader()

    data_loader.login_by_token(
        api_token=API_TOKEN
    )

    latest_trade_date = get_latest_trade_date(
        data_loader
    )

    start_date = (
        latest_trade_date
        - timedelta(days=LOOKBACK_DAYS)
    ).isoformat()

    # 使用交易日的下一天作為 API 結束日期
    api_end_date = (
        latest_trade_date
        + timedelta(days=1)
    ).isoformat()

    print(
        "\n📅 偵測日期區間："
        f"{start_date} ~ "
        f"{latest_trade_date.isoformat()}，"
        f"Offset: {args.offset} "
        f"Limit: {args.limit}"
    )

    # ================================================
    # 取得並整理股票清單
    # ================================================

    stock_list = data_loader.taiwan_stock_info()

    if stock_list.empty:
        raise RuntimeError(
            "FinMind 沒有回傳股票清單"
        )

    stock_list = stock_list.copy()

    stock_list["stock_id"] = (
        stock_list["stock_id"]
        .astype(str)
        .str.strip()
    )

    stock_list["stock_name"] = (
        stock_list["stock_name"]
        .astype(str)
        .str.strip()
    )

    # 移除同一股票代號的重複資料
    stock_list = (
        stock_list.sort_values("stock_id")
        .drop_duplicates(
            subset=["stock_id"],
            keep="first",
        )
        .reset_index(drop=True)
    )

    all_stocks = stock_list[
        "stock_id"
    ].tolist()

    selected_stocks = all_stocks[
        args.offset:
        args.offset + args.limit
    ]

    print(
        f"📊 本批共掃描 "
        f"{len(selected_stocks)} 檔股票"
    )

    result = []

    # ================================================
    # 逐檔掃描
    # ================================================

    for index, stock_id in enumerate(
        selected_stocks,
        start=1,
    ):
        try:
            print(
                f"[{index}/{len(selected_stocks)}] "
                f"掃描 {stock_id}"
            )

            message = scan_stock(
                data_loader=data_loader,
                stock_list=stock_list,
                stock_id=stock_id,
                start_date=start_date,
                end_date=api_end_date,
            )

            if message:
                result.append(message)

                print(
                    f"✅ {stock_id} "
                    "找到全新突破訊號！"
                )

        except Exception as error:
            print(
                f"⚠️ {stock_id} 發生錯誤："
                f"{type(error).__name__}: "
                f"{error}"
            )

            continue

    # ================================================
    # 發送結果
    # ================================================

    if result:
        # 再次去除完全重複的通知
        result = list(dict.fromkeys(result))

        message_batches = split_line_messages(
            result
        )

        print(
            f"\n📨 共找到 "
            f"{len(result)} 檔突破股票"
        )

        print(
            f"📨 將分成 "
            f"{len(message_batches)} 則訊息"
        )

        for batch_index, message in enumerate(
            message_batches,
            start=1,
        ):
            print(
                f"\n📨 發送第 "
                f"{batch_index}/"
                f"{len(message_batches)} 則"
            )

            success = send_line_message(
                LINE_USER_ID,
                message,
            )

            if not success:
                raise RuntimeError(
                    "LINE 訊息發送失敗"
                )

    else:
        if selected_stocks:
            scan_end = (
                args.offset
                + len(selected_stocks)
                - 1
            )
        else:
            scan_end = args.offset

        no_signal_message = (
            "😴 此批無全新突破高控的股票\n"
            f"掃描範圍: {args.offset} ~ {scan_end}\n"
            f"交易日期: {latest_trade_date.isoformat()}"
        )

        success = send_line_message(
            LINE_USER_ID,
            no_signal_message,
        )

        if not success:
            raise RuntimeError(
                "LINE 訊息發送失敗"
            )

    print("\n✅ 本批掃描完成")


if __name__ == "__main__":
    main()
