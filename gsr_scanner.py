import os
import argparse
import warnings
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from FinMind.data import DataLoader

warnings.filterwarnings("ignore")


# =====================================================
# ① 讀取 GitHub Repository Secrets
#
# GitHub Secret 名稱：
# API_TOKEN
# LINE_TOKEN
# LINE_USER_ID
# =====================================================

API_TOKEN = os.getenv("API_TOKEN", "").strip()
LINE_TOKEN = os.getenv("LINE_TOKEN", "").strip()
LINE_USER_ID = os.getenv("LINE_USER_ID", "").strip()


# =====================================================
# ② 策略設定
# =====================================================

WINDOW = 12
LOOKBACK_DAYS = 150


# =====================================================
# ③ GitHub Actions 分批掃描參數
# =====================================================

parser = argparse.ArgumentParser()

parser.add_argument(
    "--offset",
    type=int,
    default=0,
    help="從股票清單第幾檔開始掃描",
)

parser.add_argument(
    "--limit",
    type=int,
    default=100,
    help="本次掃描的股票數量",
)

args = parser.parse_args()


# =====================================================
# ④ 檢查 Secrets 是否正確傳入
# =====================================================

def validate_environment():
    missing = []

    if not API_TOKEN:
        missing.append("API_TOKEN")

    if not LINE_TOKEN:
        missing.append("LINE_TOKEN")

    if not LINE_USER_ID:
        missing.append("LINE_USER_ID")

    if missing:
        missing_text = "\n".join(
            f"- {name}" for name in missing
        )

        raise RuntimeError(
            "找不到以下環境變數：\n"
            f"{missing_text}\n\n"
            "請確認 GitHub Actions workflow 有將 "
            "Repository Secrets 傳入 env。"
        )

    # LINE Token 應該只包含英文、數字及符號。
    # 若讀到中文，代表仍在使用舊的提示文字。
    try:
        LINE_TOKEN.encode("ascii")
    except UnicodeEncodeError as error:
        raise RuntimeError(
            "LINE_TOKEN 含有中文字元。\n"
            "請確認程式沒有保留「你的_LINE_Token」等提示文字，"
            "並確認 GitHub Secret LINE_TOKEN 儲存的是真正金鑰。"
        ) from error

    print("✅ 已成功讀取 GitHub Secrets")
    print(f"✅ API_TOKEN 長度：{len(API_TOKEN)}")
    print(f"✅ LINE_TOKEN 長度：{len(LINE_TOKEN)}")
    print(f"✅ LINE_USER_ID：{LINE_USER_ID[:6]}***")


# =====================================================
# ⑤ 找最近台股交易日
# =====================================================

def get_latest_trade_date(data_loader):
    """
    GitHub Actions Runner 通常使用 UTC，
    因此明確使用台灣時間。
    """

    check_date = datetime.now(
        ZoneInfo("Asia/Taipei")
    ).date()

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

    raise RuntimeError(
        "❌ 找不到最近十天的台股交易日"
    )


# =====================================================
# ⑥ 切割 LINE 長訊息
# =====================================================

def split_line_messages(messages, max_length=4800):
    """
    LINE 單一文字訊息有長度限制。
    突破股票太多時，自動拆成多則訊息。
    """

    batches = []
    current_message = ""

    for message in messages:
        if not current_message:
            current_message = message
            continue

        combined = current_message + "\n\n" + message

        if len(combined) <= max_length:
            current_message = combined
        else:
            batches.append(current_message)
            current_message = message

    if current_message:
        batches.append(current_message)

    return batches


# =====================================================
# ⑦ 發送 LINE 訊息
# =====================================================

def send_line_message(user_id, message):
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

        if response.status_code == 200:
            print("✅ LINE 訊息發送成功")
            return True

        print(
            f"❌ LINE 訊息發送失敗："
            f"{response.status_code}"
        )
        print(response.text)

        return False

    except requests.RequestException as error:
        print(f"❌ LINE API 連線錯誤：{error}")
        return False


# =====================================================
# ⑧ 主程式
# =====================================================

def main():
    validate_environment()

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

    # FinMind 的 end_date 通常不包含結束當日，
    # 因此使用最近交易日的下一天。
    api_end_date = (
        latest_trade_date
        + timedelta(days=1)
    ).isoformat()

    print(
        "\n📅 偵測日期區間："
        f"{start_date} ～ "
        f"{latest_trade_date.isoformat()}"
    )

    print(f"📦 Offset：{args.offset}")
    print(f"📦 Limit：{args.limit}")

    # =================================================
    # 取得股票清單
    # =================================================

    stock_list = data_loader.taiwan_stock_info()

    if stock_list.empty:
        raise RuntimeError(
            "❌ FinMind 沒有回傳股票清單"
        )

    stock_list = stock_list.copy()

    stock_list["stock_id"] = (
        stock_list["stock_id"]
        .astype(str)
        .str.strip()
    )

    stock_list = (
        stock_list
        .sort_values("stock_id")
        .reset_index(drop=True)
    )

    all_stocks = (
        stock_list["stock_id"]
        .dropna()
        .drop_duplicates()
        .tolist()
    )

    selected_stocks = all_stocks[
        args.offset:
        args.offset + args.limit
    ]

    print(
        f"📊 本批共掃描 "
        f"{len(selected_stocks)} 檔股票"
    )

    result = []

    # =================================================
    # 逐檔掃描
    # =================================================

    for number, stock_id in enumerate(
        selected_stocks,
        start=1,
    ):
        try:
            print(
                f"[{number}/{len(selected_stocks)}] "
                f"掃描 {stock_id}"
            )

            df = data_loader.taiwan_stock_daily(
                stock_id=stock_id,
                start_date=start_date,
                end_date=api_end_date,
            )

            if df.empty or len(df) < 60:
                continue

            df = (
                df
                .sort_values("date")
                .drop_duplicates(
                    subset=["date"],
                    keep="last",
                )
                .reset_index(drop=True)
            )

            # 確保價格欄位是數值
            for column in ["close", "max", "min"]:
                df[column] = pd.to_numeric(
                    df[column],
                    errors="coerce",
                )

            df = (
                df
                .dropna(
                    subset=["close", "max", "min"]
                )
                .reset_index(drop=True)
            )

            if len(df) < 60:
                continue

            # =========================================
            # 計算 MA55、12日高低點、HC、LC
            # =========================================

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
                df
                .dropna(
                    subset=["MA55", "HC", "LC"]
                )
                .reset_index(drop=True)
            )

            if len(df) < 2:
                continue

            # =========================================
            # 突破與跌破訊號
            # =========================================

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

            # =========================================
            # 模擬持股狀態
            # =========================================

            is_holding = False
            today_is_new_breakout = False

            for i in range(len(df)):
                current_close = float(
                    df.at[i, "close"]
                )

                current_ma55 = float(
                    df.at[i, "MA55"]
                )

                if not is_holding:
                    if bool(
                        df.at[i, "cross_up_HC"]
                    ):
                        is_holding = True

                        if i == len(df) - 1:
                            today_is_new_breakout = True

                else:
                    exit_by_ma55 = bool(
                        df.at[
                            i,
                            "cross_down_MA55",
                        ]
                    )

                    exit_by_lc = (
                        current_close < current_ma55
                        and bool(
                            df.at[
                                i,
                                "cross_down_LC",
                            ]
                        )
                    )

                    if exit_by_ma55 or exit_by_lc:
                        is_holding = False

            # =========================================
            # 今天出現新突破才通知
            # =========================================

            if not today_is_new_breakout:
                continue

            last_row = df.iloc[-1]

            close_price = float(
                last_row["close"]
            )

            hc_value = float(
                last_row["HC"]
            )

            ma55_value = float(
                last_row["MA55"]
            )

            breakout_ratio = (
                (close_price - hc_value)
                / hc_value
                * 100
            )

            stock_name_matches = stock_list.loc[
                stock_list["stock_id"] == stock_id,
                "stock_name",
            ].values

            if len(stock_name_matches) > 0:
                stock_name = str(
                    stock_name_matches[0]
                )
            else:
                stock_name = "未知名稱"

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

            result.append(message)

            print(
                f"✅ {stock_id} {stock_name} "
                "找到全新突破訊號"
            )

        except Exception as error:
            print(
                f"⚠️ {stock_id} 發生錯誤："
                f"{type(error).__name__}: "
                f"{error}"
            )

            continue

    # =================================================
    # 發送掃描結果
    # =================================================

    if result:
        message_batches = split_line_messages(
            result
        )

        print(
            f"\n📨 共找到 {len(result)} 檔突破股票"
        )

        for batch_number, message in enumerate(
            message_batches,
            start=1,
        ):
            print(
                f"📨 發送第 {batch_number}/"
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
            f"掃描範圍：{args.offset} ～ {scan_end}\n"
            f"交易日期：{latest_trade_date.isoformat()}"
        )

        success = send_line_message(
            LINE_USER_ID,
            no_signal_message,
        )

        if not success:
            raise RuntimeError(
                "LINE 訊息發送失敗"
            )

    print("\n✅ 本批股票掃描完成")


if __name__ == "__main__":
    main()
