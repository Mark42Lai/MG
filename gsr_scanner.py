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
LOOKBACK_DAYS = 450


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
    signal_date: str,
):
    df = data_loader.taiwan_stock_daily(
        stock_id=stock_id,
        start_date=start_date,
        end_date=end_date,
    )

    if df.empty or len(df) < WINDOW + 1:
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

    # FinMind 無公告價格的日線可能是 0，不能當作有效收盤或高低價。
    df = df.loc[
        (df["close"] > 0) & (df["max"] > 0) & (df["min"] > 0)
    ].reset_index(drop=True)

    if len(df) < WINDOW + 1:
        return None

    # ================================================
    # 只計算 GM Day0 所需的 12 日 HC（含當日）
    # ================================================

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

    today = df.iloc[-1]
    yesterday = df.iloc[-2]

    if str(today["date"])[:10] != signal_date:
        return None

    # ================================================
    # 今日收盤突破今日 HC，且前一交易日尚未站上其 HC
    # ================================================
    if not (
        pd.notna(today["HC"])
        and pd.notna(yesterday["HC"])
        and today["close"] > today["HC"]
        and yesterday["close"] <= yesterday["HC"]
    ):
        return None

    # ================================================
    # 產生通知內容
    # ================================================

    close_price = float(today["close"])
    hc_value = float(today["HC"])

    breakout_ratio = (
        (close_price - hc_value)
        / hc_value
        * 100
    )

    stock_name = get_stock_name(
        stock_list,
        stock_id,
    )

    message = (
        f"📈【{stock_id} {stock_name}】\n"
        f"🔥 今日突破高控！\n"
        f"收盤價: {close_price:.2f}\n"
        f"高控(HC): {hc_value:.2f}\n"
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

    # 定時任務只通報該交易日的突破；休市時不重送前次訊號。
    if (
        os.environ.get("GITHUB_EVENT_NAME") == "schedule"
        and latest_trade_date != datetime.now(timezone.utc).date()
    ):
        print(f"ℹ️ 今天無新台股交易資料（最近交易日：{latest_trade_date}），不發通知")
        return

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

    # FinMind 清單也包含類股指數與 ETF；只掃描四碼個股代號。
    stock_list = stock_list.loc[
        stock_list["stock_id"].str.fullmatch(r"[1-9][0-9]{3}")
    ].copy()

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

    print(f"📋 股票清單共 {len(all_stocks)} 檔")
    if not all_stocks:
        raise RuntimeError("股票清單為空，停止掃描")

    selected_stocks = all_stocks[
        args.offset:
        args.offset + args.limit
    ]

    print(
        f"📊 本批共掃描 "
        f"{len(selected_stocks)} 檔股票"
    )

    if not selected_stocks:
        print("ℹ️ 本批 offset 已超過股票清單，無須掃描或發送無訊號通知")
        return

    # 最後一批必須涵蓋整份清單；新增股票不能悄悄漏掃。
    if os.environ.get("FINAL_BATCH") == "1" and args.offset + args.limit < len(all_stocks):
        raise RuntimeError(
            f"最後一批只涵蓋至 {args.offset + args.limit}，"
            f"股票清單有 {len(all_stocks)} 檔，請增加批次"
        )

    result = []
    rate_limit_error = None

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
                signal_date=latest_trade_date.isoformat(),
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

            if "Requests reach the upper limit" in str(error):
                rate_limit_error = error
                print("❌ FinMind 額度已滿，本批停止；未掃描的股票不可視為無訊號")
                break

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

    elif not rate_limit_error:
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

    if rate_limit_error:
        raise RuntimeError("FinMind 額度不足，本批掃描不完整") from rate_limit_error

    print("\n✅ 本批掃描完成")


if __name__ == "__main__":
    main()
