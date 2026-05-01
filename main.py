import os
import re
import json
import fitz  # PyMuPDF
from datetime import datetime, timezone, timedelta

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# ==================================================
# 設定
# ==================================================

# Spreadsheet ID（直書き）
SPREADSHEET_ID = "1MO5x_WfB3IUU4RlwlcjZzWUbg-CgQT5Yz68qJWYRfT4"

CELL = "F4"
DPI = 200
WORK_DIR = "./pdf_work"
LOG_SHEET_NAME = "ログ"

# 日本時間（JST）
JST = timezone(timedelta(hours=9))

# ==================================================
# Service Account（Secret から取得）
# ==================================================
SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT")
if not SERVICE_ACCOUNT_JSON:
    raise RuntimeError("GOOGLE_SERVICE_ACCOUNT が設定されていません")

creds = Credentials.from_service_account_info(
    json.loads(SERVICE_ACCOUNT_JSON),
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ],
)

gc = gspread.authorize(creds)
sh = gc.open_by_key(SPREADSHEET_ID)

# ==================================================
# ログシート取得
# ==================================================
try:
    log_sheet = sh.worksheet(LOG_SHEET_NAME)
except gspread.exceptions.WorksheetNotFound:
    log_sheet = sh.add_worksheet(title=LOG_SHEET_NAME, rows=1, cols=10)
    log_sheet.insert_row(["日時", "種別", "内容"], 1)

drive = build("drive", "v3", credentials=creds)

# ==================================================
# ログ関数（JST）
# ==================================================
def log(action, memo=""):
    now_jst = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    log_sheet.insert_row([now_jst, action, memo], index=2)

# ==================================================
# 開始
# ==================================================
log("開始", "PDFフラット化（再帰・圧縮）")

# 以降はこれまで作った処理をそのまま続ける
