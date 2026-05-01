import os
import re
import json
import fitz  # PyMuPDF
from datetime import datetime, timezone, timedelta
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# ========================
# 環境変数（必須）
# ========================
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
if not SPREADSHEET_ID:
    raise RuntimeError("SPREADSHEET_ID が設定されていません")

CELL = "F4"
DPI = 200
WORK_DIR = "./pdf_work"
LOG_SHEET_NAME = "ログ"

# ========================
# 日本時間（JST）
# ========================
JST = timezone(timedelta(hours=9))

# ========================
# Service Account
# ========================
creds = Credentials.from_service_account_info(
    json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT"]),
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ],
)

gc = gspread.authorize(creds)
sh = gc.open_by_key(SPREADSHEET_ID)
drive = build("drive", "v3", credentials=creds)

# ========================
# ログシート初期化
# ========================
headers = [
    "日時",
    "種別",
    "ファイル名",
    "元サイズ(MB)",
    "後サイズ(MB)",
    "圧縮率(%)",
    "処理秒数",
    "メモ",
]

try:
    log_sheet = sh.worksheet(LOG_SHEET_NAME)
except gspread.WorksheetNotFound:
    log_sheet = sh.add_worksheet(
        title=LOG_SHEET_NAME,
        rows=1000,
        cols=len(headers),
    )

if not log_sheet.acell("A1").value:
    log_sheet.append_row(headers)

# ========================
# ログ行数が多すぎたらリセット
# ========================
def reset_log_if_needed():
    MAX_ROWS = 50000
    rows = len(log_sheet.get_all_values()) - 1
    if rows <= MAX_ROWS:
        return

    headers = log_sheet.row_values(1)
    log_sheet.clear()
    log_sheet.resize(rows=1)
    log_sheet.update("A1", [headers])

# ========================
# ログ出力（確実版）
# ========================
def log(action, filename="", before_mb="", after_mb="", rate="", seconds="", memo=""):
    try:
        reset_log_if_needed()

        now_str = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")

        if log_sheet.row_count - len(log_sheet.get_all_values()) < 10:
            log_sheet.add_rows(1000)

        log_sheet.append_row(
            [
                now_str,
                action,
                filename,
                before_mb,
                after_mb,
                rate,
                seconds,
                memo,
            ],
            value_input_option="USER_ENTERED",
        )
    except Exception as e:
        print("LOG ERROR:", e)

# ========================
# PDF一覧を再帰取得（ページネーション対応）
# ========================
def list_pdfs_recursive(folder_id):
    pdfs = []
    page_token = None

    while True:
        res = drive.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken, files(id, name, mimeType, size)",
            pageToken=page_token,
        ).execute()

        for f in res.get("files", []):
            if f["mimeType"] == "application/pdf":
                pdfs.append(f)
            elif f["mimeType"] == "application/vnd.google-apps.folder":
                pdfs.extend(list_pdfs_recursive(f["id"]))

        page_token = res.get("nextPageToken")
        if not page_token:
            break

    return pdfs

# ========================
# PDFフラット化
# ========================
def flatten_pdf(input_path, output_path):
    src = fitz.open(input_path)
    dst = fitz.open()

    for page in src:
        rect = page.rect
        mat = fitz.Matrix(DPI / 72, DPI / 72)
        pix = page.get_pixmap(matrix=mat, annots=True, alpha=False)

        new_page = dst.new_page(width=rect.width, height=rect.height)
        new_page.insert_image(rect, pixmap=pix)

        for link in page.get_links():
            new_page.insert_link(link)

    dst.save(output_path, garbage=4, deflate=True)
    src.close()
    dst.close()

# ========================
# 開始ログ
# ========================
start_time = datetime.now(JST)
log("開始", memo="PDFフラット化（再帰・圧縮）")

# ========================
# フォルダID取得
# ========================
folder_url = sh.sheet1.acell(CELL).value
match = re.search(r"folders/([a-zA-Z0-9_-]+)", folder_url)
if not match:
    log("失敗", memo="フォルダURL不正")
    raise ValueError("フォルダURLが不正です")

root_folder_id = match.group(1)
all_pdfs = list_pdfs_recursive(root_folder_id)
log("情報", memo=f"検出PDF総数: {len(all_pdfs)}")

# ========================
# PDF処理
# ========================
os.makedirs(WORK_DIR, exist_ok=True)
done = 0

for pdf in all_pdfs:
    t0 = datetime.now(JST)
    file_id = pdf["id"]
    name = pdf["name"]

    in_p = os.path.join(WORK_DIR, f"{file_id}_in.pdf")
    out_p = os.path.join(WORK_DIR, f"{file_id}_out.pdf")

    try:
        before = int(pdf.get("size", 0))

        # ダウンロード
        req = drive.files().get_media(fileId=file_id)
        with open(in_p, "wb") as f:
            downloader = MediaIoBaseDownload(f, req)
            done_dl = False
            while not done_dl:
                _, done_dl = downloader.next_chunk()

        # フラット化
        flatten_pdf(in_p, out_p)
        after = os.path.getsize(out_p)

        before_mb = round(before / 1024 / 1024, 2)
        after_mb = round(after / 1024 / 1024, 2)
        rate = round((1 - after / before) * 100, 1) if before > 0 else 0

        # 上書き
        media = MediaFileUpload(out_p, mimetype="application/pdf")
        drive.files().update(fileId=file_id, media_body=media).execute()

        sec = round((datetime.now(JST) - t0).total_seconds(), 2)

        log(
            "処理",
            filename=name,
            before_mb=before_mb,
            after_mb=after_mb,
            rate=rate,
            seconds=sec,
        )

        done += 1

    except Exception as e:
        log("失敗", filename=name, memo=str(e))

    finally:
        for p in (in_p, out_p):
            if os.path.exists(p):
                os.remove(p)

# ========================
# 完了ログ
# ========================
total_sec = round((datetime.now(JST) - start_time).total_seconds(), 1)
log("成功", seconds=total_sec, memo=f"{done} 件処理完了")

print("✅ 完了")
