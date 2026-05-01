import os
import re
import json
import fitz  # PyMuPDF
from datetime import datetime
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

now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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

# ========================
# ログシート取得（なければ自動作成）
# ========================
try:
    log_sheet = sh.worksheet(LOG_SHEET_NAME)
except gspread.exceptions.WorksheetNotFound:
    log_sheet = sh.add_worksheet(title=LOG_SHEET_NAME, rows=1000, cols=10)
    log_sheet.append_row(["日時", "種別", "内容"])

drive = build("drive", "v3", credentials=creds)

def log(action, memo=""):
    log_sheet.append_row([now, action, memo])

# ========================
# 処理開始ログ
# ========================
log("開始", "PDFフラット化（再帰・圧縮）")

# ========================
# PDF一覧を再帰取得
# ========================
def list_pdfs_recursive(folder_id):
    pdfs = []
    q = f"'{folder_id}' in parents and trashed=false"
    res = drive.files().list(
        q=q,
        fields="files(id, name, mimeType, size)",
    ).execute()

    for f in res.get("files", []):
        if f["mimeType"] == "application/pdf":
            pdfs.append(f)
        elif f["mimeType"] == "application/vnd.google-apps.folder":
            pdfs.extend(list_pdfs_recursive(f["id"]))
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
# フォルダID取得
# ========================
folder_url = sh.sheet1.acell(CELL).value
match = re.search(r"folders/([a-zA-Z0-9_-]+)", folder_url)
if not match:
    log("失敗", "フォルダURL不正")
    raise ValueError("フォルダURLが不正です")

root_folder_id = match.group(1)
all_pdfs = list_pdfs_recursive(root_folder_id)

log("情報", f"検出PDF総数: {len(all_pdfs)}")

os.makedirs(WORK_DIR, exist_ok=True)
done = 0

# ========================
# PDF 処理ループ
# ========================
for pdf in all_pdfs:
    file_id = pdf["id"]
    name = pdf["name"]
    before = int(pdf.get("size", 0))

    in_p = os.path.join(WORK_DIR, "in.pdf")
    out_p = os.path.join(WORK_DIR, "out.pdf")

    req = drive.files().get_media(fileId=file_id)
    with open(in_p, "wb") as f:
        downloader = MediaIoBaseDownload(f, req)
        done_dl = False
        while not done_dl:
            _, done_dl = downloader.next_chunk()

    flatten_pdf(in_p, out_p)
    after = os.path.getsize(out_p)

    rate = round((1 - after / before) * 100, 1) if before > 0 else 0

    media = MediaFileUpload(out_p, mimetype="application/pdf")
    drive.files().update(
        fileId=file_id,
        media_body=media,
    ).execute()

    done += 1
    log(
        "処理",
        f"{name} → {round(before/1024/1024,1)}MB → {round(after/1024/1024,1)}MB（{rate}%）",
    )

    os.remove(in_p)
    os.remove(out_p)

# ========================
# 完了ログ
# ========================
log("成功", f"{done} 件処理完了")
print("✅ 完了")
