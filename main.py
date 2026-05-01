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
JST = timezone(timedelta(hours=9))  # 日本時間（UTC+9）

# ========================
# Service Account 認証
# ========================
creds = Credentials.from_service_account_info(
    json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT"]),
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ],
)

gc = gspread.Client(auth=creds)          # gspread 6.x 対応
sh = gc.open_by_key(SPREADSHEET_ID)
drive = build("drive", "v3", credentials=creds)

# ========================
# ログシート準備
# ========================
try:
    log_sheet = sh.worksheet(LOG_SHEET_NAME)
except gspread.exceptions.WorksheetNotFound:
    log_sheet = sh.add_worksheet(title=LOG_SHEET_NAME, rows=1000, cols=3)

# ヘッダーが空なら書き込む
if log_sheet.row_values(1) == []:
    log_sheet.append_row(["日時（JST）", "アクション", "詳細"])

def log(action, memo=""):
    """ログシートに1行追記する（日本時間で記録）"""
    now = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    log_sheet.append_row([now, action, memo])
    print(f"[{now}] {action} | {memo}")

# ========================
# PDF処理関数
# ========================
def list_pdfs_recursive(folder_id):
    """フォルダ内のPDFをサブフォルダも含めて再帰的に取得"""
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

def flatten_pdf(input_path, output_path):
    """PDFをラスタライズしてアノテーションを焼き込む（ハイパーリンクは保持）"""
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
# メイン処理
# ========================
log("開始", "PDFフラット化（再帰・圧縮）")

try:
    # フォルダID取得
    folder_url = sh.sheet1.acell(CELL).value
    if not folder_url:
        log("失敗", f"セル {CELL} が空です")
        raise ValueError(f"セル {CELL} が空です")

    match = re.search(r"folders/([a-zA-Z0-9_-]+)", folder_url)
    if not match:
        log("失敗", "フォルダURL不正")
        raise ValueError("フォルダURLが不正です")

    root_folder_id = match.group(1)
    all_pdfs = list_pdfs_recursive(root_folder_id)
    log("情報", f"検出PDF総数: {len(all_pdfs)} 件")

    if len(all_pdfs) == 0:
        log("情報", "処理対象のPDFがありませんでした")
    else:
        os.makedirs(WORK_DIR, exist_ok=True)
        done = 0

        for pdf in all_pdfs:
            file_id = pdf["id"]
            name = pdf["name"]
            before = int(pdf.get("size", 0))
            in_p = os.path.join(WORK_DIR, "in.pdf")
            out_p = os.path.join(WORK_DIR, "out.pdf")

            try:
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
                rate = round((1 - after / before) * 100, 1) if before > 0 else 0

                # アップロード（上書き）
                media = MediaFileUpload(out_p, mimetype="application/pdf")
                drive.files().update(
                    fileId=file_id,
                    media_body=media,
                ).execute()

                done += 1
                log(
                    "処理完了",
                    f"{name} | {round(before/1024/1024, 1)}MB → {round(after/1024/1024, 1)}MB（{rate}% 削減）",
                )

            except Exception as e:
                log("エラー", f"{name} | {str(e)}")

            finally:
                if os.path.exists(in_p):
                    os.remove(in_p)
                if os.path.exists(out_p):
                    os.remove(out_p)

        log("成功", f"{done} / {len(all_pdfs)} 件処理完了")

except Exception as e:
    log("致命的エラー", str(e))
    raise

print("✅ 完了")
