import os
import re
import json
import subprocess
import fitz
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

CELL           = "F4"
DPI            = 200
WORK_DIR       = "./pdf_work"
LOG_SHEET_NAME = "ログ"
JST            = timezone(timedelta(hours=9))

# ========================
# Service Account 認証
# ========================
_sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT")
if not _sa_json:
    raise RuntimeError("GOOGLE_SERVICE_ACCOUNT が設定されていません")

creds = Credentials.from_service_account_info(
    json.loads(_sa_json),
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ],
)

gc    = gspread.Client(auth=creds)
sh    = gc.open_by_key(SPREADSHEET_ID)
drive = build("drive", "v3", credentials=creds)

# ========================
# Ghostscript 存在確認
# ========================
_gs_check = subprocess.run(["gs", "--version"], capture_output=True, text=True)
if _gs_check.returncode != 0:
    raise EnvironmentError(
        "Ghostscriptが見つかりません。"
        "workflow.yml に 'sudo apt-get install -y ghostscript' を追加してください。"
    )

# ========================
# ログシート準備
# ========================
try:
    log_sheet = sh.worksheet(LOG_SHEET_NAME)
    log_sheet.clear()
except gspread.exceptions.WorksheetNotFound:
    try:
        log_sheet = sh.add_worksheet(title=LOG_SHEET_NAME, rows=5000, cols=4)
    except Exception as e:
        raise RuntimeError("ログシートの作成に失敗しました: {}".format(e)) from e
except Exception as e:
    raise RuntimeError("ログシートの初期化に失敗しました: {}".format(e)) from e

log_sheet.append_row(["", "日時（JST）", "アクション", "詳細"])

def log(action, memo=""):
    now = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    log_sheet.append_row(["", now, action, memo])
    print("[{}] {} | {}".format(now, action, memo))

# ========================
# PDF処理関数
# ========================
def list_pdfs_recursive(folder_id):
    """フォルダを再帰的に探索してPDF一覧を返す"""
    pdfs       = []
    q          = "'" + folder_id + "' in parents and trashed=false"
    page_token = None
    while True:
        res = drive.files().list(
            q=q,
            fields="nextPageToken, files(id, name, mimeType, size)",
            pageSize=1000,
            pageToken=page_token,
        ).execute()
        for item in res.get("files", []):
            if item["mimeType"] == "application/pdf":
                pdfs.append(item)
            elif item["mimeType"] == "application/vnd.google-apps.folder":
                pdfs.extend(list_pdfs_recursive(item["id"]))
        page_token = res.get("nextPageToken")
        if not page_token:
            break
    return pdfs


def open_pdf_and_get_target(input_path):
    """
    PDFを開いてページ数を確認し、0なら修復を試みる。

    GASの normalizePdfLinks() が追記する増分更新の trailer に
    '/Size 9999' のような異常値が入ることで PyMuPDF がページ数0と
    誤認識する問題に対処する。

    修復方法:
      1つ目の %%EOF までのバイト列を stream として開き直すことで
      増分更新部分を無視した正常な PDF として読み込む。

    戻り値:
      (target_path, repaired_path)
        target_path  : 以後の処理で使う PDF パス
        repaired_path: 修復一時ファイルのパス（修復不要なら None）
                       → 呼び出し元の finally で必ず削除すること
    """
    # まず通常オープン
    src = fitz.open(input_path)
    if len(src) > 0:
        src.close()
        return input_path, None   # 修復不要

    src.close()

    # ページ数0 → 1つ目の %%EOF までで切り出して stream として開く
    with open(input_path, "rb") as f:
        data = f.read()

    first_eof = data.find(b"%%EOF")
    if first_eof == -1:
        raise ValueError("%%EOF が見つかりません。PDFが壊れています: " + input_path)

    data_trimmed = data[:first_eof + 5]
    src = fitz.open(stream=data_trimmed, filetype="pdf")

    if len(src) == 0:
        src.close()
        raise ValueError("PDFにページが存在しません（修復後も0ページ）: " + input_path)

    # 修復済みファイルとして保存
    repaired_path = input_path + ".__repaired__.pdf"
    src.save(repaired_path, garbage=4, deflate=True, clean=True)
    src.close()

    return repaired_path, repaired_path   # target と repaired は同じパス


def flatten_pdf(input_path, output_path):
    """
    Ghostscript でレンダリングしてアノテーションを焼き付ける。
    PyMuPDF の ExtGState 欠損エラー（AFSE6, GState8 等）を回避できる。
    増分更新の /Size 異常値によるページ数0エラーにも対処済み。

    手順:
      ① PDF を開く（増分更新 /Size 異常値でページ数0の場合は修復して再試行）
      ② PyMuPDF でリンク情報とページサイズを取得
      ③ Ghostscript で全ページを PNG にレンダリング
      ④ PNG を元サイズの新規 PDF に組み立て
      ⑤ PyMuPDF でリンクを再付与して保存
    """
    work_dir      = os.path.dirname(os.path.abspath(output_path))
    repaired_path = None

    try:
        # ① PDF を開く（必要なら修復）
        target_path, repaired_path = open_pdf_and_get_target(input_path)

        # ② リンク・ページサイズを取得
        src            = fitz.open(target_path)
        links_per_page = []
        rects_per_page = []
        total_links    = 0
        for page in src:
            lks = page.get_links()
            links_per_page.append(lks)
            rects_per_page.append(page.rect)
            total_links += len(lks)
        page_count = len(src)
        src.close()

        # ③ Ghostscript で PNG に変換（連番: gs_page_0001.png, 0002.png ...）
        png_pattern = os.path.join(work_dir, "gs_page_%04d.png")
        gs_cmd = [
            "gs",
            "-dBATCH", "-dNOPAUSE", "-dSAFER", "-dQUIET",
            "-sDEVICE=png16m",
            "-r" + str(DPI),
            "-sOutputFile=" + png_pattern,
            target_path,          # 修復済みファイル（あれば）を渡す
        ]
        result = subprocess.run(gs_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError("Ghostscriptエラー:\n" + result.stderr)

        # ④ PNG を PDF に組み立て（Ghostscript は 1 始まり連番）
        dst = fitz.open()
        for i in range(page_count):
            png_path = os.path.join(work_dir, "gs_page_{:04d}.png".format(i + 1))
            if not os.path.exists(png_path):
                raise FileNotFoundError("PNGが見つかりません: " + png_path)

            orig_rect = rects_per_page[i]
            dst_page  = dst.new_page(width=orig_rect.width, height=orig_rect.height)
            dst_page.insert_image(orig_rect, filename=png_path)
            os.remove(png_path)   # 使い終わったPNGを即削除してディスク節約

        # ⑤ リンクを再付与
        for i, links in enumerate(links_per_page):
            for link in links:
                dst[i].insert_link(link)

        dst.save(output_path, garbage=4, deflate=True)
        dst.close()

    finally:
        # 修復用一時ファイルを必ず削除（エラー時も）
        if repaired_path and os.path.exists(repaired_path):
            os.remove(repaired_path)

    return total_links


# ========================
# メイン処理
# ========================
log("開始", "PDFフラット化（Ghostscript版・再帰）")

try:
    folder_url = sh.sheet1.acell(CELL).value
    if not folder_url:
        log("失敗", "セル {} が空です".format(CELL))
        raise ValueError("セル {} が空です".format(CELL))

    match = re.search(r"folders/([a-zA-Z0-9_-]+)", folder_url)
    if not match:
        log("失敗", "フォルダURL不正")
        raise ValueError("フォルダURLが不正です")

    root_folder_id = match.group(1)
    all_pdfs       = list_pdfs_recursive(root_folder_id)
    log("情報", "検出PDF総数: {} 件".format(len(all_pdfs)))

    if len(all_pdfs) == 0:
        log("情報", "処理対象のPDFがありませんでした")
    else:
        os.makedirs(WORK_DIR, exist_ok=True)
        done  = 0
        in_p  = os.path.join(WORK_DIR, "in.pdf")
        out_p = os.path.join(WORK_DIR, "out.pdf")

        for pdf in all_pdfs:
            file_id = pdf["id"]
            name    = pdf["name"]
            before  = int(pdf.get("size", 0))

            try:
                # Drive からダウンロード
                req = drive.files().get_media(fileId=file_id)
                with open(in_p, "wb") as f:
                    downloader = MediaIoBaseDownload(f, req)
                    done_dl = False
                    while not done_dl:
                        _, done_dl = downloader.next_chunk()

                # フラット化（Ghostscript）
                kept_links = flatten_pdf(in_p, out_p)

                after = os.path.getsize(out_p)
                rate  = round((1 - after / before) * 100, 1) if before > 0 else 0

                # Drive に上書きアップロード
                media = MediaFileUpload(out_p, mimetype="application/pdf")
                drive.files().update(
                    fileId=file_id,
                    media_body=media,
                ).execute()

                done += 1
                log(
                    "処理完了",
                    "{} | {:.1f}MB → {:.1f}MB（{}% 削減）リンク {}件".format(
                        name,
                        before / 1024 / 1024,
                        after  / 1024 / 1024,
                        rate,
                        kept_links,
                    ),
                )

            except Exception as e:
                log("エラー", "{} | {}".format(name, str(e)))

            finally:
                for p in [in_p, out_p]:
                    if os.path.exists(p):
                        os.remove(p)

        log("成功", "{} / {} 件処理完了".format(done, len(all_pdfs)))

except Exception as e:
    log("致命的エラー", str(e))
    raise

print("✅ 完了")
