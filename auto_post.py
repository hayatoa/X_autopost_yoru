import os, json, time, traceback, sys
import datetime as dt
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import requests
from requests_oauthlib import OAuth1

TZ = dt.timezone(dt.timedelta(hours=9), name="JST")

def log(*a): print(*a); sys.stdout.flush()

def need(name):
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"[FATAL] missing env: {name}")
    return v

# ===== ENV =====
SHEET_ID = need("SHEET_ID")
GCP_SA_JSON = need("GCP_SA_JSON")
SHEET_TAB = os.environ.get("SHEET_TAB", "x_autopost_yoru")

X_API_KEY = need("X_API_KEY")
X_API_SECRET = need("X_API_SECRET")
X_ACCESS_TOKEN = need("X_ACCESS_TOKEN")
X_ACCESS_TOKEN_SECRET = need("X_ACCESS_TOKEN_SECRET")

TWEET_URL = "https://api.x.com/2/tweets"

def now_jst_floor_minute():
    n = dt.datetime.now(TZ)
    return n.replace(second=0, microsecond=0)

def get_sheet():
    # GCP JSONの妥当性チェック
    try:
        info = json.loads(GCP_SA_JSON)
        if "client_email" not in info:
            raise ValueError("service account json missing client_email")
        log("[OK] GCP_SA_JSON parsed. client_email =", info.get("client_email"))
    except Exception as e:
        raise RuntimeError(f"[FATAL] invalid GCP_SA_JSON: {e}")

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    tabs = [ws.title for ws in sh.worksheets()]
    log("[INFO] worksheets:", tabs)
    try:
        ws = sh.worksheet(SHEET_TAB)
        log(f"[OK] use worksheet: {SHEET_TAB}")
        return ws
    except gspread.exceptions.WorksheetNotFound:
        log(f"[WARN] worksheet '{SHEET_TAB}' not found. fallback to first sheet")
        ws = sh.sheet1
        log(f"[OK] fallback worksheet: {ws.title}")
        return ws

def post_tweet_oauth1(text):
    auth = OAuth1(
        client_key=X_API_KEY,
        client_secret=X_API_SECRET,
        resource_owner_key=X_ACCESS_TOKEN,
        resource_owner_secret=X_ACCESS_TOKEN_SECRET,
        signature_method="HMAC-SHA1",
    )
    r = requests.post(TWEET_URL, auth=auth, json={"text": text})
    if r.status_code >= 400:
        log("[HTTP]", r.status_code, r.text)
    r.raise_for_status()
    return r.json()["data"]["id"]

def parse_dt(s):
    if not s: return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M"):
        try:
            return dt.datetime.strptime(s, fmt).replace(tzinfo=TZ)
        except:
            pass
    return None

def run():
    log("[ENV] SHEET_ID len:", len(SHEET_ID), "TAB:", SHEET_TAB)
    sheet = get_sheet()
    rows = sheet.get_all_records()
    log("[INFO] rows:", len(rows))
    if not rows:
        log("[DONE] no rows. exit 0")
        return

    df = pd.DataFrame(rows)
    log("[INFO] columns:", list(df.columns))
    for col in ["datetime_jst", "text"]:
        if col not in df.columns:
            raise RuntimeError(f"[FATAL] missing column in sheet: '{col}' (need: datetime_jst | text | done | tweet_id | note)")

    for c in ["done","tweet_id","note"]:
        if c not in df.columns:
            df[c] = ""

    now = now_jst_floor_minute()
    log("[INFO] now JST:", now.strftime("%Y-%m-%d %H:%M"))
    updated = False
    for i, row in df.iterrows():
        if str(row.get("done","")).strip() == "1":
            continue
        when = parse_dt(row.get("datetime_jst",""))
        if not when:
            df.loc[i,"note"] = "日時形式NG(YYYY-MM-DD HH:MM)"
            continue
        if when > now:
            continue
        text = str(row.get("text","")).strip()
        if not text:
            df.loc[i,"note"] = "本文なし"
            continue
        log(f"[POST] row={i} when={when} text='{text[:40]}'")
        try:
            tid = post_tweet_oauth1(text)
            df.loc[i,"done"] = "1"
            df.loc[i,"tweet_id"] = tid
            df.loc[i,"note"] = f"OK {now.strftime('%Y-%m-%d %H:%M')}"
            updated = True
            time.sleep(1.0)
            log(f"[OK] tweeted id={tid}")
        except Exception as e:
            df.loc[i,"note"] = f"ERR: {e}"
            log("[ERR] post failed:", e)
            traceback.print_exc()

    # 書き戻し
    values = [list(df.columns)] + df.fillna("").astype(str).values.tolist()
    sheet.clear()
    sheet.update(values)
    log("[DONE] sheet updated. updated_rows:", int(updated))

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        log("[FATAL] exception:", e)
        traceback.print_exc()
        sys.exit(1)
