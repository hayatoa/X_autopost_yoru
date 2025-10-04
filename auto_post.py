import os, json, time
import datetime as dt
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import requests
from requests_oauthlib import OAuth1

TZ = dt.timezone(dt.timedelta(hours=9), name="JST")

SHEET_ID = os.environ["SHEET_ID"]
GCP_SA_JSON = os.environ["GCP_SA_JSON"]
SHEET_TAB = os.environ.get("SHEET_TAB", "x_autopost_yoru")  # ← タブ名（変更可）

X_API_KEY = os.environ["X_API_KEY"]
X_API_SECRET = os.environ["X_API_SECRET"]
X_ACCESS_TOKEN = os.environ["X_ACCESS_TOKEN"]
X_ACCESS_TOKEN_SECRET = os.environ["X_ACCESS_TOKEN_SECRET"]

TWEET_URL = "https://api.x.com/2/tweets"

def now_jst_floor_minute():
    n = dt.datetime.now(TZ)
    return n.replace(second=0, microsecond=0)

def get_sheet():
    info = json.loads(GCP_SA_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    try:
        return sh.worksheet(SHEET_TAB)  # 指定タブ
    except gspread.exceptions.WorksheetNotFound:
        return sh.sheet1               # フォールバック

def post_tweet_oauth1(text):
    auth = OAuth1(
        client_key=X_API_KEY,
        client_secret=X_API_SECRET,
        resource_owner_key=X_ACCESS_TOKEN,
        resource_owner_secret=X_ACCESS_TOKEN_SECRET,
        signature_method="HMAC-SHA1",
    )
    r = requests.post(TWEET_URL, auth=auth, json={"text": text})
    r.raise_for_status()
    return r.json()["data"]["id"]

def parse_dt(s):
    if not s: return None
    s = str(s).strip()
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=TZ)
    except:
        return None

def run():
    sheet = get_sheet()
    rows = sheet.get_all_records()
    if not rows:
        return
    df = pd.DataFrame(rows)
    if "done" not in df.columns: df["done"] = ""
    if "tweet_id" not in df.columns: df["tweet_id"] = ""
    if "note" not in df.columns: df["note"] = ""
    now = now_jst_floor_minute()
    updated = False
    for i, row in df.iterrows():
        if str(row.get("done","")).strip() == "1":
            continue
        when = parse_dt(row.get("datetime_jst",""))
        if not when or when > now:
            continue
        text = str(row.get("text","")).strip()
        if not text:
            df.loc[i,"note"] = "本文なし"
            continue
        try:
            tid = post_tweet_oauth1(text)
            df.loc[i,"done"] = "1"
            df.loc[i,"tweet_id"] = tid
            df.loc[i,"note"] = f"OK {dt.datetime.now(TZ).strftime('%Y-%m-%d %H:%M')}"
            updated = True
            time.sleep(2)
        except Exception as e:
            df.loc[i,"note"] = f"ERR: {e}"
    if updated or True:
        values = [list(df.columns)] + df.fillna("").astype(str).values.tolist()
        sheet.clear()
        sheet.update(values)

if __name__ == "__main__":
    run()
