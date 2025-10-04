import os, json, time, traceback, sys
import datetime as dt
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import requests
from requests_oauthlib import OAuth1

TZ = dt.timezone(dt.timedelta(hours=9), name="JST")
WINDOW_MIN = int(os.environ.get("WINDOW_MIN", "15"))  # ±何分

def log(*a): print(*a); sys.stdout.flush()
def need(name):
    v=os.environ.get(name)
    if not v: raise RuntimeError(f"[FATAL] missing env: {name}")
    return v

SHEET_ID=need("SHEET_ID"); GCP_SA_JSON=need("GCP_SA_JSON")
SHEET_TAB=os.environ.get("SHEET_TAB","x_autopost_yoru")
SHEET_GID=os.environ.get("SHEET_GID")          # 例: 286023080
FORCE_ONE=os.environ.get("FORCE_ONE","0")      # "1"で未処理の先頭1件を時間無視で投稿
DRY_RUN=os.environ.get("DRY_RUN","0")          # "1"で投稿せず判定のみ

X_API_KEY=need("X_API_KEY"); X_API_SECRET=need("X_API_SECRET")
X_ACCESS_TOKEN=need("X_ACCESS_TOKEN"); X_ACCESS_TOKEN_SECRET=need("X_ACCESS_TOKEN_SECRET")
TWEET_URL="https://api.x.com/2/tweets"

SLOTS = {"00:00":0,"03:00":3,"06:00":6,"09:00":9,"12:00":12,"15:00":15,"18:00":18,"21:00":21}
HEADER = ["slot","text","last_posted","done","tweet_id","note","datetime_jst"]  # A..G

def now_jst_floor_minute():
    n=dt.datetime.now(TZ); return n.replace(second=0, microsecond=0)

def get_sheet():
    info=json.loads(GCP_SA_JSON)
    log("[OK] service acct:", info.get("client_email"))
    creds=Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    gc=gspread.authorize(creds)
    sh=gspread.Spreadsheet(gc, sh=gc.open_by_key(SHEET_ID))
    if SHEET_GID:
        try:
            ws=sh.get_worksheet_by_id(int(SHEET_GID))
            log(f"[OK] use gid: {ws.id} ({ws.title})"); return ws
        except Exception as e:
            log("[WARN] gid lookup failed:", e)
    try:
        ws=sh.worksheet(SHEET_TAB); log(f"[OK] use name: {SHEET_TAB}"); return ws
    except gspread.exceptions.WorksheetNotFound:
        ws=sh.sheet1; log(f"[WARN] name not found. fallback:", ws.title); return ws

def read_rows(ws):
    # 1行目は信用せず、A〜G列の並びで解釈
    vals = ws.get_all_values()
    if not vals or len(vals) < 2:
        return []
    data = []
    for r in vals[1:]:
        row = (r + [""]*7)[:7]  # A..Gにパディング
        data.append(row)
    return data

def write_back(ws, df):
    # 正しいヘッダーを強制的に1行目に書き戻す
    values = [HEADER] + df.fillna("").astype(str).values.tolist()
    ws.clear()
    ws.update(values)

def post_tweet(text):
    if DRY_RUN=="1":
        log("[DRY] would post:", text[:50]); return "dry_run_id"
    auth=OAuth1(client_key=X_API_KEY, client_secret=X_API_SECRET,
                resource_owner_key=X_ACCESS_TOKEN, resource_owner_secret=X_ACCESS_TOKEN_SECRET,
                signature_method="HMAC-SHA1")
    r=requests.post(TWEET_URL, auth=auth, json={"text": text})
    if r.status_code>=400: log("[HTTP]", r.status_code, r.text)
    r.raise_for_status()
    return r.json()["data"]["id"]

def in_window(target_dt, now):
    if target_dt is None: return False
    delta = abs((now - target_dt).total_seconds())/60.0
    return delta <= WINDOW_MIN

def next_target_today(slot_str, base):
    s = str(slot_str).strip()
    if s not in SLOTS: return None
    h = SLOTS[s]
    return base.replace(hour=h, minute=0)

def parse_dt(s):
    if not s: return None
    s=str(s).strip()
    for fmt in ("%Y-%m-%d %H:%M","%Y/%m/%d %H:%M"):
        try: return dt.datetime.strptime(s, fmt).replace(tzinfo=TZ)
        except: pass
    return None

def run():
    now = now_jst_floor_minute()
    log("[INFO] now JST:", now.strftime("%Y-%m-%d %H:%M"), "window=±", WINDOW_MIN, "min")
    ws=get_sheet()

    rows = read_rows(ws)
    log("[INFO] rows:", len(rows))
    if not rows:
        log("[DONE] no rows"); return

    df = pd.DataFrame(rows, columns=HEADER)

    # 補完
    for c in HEADER:
        if c not in df.columns: df[c]=""

    posted=False

    # 1) slot方式（毎日）
    for i,row in df.iterrows():
        if str(row.get("done","")).strip()=="1": 
            continue  # 旧互換列。普段は空推奨
        slot = str(row.get("slot","")).strip()
        txt  = str(row.get("text","")).strip()
        if not txt:
            df.loc[i,"note"]="本文なし"; continue

        if slot:
            tgt = next_target_today(slot, now)
            if tgt and in_window(tgt, now):
                # 同日二重防止
                if str(row.get("last_posted","")).strip() == now.strftime("%Y-%m-%d"):
                    df.loc[i,"note"]="今日分は済"; continue
                log(f"[TRY slot] row={i} slot={slot} text='{txt[:40]}'")
                try:
                    tid=post_tweet(txt)
                    df.loc[i,"last_posted"]=now.strftime("%Y-%m-%d")
                    df.loc[i,"tweet_id"]=tid
                    df.loc[i,"note"]=f"OK {now.strftime('%Y-%m-%d %H:%M')}"
                    posted=True
                    log(f"[OK] tweeted id={tid}")
                    break
                except Exception as e:
                    df.loc[i,"note"]=f"ERR: {e}"
                    log("[ERR] post failed:", e); traceback.print_exc()
            else:
                if slot and slot not in SLOTS:
                    df.loc[i,"note"]="slotは 00:00/03:00/.../21:00 のいずれか"
                continue

    # 2) 旧互換：datetime_jst がある行（FORCE_ONE or 窓内）
    if not posted:
        for i,row in df.iterrows():
            if str(row.get("done","")).strip()=="1": continue
            txt=str(row.get("text","")).strip()
            when=parse_dt(row.get("datetime_jst",""))
            if not txt: df.loc[i,"note"]="本文なし"; continue
            if FORCE_ONE!="1":
                if not in_window(when, now):
                    if when is None: df.loc[i,"note"]="日時形式NG(YYYY-MM-DD HH:MM)"
                    continue
            log(f"[TRY dt] row={i} when={when} text='{txt[:40]}' force={FORCE_ONE}")
            try:
                tid=post_tweet(txt)
                df.loc[i,"done"]="1"; df.loc[i,"tweet_id"]=tid
                df.loc[i,"note"]=f"OK {now.strftime('%Y-%m-%d %H:%M')}"
                posted=True
                log(f"[OK] tweeted id={tid}")
                break
            except Exception as e:
                df.loc[i,"note"]=f"ERR: {e}"
                log("[ERR] post failed:", e); traceback.print_exc()

    # 書き戻し（正しいヘッダーを強制）
    write_back(ws, df)
    log("[DONE] updated. posted:", posted)

if __name__=="__main__":
    try: run()
    except Exception as e:
        print("[FATAL]", e); traceback.print_exc(); sys.exit(1)
