import base64
import csv
from datetime import date, datetime
import hashlib
import hmac
from io import StringIO
import json
from pathlib import Path
import socket
import threading
import time
from urllib.parse import parse_qs, urlparse
from urllib.request import urlopen

import altair as alt
import pandas as pd
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh  # type: ignore[import-not-found]
    AUTOREFRESH_AVAILABLE = True
except ImportError:
    AUTOREFRESH_AVAILABLE = False

    def st_autorefresh(*args, **kwargs):
        return None

from report_utils import calculate_thermal_stats, create_sanification_pdf, normalize_temperature_df, parse_manual_data
from storage import (
    frames_for_activity,
    init_db,
    list_activity_codes,
    recent_batch_runs,
    recent_frames,
    save_batch_run,
    save_frame,
)
from udp_client import MockUdpControllerClient, UdpControllerClient


st.set_page_config(
    page_title="UDP Discovery + Report Sanificazione",
    layout="wide",
    initial_sidebar_state="collapsed",
)

APP_SETTINGS_PATH = Path("app_discovery/data/app_settings.json")
DRIVE_CACHE_DIR = Path("app_discovery/data/drive_cache")

APP_AUTH_PBKDF2_ITERATIONS = 200000
APP_AUTH_SALT_B64 = "ICXnAqhR/6XP7px6JsgTcQ=="
APP_AUTH_HASH_B64 = "lNwzg1owgIVJBl+4JyFCEnDuLfy+i2WofRraszSlBe4="
APP_AUTH_CACHE_TTL_SEC = 60 * 60 * 12

# URL Google Sheet cifrato con chiave applicativa (password utente).
APP_REMOTE_CONFIG_URL_ENC = (
    "OxUaGUIIHA43Dg0aH1VcTjQNC0dSXV4OIBEcDFBWQEk2BBoaHlYcEAJVJxx1SgQVBTMZK1xLRkghVwdbUH9GRTo+CQYHS2QUOCQULWZ8V1E6VAlGVFZaVQ=="
)
APP_REMOTE_CONFIG_KEY = "Sani123!"


def _hash_password_pbkdf2(password: str, salt_b64: str, iterations: int) -> bytes:
    salt = base64.b64decode(salt_b64.encode("utf-8"))
    return hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)


def _verify_app_password(candidate: str) -> bool:
    try:
        expected = base64.b64decode(APP_AUTH_HASH_B64.encode("utf-8"))
        current = _hash_password_pbkdf2(candidate, APP_AUTH_SALT_B64, APP_AUTH_PBKDF2_ITERATIONS)
        return hmac.compare_digest(current, expected)
    except Exception:
        return False


def _xor_decrypt_b64(cipher_text_b64: str, key: str) -> str:
    cipher_bytes = base64.b64decode(cipher_text_b64.encode("utf-8"))
    key_bytes = key.encode("utf-8")
    if not key_bytes:
        raise ValueError("Chiave di decrittazione non valida")
    plain = bytes([b ^ key_bytes[i % len(key_bytes)] for i, b in enumerate(cipher_bytes)])
    return plain.decode("utf-8")


def _sheet_csv_url_from_sheet_url(sheet_url: str) -> str:
    parsed = urlparse(sheet_url)
    query = parse_qs(parsed.query or "")
    gid = query.get("gid", ["0"])[0]

    path = parsed.path
    if "/edit" in path:
        path = path.split("/edit", 1)[0]

    return f"{parsed.scheme}://{parsed.netloc}{path}/export?format=csv&gid={gid}"


@st.cache_data(ttl=300, show_spinner=False)
def _load_remote_kv_config() -> dict[str, str]:
    try:
        sheet_url = _xor_decrypt_b64(APP_REMOTE_CONFIG_URL_ENC, APP_REMOTE_CONFIG_KEY)
        csv_url = _sheet_csv_url_from_sheet_url(sheet_url)
        with urlopen(csv_url, timeout=8) as resp:  # nosec B310 - trusted fixed host and path
            raw = resp.read().decode("utf-8-sig", errors="ignore")

        rows = csv.DictReader(StringIO(raw))
        out: dict[str, str] = {}
        for row in rows:
            key = str(row.get("Key", "")).strip()
            value = str(row.get("Value", "")).strip()
            if key:
                out[key] = value
        return out
    except Exception:
        return {}


@st.cache_resource
def _auth_cache_store() -> dict:
    return {}


def _auth_fingerprint() -> str:
    try:
        headers = st.context.headers
        ua = str(headers.get("User-Agent", ""))
        host = str(headers.get("Host", ""))
        lang = str(headers.get("Accept-Language", ""))
        return f"{host}|{ua}|{lang}"
    except Exception:
        return "local-client"


def _set_cached_auth_authenticated(fingerprint: str) -> None:
    cache = _auth_cache_store()
    cache[fingerprint] = time.time() + APP_AUTH_CACHE_TTL_SEC


def _is_cached_auth_authenticated(fingerprint: str) -> bool:
    cache = _auth_cache_store()
    expire_at = cache.get(fingerprint)
    if not expire_at:
        return False
    if float(expire_at) < time.time():
        cache.pop(fingerprint, None)
        return False
    return True


def _clear_cached_auth_authenticated(fingerprint: str) -> None:
    cache = _auth_cache_store()
    cache.pop(fingerprint, None)


def _render_auth_gate() -> None:
    st.markdown("### Accesso Applicazione")
    st.caption("Inserisci la password per continuare.")
    pwd = st.text_input("Password", type="password", key="app_login_password")
    login = st.button("Accedi", type="primary", use_container_width=True, key="app_login_button")
    if login:
        if _verify_app_password(pwd):
            st.session_state.app_authenticated = True
            _set_cached_auth_authenticated(_auth_fingerprint())
            st.rerun()
        else:
            st.error("Password non valida.")


def load_app_settings() -> dict:
    defaults = {
        "drive_folder_path": "",
        "drive_folder_url": "",
        "report_source_mode": "Import from Drive",
        "drive_import_mode": "URL Google Drive",
        "report_threshold_rule_c": 55.0,
        "report_required_min_above": 30.0,
    }
    try:
        if not APP_SETTINGS_PATH.exists():
            return defaults
        raw = json.loads(APP_SETTINGS_PATH.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return defaults
        drive_folder_path = str(raw.get("drive_folder_path", "")).strip()
        drive_folder_url = str(raw.get("drive_folder_url", "")).strip()
        if not drive_folder_url and drive_folder_path.lower().startswith(("http://", "https://")):
            # Migrazione retrocompatibile: URL salvato nel vecchio campo path.
            drive_folder_url = drive_folder_path

        return {
            "drive_folder_path": drive_folder_path,
            "drive_folder_url": drive_folder_url,
            "report_source_mode": str(raw.get("report_source_mode", defaults["report_source_mode"])).strip(),
            "drive_import_mode": str(raw.get("drive_import_mode", defaults["drive_import_mode"])).strip(),
            "report_threshold_rule_c": float(raw.get("report_threshold_rule_c", defaults["report_threshold_rule_c"])),
            "report_required_min_above": float(
                raw.get("report_required_min_above", defaults["report_required_min_above"])
            ),
        }
    except Exception:
        return defaults


def save_app_settings(settings: dict) -> None:
    APP_SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "drive_folder_path": str(settings.get("drive_folder_path", "")).strip(),
        "drive_folder_url": str(settings.get("drive_folder_url", "")).strip(),
        "report_source_mode": str(settings.get("report_source_mode", "Import from Drive")).strip(),
        "drive_import_mode": str(settings.get("drive_import_mode", "URL Google Drive")).strip(),
        "report_threshold_rule_c": float(settings.get("report_threshold_rule_c", 55.0)),
        "report_required_min_above": float(settings.get("report_required_min_above", 30.0)),
    }
    APP_SETTINGS_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _download_json_files_from_drive_url(folder_url: str, cache_dir: Path) -> tuple[list[Path], str | None]:
    url = folder_url.strip()
    if url.startswith("https:\\") or url.startswith("http:\\"):
        url = url.replace("\\", "/")
        if url.startswith("https:/") and not url.startswith("https://"):
            url = url.replace("https:/", "https://", 1)
        if url.startswith("http:/") and not url.startswith("http://"):
            url = url.replace("http:/", "http://", 1)
    if not url:
        return [], "Inserisci un URL cartella Google Drive."
    if "drive.google.com" not in url:
        return [], "URL non valido: usa un link cartella Google Drive (drive.google.com)."

    try:
        import gdown  # type: ignore[import-not-found]
    except Exception:
        return [], "Dipendenza mancante: installa gdown (pip install gdown)."

    cache_dir.mkdir(parents=True, exist_ok=True)

    # Pulizia cache precedente per mantenere solo l'ultimo download.
    for old_json in cache_dir.rglob("*.json"):
        try:
            old_json.unlink()
        except Exception:
            pass

    try:
        try:
            gdown.download_folder(
                url=url,
                output=str(cache_dir),
                quiet=True,
                use_cookies=False,
                remaining_ok=True,
            )
        except TypeError:
            # Compatibilita con versioni gdown che non espongono remaining_ok.
            gdown.download_folder(
                url=url,
                output=str(cache_dir),
                quiet=True,
                use_cookies=False,
            )
    except Exception as exc:
        return [], f"Download da Google Drive fallito: {exc}"

    json_files = sorted(cache_dir.rglob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not json_files:
        return [], "Nessun file JSON trovato nel link Drive indicato."
    return json_files, None


def _build_json_file_metadata_df(files: list[Path]) -> pd.DataFrame:
    rows = []
    for f in files:
        try:
            stat = f.stat()
            created = datetime.fromtimestamp(stat.st_ctime).strftime("%Y-%m-%d %H:%M:%S")
            size_kb = stat.st_size / 1024.0
        except Exception:
            created = "-"
            size_kb = 0.0
        rows.append(
            {
                "nome file": f.name,
                "data creazione": created,
                "size": f"{size_kb:.1f} KB",
            }
        )
    return pd.DataFrame(rows)


if "app_authenticated" not in st.session_state:
    st.session_state.app_authenticated = False

if not st.session_state.app_authenticated and _is_cached_auth_authenticated(_auth_fingerprint()):
    st.session_state.app_authenticated = True

if not st.session_state.app_authenticated:
    left, center, right = st.columns([2, 2.2, 2])
    with center:
        _render_auth_gate()
    st.stop()

header_left, header_center, header_right = st.columns([1, 4, 1])
with header_left:
    st.image("assets/images/saniservice_antitarlo.png", width=300)
with header_right:
    _, logo_right_col = st.columns([1, 1])
    with logo_right_col:
        st.image("assets/images/saniservice.png", width=150)

st.markdown(
    """
    <style>
    .main-title {font-size: 2rem; font-weight: 700; color: #0f172a; margin-bottom: 0.1rem;}
    .subtitle {color: #334155; margin-bottom: 1rem;}
    .card {
        background: linear-gradient(165deg, #ffffff 0%, #f8fafc 100%);
        border: 1px solid #e2e8f0;
        border-radius: 14px;
        padding: 14px;
        box-shadow: 0 3px 10px rgba(15, 23, 42, 0.05);
    }
    .metric-ok {color: #166534; font-weight: 700;}
    .metric-ko {color: #991b1b; font-weight: 700;}
    .timer-card {
        background: #f8fafc;
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        padding: 8px 12px;
        margin-top: 6px;
    }
    .clock-label {font-size: 0.8rem; color: #475569;}
    .clock-value {font-size: 1.35rem; font-weight: 700; color: #0f172a;}
    .temp-circle-grid {
        display: grid;
        grid-template-columns: repeat(8, minmax(82px, 1fr));
        gap: 8px;
        margin-top: 8px;
        margin-bottom: 4px;
    }
    .temp-circle {
        border-radius: 999px;
        min-height: 82px;
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        color: #ffffff;
        box-shadow: 0 4px 10px rgba(0,0,0,0.12);
    }
    .temp-circle-label {font-size: 0.75rem; opacity: 0.95;}
    .temp-circle-value {font-size: 1rem; font-weight: 700;}
    div.stButton > button[kind="primary"] {
        background-color: #dc2626;
        border-color: #b91c1c;
        color: #ffffff;
    }
    div.stButton > button[kind="primary"]:hover {
        background-color: #b91c1c;
        border-color: #991b1b;
    }
    div[class*="st-key-select_drive_json_"] div.stButton > button {
        background-color: #16a34a;
        border-color: #15803d;
        color: #ffffff;
    }
    div[class*="st-key-select_drive_json_"] div.stButton > button:hover {
        background-color: #15803d;
        border-color: #166534;
        color: #ffffff;
    }
    div[class*="st-key-drive_file_search_query"] {
        max-width: 200px;
        margin-left: auto;
    }
    div[class*="st-key-download_report_pdf_btn"] div.stDownloadButton > button {
        background-color: #16a34a;
        border-color: #15803d;
        color: #ffffff;
    }
    div[class*="st-key-download_report_pdf_btn"] div.stDownloadButton > button:hover {
        background-color: #15803d;
        border-color: #166534;
        color: #ffffff;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown('<div class="main-title">UDP Sensor Discovery & Sanification Report</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="subtitle">Collector UDP in LAN, storico SQLite, e generazione report PDF per attivita di sanificazione.</div>',
    unsafe_allow_html=True,
)

if "controller_ip" not in st.session_state:
    st.session_state.controller_ip = ""
if "version_info" not in st.session_state:
    st.session_state.version_info = {"hw": None, "fw": None}
if "activity_code" not in st.session_state:
    st.session_state.activity_code = ""
if "batch_state" not in st.session_state:
    st.session_state.batch_state = {
        "running": False,
        "current_cycle": 0,
        "completed_frames": 0,
        "start_utc": None,
        "start_monotonic": None,
        "end_utc": None,
        "last_values": None,
        "last_latency_ms": None,
        "last_error": None,
        "stop_reason": None,
        "mode": None,
        "duration_sec": 0,
        "threshold_event": False,
        "activity_code": None,
    }
if "batch_thread" not in st.session_state:
    st.session_state.batch_thread = None
if "batch_stop_event" not in st.session_state:
    st.session_state.batch_stop_event = None
if "send_sim_state" not in st.session_state:
    st.session_state.send_sim_state = {
        "running": False,
        "start_utc": None,
        "end_utc": None,
        "sent_packets": 0,
        "last_payload": None,
        "last_send_utc": None,
        "last_error": None,
    }
if "send_sim_thread" not in st.session_state:
    st.session_state.send_sim_thread = None
if "send_sim_stop_event" not in st.session_state:
    st.session_state.send_sim_stop_event = None
if "app_settings" not in st.session_state:
    st.session_state.app_settings = load_app_settings()
if "drive_folder_path" not in st.session_state:
    st.session_state.drive_folder_path = st.session_state.app_settings.get("drive_folder_path", "")
if "drive_folder_url" not in st.session_state:
    st.session_state.drive_folder_url = st.session_state.app_settings.get("drive_folder_url", "")
if (
    not st.session_state.get("drive_folder_url")
    and str(st.session_state.get("drive_folder_path", "")).lower().startswith(("http://", "https://"))
):
    # Fallback retrocompatibile: URL ancora presente nel vecchio campo path.
    st.session_state.drive_folder_url = str(st.session_state.get("drive_folder_path", "")).strip()
if "drive_url_input" not in st.session_state:
    st.session_state.drive_url_input = st.session_state.drive_folder_url
if "sidebar_drive_url_input" not in st.session_state:
    st.session_state.sidebar_drive_url_input = st.session_state.drive_folder_url
if "selected_drive_json_path" not in st.session_state:
    st.session_state.selected_drive_json_path = ""
if "report_source_mode_default" not in st.session_state:
    st.session_state.report_source_mode_default = st.session_state.app_settings.get(
        "report_source_mode", "Import from Drive"
    )
if "drive_import_mode_default" not in st.session_state:
    st.session_state.drive_import_mode_default = st.session_state.app_settings.get(
        "drive_import_mode", "URL Google Drive"
    )
if "drive_file_sort_col" not in st.session_state:
    st.session_state.drive_file_sort_col = "created_ts"
if "drive_file_sort_dir" not in st.session_state:
    st.session_state.drive_file_sort_dir = "desc"
if "drive_file_page" not in st.session_state:
    st.session_state.drive_file_page = 1
if "report_cliente" not in st.session_state:
    st.session_state.report_cliente = ""
if "report_indirizzo" not in st.session_state:
    st.session_state.report_indirizzo = ""
if "report_data_intervento" not in st.session_state:
    st.session_state.report_data_intervento = date.today()
if "report_luogo_intervento" not in st.session_state:
    st.session_state.report_luogo_intervento = "Sede Saniservice"
if "report_tecnico" not in st.session_state:
    st.session_state.report_tecnico = ""
if "report_codice_intervento" not in st.session_state:
    st.session_state.report_codice_intervento = ""
if "report_oggetto_trattato" not in st.session_state:
    st.session_state.report_oggetto_trattato = ""
if "report_note" not in st.session_state:
    st.session_state.report_note = ""
if "report_loaded_json_path" not in st.session_state:
    st.session_state.report_loaded_json_path = ""
if "report_header_pending" not in st.session_state:
    st.session_state.report_header_pending = None
if "report_threshold_rule_c" not in st.session_state:
    st.session_state.report_threshold_rule_c = float(st.session_state.app_settings.get("report_threshold_rule_c", 55.0))
if "report_required_min_above" not in st.session_state:
    st.session_state.report_required_min_above = float(
        st.session_state.app_settings.get("report_required_min_above", 30.0)
    )
if "import_data_from_collection" not in st.session_state:
    st.session_state.import_data_from_collection = False
if "remote_drive_defaults_applied" not in st.session_state:
    st.session_state.remote_drive_defaults_applied = False

remote_kv = _load_remote_kv_config()
remote_drive_url = str(remote_kv.get("GDrive", "")).strip()
if remote_drive_url and not st.session_state.get("remote_drive_defaults_applied", False):
    # Il default deve arrivare dal foglio Google Sheet.
    st.session_state.drive_folder_url = remote_drive_url
    st.session_state.drive_url_input = remote_drive_url
    st.session_state.sidebar_drive_url_input = remote_drive_url
    st.session_state.remote_drive_defaults_applied = True

user_agent = ""
try:
    user_agent = str(st.context.headers.get("User-Agent", ""))
except Exception:
    user_agent = ""
is_mobile_client = any(token in user_agent.lower() for token in ["iphone", "android", "mobile", "ipad"])


def activity_frames_to_temp_df(rows: list[dict], metric: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["tempo_min", "temperatura_c"])

    df = pd.DataFrame([dict(r) for r in rows])
    df = df.sort_values("id").copy()
    df["ts_dt"] = pd.to_datetime(df["ts"], errors="coerce", utc=True)
    df = df.dropna(subset=["ts_dt"])
    if df.empty:
        return pd.DataFrame(columns=["tempo_min", "temperatura_c"])

    first_ts = df["ts_dt"].iloc[0]
    df["tempo_min"] = (df["ts_dt"] - first_ts).dt.total_seconds() / 60.0

    if metric in {"s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"}:
        df["temperatura_c"] = pd.to_numeric(df[metric], errors="coerce")
    elif metric == "media_sensori":
        sensor_cols = ["s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"]
        for col in sensor_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["temperatura_c"] = df[sensor_cols].mean(axis=1)
    else:
        sensor_cols = ["s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"]
        for col in sensor_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["temperatura_c"] = df[sensor_cols].max(axis=1)

    out = df[["tempo_min", "temperatura_c"]].dropna().sort_values("tempo_min")
    return out.reset_index(drop=True)


def activity_frames_to_sensor_df(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["tempo_min", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"])

    df = pd.DataFrame([dict(r) for r in rows])
    df = df.sort_values("id").copy()
    df["ts_dt"] = pd.to_datetime(df["ts"], errors="coerce", utc=True)
    df = df.dropna(subset=["ts_dt"])
    if df.empty:
        return pd.DataFrame(columns=["tempo_min", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"])

    first_ts = df["ts_dt"].iloc[0]
    df["tempo_min"] = (df["ts_dt"] - first_ts).dt.total_seconds() / 60.0
    sensor_cols = ["s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"]
    for col in sensor_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    out = df[["tempo_min", *sensor_cols]].sort_values("tempo_min")
    return out.reset_index(drop=True)


def _drive_frames_to_sensor_df(frames: list[dict]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame(columns=["tempo_min", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"])

    df = pd.DataFrame(frames).copy()
    if "ts" not in df.columns:
        return pd.DataFrame(columns=["tempo_min", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"])

    df["ts_dt"] = pd.to_datetime(df["ts"], errors="coerce", utc=True)
    df = df.dropna(subset=["ts_dt"]).sort_values("ts_dt")
    if df.empty:
        return pd.DataFrame(columns=["tempo_min", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"])

    first_ts = df["ts_dt"].iloc[0]
    df["tempo_min"] = (df["ts_dt"] - first_ts).dt.total_seconds() / 60.0
    # Per import da Drive consideriamo solo i canali p1..p6 e s1..s2 del JSON.
    source_to_internal = {
        "p1": "s1",
        "p2": "s2",
        "p3": "s3",
        "p4": "s4",
        "p5": "s5",
        "p6": "s6",
        "s1": "s7",
        "s2": "s8",
    }
    for src_col, dst_col in source_to_internal.items():
        if src_col not in df.columns:
            df[src_col] = None
        df[dst_col] = pd.to_numeric(df[src_col], errors="coerce")

    sensor_cols = ["s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"]

    out = df[["tempo_min", *sensor_cols]].dropna(subset=["tempo_min"]).sort_values("tempo_min")
    return out.reset_index(drop=True)


def _drive_frames_to_temp_df(frames: list[dict], metric: str) -> pd.DataFrame:
    sensor_df = _drive_frames_to_sensor_df(frames)
    if sensor_df.empty:
        return pd.DataFrame(columns=["tempo_min", "temperatura_c"])

    out = sensor_df[["tempo_min"]].copy()
    metric_to_internal = {
        "p1": "s1",
        "p2": "s2",
        "p3": "s3",
        "p4": "s4",
        "p5": "s5",
        "p6": "s6",
        "s1": "s7",
        "s2": "s8",
    }
    if metric in metric_to_internal:
        out["temperatura_c"] = sensor_df[metric_to_internal[metric]]
    elif metric == "sonde_s1_s2":
        out["temperatura_c"] = sensor_df[["s7", "s8"]].mean(axis=1)
    elif metric == "media_sensori":
        out["temperatura_c"] = sensor_df[["s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"]].mean(axis=1)
    else:
        out["temperatura_c"] = sensor_df[["s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"]].max(axis=1)

    out = out.dropna(subset=["temperatura_c"]).sort_values("tempo_min")
    return out.reset_index(drop=True)


def format_hhmmss(total_seconds: float) -> str:
    sec = max(0, int(total_seconds))
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def parse_utc(ts: str | None) -> pd.Timestamp | None:
    if not ts:
        return None
    dt = pd.to_datetime(ts, errors="coerce", utc=True)
    if pd.isna(dt):
        return None
    return dt


def measurement_window_from_timestamps(values: list[str | None]) -> tuple[str, str, str] | None:
    parsed = [parse_utc(v) for v in values]
    parsed = [p for p in parsed if p is not None]
    if not parsed:
        return None

    start_ts = min(parsed)
    end_ts = max(parsed)
    duration_sec = max(0, (end_ts - start_ts).total_seconds())
    return (
        start_ts.tz_convert("Europe/Rome").strftime("%Y-%m-%d %H:%M:%S"),
        end_ts.tz_convert("Europe/Rome").strftime("%Y-%m-%d %H:%M:%S"),
        format_hhmmss(duration_sec),
    )


def time_ticks_10min(df: pd.DataFrame, col_name: str = "timestamp") -> list:
    if df.empty or col_name not in df.columns:
        return []
    tmin = df[col_name].min()
    tmax = df[col_name].max()
    if pd.isna(tmin) or pd.isna(tmax):
        return []
    start = pd.Timestamp(tmin).floor("10min")
    end = pd.Timestamp(tmax).ceil("10min")
    ticks = pd.date_range(start=start, end=end, freq="10min", tz="UTC")
    return [t.to_pydatetime() for t in ticks]


def render_instant_temp_circles(values: list | None) -> None:
    labels = ["P1", "P2", "P3", "P4", "P5", "P6", "S1", "S2"]
    if not values:
        values = [None] * 8
    safe_values = list(values)[:8]
    while len(safe_values) < 8:
        safe_values.append(None)

    def color_for(v):
        if v is None:
            return "#94a3b8"
        try:
            x = float(v)
        except Exception:
            return "#94a3b8"
        if x < 40:
            return "#0284c7"
        if x < 50:
            return "#16a34a"
        if x < 60:
            return "#ca8a04"
        return "#dc2626"

    html = ""
    for idx, v in enumerate(safe_values):
        txt = "--" if v is None else f"{v}Â°"
        html += (
            f'<div class="temp-circle" style="background:{color_for(v)}">'
            f'<div class="temp-circle-label">{labels[idx]}</div>'
            f'<div class="temp-circle-value">{txt}</div>'
            "</div>"
        )

    st.markdown(f'<div class="temp-circle-grid">{html}</div>', unsafe_allow_html=True)


def render_instant_temp_circles_idle() -> None:
    # In stato idle mostriamo i canali in grigio per evitare ambiguita su letture non correnti.
    labels = ["P1", "P2", "P3", "P4", "P5", "P6", "S1", "S2"]
    html = ""
    for label in labels:
        html += (
            '<div class="temp-circle" style="background:#94a3b8">'
            f'<div class="temp-circle-label">{label}</div>'
            '<div class="temp-circle-value">--</div>'
            "</div>"
        )
    st.markdown(f'<div class="temp-circle-grid">{html}</div>', unsafe_allow_html=True)


def _extract_report_header_from_payload(payload: dict, source_json: Path) -> dict:
    first_frame = {}
    frames = payload.get("frames")
    if isinstance(frames, list) and frames and isinstance(frames[0], dict):
        first_frame = frames[0]

    def _pick(*keys: str) -> str:
        for key in keys:
            val = payload.get(key)
            if val is None:
                val = first_frame.get(key)
            txt = str(val or "").strip()
            if txt:
                return txt
        return ""

    raw_date = _pick("data_intervento", "timestamp", "ts")
    parsed_date = st.session_state.get("report_data_intervento", date.today())
    if raw_date:
        try:
            parsed_ts = pd.to_datetime(raw_date, errors="coerce", utc=True)
            if not pd.isna(parsed_ts):
                parsed_date = parsed_ts.tz_convert("Europe/Rome").date()
        except Exception:
            pass

    oggetto = str(payload.get("oggetto_trattato") or "").strip()
    if not oggetto:
        objects = payload.get("objects")
        if isinstance(objects, list):
            descriptions = [
                str(obj.get("description", "")).strip()
                for obj in objects
                if isinstance(obj, dict) and str(obj.get("description", "")).strip()
            ]
            if descriptions:
                oggetto = ", ".join(descriptions)

    luogo_raw = _pick("luogo_intervento", "intervention_place")
    luogo_norm = "Sede Cliente"
    if luogo_raw.lower() in {"sani service", "sani-service", "sede saniservice"}:
        luogo_norm = "Sede Saniservice"

    return {
        "report_cliente": _pick("customer_name", "cliente"),
        "report_indirizzo": _pick("address", "indirizzo"),
        "report_data_intervento": parsed_date,
        "report_luogo_intervento": luogo_norm,
        "report_tecnico": _pick("tecnico"),
        "report_codice_intervento": str(
            _pick("codice_intervento", "timestamp") or source_json.stem
        ).strip(),
        "report_oggetto_trattato": oggetto,
        "report_note": _pick("notes", "note"),
    }


def _apply_report_header_from_payload(payload: dict, source_json: Path) -> None:
    values = _extract_report_header_from_payload(payload, source_json)
    # Non scriviamo direttamente su chiavi gia collegate ai widget nello stesso run.
    st.session_state.report_header_pending = values


def _apply_pending_report_header_if_any() -> None:
    pending = st.session_state.get("report_header_pending")
    if not isinstance(pending, dict):
        return
    for key, value in pending.items():
        st.session_state[key] = value
    st.session_state.report_header_pending = None


def make_client_from_config(cfg: dict) -> UdpControllerClient | MockUdpControllerClient:
    if cfg["use_mock"]:
        return MockUdpControllerClient(
            timeout_sec=float(cfg["timeout_sec"]),
            seed=cfg.get("mock_seed"),
            timeout_rate=float(cfg.get("mock_timeout_rate", 0.08)),
            ser_rate=float(cfg.get("mock_ser_rate", 0.05)),
            scenario=cfg.get("mock_scenario", "normal"),
        )
    return UdpControllerClient(port=int(cfg["port"]), timeout_sec=float(cfg["timeout_sec"]))


def run_batch_worker(cfg: dict, stop_event: threading.Event, state: dict) -> None:
    client_local = make_client_from_config(cfg)
    start_ts = time.time()
    state["running"] = True
    state["current_cycle"] = 0
    state["completed_frames"] = 0
    state["start_utc"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    state["start_monotonic"] = start_ts
    state["end_utc"] = None
    state["stop_reason"] = None
    state["mode"] = cfg["mode"]
    state["duration_sec"] = int(cfg.get("duration_sec", 0) or 0)
    state["threshold_event"] = False
    state["activity_code"] = cfg["activity_code"]

    try:
        while True:
            if stop_event.is_set():
                state["stop_reason"] = "Arresto manuale richiesto"
                break

            state["current_cycle"] += 1
            result = client_local.request_sensors(cfg["controller_ip"])
            frame = {
                "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "activity_code": cfg["activity_code"],
                "controller_ip": cfg["controller_ip"],
                "hw_version": cfg.get("hw_version"),
                "fw_version": cfg.get("fw_version"),
                "values": result["values"],
                "frame_complete": result["frame_complete"],
                "latency_ms": result["latency_ms"],
                "error_text": result["error_text"],
                "raw_messages": result["raw_messages"],
            }
            save_frame(cfg["db_path"], frame)

            if frame["frame_complete"]:
                state["completed_frames"] += 1
            state["last_values"] = frame["values"]
            state["last_latency_ms"] = frame["latency_ms"]
            state["last_error"] = frame["error_text"]

            if cfg["mode"] == "cycles" and state["current_cycle"] >= cfg["max_cycles"]:
                state["stop_reason"] = f"Raggiunto limite cicli: {cfg['max_cycles']}"
                break

            elapsed_sec = time.time() - start_ts
            if cfg["mode"] in {"timer", "timer_or_threshold"} and elapsed_sec >= cfg["duration_sec"]:
                state["stop_reason"] = "Timer completato"
                break

            if cfg["mode"] in {"threshold", "timer_or_threshold"}:
                for idx, value in enumerate(frame["values"]):
                    threshold = cfg["sensor_thresholds"][idx]
                    if threshold is None or value is None:
                        continue
                    if float(value) >= float(threshold):
                        state["stop_reason"] = (
                            f"Soglia raggiunta su s{idx + 1}: {value} >= {threshold}"
                        )
                        state["threshold_event"] = True
                        stop_event.set()
                        break
                if stop_event.is_set() and state["stop_reason"]:
                    break

            if stop_event.wait(timeout=float(cfg["poll_interval_sec"])):
                state["stop_reason"] = "Arresto manuale richiesto"
                break
    except Exception as exc:
        state["stop_reason"] = f"Errore batch: {exc}"
    finally:
        state["running"] = False
        state["end_utc"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        try:
            save_batch_run(
                cfg["db_path"],
                {
                    "activity_code": cfg.get("activity_code"),
                    "mode": cfg.get("mode"),
                    "start_utc": state.get("start_utc"),
                    "end_utc": state.get("end_utc"),
                    "duration_sec": int(time.time() - start_ts),
                    "cycles_executed": state.get("current_cycle") or 0,
                    "completed_frames": state.get("completed_frames") or 0,
                    "stop_reason": state.get("stop_reason"),
                    "threshold_event": state.get("threshold_event") or False,
                    "controller_ip": cfg.get("controller_ip"),
                },
            )
        except Exception:
            pass


def _build_send_payload(payload_format: str, values: list[int], seq: int) -> str:
    if payload_format == "CSV (s1..s8)":
        return ",".join(str(v) for v in values)
    if payload_format == "JSON":
        return json.dumps(
            {
                "seq": seq,
                "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "s1": values[0],
                "s2": values[1],
                "s3": values[2],
                "s4": values[3],
                "s5": values[4],
                "s6": values[5],
                "s7": values[6],
                "s8": values[7],
            }
        )
    return "\n".join([*(str(v) for v in values), "255"])


def _send_udp_frame_packets(sock: socket.socket, broadcast_ip: str, port: int, values: list[int]) -> int:
    # Replica il protocollo letto lato client: ogni valore in un datagramma separato, poi 255.
    sent = 0
    for value in values:
        sock.sendto(str(value).encode("utf-8", errors="ignore"), (broadcast_ip, port))
        sent += 1
    sock.sendto(b"255", (broadcast_ip, port))
    return sent + 1


def run_send_simulator_worker(cfg: dict, stop_event: threading.Event, state: dict) -> None:
    state["running"] = True
    state["start_utc"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    state["end_utc"] = None
    state["sent_packets"] = 0
    state["last_payload"] = None
    state["last_send_utc"] = None
    state["last_error"] = None

    base_values = [int(v) for v in cfg["base_values"]]
    seq = 0

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

            while not stop_event.is_set():
                seq += 1
                if cfg["value_mode"] == "Random":
                    values = [
                        max(
                            cfg["min_temp"],
                            min(cfg["max_temp"], int(v + cfg["jitter_sign"] * ((seq % 3) - 1))),
                        )
                        for v in base_values
                    ]
                elif cfg["value_mode"] == "Ramp":
                    step = seq % max(1, cfg["ramp_steps"])
                    values = [
                        max(cfg["min_temp"], min(cfg["max_temp"], int(v + step * cfg["ramp_delta"])))
                        for v in base_values
                    ]
                else:
                    values = base_values

                payload = _build_send_payload(cfg["payload_format"], values, seq)
                if cfg["payload_format"] == "Protocollo frame (8 valori + 255)":
                    pkt_count = _send_udp_frame_packets(sock, cfg["broadcast_ip"], cfg["port"], values)
                    state["sent_packets"] = int(state.get("sent_packets", 0)) + pkt_count
                else:
                    sock.sendto(payload.encode("utf-8", errors="ignore"), (cfg["broadcast_ip"], cfg["port"]))
                    state["sent_packets"] = int(state.get("sent_packets", 0)) + 1

                state["last_payload"] = payload
                state["last_send_utc"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"

                if stop_event.wait(timeout=float(cfg["interval_sec"])):
                    break
    except Exception as exc:
        state["last_error"] = str(exc)
    finally:
        state["running"] = False
        state["end_utc"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"

use_mock = False
auto_refresh_batch = True
broadcast_ip = "192.168.1.255"
port = 3274
timeout_sec = 2.0
poll_interval_sec = 2.0
db_path = "app_discovery/data/sensor_data.db"
mock_seed = None
mock_timeout_rate = 0.08
mock_ser_rate = 0.05
mock_scenario = "normal"

with st.sidebar:
    if st.button("Logout", use_container_width=True, key="logout_btn"):
        _clear_cached_auth_authenticated(_auth_fingerprint())
        st.session_state.app_authenticated = False
        st.rerun()

    show_advanced_sections = st.toggle(
        "Advance",
        value=st.session_state.get("show_advanced_sections", False),
        key="show_advanced_sections",
    )

    if show_advanced_sections:
        st.header("Collector UDP")
        use_mock = st.toggle("Modalita mock (simulazione)", value=False)
        auto_refresh_batch = st.toggle("Auto-refresh stato batch", value=True)
        broadcast_ip = st.text_input("Broadcast IP", value="192.168.1.255")
        port = st.number_input("Porta UDP", min_value=1, max_value=65535, value=3274)
        timeout_sec = st.number_input("Timeout (s)", min_value=0.2, max_value=10.0, value=2.0, step=0.2)
        poll_interval_sec = st.number_input("Intervallo polling (s)", min_value=0.2, max_value=30.0, value=2.0, step=0.2)
        db_path = st.text_input("SQLite DB path", value="app_discovery/data/sensor_data.db")
    else:
        st.caption("Attiva 'Advance' per mostrare le sezioni Collector UDP e Send data.")

    init_db(db_path)

    st.markdown("---")
    st.subheader("Settings")
    sidebar_drive_folder = st.text_input(
        "Google Drive folder path",
        value=st.session_state.get("drive_folder_path", ""),
        help="Percorso locale sincronizzato da Google Drive da usare per Import from Drive.",
    )
    sidebar_drive_url = st.text_input(
        "Google Drive folder URL",
        value=st.session_state.get("sidebar_drive_url_input", ""),
        key="sidebar_drive_url_input",
        help="Link della cartella condivisa Google Drive da usare per Import from Drive.",
    )
    st.session_state.drive_folder_path = sidebar_drive_folder.strip()
    st.session_state.drive_folder_url = sidebar_drive_url.strip()

    st.markdown("##### Regola conformita report")
    st.number_input(
        "Soglia letale (C)",
        min_value=0.0,
        max_value=200.0,
        step=0.5,
        key="report_threshold_rule_c",
    )
    st.number_input(
        "Minuti minimi sopra soglia",
        min_value=0.0,
        max_value=600.0,
        step=1.0,
        key="report_required_min_above",
    )

    if st.button("Salva settings", use_container_width=True, key="save_settings_btn"):
        try:
            st.session_state.app_settings = {
                **st.session_state.get("app_settings", {}),
                "drive_folder_path": st.session_state.drive_folder_path,
                "drive_folder_url": st.session_state.drive_folder_url,
                "report_source_mode": st.session_state.get("report_source_mode_default", "Import from Drive"),
                "drive_import_mode": st.session_state.get("drive_import_mode_default", "URL Google Drive"),
                "report_threshold_rule_c": float(st.session_state.get("report_threshold_rule_c", 55.0)),
                "report_required_min_above": float(st.session_state.get("report_required_min_above", 30.0)),
            }
            save_app_settings(st.session_state.app_settings)
            st.session_state.drive_url_input = st.session_state.drive_folder_url
            st.success("Settings salvati.")
        except Exception as exc:
            st.error(f"Errore salvataggio settings: {exc}")

    if show_advanced_sections and use_mock:
        st.caption("Parametri simulazione")
        mock_scenario = st.selectbox(
            "Scenario mock",
            options=["normal", "warmup", "drift", "spike", "burst_loss"],
            index=0,
        )
        mock_seed_raw = st.text_input("Seed random (opzionale)", value="")
        mock_timeout_rate = st.slider("Probabilita timeout", min_value=0.0, max_value=0.9, value=0.08, step=0.01)
        mock_ser_rate = st.slider("Probabilita errore ser", min_value=0.0, max_value=0.9, value=0.05, step=0.01)
        if mock_seed_raw.strip():
            try:
                mock_seed = int(mock_seed_raw.strip())
            except ValueError:
                st.warning("Seed non valido: uso casualita non deterministica")

if use_mock:
    client = MockUdpControllerClient(
        timeout_sec=float(timeout_sec),
        seed=mock_seed,
        timeout_rate=float(mock_timeout_rate),
        ser_rate=float(mock_ser_rate),
        scenario=mock_scenario,
    )
else:
    client = UdpControllerClient(port=int(port), timeout_sec=float(timeout_sec))

batch_thread = st.session_state.get("batch_thread")
batch_running = bool(st.session_state.batch_state.get("running"))
if batch_running and batch_thread is not None and not batch_thread.is_alive():
    st.session_state.batch_state["running"] = False
    batch_running = False

if batch_running and auto_refresh_batch:
    if AUTOREFRESH_AVAILABLE:
        st_autorefresh(interval=1000, key="batch_live_refresh")
    elif not st.session_state.get("autorefresh_missing_warned", False):
        st.warning("Auto-refresh non disponibile: installa/aggiorna streamlit-autorefresh.")
        st.session_state.autorefresh_missing_warned = True

if show_advanced_sections:
    tab_report, tab_collector, tab_send = st.tabs(["Report Sanificazione", "Collector UDP", "Send data"])
else:
    tab_report = st.tabs(["Report Sanificazione"])[0]

if show_advanced_sections:
    with tab_collector:
        pass
        if use_mock:
            st.info(f"Modalita mock attiva: scenario '{mock_scenario}'.")
    
        col1, col2, col3 = st.columns(3)
    
        with col1:
            st.markdown('<div class="card">', unsafe_allow_html=True)
            if st.button("1) Discovery controller", use_container_width=True):
                found_ip = client.discover_controller(broadcast_ip=broadcast_ip)
                if found_ip:
                    st.session_state.controller_ip = found_ip
                    st.success(f"Controller trovato: {found_ip}")
                else:
                    st.warning("Nessun controller trovato. Verifica LAN, firewall e broadcast IP.")
            st.markdown("</div>", unsafe_allow_html=True)
    
        with col2:
            st.markdown('<div class="card">', unsafe_allow_html=True)
            controller_ip_input = st.text_input(
                "Controller IP", value=st.session_state.controller_ip, key="controller_ip_input"
            )
            st.session_state.controller_ip = controller_ip_input.strip()
            st.markdown("</div>", unsafe_allow_html=True)
    
        with col3:
            st.markdown('<div class="card">', unsafe_allow_html=True)
            if st.button("2) Leggi versione (ver)", use_container_width=True):
                if not st.session_state.controller_ip:
                    st.error("Inserisci prima il Controller IP")
                else:
                    version = client.request_version(st.session_state.controller_ip)
                    st.session_state.version_info = {"hw": version.get("hw"), "fw": version.get("fw")}
                    st.info(f"Versione: HW={version.get('hw')} FW={version.get('fw')} Raw={version.get('raw')}")
            st.markdown("</div>", unsafe_allow_html=True)
    
        st.divider()
        left, right = st.columns([1, 1])
    
        with left:
            st.subheader("Polling")
            activity_code_input = st.text_input(
                "Codice attivita per salvataggio",
                value=st.session_state.activity_code,
                help="Usa sempre lo stesso codice per associare i frame a uno specifico intervento.",
            )
            st.session_state.activity_code = activity_code_input.strip()
    
            if st.button("3) Poll once (sens)", use_container_width=True):
                if not st.session_state.controller_ip:
                    st.error("Controller IP mancante")
                elif not st.session_state.activity_code:
                    st.error("Inserisci un codice attivita prima di salvare misurazioni")
                else:
                    result = client.request_sensors(st.session_state.controller_ip)
                    frame = {
                        "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                        "activity_code": st.session_state.activity_code,
                        "controller_ip": st.session_state.controller_ip,
                        "hw_version": st.session_state.version_info.get("hw"),
                        "fw_version": st.session_state.version_info.get("fw"),
                        "values": result["values"],
                        "frame_complete": result["frame_complete"],
                        "latency_ms": result["latency_ms"],
                        "error_text": result["error_text"],
                        "raw_messages": result["raw_messages"],
                    }
                    save_frame(db_path, frame)
                    st.success(f"Frame salvato. Values={frame['values']} complete={frame['frame_complete']}")
    
            st.markdown("#### Batch raccolta")
            batch_mode = st.selectbox(
                "Modalita arresto batch",
                options=["Numero cicli", "Timer (hh:mm)", "Soglie sensori", "Timer o soglie (prima condizione)"],
                index=1,
            )
    
            max_cycles = 30
            duration_hours = 0
            duration_minutes = 30
            if batch_mode == "Numero cicli":
                max_cycles = int(st.number_input("Numero cicli batch", min_value=1, max_value=100000, value=30))
            if batch_mode in {"Timer (hh:mm)", "Timer o soglie (prima condizione)"}:
                h_col, m_col = st.columns(2)
                with h_col:
                    duration_hours = int(st.number_input("Ore", min_value=0, max_value=48, value=0))
                with m_col:
                    duration_minutes = int(st.number_input("Minuti", min_value=0, max_value=59, value=30))
    
            sensor_thresholds = [None] * 8
            if batch_mode in {"Soglie sensori", "Timer o soglie (prima condizione)"}:
                st.caption("Interrompi il batch se una qualsiasi sonda supera la soglia impostata.")
                tcols1 = st.columns(4)
                tcols2 = st.columns(4)
                labels = ["s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"]
                for i in range(4):
                    with tcols1[i]:
                        sensor_thresholds[i] = float(st.number_input(f"Soglia {labels[i]} (C)", value=55.0, step=0.5, key=f"thr_{labels[i]}"))
                for i in range(4, 8):
                    with tcols2[i - 4]:
                        sensor_thresholds[i] = float(st.number_input(f"Soglia {labels[i]} (C)", value=55.0, step=0.5, key=f"thr_{labels[i]}"))
    
            mode_map = {
                "Numero cicli": "cycles",
                "Timer (hh:mm)": "timer",
                "Soglie sensori": "threshold",
                "Timer o soglie (prima condizione)": "timer_or_threshold",
            }
    
            mode_key = mode_map[batch_mode]
            duration_sec = (duration_hours * 3600) + (duration_minutes * 60)
    
            start_col, stop_col = st.columns(2)
            is_running = batch_running
            with start_col:
                start_batch = st.button("4) Avvia batch", use_container_width=True, disabled=is_running)
            with stop_col:
                stop_batch = st.button(
                    "STOP batch (manuale)",
                    use_container_width=True,
                    type="primary" if is_running else "secondary",
                    disabled=not is_running,
                )
    
            if stop_batch and st.session_state.batch_state.get("running") and st.session_state.batch_stop_event is not None:
                st.session_state.batch_stop_event.set()
                st.warning("Richiesta di arresto batch inviata.")
    
            if start_batch:
                if st.session_state.batch_state.get("running"):
                    st.warning("Batch gia in esecuzione.")
                elif not st.session_state.controller_ip:
                    st.error("Controller IP mancante")
                elif not st.session_state.activity_code:
                    st.error("Inserisci un codice attivita prima di avviare il batch")
                elif mode_key in {"timer", "timer_or_threshold"} and duration_sec <= 0:
                    st.error("Imposta una durata valida maggiore di 0 minuti")
                else:
                    cfg = {
                        "db_path": db_path,
                        "activity_code": st.session_state.activity_code,
                        "controller_ip": st.session_state.controller_ip,
                        "hw_version": st.session_state.version_info.get("hw"),
                        "fw_version": st.session_state.version_info.get("fw"),
                        "poll_interval_sec": float(poll_interval_sec),
                        "mode": mode_key,
                        "max_cycles": max_cycles,
                        "duration_sec": duration_sec,
                        "sensor_thresholds": sensor_thresholds,
                        "use_mock": use_mock,
                        "port": int(port),
                        "timeout_sec": float(timeout_sec),
                        "mock_seed": mock_seed,
                        "mock_timeout_rate": float(mock_timeout_rate),
                        "mock_ser_rate": float(mock_ser_rate),
                        "mock_scenario": mock_scenario,
                    }
                    stop_event = threading.Event()
                    worker = threading.Thread(
                        target=run_batch_worker,
                        args=(cfg, stop_event, st.session_state.batch_state),
                        daemon=True,
                    )
                    st.session_state.batch_stop_event = stop_event
                    st.session_state.batch_thread = worker
                    worker.start()
                    st.success("Batch avviato in background.")
    
            batch_state = st.session_state.batch_state
            st.markdown("##### Stato batch")
            sb1, sb2, sb3 = st.columns(3)
            sb1.metric("Running", "SI" if batch_state.get("running") else "NO")
            sb2.metric("Cicli eseguiti", str(batch_state.get("current_cycle") or 0))
            sb3.metric("Frame completi", str(batch_state.get("completed_frames") or 0))
    
            elapsed = 0.0
            if batch_state.get("start_monotonic") is not None:
                elapsed = max(0.0, time.time() - float(batch_state["start_monotonic"]))
    
            timer_cols = st.columns(2)
            with timer_cols[0]:
                st.markdown(
                    f'<div class="timer-card"><div class="clock-label">Tempo trascorso</div><div class="clock-value">{format_hhmmss(elapsed)}</div></div>',
                    unsafe_allow_html=True,
                )
    
            with timer_cols[1]:
                if batch_state.get("mode") in {"timer", "timer_or_threshold"} and (batch_state.get("duration_sec") or 0) > 0:
                    remaining = max(0.0, float(batch_state["duration_sec"]) - elapsed)
                    st.markdown(
                        f'<div class="timer-card"><div class="clock-label">Countdown timer</div><div class="clock-value">{format_hhmmss(remaining)}</div></div>',
                        unsafe_allow_html=True,
                    )
                    progress = min(1.0, elapsed / float(batch_state["duration_sec"]))
                    st.progress(progress, text=f"Timer batch: {format_hhmmss(elapsed)} / {format_hhmmss(batch_state['duration_sec'])}")
                else:
                    st.markdown(
                        '<div class="timer-card"><div class="clock-label">Countdown timer</div><div class="clock-value">--:--:--</div></div>',
                        unsafe_allow_html=True,
                    )
    
            st.caption(
                f"Attivita: {batch_state.get('activity_code') or '-'} | Start: {batch_state.get('start_utc') or '-'} | Stop reason: {batch_state.get('stop_reason') or '-'}"
            )
    
            if batch_state.get("threshold_event"):
                st.error(f"Arresto per soglia: {batch_state.get('stop_reason')}")
    
            if batch_state.get("last_values") is not None:
                st.caption(
                    f"Ultimo frame: {batch_state.get('last_values')} | Latency: {batch_state.get('last_latency_ms')} ms | Error: {batch_state.get('last_error') or '-'}"
                )
    
            st.markdown("##### Storico esecuzioni batch")
            run_rows = recent_batch_runs(db_path, limit=30)
            if run_rows:
                runs_df = pd.DataFrame([dict(r) for r in run_rows])
                runs_view = runs_df[
                    [
                        "id",
                        "activity_code",
                        "mode",
                        "start_utc",
                        "end_utc",
                        "cycles_executed",
                        "completed_frames",
                        "stop_reason",
                    ]
                ].copy()
                selected_event = st.dataframe(
                    runs_view,
                    use_container_width=True,
                    hide_index=True,
                    on_select="rerun",
                    selection_mode="single-row",
                    key="batch_runs_select_table",
                )
                selected_rows = selected_event.get("selection", {}).get("rows", []) if isinstance(selected_event, dict) else []
                if selected_rows:
                    selected_idx = int(selected_rows[0])
                    st.session_state["selected_batch_run_id"] = int(runs_view.iloc[selected_idx]["id"])
                elif "selected_batch_run_id" not in st.session_state:
                    st.session_state["selected_batch_run_id"] = int(runs_view.iloc[0]["id"])
            else:
                st.info("Nessuna esecuzione batch storicizzata.")
    
        with right:
            st.subheader("Ultimi dati")
            rows = []
            selected_run_id = st.session_state.get("selected_batch_run_id")
            selected_run = None
            run_rows_for_right = recent_batch_runs(db_path, limit=200)
            if selected_run_id is not None and run_rows_for_right:
                runs_df_right = pd.DataFrame([dict(r) for r in run_rows_for_right])
                selected_match = runs_df_right[runs_df_right["id"] == selected_run_id]
                if not selected_match.empty:
                    selected_run = selected_match.iloc[0]
                    sel_activity = selected_run.get("activity_code")
                    sel_start = parse_utc(selected_run.get("start_utc"))
                    sel_end = parse_utc(selected_run.get("end_utc"))
                    activity_rows = frames_for_activity(db_path, sel_activity, limit=6000) if sel_activity else []
                    if activity_rows:
                        tmp_df = pd.DataFrame([dict(r) for r in activity_rows])
                        tmp_df["timestamp"] = pd.to_datetime(tmp_df["ts"], errors="coerce", utc=True)
                        tmp_df = tmp_df.dropna(subset=["timestamp"])
                        if sel_start is not None:
                            tmp_df = tmp_df[tmp_df["timestamp"] >= sel_start]
                        if sel_end is not None:
                            tmp_df = tmp_df[tmp_df["timestamp"] <= sel_end]
                        rows = [r for r in tmp_df.to_dict("records")]
    
            if not rows:
                rows = [dict(r) for r in recent_frames(db_path, limit=300)]
    
            if selected_run is not None:
                st.caption(
                    f"Vista filtrata su run #{int(selected_run['id'])} | attivita={selected_run.get('activity_code') or '-'}"
                )
    
            if not rows:
                st.info("Nessun dato salvato ancora")
            else:
                df = pd.DataFrame(rows).sort_values("id")
                st.dataframe(df.tail(20), use_container_width=True)
    
                st.markdown("#### Andamento multi-sensore")
                sensor_cols = ["s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"]
                sensor_labels = {
                    "s1": "Pannello 1",
                    "s2": "Pannello 2",
                    "s3": "Pannello 3",
                    "s4": "Pannello 4",
                    "s5": "Pannello 5",
                    "s6": "Pannello 6",
                    "s7": "Sonda Ispezione 1",
                    "s8": "Sonda Ispezione 2",
                }
    
                plot_df = df[["ts", *sensor_cols]].copy()
                plot_df["timestamp"] = pd.to_datetime(plot_df["ts"], errors="coerce", utc=True)
                plot_df = plot_df.dropna(subset=["timestamp"])
                for col in sensor_cols:
                    plot_df[col] = pd.to_numeric(plot_df[col], errors="coerce")
    
                long_df = plot_df.melt(
                    id_vars=["timestamp"],
                    value_vars=sensor_cols,
                    var_name="sensor",
                    value_name="temperatura_c",
                ).dropna(subset=["temperatura_c"])
    
                if not long_df.empty:
                    long_df["sensor"] = long_df["sensor"].map(sensor_labels)
                    color_domain = [
                        "Pannello 1",
                        "Pannello 2",
                        "Pannello 3",
                        "Pannello 4",
                        "Pannello 5",
                        "Pannello 6",
                        "Sonda Ispezione 1",
                        "Sonda Ispezione 2",
                    ]
                    color_range = [
                        "#1d4ed8",
                        "#0284c7",
                        "#0891b2",
                        "#16a34a",
                        "#65a30d",
                        "#ca8a04",
                        "#ea580c",
                        "#be123c",
                    ]
                    color_map = dict(zip(color_domain, color_range))
    
                    filter_mode = st.radio(
                        "Filtro sensori",
                        options=["Tutti", "Solo pannelli", "Solo sonde", "Selezione manuale"],
                        horizontal=True,
                    )
    
                    default_sensor_view = color_domain
                    if filter_mode == "Solo pannelli":
                        default_sensor_view = color_domain[:6]
                    elif filter_mode == "Solo sonde":
                        default_sensor_view = color_domain[6:]
                    elif filter_mode == "Selezione manuale":
                        selected_manual = st.multiselect(
                            "Scegli sensori da mostrare",
                            options=color_domain,
                            default=color_domain,
                        )
                        default_sensor_view = selected_manual
    
                    filtered_df = long_df[long_df["sensor"].isin(default_sensor_view)].copy()
                    if filtered_df.empty:
                        st.warning("Nessun sensore selezionato.")
                    else:
                        selected_colors = [color_map[s] for s in default_sensor_view]
                        multi_chart = (
                            alt.Chart(filtered_df)
                            .mark_line(strokeWidth=2)
                            .encode(
                                x=alt.X(
                                    "timestamp:T",
                                    title="Tempo",
                                    axis=alt.Axis(
                                        format="%H:%M",
                                        values=time_ticks_10min(filtered_df, "timestamp") or alt.Undefined,
                                    ),
                                ),
                                y=alt.Y("temperatura_c:Q", title="Temperatura (C)"),
                                color=alt.Color(
                                    "sensor:N",
                                    title="Sensori",
                                    scale=alt.Scale(domain=default_sensor_view, range=selected_colors),
                                ),
                                tooltip=[
                                    alt.Tooltip("timestamp:T", title="Ora", format="%H:%M:%S"),
                                    alt.Tooltip("sensor:N", title="Sensore"),
                                    alt.Tooltip("temperatura_c:Q", title="Temperatura (C)", format=".2f"),
                                ],
                            )
                            .properties(height=340)
                            .interactive()
                        )
                        st.altair_chart(multi_chart, use_container_width=True)
                else:
                    st.info("Dati insufficienti per il grafico multi-sensore.")
    
        if batch_running and batch_state.get("activity_code"):
            st.markdown("#### Andamento live batch")
            live_rows = frames_for_activity(db_path, batch_state.get("activity_code"), limit=1200)
            live_df = pd.DataFrame([dict(r) for r in live_rows]) if live_rows else pd.DataFrame()
            if not live_df.empty:
                live_df["timestamp"] = pd.to_datetime(live_df["ts"], errors="coerce", utc=True)
                live_df = live_df.dropna(subset=["timestamp"]).sort_values("timestamp")
                sensor_cols = ["s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"]
                for col in sensor_cols:
                    live_df[col] = pd.to_numeric(live_df[col], errors="coerce")
                sensor_labels = {
                    "s1": "Pannello 1",
                    "s2": "Pannello 2",
                    "s3": "Pannello 3",
                    "s4": "Pannello 4",
                    "s5": "Pannello 5",
                    "s6": "Pannello 6",
                    "s7": "Sonda Ispezione 1",
                    "s8": "Sonda Ispezione 2",
                }
                live_long = live_df.melt(
                    id_vars=["timestamp"],
                    value_vars=sensor_cols,
                    var_name="sensor",
                    value_name="temperatura_c",
                ).dropna(subset=["temperatura_c"])
                live_long["sensor"] = live_long["sensor"].map(sensor_labels)
                if not live_long.empty:
                    live_ticks = time_ticks_10min(live_long, "timestamp")
                    live_chart = (
                        alt.Chart(live_long)
                        .mark_line(strokeWidth=2)
                        .encode(
                            x=alt.X(
                                "timestamp:T",
                                title="Tempo",
                                axis=alt.Axis(format="%H:%M", values=live_ticks if live_ticks else alt.Undefined),
                            ),
                            y=alt.Y("temperatura_c:Q", title="Temperatura (C)"),
                            color=alt.Color("sensor:N", title="Sensori"),
                        )
                        .properties(height=260)
                        .interactive()
                    )
                    st.altair_chart(live_chart, use_container_width=True)
                    st.dataframe(
                        live_df[["timestamp", *sensor_cols]].sort_values("timestamp").tail(60),
                        use_container_width=True,
                        height=360,
                    )
    
        st.markdown("#### Temperature istantanee (6 pannelli + 2 sonde)")
        if not batch_running:
            render_instant_temp_circles_idle()
        else:
            instant_values = batch_state.get("last_values")
            if instant_values is None:
                latest = recent_frames(db_path, limit=1)
                if latest:
                    row = dict(latest[0])
                    instant_values = [row.get(f"s{i}") for i in range(1, 9)]
            render_instant_temp_circles(instant_values)
    
        st.caption("Nota: se l'app Android originale e questo collector interrogano insieme lo stesso controller, possono esserci interferenze.")
    
with tab_report:
    title_col, toggle_col = st.columns([3, 1])
    with title_col:
        st.subheader("Report Attivita di Sanificazione")
    with toggle_col:
        st.toggle(
            "Importa dati da raccolta",
            value=st.session_state.get("import_data_from_collection", False),
            key="import_data_from_collection",
        )

    is_import_mode = st.session_state.get("import_data_from_collection", False)
    if is_import_mode:
        _apply_pending_report_header_if_any()

    a1, a2 = st.columns([1, 1])
    with a1:
        st.text_input("Cliente", key="report_cliente", disabled=is_import_mode)
        st.text_input("Indirizzo", key="report_indirizzo", disabled=is_import_mode)
        st.date_input("Data intervento", key="report_data_intervento", disabled=is_import_mode)
        st.selectbox(
            "Luogo Intervento",
            options=["Sede Saniservice", "Sede Cliente"],
            key="report_luogo_intervento",
            disabled=is_import_mode,
        )
        st.text_input("Tecnico", key="report_tecnico", disabled=is_import_mode)
    with a2:
        st.text_input("Codice intervento", key="report_codice_intervento", disabled=is_import_mode)
        st.text_input("Oggetto trattato", key="report_oggetto_trattato", disabled=is_import_mode)
        st.text_area("Note", height=180, key="report_note", disabled=is_import_mode)

    cliente = st.session_state.get("report_cliente", "")
    indirizzo = st.session_state.get("report_indirizzo", "")
    data_intervento = st.session_state.get("report_data_intervento", date.today())
    luogo_intervento = st.session_state.get("report_luogo_intervento", "Sede Saniservice")
    tecnico = st.session_state.get("report_tecnico", "")
    codice_intervento = st.session_state.get("report_codice_intervento", "")
    oggetto_trattato = st.session_state.get("report_oggetto_trattato", "")
    note = st.session_state.get("report_note", "")

    st.markdown("<hr style='border:0; border-top:1px solid #d1d5db; margin: 10px 0 12px 0;'>", unsafe_allow_html=True)
    st.markdown("#### Dati termici")
    source_options = ["CSV", "Manuale", "Storico collector", "Import from Drive"]
    source_default = st.session_state.get("report_source_mode_default", "Import from Drive")
    if source_default not in source_options:
        source_default = "Import from Drive"
    source_mode = st.radio(
        "Sorgente dati",
        options=source_options,
        index=source_options.index(source_default),
        horizontal=True,
    )
    st.session_state.report_source_mode_default = source_mode

    temp_df = pd.DataFrame(columns=["tempo_min", "temperatura_c"])
    sensor_df_for_pdf = pd.DataFrame(columns=["tempo_min", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"])
    report_metric_mode = ""
    measurement_start = "-"
    measurement_end = "-"
    intervention_duration = "-"
    if source_mode == "CSV":
        uploaded = st.file_uploader("Carica CSV (tempo_min,temperatura_c)", type=["csv"])
        if uploaded is not None:
            try:
                csv_df = pd.read_csv(uploaded)
                temp_df = normalize_temperature_df(csv_df)
            except Exception as exc:
                st.error(f"Errore nel CSV: {exc}")
    else:
        if source_mode == "Manuale":
            manual_default = "tempo_min,temperatura_c\n0,21\n10,36\n20,50\n30,58\n45,61\n60,62\n75,60"
            manual_text = st.text_area(
                "Inserimento manuale (una riga: tempo_min,temperatura_c)", value=manual_default, height=200
            )
            try:
                temp_df = normalize_temperature_df(parse_manual_data(manual_text))
            except Exception as exc:
                st.error(f"Errore nei dati manuali: {exc}")
        elif source_mode == "Storico collector":
            activity_codes = list_activity_codes(db_path)
            if not activity_codes:
                st.warning("Nessuna attivita trovata nello storico collector.")
            else:
                selected_activity = st.selectbox("Codice attivita", options=activity_codes)
                metric_mode = st.selectbox(
                    "Metrica temperatura dal collector",
                    options=["media_sensori", "max_sensori", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"],
                    format_func=lambda x: {
                        "media_sensori": "Media sensori (s1..s8)",
                        "max_sensori": "Massimo sensori (s1..s8)",
                    }.get(x, x.upper()),
                )
                report_metric_mode = metric_mode
                rows = frames_for_activity(db_path, selected_activity)
                temp_df = activity_frames_to_temp_df(rows, metric_mode)
                sensor_df_for_pdf = activity_frames_to_sensor_df(rows)
                window = measurement_window_from_timestamps([str(dict(r).get("ts", "")) for r in rows])
                if window is not None:
                    measurement_start, measurement_end, intervention_duration = window
                if temp_df.empty:
                    st.warning("Nessun frame utile per questa attivita.")
                else:
                    if not codice_intervento:
                        codice_intervento = selected_activity
                    st.caption(f"Caricati {len(temp_df)} punti da attivita '{selected_activity}'.")
        else:
            drive_import_options = ["Cartella locale", "URL Google Drive"]
            drive_import_default = st.session_state.get("drive_import_mode_default", "URL Google Drive")
            if drive_import_default not in drive_import_options:
                drive_import_default = "URL Google Drive"
            drive_import_mode = st.radio(
                "Origine Import from Drive",
                options=drive_import_options,
                index=drive_import_options.index(drive_import_default),
                horizontal=True,
            )
            st.session_state.drive_import_mode_default = drive_import_mode

            json_files: list[Path] = []
            if drive_import_mode == "Cartella locale":
                drive_folder = st.text_input(
                    "Cartella Google Drive (locale)",
                    value="",
                    key="drive_local_folder_input",
                    help="Inserisci il percorso locale sincronizzato da Google Drive (es. C:/Users/<utente>/My Drive/Export).",
                ).strip()
                if drive_folder:
                    st.session_state.drive_folder_path = drive_folder

                if not drive_folder:
                    st.info("Specifica la cartella Google Drive locale per cercare i file JSON.")
                elif drive_folder.lower().startswith("http://") or drive_folder.lower().startswith("https://"):
                    st.error("Hai inserito un URL web: seleziona 'URL Google Drive' come origine import.")
                else:
                    folder_path = Path(drive_folder)
                    if not folder_path.exists():
                        st.error(f"Percorso inesistente: {folder_path}")
                    elif not folder_path.is_dir():
                        st.error(f"Il percorso non e una cartella: {folder_path}")
                    else:
                        json_files = sorted(folder_path.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
                        if not json_files:
                            st.warning("Nessun file JSON trovato nella cartella indicata.")
            else:
                if not st.session_state.get("drive_url_input") and st.session_state.get("drive_folder_url"):
                    st.session_state.drive_url_input = st.session_state.drive_folder_url
                drive_url = st.text_input(
                    "URL cartella Google Drive",
                    key="drive_url_input",
                    help="Incolla il link cartella condivisa (es. https://drive.google.com/drive/folders/...).",
                ).strip()
                if drive_url.startswith("https:\\") or drive_url.startswith("http:\\"):
                    drive_url = drive_url.replace("\\", "/")
                    if drive_url.startswith("https:/") and not drive_url.startswith("https://"):
                        drive_url = drive_url.replace("https:/", "https://", 1)
                    if drive_url.startswith("http:/") and not drive_url.startswith("http://"):
                        drive_url = drive_url.replace("http:/", "http://", 1)
                    st.session_state.drive_url_input = drive_url
                if drive_url:
                    st.session_state.drive_folder_url = drive_url

                download_now = st.button("Scarica/aggiorna da URL", key="download_drive_url_btn")
                if not drive_url:
                    st.info("Inserisci l'URL cartella Google Drive per scaricare i JSON.")
                elif download_now or st.session_state.get("last_drive_url_loaded") != drive_url:
                    with st.spinner("Scarico file JSON da Google Drive..."):
                        downloaded_files, err = _download_json_files_from_drive_url(drive_url, DRIVE_CACHE_DIR)
                    if err:
                        st.error(err)
                    else:
                        st.session_state.last_drive_url_loaded = drive_url
                        st.session_state.last_drive_json_files = [str(p) for p in downloaded_files]
                        st.success(f"Scaricati {len(downloaded_files)} file JSON da Drive.")

                cached_json = [Path(p) for p in st.session_state.get("last_drive_json_files", [])]
                json_files = [p for p in cached_json if p.exists()]
                if drive_url and not json_files:
                    st.warning("Nessun JSON disponibile in cache. Premi 'Scarica/aggiorna da URL'.")

            if json_files:
                st.markdown("<div style='height: 12px;'></div>", unsafe_allow_html=True)
                st.markdown("<hr style='border:0; border-top:1px solid #d1d5db; margin: 6px 0 12px 0;'>", unsafe_allow_html=True)
                fh1, fh2 = st.columns([8, 2])
                fh1.markdown("##### File disponibili")
                available_paths = {str(p) for p in json_files}
                if st.session_state.selected_drive_json_path not in available_paths:
                    st.session_state.selected_drive_json_path = ""

                file_records = []
                for file_path in json_files:
                    try:
                        stat = file_path.stat()
                        created_ts = float(stat.st_ctime)
                        created = datetime.fromtimestamp(created_ts).strftime("%Y-%m-%d %H:%M:%S")
                        size_bytes = int(stat.st_size)
                    except Exception:
                        created_ts = 0.0
                        created = "-"
                        size_bytes = 0
                    file_records.append(
                        {
                            "path": str(file_path),
                            "name": file_path.name,
                            "created": created,
                            "created_ts": created_ts,
                            "size_bytes": size_bytes,
                            "size_txt": f"{(size_bytes / 1024.0):.1f} KB",
                        }
                    )

                with fh2:
                    search_query = st.text_input(
                        "Cerca per nome file",
                        value=st.session_state.get("drive_file_search_query", ""),
                        key="drive_file_search_query",
                        label_visibility="collapsed",
                        placeholder="Cerca per nome file",
                    ).strip().lower()
                if search_query:
                    file_records = [r for r in file_records if search_query in r["name"].lower()]

                if not file_records:
                    st.warning("Nessun file corrisponde al filtro impostato.")
                    selected_json = None
                else:
                    sort_col = st.session_state.get("drive_file_sort_col", "created_ts")
                    sort_dir = st.session_state.get("drive_file_sort_dir", "desc")
                    if is_mobile_client and sort_col == "size_bytes":
                        sort_col = "created_ts"
                        st.session_state.drive_file_sort_col = "created_ts"

                    def _sort_indicator(col: str) -> str:
                        if sort_col != col:
                            return ""
                        return " v" if sort_dir == "desc" else " ^"

                    def _toggle_sort(col: str) -> None:
                        if st.session_state.drive_file_sort_col == col:
                            st.session_state.drive_file_sort_dir = (
                                "asc" if st.session_state.drive_file_sort_dir == "desc" else "desc"
                            )
                        else:
                            st.session_state.drive_file_sort_col = col
                            st.session_state.drive_file_sort_dir = "desc" if col == "created_ts" else "asc"
                        st.session_state.drive_file_page = 1

                    if is_mobile_client:
                        h0, h1, h2 = st.columns([0.8, 6.2, 3.0])
                        h0.markdown(" ")
                        h1.button(
                            f"nome file{_sort_indicator('name')}",
                            key="sort_drive_name",
                            use_container_width=True,
                            on_click=_toggle_sort,
                            args=("name",),
                        )
                        h2.button(
                            f"data creazione{_sort_indicator('created_ts')}",
                            key="sort_drive_created",
                            use_container_width=True,
                            on_click=_toggle_sort,
                            args=("created_ts",),
                        )
                    else:
                        h1, h2, h3, h4 = st.columns([5, 2, 1.2, 1.6])
                        h1.button(
                            f"nome file{_sort_indicator('name')}",
                            key="sort_drive_name",
                            use_container_width=True,
                            on_click=_toggle_sort,
                            args=("name",),
                        )
                        h2.button(
                            f"data creazione{_sort_indicator('created_ts')}",
                            key="sort_drive_created",
                            use_container_width=True,
                            on_click=_toggle_sort,
                            args=("created_ts",),
                        )
                        h3.button(
                            f"size{_sort_indicator('size_bytes')}",
                            key="sort_drive_size",
                            use_container_width=True,
                            on_click=_toggle_sort,
                            args=("size_bytes",),
                        )
                        h4.markdown(" ")

                    reverse = sort_dir == "desc"
                    if sort_col == "name":
                        file_records = sorted(file_records, key=lambda r: r["name"].lower(), reverse=reverse)
                    elif sort_col == "size_bytes":
                        file_records = sorted(file_records, key=lambda r: r["size_bytes"], reverse=reverse)
                    else:
                        file_records = sorted(file_records, key=lambda r: r["created_ts"], reverse=reverse)

                    page_size = 10
                    total_rows = len(file_records)
                    total_pages = (total_rows + page_size - 1) // page_size
                    st.session_state.drive_file_page = max(1, min(st.session_state.drive_file_page, total_pages))
                    start_idx = (st.session_state.drive_file_page - 1) * page_size
                    end_idx = start_idx + page_size
                    page_records = file_records[start_idx:end_idx]

                    for rec in page_records:
                        if is_mobile_client:
                            c0, c1, c2 = st.columns([0.8, 6.2, 3.0])
                            select_key = f"select_drive_json_{rec['path']}"
                            if select_key not in st.session_state:
                                st.session_state[select_key] = st.session_state.selected_drive_json_path == rec["path"]

                            checked = c0.checkbox(
                                "",
                                key=select_key,
                                help=f"Seleziona {rec['name']}",
                                label_visibility="collapsed",
                            )

                            if checked and st.session_state.selected_drive_json_path != rec["path"]:
                                st.session_state.selected_drive_json_path = rec["path"]
                                for other in page_records:
                                    other_key = f"select_drive_json_{other['path']}"
                                    st.session_state[other_key] = other["path"] == rec["path"]
                            elif (not checked) and st.session_state.selected_drive_json_path == rec["path"]:
                                st.session_state.selected_drive_json_path = ""

                            c1.write(rec["name"])
                            c2.write(rec["created"])
                        else:
                            c1, c2, c3, c4 = st.columns([5, 2, 1.2, 1.6])
                            c1.write(rec["name"])
                            c2.write(rec["created"])
                            c3.write(rec["size_txt"])
                            c4l, c4r = c4.columns([0.25, 0.75])
                            if c4r.button("Seleziona", key=f"select_drive_json_{rec['path']}", use_container_width=True):
                                st.session_state.selected_drive_json_path = rec["path"]

                    current_page = st.session_state.drive_file_page
                    st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)
                    p0, p1, p2, p3, p4 = st.columns([3, 1, 2, 1, 3])
                    if p1.button("<", key="drive_page_prev", disabled=current_page <= 1, type="tertiary"):
                        st.session_state.drive_file_page = max(1, current_page - 1)
                    p2.caption(f"Pagina {current_page}/{total_pages} - {total_rows} file")
                    if p3.button(">", key="drive_page_next", disabled=current_page >= total_pages, type="tertiary"):
                        st.session_state.drive_file_page = min(total_pages, current_page + 1)
                    st.markdown("<hr style='border:0; border-top:1px solid #d1d5db; margin: 6px 0 14px 0;'>", unsafe_allow_html=True)
                    st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)

                    if st.session_state.selected_drive_json_path:
                        selected_json = Path(st.session_state.selected_drive_json_path)
                        st.caption(f"File selezionato: {selected_json.name}")
                    else:
                        selected_json = None
                        st.info("Seleziona un file dalla tabella per procedere.")

                drive_metric_mode = st.selectbox(
                    "Metrica temperatura da JSON",
                    options=[
                        "media_sensori",
                        "max_sensori",
                        "p1",
                        "p2",
                        "p3",
                        "p4",
                        "p5",
                        "p6",
                        "s1",
                        "s2",
                        "sonde_s1_s2",
                    ],
                    format_func=lambda x: {
                        "media_sensori": "Media sensori (P1..P6 + S1..S2)",
                        "max_sensori": "Massimo sensori (P1..P6 + S1..S2)",
                        "sonde_s1_s2": "Sonde S1 e S2",
                    }.get(x, x.upper()),
                )
                report_metric_mode = drive_metric_mode

                if selected_json is not None:
                    try:
                        payload = json.loads(selected_json.read_text(encoding="utf-8"))
                    except Exception as exc:
                        st.error(f"Errore lettura JSON: {exc}")
                    else:
                        selected_json_path = str(selected_json)
                        if st.session_state.get("report_loaded_json_path") != selected_json_path:
                            if is_import_mode:
                                _apply_report_header_from_payload(payload, selected_json)
                            st.session_state.report_loaded_json_path = selected_json_path
                            st.rerun()

                        frames = payload.get("frames")
                        if not isinstance(frames, list) or not frames:
                            st.warning("Il file JSON non contiene frame validi in 'frames'.")
                        else:
                            temp_df = _drive_frames_to_temp_df(frames, drive_metric_mode)
                            sensor_df_for_pdf = _drive_frames_to_sensor_df(frames)
                            window = measurement_window_from_timestamps([str(f.get("ts", "")) for f in frames])
                            if window is not None:
                                measurement_start, measurement_end, intervention_duration = window

                            if temp_df.empty:
                                st.warning("Nessun dato utile estratto dai frame del JSON.")
                            else:
                                st.caption(f"Caricati {len(temp_df)} punti dal file '{selected_json.name}'.")

    if intervention_duration == "-" and not temp_df.empty and "tempo_min" in temp_df.columns:
        try:
            mins = pd.to_numeric(temp_df["tempo_min"], errors="coerce")
            mins = mins.dropna()
            if not mins.empty:
                duration_min = float(max(0.0, mins.max() - mins.min()))
                intervention_duration = format_hhmmss(duration_min * 60.0)
        except Exception:
            pass

    threshold_c = float(st.session_state.get("report_threshold_rule_c", 55.0))
    required_min_above = float(st.session_state.get("report_required_min_above", 30.0))
    st.caption(
        f"Regola conformita da sidebar: soglia {threshold_c:.1f} C | minuti minimi {required_min_above:.1f}"
    )

    if temp_df.empty:
        st.warning("Inserisci o carica dati validi per generare analisi e report.")
    else:
        stats = calculate_thermal_stats(temp_df, threshold_c=float(threshold_c), required_min_above=float(required_min_above))

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Soglia letale (C)", f"{threshold_c:.1f}")
        m2.metric("Minuti minimi sopra soglia", f"{required_min_above:.1f}")
        m3.metric("Soglia raggiunta", "SI" if stats["threshold_reached"] else "NO")
        m4.metric("Esito", "CONFORME" if stats["conforme"] else "NON CONFORME")

        if not pd.isna(stats["max_temp_c"]):
            st.caption(
                f"Temperatura massima rilevata: {float(stats['max_temp_c']):.2f} C | Minuti sopra soglia: {float(stats['minutes_above_threshold']):.2f}"
            )

        if stats["conforme"]:
            st.markdown('<div class="metric-ok">Esito automatico: CONFORME</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="metric-ko">Esito automatico: NON CONFORME</div>', unsafe_allow_html=True)

        st.markdown("#### Grafico Tempo / Temperatura")
        if report_metric_mode == "sonde_s1_s2" and not sensor_df_for_pdf.empty:
            sondes_chart_df = sensor_df_for_pdf[["tempo_min", "s7", "s8"]].copy()
            sondes_chart_df = sondes_chart_df.rename(columns={"s7": "S1", "s8": "S2"}).set_index("tempo_min")
            st.line_chart(sondes_chart_df[["S1", "S2"]])
            st.caption("Per i calcoli automatici viene usata la media tra S1 e S2.")
        else:
            chart_df = temp_df.copy().set_index("tempo_min")
            st.line_chart(chart_df[["temperatura_c"]])
        st.dataframe(temp_df, use_container_width=True)

        st.markdown("<div style='height: 12px;'></div>", unsafe_allow_html=True)
        st.markdown("<hr style='border:0; border-top:1px solid #d1d5db; margin: 6px 0 12px 0;'>", unsafe_allow_html=True)

        pdf_flag_col1, pdf_flag_col2, pdf_flag_col3, pdf_dl_col = st.columns([1.3, 1.5, 1.5, 2.0])
        with pdf_flag_col1:
            include_temp_chart = st.checkbox("Grafico Temperatura", value=True, key="pdf_include_temp_chart")
        with pdf_flag_col2:
            include_temp_table = st.checkbox("Tabella Temperatura", value=True, key="pdf_include_temp_table")
        with pdf_flag_col3:
            include_8_sensors_chart = st.checkbox("Grafico 8 Sensori", value=True, key="pdf_include_8_sensors_chart")

        intervention_payload = {
            "cliente": cliente,
            "indirizzo": indirizzo,
            "data_intervento": data_intervento.isoformat(),
            "luogo_intervento": luogo_intervento,
            "tecnico": tecnico,
            "codice_intervento": codice_intervento,
            "oggetto_trattato": oggetto_trattato,
            "note": note,
            "measurement_start": measurement_start,
            "measurement_end": measurement_end,
            "intervention_duration": intervention_duration,
        }

        pdf_bytes = create_sanification_pdf(
            intervention=intervention_payload,
            stats=stats,
            threshold_c=float(threshold_c),
            required_min_above=float(required_min_above),
            df=temp_df,
            sensor_df=sensor_df_for_pdf,
            logo_left_path="assets/images/saniservice_antitarlo.png",
            logo_right_path="assets/images/saniservice.png",
            include_temp_chart=include_temp_chart,
            include_temp_table=include_temp_table,
            include_8_sensors_chart=include_8_sensors_chart,
            chart_metric_mode=report_metric_mode,
        )
        pdf_file_name = f"report_sanificazione_{data_intervento.isoformat()}.pdf"
        if source_mode == "Import from Drive" and st.session_state.get("selected_drive_json_path"):
            pdf_file_name = f"{Path(st.session_state.selected_drive_json_path).stem}.pdf"

        with pdf_dl_col:
            st.download_button(
                label="Scarica report PDF",
                data=pdf_bytes,
                file_name=pdf_file_name,
                mime="application/pdf",
                use_container_width=True,
                key="download_report_pdf_btn",
            )

if show_advanced_sections:
    with tab_send:
        st.subheader("Send data (UDP broadcast)")
        st.caption("Invio periodico dati simulati in broadcast UDP sulla porta 3274.")

        send_thread = st.session_state.get("send_sim_thread")
        send_running = bool(st.session_state.send_sim_state.get("running"))
        if send_running and send_thread is not None and not send_thread.is_alive():
            st.session_state.send_sim_state["running"] = False
            send_running = False

        if send_running and auto_refresh_batch and AUTOREFRESH_AVAILABLE:
            st_autorefresh(interval=1000, key="send_data_refresh")

        cfg_col1, cfg_col2 = st.columns([1, 1])
        with cfg_col1:
            send_broadcast_ip = st.text_input("Broadcast IP destinazione", value=broadcast_ip, key="send_broadcast_ip")
            send_port = st.number_input("Porta UDP", min_value=1, max_value=65535, value=3274, key="send_port")
            send_interval = st.number_input("Intervallo invio (s)", min_value=0.1, max_value=10.0, value=1.0, step=0.1)
            payload_format = st.selectbox(
                "Formato payload",
                options=["Protocollo frame (8 valori + 255)", "CSV (s1..s8)", "JSON"],
                index=0,
            )
    
        with cfg_col2:
            value_mode = st.selectbox("Profilo valori", options=["Statico", "Ramp", "Random"], index=0)
            min_temp = int(st.number_input("Min temperatura", min_value=0, max_value=200, value=20))
            max_temp = int(st.number_input("Max temperatura", min_value=0, max_value=200, value=80))
            ramp_delta = int(st.number_input("Ramp delta", min_value=1, max_value=10, value=1))
            ramp_steps = int(st.number_input("Ramp steps", min_value=1, max_value=200, value=20))
            jitter_sign = int(st.number_input("Random jitter base", min_value=1, max_value=5, value=2))
    
        st.markdown("##### Valori base sensori (s1..s8)")
        s_cols_top = st.columns(4)
        s_cols_bottom = st.columns(4)
        base_values = [0] * 8
        for i in range(4):
            with s_cols_top[i]:
                base_values[i] = int(st.number_input(f"s{i + 1}", min_value=0, max_value=200, value=45 + i, key=f"send_s{i + 1}"))
        for i in range(4, 8):
            with s_cols_bottom[i - 4]:
                base_values[i] = int(st.number_input(f"s{i + 1}", min_value=0, max_value=200, value=45 + i, key=f"send_s{i + 1}"))
    
        preview_payload = _build_send_payload(payload_format, base_values, 1)
        st.code(preview_payload, language="text")
    
        btn_start_col, btn_stop_col = st.columns(2)
        with btn_start_col:
            start_send = st.button("Start", use_container_width=True, disabled=send_running)
        with btn_stop_col:
            stop_send = st.button("Stop", use_container_width=True, disabled=not send_running)
    
        if stop_send and st.session_state.send_sim_stop_event is not None:
            st.session_state.send_sim_stop_event.set()
            st.warning("Richiesta stop simulatore inviata.")
    
        if start_send:
            cfg = {
                "broadcast_ip": send_broadcast_ip.strip(),
                "port": int(send_port),
                "interval_sec": float(send_interval),
                "payload_format": payload_format,
                "value_mode": value_mode,
                "base_values": base_values,
                "min_temp": min_temp,
                "max_temp": max_temp,
                "ramp_delta": ramp_delta,
                "ramp_steps": ramp_steps,
                "jitter_sign": jitter_sign,
            }
            stop_event = threading.Event()
            worker = threading.Thread(
                target=run_send_simulator_worker,
                args=(cfg, stop_event, st.session_state.send_sim_state),
                daemon=True,
            )
            st.session_state.send_sim_stop_event = stop_event
            st.session_state.send_sim_thread = worker
            worker.start()
            st.success("Simulatore UDP avviato.")
    
        send_state = st.session_state.send_sim_state
        info1, info2, info3 = st.columns(3)
        info1.metric("Running", "SI" if send_state.get("running") else "NO")
        info2.metric("Pacchetti inviati", str(send_state.get("sent_packets") or 0))
        info3.metric("Ultimo invio", str(send_state.get("last_send_utc") or "-"))
    
        st.caption(
            f"Start: {send_state.get('start_utc') or '-'} | End: {send_state.get('end_utc') or '-'} | Error: {send_state.get('last_error') or '-'}"
        )
        if send_state.get("last_payload"):
            st.markdown("##### Ultimo payload inviato")
            st.code(str(send_state.get("last_payload")), language="text")
