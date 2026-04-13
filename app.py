from datetime import date, datetime
import threading
import time

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


st.set_page_config(page_title="UDP Discovery + Report Sanificazione", layout="wide")

header_left, header_center, header_right = st.columns([1, 4, 1])
with header_left:
    st.image("assets/images/saniservice_antitarlo.png", width=300)
with header_right:
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
        txt = "--" if v is None else f"{v}°"
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

with st.sidebar:
    st.header("Collector UDP")
    use_mock = st.toggle("Modalita mock (simulazione)", value=False)
    auto_refresh_batch = st.toggle("Auto-refresh stato batch", value=True)
    broadcast_ip = st.text_input("Broadcast IP", value="192.168.1.255")
    port = st.number_input("Porta UDP", min_value=1, max_value=65535, value=3274)
    timeout_sec = st.number_input("Timeout (s)", min_value=0.2, max_value=10.0, value=2.0, step=0.2)
    poll_interval_sec = st.number_input("Intervallo polling (s)", min_value=0.2, max_value=30.0, value=2.0, step=0.2)
    db_path = st.text_input("SQLite DB path", value="app_discovery/data/sensor_data.db")
    init_db(db_path)

    mock_seed = None
    mock_timeout_rate = 0.08
    mock_ser_rate = 0.05
    mock_scenario = "normal"
    if use_mock:
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

tab1, tab2 = st.tabs(["Collector UDP", "Report Sanificazione"])

with tab1:
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

with tab2:
    st.subheader("Report Attivita di Sanificazione")

    a1, a2 = st.columns([1, 1])
    with a1:
        cliente = st.text_input("Cliente")
        indirizzo = st.text_input("Indirizzo")
        data_intervento = st.date_input("Data intervento", value=date.today())
        tecnico = st.text_input("Tecnico")
    with a2:
        codice_intervento = st.text_input("Codice intervento")
        oggetto_trattato = st.text_input("Oggetto trattato")
        note = st.text_area("Note", height=140)

    st.markdown("#### Dati termici")
    source_mode = st.radio("Sorgente dati", options=["CSV", "Manuale", "Storico collector"], horizontal=True)

    temp_df = pd.DataFrame(columns=["tempo_min", "temperatura_c"])
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
        else:
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
                rows = frames_for_activity(db_path, selected_activity)
                temp_df = activity_frames_to_temp_df(rows, metric_mode)
                if temp_df.empty:
                    st.warning("Nessun frame utile per questa attivita.")
                else:
                    if not codice_intervento:
                        codice_intervento = selected_activity
                    st.caption(f"Caricati {len(temp_df)} punti da attivita '{selected_activity}'.")

    b1, b2 = st.columns(2)
    with b1:
        threshold_c = st.number_input("Soglia letale (C)", min_value=0.0, max_value=200.0, value=55.0, step=0.5)
    with b2:
        required_min_above = st.number_input("Minuti minimi sopra soglia", min_value=0.0, max_value=600.0, value=30.0, step=1.0)

    if temp_df.empty:
        st.warning("Inserisci o carica dati validi per generare analisi e report.")
    else:
        stats = calculate_thermal_stats(temp_df, threshold_c=float(threshold_c), required_min_above=float(required_min_above))

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Temp. massima", f"{stats['max_temp_c']:.2f} C")
        m2.metric("Minuti sopra soglia", f"{stats['minutes_above_threshold']:.2f}")
        m3.metric("Soglia raggiunta", "SI" if stats["threshold_reached"] else "NO")
        m4.metric("Esito", "CONFORME" if stats["conforme"] else "NON CONFORME")

        if stats["conforme"]:
            st.markdown('<div class="metric-ok">Esito automatico: CONFORME</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="metric-ko">Esito automatico: NON CONFORME</div>', unsafe_allow_html=True)

        st.markdown("#### Grafico Tempo / Temperatura")
        chart_df = temp_df.copy().set_index("tempo_min")
        st.line_chart(chart_df[["temperatura_c"]])
        st.dataframe(temp_df, use_container_width=True)

        intervention_payload = {
            "cliente": cliente,
            "indirizzo": indirizzo,
            "data_intervento": data_intervento.isoformat(),
            "tecnico": tecnico,
            "codice_intervento": codice_intervento,
            "oggetto_trattato": oggetto_trattato,
            "note": note,
        }

        pdf_bytes = create_sanification_pdf(
            intervention=intervention_payload,
            stats=stats,
            threshold_c=float(threshold_c),
            required_min_above=float(required_min_above),
            df=temp_df,
        )

        st.download_button(
            label="Scarica report PDF",
            data=pdf_bytes,
            file_name=f"report_sanificazione_{data_intervento.isoformat()}.pdf",
            mime="application/pdf",
            use_container_width=True,
        )
