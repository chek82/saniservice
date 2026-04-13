from datetime import date, datetime
import time

import pandas as pd
import streamlit as st

from report_utils import calculate_thermal_stats, create_sanification_pdf, normalize_temperature_df, parse_manual_data
from storage import frames_for_activity, init_db, list_activity_codes, recent_frames, save_frame
from udp_client import MockUdpControllerClient, UdpControllerClient


st.set_page_config(page_title="UDP Discovery + Report Sanificazione", layout="wide")

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

with st.sidebar:
    st.header("Collector UDP")
    use_mock = st.toggle("Modalita mock (simulazione)", value=False)
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

        cycles = st.number_input("Numero cicli batch", min_value=1, max_value=5000, value=30)
        if st.button("4) Batch polling + save", use_container_width=True):
            if not st.session_state.controller_ip:
                st.error("Controller IP mancante")
            elif not st.session_state.activity_code:
                st.error("Inserisci un codice attivita prima di avviare il batch")
            else:
                progress = st.progress(0)
                status = st.empty()
                ok = 0
                for i in range(int(cycles)):
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
                    if frame["frame_complete"]:
                        ok += 1
                    progress.progress((i + 1) / int(cycles))
                    status.write(
                        f"[{st.session_state.activity_code}] Ciclo {i + 1}/{int(cycles)} - complete={ok} - last={frame['values']} - latency={frame['latency_ms']}ms"
                    )
                    time.sleep(float(poll_interval_sec))
                st.success(f"Batch completato. Frame completi: {ok}/{int(cycles)}")

    with right:
        st.subheader("Ultimi dati")
        rows = recent_frames(db_path, limit=300)
        if not rows:
            st.info("Nessun dato salvato ancora")
        else:
            df = pd.DataFrame([dict(r) for r in rows]).sort_values("id")
            st.dataframe(df.tail(20), use_container_width=True)

            sensor_col = st.selectbox(
                "Sensore da graficare", ["s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"]
            )
            chart_df = df[["ts", sensor_col]].copy().rename(columns={"ts": "timestamp", sensor_col: "value"})
            chart_df = chart_df.dropna().set_index("timestamp")
            if not chart_df.empty:
                st.line_chart(chart_df)

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
