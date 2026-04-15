from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Dict

import matplotlib.pyplot as plt
import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import Image, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


def parse_manual_data(manual_text: str) -> pd.DataFrame:
    rows = []
    for raw_line in manual_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.lower().startswith("tempo_min"):
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) != 2:
            continue
        try:
            t = float(parts[0])
            temp = float(parts[1])
        except ValueError:
            continue
        rows.append((t, temp))

    return pd.DataFrame(rows, columns=["tempo_min", "temperatura_c"])


def normalize_temperature_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    out.columns = [c.strip().lower() for c in out.columns]
    if "tempo_min" not in out.columns or "temperatura_c" not in out.columns:
        raise ValueError("Il dataset deve contenere le colonne 'tempo_min' e 'temperatura_c'.")

    out = out[["tempo_min", "temperatura_c"]].copy()
    out["tempo_min"] = pd.to_numeric(out["tempo_min"], errors="coerce")
    out["temperatura_c"] = pd.to_numeric(out["temperatura_c"], errors="coerce")
    out = out.dropna().sort_values("tempo_min").drop_duplicates(subset=["tempo_min"])
    return out.reset_index(drop=True)


def _segment_above_duration(t1: float, v1: float, t2: float, v2: float, threshold: float) -> float:
    if t2 <= t1:
        return 0.0

    above1 = v1 >= threshold
    above2 = v2 >= threshold

    if above1 and above2:
        return t2 - t1
    if (not above1) and (not above2):
        return 0.0

    # Linear interpolation of threshold crossing.
    if v2 == v1:
        return 0.0

    crossing_t = t1 + (threshold - v1) * (t2 - t1) / (v2 - v1)
    crossing_t = max(min(crossing_t, t2), t1)

    if above1 and not above2:
        return crossing_t - t1
    return t2 - crossing_t


def calculate_thermal_stats(df: pd.DataFrame, threshold_c: float, required_min_above: float) -> Dict[str, float | bool]:
    if df.empty or len(df) < 2:
        return {
            "max_temp_c": float("nan"),
            "minutes_above_threshold": 0.0,
            "threshold_reached": False,
            "conforme": False,
        }

    max_temp = float(df["temperatura_c"].max())
    minutes_above = 0.0

    for i in range(1, len(df)):
        t1 = float(df.loc[i - 1, "tempo_min"])
        v1 = float(df.loc[i - 1, "temperatura_c"])
        t2 = float(df.loc[i, "tempo_min"])
        v2 = float(df.loc[i, "temperatura_c"])
        minutes_above += _segment_above_duration(t1, v1, t2, v2, threshold_c)

    threshold_reached = max_temp >= threshold_c
    conforme = threshold_reached and minutes_above >= required_min_above

    return {
        "max_temp_c": max_temp,
        "minutes_above_threshold": minutes_above,
        "threshold_reached": threshold_reached,
        "conforme": conforme,
    }


def create_sanification_pdf(
    intervention: Dict[str, str],
    stats: Dict[str, float | bool],
    threshold_c: float,
    required_min_above: float,
    df: pd.DataFrame,
    sensor_df: pd.DataFrame | None = None,
    logo_left_path: str | None = None,
    logo_right_path: str | None = None,
    include_temp_chart: bool = True,
    include_temp_table: bool = True,
    include_8_sensors_chart: bool = True,
    chart_metric_mode: str = "",
) -> bytes:
    def _build_chart_image_bytes(data: pd.DataFrame) -> bytes:
        fig, ax = plt.subplots(figsize=(7.2, 3.2), dpi=150)
        ax.plot(data["tempo_min"], data["temperatura_c"], color="#0f766e", linewidth=2)
        ax.axhline(y=threshold_c, color="#dc2626", linestyle="--", linewidth=1.4, label=f"Soglia {threshold_c:.1f} C")
        ax.set_xlabel("Tempo (min)")
        ax.set_ylabel("Temperatura (C)")
        ax.set_title("Andamento Temperatura")
        ax.grid(alpha=0.25)
        ax.legend(loc="best")
        fig.tight_layout()
        img_buf = BytesIO()
        fig.savefig(img_buf, format="png")
        plt.close(fig)
        img_buf.seek(0)
        return img_buf.read()

    def _build_multi_sensor_chart_image_bytes(data: pd.DataFrame) -> bytes:
        fig, ax = plt.subplots(figsize=(7.2, 3.4), dpi=150)
        sensor_cols = [f"s{i}" for i in range(1, 9)]
        labels = ["P1", "P2", "P3", "P4", "P5", "P6", "S1", "S2"]
        colors_map = [
            "#1d4ed8",
            "#0284c7",
            "#0891b2",
            "#16a34a",
            "#65a30d",
            "#ca8a04",
            "#ea580c",
            "#be123c",
        ]
        for idx, col in enumerate(sensor_cols):
            if col not in data.columns:
                continue
            series = pd.to_numeric(data[col], errors="coerce")
            if series.notna().sum() == 0:
                continue
            ax.plot(data["tempo_min"], series, color=colors_map[idx], linewidth=1.6, label=labels[idx])

        ax.axhline(y=threshold_c, color="#dc2626", linestyle="--", linewidth=1.2, label=f"Soglia {threshold_c:.1f} C")
        ax.set_xlabel("Tempo (min)")
        ax.set_ylabel("Temperatura (C)")
        ax.set_title("Andamento Completo 8 Sensori")
        ax.grid(alpha=0.25)
        ax.legend(loc="upper left", ncol=3, fontsize=8)
        fig.tight_layout()
        img_buf = BytesIO()
        fig.savefig(img_buf, format="png")
        plt.close(fig)
        img_buf.seek(0)
        return img_buf.read()

    def _build_sonde_chart_image_bytes(data: pd.DataFrame) -> bytes:
        fig, ax = plt.subplots(figsize=(7.2, 3.2), dpi=150)
        s1 = pd.to_numeric(data.get("s7"), errors="coerce")
        s2 = pd.to_numeric(data.get("s8"), errors="coerce")
        ax.plot(data["tempo_min"], s1, color="#1d4ed8", linewidth=2, label="S1")
        ax.plot(data["tempo_min"], s2, color="#be123c", linewidth=2, label="S2")
        ax.axhline(y=threshold_c, color="#dc2626", linestyle="--", linewidth=1.2, label=f"Soglia {threshold_c:.1f} C")
        ax.set_xlabel("Tempo (min)")
        ax.set_ylabel("Temperatura (C)")
        ax.set_title("Andamento Sonde S1 e S2")
        ax.grid(alpha=0.25)
        ax.legend(loc="best")
        fig.tight_layout()
        img_buf = BytesIO()
        fig.savefig(img_buf, format="png")
        plt.close(fig)
        img_buf.seek(0)
        return img_buf.read()

    pdf_buffer = BytesIO()
    doc = SimpleDocTemplate(
        pdf_buffer,
        pagesize=A4,
        leftMargin=1.6 * cm,
        rightMargin=1.6 * cm,
        topMargin=1.4 * cm,
        bottomMargin=1.4 * cm,
        title="Report Sanificazione Termica",
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "ReportTitle",
        parent=styles["Title"],
        fontSize=18,
        leading=22,
        textColor=colors.HexColor("#0f172a"),
        spaceAfter=8,
    )
    subtitle_style = ParagraphStyle(
        "Subtitle",
        parent=styles["Normal"],
        fontSize=9,
        textColor=colors.HexColor("#475569"),
        spaceAfter=12,
    )
    section_style = ParagraphStyle(
        "Section",
        parent=styles["Heading2"],
        fontSize=12,
        textColor=colors.HexColor("#0f172a"),
        spaceAfter=6,
        spaceBefore=10,
    )

    story = []
    left_logo_path = Path(logo_left_path) if logo_left_path else Path("assets/images/saniservice_antitarlo.png")
    right_logo_path = Path(logo_right_path) if logo_right_path else Path("assets/images/saniservice.png")
    logo_row = []
    if left_logo_path.exists():
        logo_row.append(Image(str(left_logo_path), width=5.2 * cm, height=1.6 * cm))
    else:
        logo_row.append(Paragraph("", styles["Normal"]))
    logo_row.append(Paragraph("", styles["Normal"]))
    if right_logo_path.exists():
        logo_row.append(Image(str(right_logo_path), width=3.0 * cm, height=1.6 * cm))
    else:
        logo_row.append(Paragraph("", styles["Normal"]))

    logo_table = Table([logo_row], colWidths=[6.6 * cm, 7.0 * cm, 3.2 * cm])
    logo_table.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("ALIGN", (0, 0), (0, 0), "LEFT"),
                ("ALIGN", (2, 0), (2, 0), "RIGHT"),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                ("TOPPADDING", (0, 0), (-1, -1), 0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
            ]
        )
    )
    story.append(logo_table)
    story.append(Spacer(1, 0.18 * cm))
    story.append(Paragraph("Report Sanificazione Termica", title_style))
    story.append(Paragraph("Generato da app_discovery", subtitle_style))

    anagrafica_rows = [
        ["Cliente", intervention.get("cliente", "")],
        ["Indirizzo", intervention.get("indirizzo", "")],
        ["Data intervento", intervention.get("data_intervento", "")],
        ["Luogo intervento", intervention.get("luogo_intervento", "")],
        ["Ora prima misurazione", intervention.get("measurement_start", "-")],
        ["Ora fine misurazione", intervention.get("measurement_end", "-")],
        ["Durata intervento", intervention.get("intervention_duration", "-")],
        ["Tecnico", intervention.get("tecnico", "")],
        ["Codice intervento", intervention.get("codice_intervento", "")],
        ["Oggetto trattato", intervention.get("oggetto_trattato", "")],
        ["Note", intervention.get("note", "")],
    ]

    story.append(Paragraph("Anagrafica Intervento", section_style))
    anagrafica_table = Table(anagrafica_rows, colWidths=[4.0 * cm, 13.8 * cm], repeatRows=0)
    anagrafica_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#f1f5f9")),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cbd5e1")),
                ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
                ("FONTNAME", (1, 0), (1, -1), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    story.append(anagrafica_table)

    story.append(Paragraph("Verifica Automatica", section_style))
    esito = "CONFORME" if stats["conforme"] else "NON CONFORME"
    verifica_rows = [
        ["Soglia letale impostata", f"{threshold_c:.1f} C"],
        ["Minuti richiesti sopra soglia", f"{required_min_above:.1f}"],
        ["Temperatura massima", f"{stats['max_temp_c']:.2f} C"],
        ["Minuti sopra soglia", f"{stats['minutes_above_threshold']:.2f}"],
        ["Soglia raggiunta", "SI" if stats["threshold_reached"] else "NO"],
        ["Esito finale", esito],
    ]
    verifica_table = Table(verifica_rows, colWidths=[7.2 * cm, 10.6 * cm])
    verifica_table.setStyle(
        TableStyle(
            [
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cbd5e1")),
                ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#f8fafc")),
                ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("TEXTCOLOR", (1, 5), (1, 5), colors.HexColor("#166534") if stats["conforme"] else colors.HexColor("#991b1b")),
                ("FONTNAME", (1, 5), (1, 5), "Helvetica-Bold"),
            ]
        )
    )
    story.append(verifica_table)

    sensor_data = None
    if sensor_df is not None and not sensor_df.empty:
        sensor_data = sensor_df.copy()
        if "tempo_min" in sensor_data.columns:
            sensor_data["tempo_min"] = pd.to_numeric(sensor_data["tempo_min"], errors="coerce")
            for col in [f"s{i}" for i in range(1, 9)]:
                if col in sensor_data.columns:
                    sensor_data[col] = pd.to_numeric(sensor_data[col], errors="coerce")
            sensor_data = sensor_data.dropna(subset=["tempo_min"]).sort_values("tempo_min")
            if sensor_data.empty:
                sensor_data = None
        else:
            sensor_data = None

    if include_temp_chart:
        if chart_metric_mode == "sonde_s1_s2" and sensor_data is not None:
            story.append(Paragraph("Grafico Sonde S1 e S2", section_style))
            chart_png = _build_sonde_chart_image_bytes(sensor_data)
        else:
            story.append(Paragraph("Grafico Tempo / Temperatura", section_style))
            chart_png = _build_chart_image_bytes(df)
        chart = Image(BytesIO(chart_png), width=17.2 * cm, height=6.9 * cm)
        story.append(chart)

    if include_8_sensors_chart and sensor_data is not None:
        story.append(Paragraph("Grafico Completo 8 Sensori", section_style))
        sensor_chart_png = _build_multi_sensor_chart_image_bytes(sensor_data)
        sensor_chart = Image(BytesIO(sensor_chart_png), width=17.2 * cm, height=7.2 * cm)
        story.append(sensor_chart)

    if include_temp_table:
        story.append(Paragraph("Tabella Misurazioni", section_style))
        data_rows = [["tempo_min", "temperatura_c"]]
        for _, row in df.iterrows():
            data_rows.append([f"{float(row['tempo_min']):.2f}", f"{float(row['temperatura_c']):.2f}"])

        data_table = Table(data_rows, colWidths=[8.6 * cm, 8.6 * cm], repeatRows=1)
        data_table_style = TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e2e8f0")),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#cbd5e1")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, -1), 8.0),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (-1, -1), 3),
                ("RIGHTPADDING", (0, 0), (-1, -1), 3),
                ("TOPPADDING", (0, 0), (-1, -1), 2),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
            ]
        )
        for row_index in range(1, len(data_rows)):
            if row_index % 2 == 0:
                data_table_style.add("BACKGROUND", (0, row_index), (-1, row_index), colors.HexColor("#f8fafc"))
        data_table.setStyle(data_table_style)
        story.append(data_table)
        story.append(Spacer(1, 0.2 * cm))

    doc.build(story)
    pdf_buffer.seek(0)
    return pdf_buffer.read()
