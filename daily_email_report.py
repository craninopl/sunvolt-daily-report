"""
SunVolt — Reporte Diario de Generación Solar
Envía email con KPIs del día anterior a destinatario(s) configurado(s).
Diseñado para ejecutarse via crontab a las 7:00 AM COL (12:00 UTC).
"""

import os
import sys
import smtplib
import traceback
import psycopg2
import pandas as pd
import requests as http_requests
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv

# ==========================================
# 1. CONFIGURACIÓN
# ==========================================

COL_TZ = timezone(timedelta(hours=-5))

def load_config():
    # En GitHub Actions las env vars se inyectan directamente.
    # En local, intentar cargar desde archivos .env si existen.
    base = os.path.dirname(os.path.abspath(__file__))
    for env_file in ['dashboard_supabase.env', 'email.env']:
        path = os.path.join(base, 'config', env_file)
        if os.path.exists(path):
            load_dotenv(path, override=True)

    required = ['PG_DSN', 'SMTP_USER', 'SMTP_PASSWORD', 'REPORT_RECIPIENT']
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise EnvironmentError(f"Variables faltantes: {', '.join(missing)}")


def get_db():
    dsn = os.getenv("PG_DSN", "").strip()
    if "sslmode=" not in dsn and "?" not in dsn:
        dsn += "?sslmode=require"
    return psycopg2.connect(dsn, connect_timeout=10)


# ==========================================
# 2. QUERIES
# ==========================================

Q_ALL_PLANTS = """
    SELECT id_externo, nombre, marca_origen, potencia_instalada_kwp,
           hsp_teorico, estado, patrimonio
    FROM plantas
    -- Excluir plantas retiradas de la flota (EPC fuera de alcance, contratos
    -- terminados). Antes salian en el correo con 0 kWh y 0% arrastrando el
    -- promedio y confundiendo al lector (casos: La Gran Esquina, Agris).
    WHERE COALESCE(estado, '') <> 'desvinculada'
    ORDER BY nombre
"""

# --- Mapeo dispositivo->planta (incluye plantas hibridas) -------------------
# Algunas plantas (ej. CDA La 30 id_externo=2602400) reciben generacion de
# varias plataformas: una fila plant-level (device_sn = id_externo, Growatt) y
# filas inverter-level (device_sn = inverter_metadata.inverter_id, AuroraVision/
# FIMER). El cruce ingenuo `device_sn = id_externo` descartaba las filas
# inverter-level y subreportaba la planta (CDA La 30: 21% vs 93% real).
# Este CTE replica el mapeo de la RPC get_plants_summary (plant_devices):
# inverter_metadata.plant_id == plantas.id_externo. El alias `device_sn` deja
# intacto el merge de pandas aguas abajo (right_on='device_sn_trim').
Q_PLANT_DEVICES = """
    SELECT p.id_externo AS plant_key, p.id_externo AS device_sn
    FROM plantas p
    UNION ALL
    SELECT im.plant_id AS plant_key, im.inverter_id AS device_sn
    FROM inverter_metadata im
    WHERE im.inverter_id <> im.plant_id
"""

Q_DAILY = f"""
    WITH plant_devices AS ({Q_PLANT_DEVICES})
    SELECT pdv.plant_key AS device_sn, SUM(pd.energia_kwh) AS energia_kwh
    FROM plant_devices pdv
    JOIN produccion_diaria pd ON pd.device_sn = pdv.device_sn
    WHERE pd.fecha = %(report_date)s
    GROUP BY pdv.plant_key
"""

Q_MONTHLY = f"""
    WITH plant_devices AS ({Q_PLANT_DEVICES})
    SELECT pdv.plant_key AS device_sn, SUM(pd.energia_kwh) AS energia_kwh
    FROM plant_devices pdv
    JOIN produccion_diaria pd ON pd.device_sn = pdv.device_sn
    WHERE pd.fecha >= %(month_start)s AND pd.fecha <= %(report_date)s
    GROUP BY pdv.plant_key
"""

def fetch_report_data(conn, report_date):
    month_start = report_date.replace(day=1)
    days_in_range = (report_date - month_start).days + 1
    params = {'report_date': report_date, 'month_start': month_start}

    df_plants = pd.read_sql(Q_ALL_PLANTS, conn)
    df_daily = pd.read_sql(Q_DAILY, conn, params=params)
    df_monthly = pd.read_sql(Q_MONTHLY, conn, params=params)

    return {
        'report_date': report_date,
        'month_start': month_start,
        'days_in_range': days_in_range,
        'df_plants': df_plants,
        'df_daily': df_daily,
        'df_monthly': df_monthly,
    }


# ==========================================
# 3. CÁLCULOS (replica frontend/views/resumen.py:216-229)
# ==========================================

def calc_meta(row, days):
    """Generación estimada. Legalización → meta = real."""
    state = str(row.get('estado', '') or '').lower()
    if state == 'legalizacion':
        return row['energia_kwh']
    return row['potencia_instalada_kwp'] * row['hsp_teorico'] * days


def calc_performance(real, meta):
    return (real / meta * 100) if meta > 0 else 0


def process_data(data):
    df_p = data['df_plants'].copy()
    df_d_raw = data['df_daily'].copy()
    df_m_raw = data['df_monthly'].copy()

    # Normalizar metadata
    df_p['potencia_instalada_kwp'] = pd.to_numeric(df_p['potencia_instalada_kwp'], errors='coerce').fillna(0)
    df_p['hsp_teorico'] = pd.to_numeric(df_p['hsp_teorico'], errors='coerce').fillna(3.5)
    df_p['id_externo_trim'] = df_p['id_externo'].astype(str).str.strip()

    # Merge diario
    df_d_raw['energia_kwh'] = pd.to_numeric(df_d_raw['energia_kwh'], errors='coerce').fillna(0)
    df_d_raw['device_sn_trim'] = df_d_raw['device_sn'].astype(str).str.strip()
    df_d = df_p.merge(df_d_raw[['device_sn_trim', 'energia_kwh']],
                      left_on='id_externo_trim', right_on='device_sn_trim', how='left')
    df_d['energia_kwh'] = df_d['energia_kwh'].fillna(0)

    # Merge mensual
    df_m_raw['energia_kwh'] = pd.to_numeric(df_m_raw['energia_kwh'], errors='coerce').fillna(0)
    df_m_raw['device_sn_trim'] = df_m_raw['device_sn'].astype(str).str.strip()
    df_m = df_p.merge(df_m_raw[['device_sn_trim', 'energia_kwh']],
                      left_on='id_externo_trim', right_on='device_sn_trim', how='left',
                      suffixes=('', '_mes'))
    df_m['energia_kwh'] = df_m['energia_kwh'].fillna(0)

    # --- Cálculos diarios ---
    df_d['meta'] = df_d.apply(lambda r: calc_meta(r, 1), axis=1)
    df_d['performance'] = df_d.apply(lambda r: calc_performance(r['energia_kwh'], r['meta']), axis=1)

    gen_dia_real = df_d['energia_kwh'].sum()
    gen_dia_meta = df_d['meta'].sum()
    pr_dia = calc_performance(gen_dia_real, gen_dia_meta)
    plantas_operando = len(df_d[df_d['energia_kwh'] > 0])
    total_plants = len(df_p)
    potencia_total = df_p['potencia_instalada_kwp'].sum()

    # Top 3 generación
    df_con_datos = df_d[df_d['energia_kwh'] > 0]
    top3_gen = df_con_datos.nlargest(3, 'energia_kwh')[
        ['nombre', 'marca_origen', 'energia_kwh']].reset_index(drop=True)

    # Top 3 performance (excluir legalización y limitado: % no comparable)
    # limitado = generación capada al consumo → su % real es bajo por diseño,
    # no debe aparecer como "peor performance" (sí cuenta en agregados/tabla).
    df_perf = df_con_datos[~df_con_datos['estado'].fillna('').str.lower().isin(['legalizacion', 'limitado'])]
    top3_perf = df_perf.nlargest(3, 'performance')[
        ['nombre', 'marca_origen', 'performance', 'energia_kwh', 'meta']].reset_index(drop=True)

    # Bottom 3 performance (peores, excluir legalización y sin datos)
    df_perf_active = df_perf[df_perf['energia_kwh'] > 0]
    bottom3_perf = df_perf_active.nsmallest(3, 'performance')[
        ['nombre', 'marca_origen', 'performance', 'energia_kwh', 'meta']].reset_index(drop=True)

    # --- Breakdown por marca ---
    brand_daily = df_d.groupby('marca_origen').agg(
        gen_kwh=('energia_kwh', 'sum'),
        meta_kwh=('meta', 'sum'),
        count=('nombre', 'size'),
        operando=('energia_kwh', lambda x: (x > 0).sum())
    ).reset_index()
    brand_daily['pr'] = brand_daily.apply(lambda r: calc_performance(r['gen_kwh'], r['meta_kwh']), axis=1)
    brand_daily = brand_daily.sort_values('gen_kwh', ascending=False)

    # --- Plantas offline (sin datos hoy) ---
    offline = df_d[df_d['energia_kwh'] == 0][['nombre', 'marca_origen', 'estado', 'potencia_instalada_kwp']].reset_index(drop=True)

    # --- Cálculos mensuales ---
    df_m['meta'] = df_m.apply(lambda r: calc_meta(r, data['days_in_range']), axis=1)
    gen_mes_real = df_m['energia_kwh'].sum()
    gen_mes_meta = df_m['meta'].sum()
    pr_mes = calc_performance(gen_mes_real, gen_mes_meta)

    # --- Breakdown por Patrimonio Autonomo (mensual) ---
    df_m['pa_group'] = df_m['patrimonio'].fillna('').astype(str).str.strip().apply(
        lambda p: p if p in ('PA1', 'PA2') else 'Otros'
    )
    pa_monthly = df_m.groupby('pa_group').agg(
        gen_kwh=('energia_kwh', 'sum'),
        meta_kwh=('meta', 'sum'),
        count=('nombre', 'size'),
        potencia=('potencia_instalada_kwp', 'sum'),
    ).reset_index()
    pa_monthly['pr'] = pa_monthly.apply(
        lambda r: calc_performance(r['gen_kwh'], r['meta_kwh']), axis=1
    )
    order = {'PA1': 0, 'PA2': 1, 'Otros': 2}
    pa_monthly['_ord'] = pa_monthly['pa_group'].map(order)
    pa_monthly = pa_monthly.sort_values('_ord').drop(columns='_ord').reset_index(drop=True)

    # Tabla completa de plantas (diario)
    plant_table = df_d[['nombre', 'marca_origen', 'estado', 'potencia_instalada_kwp',
                        'energia_kwh', 'meta', 'performance']].sort_values('energia_kwh', ascending=False)

    return {
        'gen_dia_real': gen_dia_real,
        'gen_dia_meta': gen_dia_meta,
        'pr_dia': pr_dia,
        'gen_mes_real': gen_mes_real,
        'gen_mes_meta': gen_mes_meta,
        'pr_mes': pr_mes,
        'plantas_operando': plantas_operando,
        'total_plants': total_plants,
        'potencia_total': potencia_total,
        'top3_gen': top3_gen,
        'top3_perf': top3_perf,
        'bottom3_perf': bottom3_perf,
        'brand_daily': brand_daily,
        'offline': offline,
        'plant_table': plant_table,
        'pa_monthly': pa_monthly,
        'report_date': data['report_date'],
        'month_start': data['month_start'],
        'days_in_range': data['days_in_range'],
    }


# ==========================================
# 4. HTML TEMPLATE
# ==========================================

NAVY = '#0F2A47'
GOLD = '#FFC107'
GREEN = '#27AE60'
RED = '#E74C3C'
ORANGE = '#F39C12'
DARK_BG = '#0D1117'
CARD_BG = '#161B22'
TEXT = '#C9D1D9'
MUTED = '#8B949E'
ROW_ALT = '#1c2128'


def color_pr(pr):
    if pr >= 85:
        return GREEN
    if pr >= 60:
        return ORANGE
    if pr > 0:
        return RED
    return MUTED


def fmt(val):
    return f"{val:,.0f}"


def build_html(kpis):
    date_str = kpis['report_date'].strftime('%d/%m/%Y')
    month_str = kpis['report_date'].strftime('%B %Y').capitalize()
    pr_dia_color = color_pr(kpis['pr_dia'])
    pr_mes_color = color_pr(kpis['pr_mes'])

    # --- Top 3 Generación ---
    rows_gen = ""
    for i, row in kpis['top3_gen'].iterrows():
        bg = ROW_ALT if i % 2 else DARK_BG
        rows_gen += f"""
        <tr style="background-color:{bg};">
            <td style="padding:8px 12px; color:{GOLD}; font-weight:bold;">{i+1}</td>
            <td style="padding:8px 12px; color:{TEXT};">{row['nombre']}</td>
            <td style="padding:8px 12px; color:{MUTED}; font-size:11px;">{row['marca_origen']}</td>
            <td style="padding:8px 12px; color:white; text-align:right; font-weight:bold;">{fmt(row['energia_kwh'])} kWh</td>
        </tr>"""

    # --- Top 3 Performance ---
    rows_perf = ""
    for i, row in kpis['top3_perf'].iterrows():
        bg = ROW_ALT if i % 2 else DARK_BG
        pc = color_pr(row['performance'])
        rows_perf += f"""
        <tr style="background-color:{bg};">
            <td style="padding:8px 12px; color:{GOLD}; font-weight:bold;">{i+1}</td>
            <td style="padding:8px 12px; color:{TEXT};">{row['nombre']}</td>
            <td style="padding:8px 12px; color:{MUTED}; font-size:11px;">{row['marca_origen']}</td>
            <td style="padding:8px 12px; color:{pc}; text-align:right; font-weight:bold;">{row['performance']:.1f}%</td>
        </tr>"""

    # --- Bottom 3 Performance ---
    rows_bottom = ""
    for i, row in kpis['bottom3_perf'].iterrows():
        bg = ROW_ALT if i % 2 else DARK_BG
        pc = color_pr(row['performance'])
        rows_bottom += f"""
        <tr style="background-color:{bg};">
            <td style="padding:8px 12px; color:{RED}; font-weight:bold;">{i+1}</td>
            <td style="padding:8px 12px; color:{TEXT};">{row['nombre']}</td>
            <td style="padding:8px 12px; color:{MUTED}; font-size:11px;">{row['marca_origen']}</td>
            <td style="padding:8px 12px; color:{pc}; text-align:right; font-weight:bold;">{row['performance']:.1f}%</td>
        </tr>"""

    # --- Brand breakdown ---
    rows_brand = ""
    for i, row in kpis['brand_daily'].iterrows():
        bg = ROW_ALT if i % 2 else DARK_BG
        pc = color_pr(row['pr'])
        rows_brand += f"""
        <tr style="background-color:{bg};">
            <td style="padding:8px 12px; color:{TEXT}; font-weight:bold;">{row['marca_origen']}</td>
            <td style="padding:8px 12px; color:{TEXT}; text-align:right;">{fmt(row['gen_kwh'])} kWh</td>
            <td style="padding:8px 12px; color:{MUTED}; text-align:right;">{fmt(row['meta_kwh'])} kWh</td>
            <td style="padding:8px 12px; color:{pc}; text-align:right; font-weight:bold;">{row['pr']:.1f}%</td>
            <td style="padding:8px 12px; color:{MUTED}; text-align:center;">{int(row['operando'])}/{int(row['count'])}</td>
        </tr>"""

    # --- Offline plants ---
    offline_section = ""
    if len(kpis['offline']) > 0:
        rows_off = ""
        for i, row in kpis['offline'].iterrows():
            bg = ROW_ALT if i % 2 else DARK_BG
            estado_txt = str(row['estado'] or '').capitalize()
            rows_off += f"""
            <tr style="background-color:{bg};">
                <td style="padding:6px 12px; color:{TEXT}; font-size:12px;">{row['nombre']}</td>
                <td style="padding:6px 12px; color:{MUTED}; font-size:11px;">{row['marca_origen']}</td>
                <td style="padding:6px 12px; color:{MUTED}; font-size:11px;">{estado_txt}</td>
                <td style="padding:6px 12px; color:{MUTED}; font-size:11px; text-align:right;">{row['potencia_instalada_kwp']:.1f} kWp</td>
            </tr>"""

        offline_section = f"""
        <tr>
            <td style="padding:0 30px 20px 30px;">
                <div style="color:{RED}; font-size:13px; font-weight:bold; text-transform:uppercase; letter-spacing:1px; margin-bottom:8px;">
                    &#128683; Plantas Sin Datos ({len(kpis['offline'])})
                </div>
                <table width="100%" cellpadding="0" cellspacing="0" style="background-color:{DARK_BG}; border-radius:8px; overflow:hidden;">
                    <tr style="background-color:rgba(231,76,60,0.1);">
                        <th style="padding:6px 12px; color:{RED}; font-size:10px; text-align:left;">PLANTA</th>
                        <th style="padding:6px 12px; color:{RED}; font-size:10px; text-align:left;">MARCA</th>
                        <th style="padding:6px 12px; color:{RED}; font-size:10px; text-align:left;">ESTADO</th>
                        <th style="padding:6px 12px; color:{RED}; font-size:10px; text-align:right;">POTENCIA</th>
                    </tr>
                    {rows_off}
                </table>
            </td>
        </tr>"""

    # --- Filas PA ---
    rows_pa = ""
    for i, row in kpis['pa_monthly'].iterrows():
        bg = ROW_ALT if i % 2 else DARK_BG
        pc = color_pr(row['pr'])
        rows_pa += f"""
        <tr style="background-color:{bg};">
            <td style="padding:8px 12px; color:{TEXT}; font-weight:bold;">{row['pa_group']}</td>
            <td style="padding:8px 12px; color:white; text-align:right;">{fmt(row['gen_kwh'])} kWh</td>
            <td style="padding:8px 12px; color:{MUTED}; text-align:right;">{fmt(row['meta_kwh'])} kWh</td>
            <td style="padding:8px 12px; color:{pc}; text-align:right; font-weight:bold;">{row['pr']:.1f}%</td>
            <td style="padding:8px 12px; color:{MUTED}; text-align:center; font-size:11px;">{int(row['count'])} ({row['potencia']:,.0f} kWp)</td>
        </tr>"""

    # --- Full plant table ---
    rows_plant = ""
    for i, (_, row) in enumerate(kpis['plant_table'].iterrows()):
        bg = ROW_ALT if i % 2 else DARK_BG
        pc = color_pr(row['performance'])
        estado_txt = str(row['estado'] or '').capitalize()
        gen_txt = fmt(row['energia_kwh']) if row['energia_kwh'] > 0 else '<span style="color:#E74C3C;">0</span>'
        rows_plant += f"""
        <tr style="background-color:{bg};">
            <td style="padding:5px 8px; color:{TEXT}; font-size:11px;">{row['nombre']}</td>
            <td style="padding:5px 8px; color:{MUTED}; font-size:10px;">{row['marca_origen']}</td>
            <td style="padding:5px 8px; color:{MUTED}; font-size:10px;">{estado_txt}</td>
            <td style="padding:5px 8px; color:{TEXT}; font-size:11px; text-align:right;">{gen_txt}</td>
            <td style="padding:5px 8px; color:{MUTED}; font-size:10px; text-align:right;">{fmt(row['meta'])}</td>
            <td style="padding:5px 8px; color:{pc}; font-size:11px; text-align:right; font-weight:bold;">{row['performance']:.0f}%</td>
        </tr>"""

    html = f"""
    <html>
    <body style="margin:0; padding:0; background-color:{DARK_BG}; font-family:Arial,Helvetica,sans-serif;">
    <table width="100%" cellpadding="0" cellspacing="0" style="background-color:{DARK_BG}; padding:20px 0;">
    <tr><td align="center">
    <table width="650" cellpadding="0" cellspacing="0" style="background-color:{CARD_BG}; border-radius:12px; overflow:hidden;">

        <!-- HEADER -->
        <tr>
            <td style="background:linear-gradient(135deg, {NAVY}, #1a3a5c); padding:28px 30px; text-align:center;">
                <div style="font-size:26px; font-weight:bold; color:{GOLD}; letter-spacing:2px;">SUNVOLT</div>
                <div style="font-size:13px; color:{TEXT}; margin-top:2px; letter-spacing:1px;">Command Center — Gestion de Activos Solares</div>
                <div style="margin-top:12px; padding:6px 16px; display:inline-block; background-color:rgba(255,193,7,0.15); border-radius:20px; border:1px solid rgba(255,193,7,0.3);">
                    <span style="font-size:15px; color:white; font-weight:bold;">Reporte Diario — {date_str}</span>
                </div>
            </td>
        </tr>

        <!-- RESUMEN EJECUTIVO -->
        <tr>
            <td style="padding:24px 30px 8px 30px;">
                <div style="color:{GOLD}; font-size:13px; font-weight:bold; text-transform:uppercase; letter-spacing:1px; margin-bottom:12px;">
                    Resumen Ejecutivo
                </div>
                <table width="100%" cellpadding="0" cellspacing="0">
                    <tr>
                        <td width="50%" style="padding:0 6px 12px 0; vertical-align:top;">
                            <div style="background-color:{DARK_BG}; border-radius:8px; padding:16px; border-left:3px solid {GOLD};">
                                <div style="color:{MUTED}; font-size:10px; text-transform:uppercase; letter-spacing:1px;">Generacion del Dia</div>
                                <div style="color:white; font-size:24px; font-weight:bold; margin-top:4px;">{fmt(kpis['gen_dia_real'])} <span style="font-size:12px; color:{MUTED};">kWh</span></div>
                                <div style="color:{MUTED}; font-size:12px; margin-top:4px;">Meta: {fmt(kpis['gen_dia_meta'])} kWh</div>
                                <div style="color:{pr_dia_color}; font-size:16px; font-weight:bold; margin-top:2px;">Cumplimiento: {kpis['pr_dia']:.1f}%</div>
                            </div>
                        </td>
                        <td width="50%" style="padding:0 0 12px 6px; vertical-align:top;">
                            <div style="background-color:{DARK_BG}; border-radius:8px; padding:16px; border-left:3px solid {GOLD};">
                                <div style="color:{MUTED}; font-size:10px; text-transform:uppercase; letter-spacing:1px;">Acumulado {month_str}</div>
                                <div style="color:white; font-size:24px; font-weight:bold; margin-top:4px;">{fmt(kpis['gen_mes_real'])} <span style="font-size:12px; color:{MUTED};">kWh</span></div>
                                <div style="color:{MUTED}; font-size:12px; margin-top:4px;">Meta: {fmt(kpis['gen_mes_meta'])} kWh ({kpis['days_in_range']} dias)</div>
                                <div style="color:{pr_mes_color}; font-size:16px; font-weight:bold; margin-top:2px;">Cumplimiento: {kpis['pr_mes']:.1f}%</div>
                            </div>
                        </td>
                    </tr>
                    <tr>
                        <td colspan="2" style="padding:0; vertical-align:top;">
                            <div style="background-color:{DARK_BG}; border-radius:8px; padding:16px; border-left:3px solid {GOLD};">
                                <div style="color:{MUTED}; font-size:10px; text-transform:uppercase; letter-spacing:1px;">Infraestructura</div>
                                <div style="color:white; font-size:22px; font-weight:bold; margin-top:4px;">{kpis['plantas_operando']} <span style="font-size:13px; color:{MUTED};">/ {kpis['total_plants']} plantas</span></div>
                                <div style="color:{MUTED}; font-size:12px; margin-top:4px;">Potencia total: {kpis['potencia_total']:,.1f} kWp</div>
                            </div>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>

        <!-- NOTA LEGALIZACION -->
        <tr>
            <td style="padding:12px 30px 16px 30px;">
                <div style="background-color:rgba(255,193,7,0.08); border-radius:6px; padding:10px 14px; border:1px solid rgba(255,193,7,0.2);">
                    <span style="color:{GOLD}; font-size:12px;">&#9888;</span>
                    <span style="color:{MUTED}; font-size:11px; font-style:italic;">
                        Los proyectos en legalizacion muestran 100% de cumplimiento (meta = generacion real). Las plantas 'limitado' tienen su generacion capada al consumo del cliente: muestran su % real en la tabla y agregados, pero se excluyen de los rankings Top/Bottom porque su bajo % es por diseno, no una falla.
                    </span>
                </div>
            </td>
        </tr>

        <!-- BREAKDOWN POR MARCA -->
        <tr>
            <td style="padding:0 30px 20px 30px;">
                <div style="color:{GOLD}; font-size:13px; font-weight:bold; text-transform:uppercase; letter-spacing:1px; margin-bottom:8px;">
                    &#9881; Generacion por Marca
                </div>
                <table width="100%" cellpadding="0" cellspacing="0" style="background-color:{DARK_BG}; border-radius:8px; overflow:hidden;">
                    <tr style="background-color:rgba(255,193,7,0.1);">
                        <th style="padding:8px 12px; color:{GOLD}; font-size:10px; text-align:left;">MARCA</th>
                        <th style="padding:8px 12px; color:{GOLD}; font-size:10px; text-align:right;">REAL</th>
                        <th style="padding:8px 12px; color:{GOLD}; font-size:10px; text-align:right;">META</th>
                        <th style="padding:8px 12px; color:{GOLD}; font-size:10px; text-align:right;">CUMPL.</th>
                        <th style="padding:8px 12px; color:{GOLD}; font-size:10px; text-align:center;">PLANTAS</th>
                    </tr>
                    {rows_brand}
                </table>
            </td>
        </tr>

        <!-- BREAKDOWN POR PATRIMONIO AUTONOMO -->
        <tr>
            <td style="padding:0 30px 20px 30px;">
                <div style="color:{GOLD}; font-size:13px; font-weight:bold; text-transform:uppercase; letter-spacing:1px; margin-bottom:8px;">
                    &#127970; Generacion por Patrimonio Autonomo — {month_str}
                </div>
                <table width="100%" cellpadding="0" cellspacing="0" style="background-color:{DARK_BG}; border-radius:8px; overflow:hidden;">
                    <tr style="background-color:rgba(255,193,7,0.1);">
                        <th style="padding:8px 12px; color:{GOLD}; font-size:10px; text-align:left;">PATRIMONIO</th>
                        <th style="padding:8px 12px; color:{GOLD}; font-size:10px; text-align:right;">REAL</th>
                        <th style="padding:8px 12px; color:{GOLD}; font-size:10px; text-align:right;">META</th>
                        <th style="padding:8px 12px; color:{GOLD}; font-size:10px; text-align:right;">CUMPL.</th>
                        <th style="padding:8px 12px; color:{GOLD}; font-size:10px; text-align:center;">PLANTAS</th>
                    </tr>
                    {rows_pa}
                </table>
            </td>
        </tr>

        <!-- TOP 3 GENERACION -->
        <tr>
            <td style="padding:0 30px 20px 30px;">
                <div style="color:{GREEN}; font-size:13px; font-weight:bold; text-transform:uppercase; letter-spacing:1px; margin-bottom:8px;">
                    &#9889; Top 3 — Mayor Generacion (kWh)
                </div>
                <table width="100%" cellpadding="0" cellspacing="0" style="background-color:{DARK_BG}; border-radius:8px; overflow:hidden;">
                    <tr style="background-color:rgba(39,174,96,0.1);">
                        <th style="padding:8px 12px; color:{GREEN}; font-size:10px; text-align:left;">#</th>
                        <th style="padding:8px 12px; color:{GREEN}; font-size:10px; text-align:left;">PLANTA</th>
                        <th style="padding:8px 12px; color:{GREEN}; font-size:10px; text-align:left;">MARCA</th>
                        <th style="padding:8px 12px; color:{GREEN}; font-size:10px; text-align:right;">kWh</th>
                    </tr>
                    {rows_gen}
                </table>
            </td>
        </tr>

        <!-- TOP 3 PERFORMANCE -->
        <tr>
            <td style="padding:0 30px 20px 30px;">
                <div style="color:{GREEN}; font-size:13px; font-weight:bold; text-transform:uppercase; letter-spacing:1px; margin-bottom:8px;">
                    &#127942; Top 3 — Mejor Performance (%)
                </div>
                <table width="100%" cellpadding="0" cellspacing="0" style="background-color:{DARK_BG}; border-radius:8px; overflow:hidden;">
                    <tr style="background-color:rgba(39,174,96,0.1);">
                        <th style="padding:8px 12px; color:{GREEN}; font-size:10px; text-align:left;">#</th>
                        <th style="padding:8px 12px; color:{GREEN}; font-size:10px; text-align:left;">PLANTA</th>
                        <th style="padding:8px 12px; color:{GREEN}; font-size:10px; text-align:left;">MARCA</th>
                        <th style="padding:8px 12px; color:{GREEN}; font-size:10px; text-align:right;">CUMPL.</th>
                    </tr>
                    {rows_perf}
                </table>
            </td>
        </tr>

        <!-- BOTTOM 3 PERFORMANCE -->
        <tr>
            <td style="padding:0 30px 20px 30px;">
                <div style="color:{RED}; font-size:13px; font-weight:bold; text-transform:uppercase; letter-spacing:1px; margin-bottom:8px;">
                    &#9888; Bottom 3 — Menor Performance (%)
                </div>
                <table width="100%" cellpadding="0" cellspacing="0" style="background-color:{DARK_BG}; border-radius:8px; overflow:hidden;">
                    <tr style="background-color:rgba(231,76,60,0.1);">
                        <th style="padding:8px 12px; color:{RED}; font-size:10px; text-align:left;">#</th>
                        <th style="padding:8px 12px; color:{RED}; font-size:10px; text-align:left;">PLANTA</th>
                        <th style="padding:8px 12px; color:{RED}; font-size:10px; text-align:left;">MARCA</th>
                        <th style="padding:8px 12px; color:{RED}; font-size:10px; text-align:right;">CUMPL.</th>
                    </tr>
                    {rows_bottom}
                </table>
            </td>
        </tr>

        <!-- PLANTAS OFFLINE -->
        {offline_section}

        <!-- TABLA COMPLETA DE PLANTAS -->
        <tr>
            <td style="padding:0 30px 24px 30px;">
                <div style="color:{GOLD}; font-size:13px; font-weight:bold; text-transform:uppercase; letter-spacing:1px; margin-bottom:8px;">
                    &#128202; Detalle Completo — Todas las Plantas
                </div>
                <table width="100%" cellpadding="0" cellspacing="0" style="background-color:{DARK_BG}; border-radius:8px; overflow:hidden;">
                    <tr style="background-color:rgba(255,193,7,0.1);">
                        <th style="padding:5px 8px; color:{GOLD}; font-size:9px; text-align:left;">PLANTA</th>
                        <th style="padding:5px 8px; color:{GOLD}; font-size:9px; text-align:left;">MARCA</th>
                        <th style="padding:5px 8px; color:{GOLD}; font-size:9px; text-align:left;">ESTADO</th>
                        <th style="padding:5px 8px; color:{GOLD}; font-size:9px; text-align:right;">REAL</th>
                        <th style="padding:5px 8px; color:{GOLD}; font-size:9px; text-align:right;">META</th>
                        <th style="padding:5px 8px; color:{GOLD}; font-size:9px; text-align:right;">CUMPL.</th>
                    </tr>
                    {rows_plant}
                </table>
            </td>
        </tr>

        <!-- FOOTER -->
        <tr>
            <td style="background:linear-gradient(135deg, {NAVY}, #1a3a5c); padding:20px 30px; text-align:center;">
                <div style="color:{GOLD}; font-size:12px; font-weight:bold;">SunVolt Command Center</div>
                <div style="color:{MUTED}; font-size:10px; margin-top:4px;">Generado automaticamente — {datetime.now(COL_TZ).strftime('%Y-%m-%d %H:%M:%S')} COL</div>
                <div style="color:{MUTED}; font-size:10px; margin-top:2px;">Gestion de Activos Solares | Supervision Tecnica & Financiera</div>
            </td>
        </tr>

    </table>
    </td></tr>
    </table>
    </body>
    </html>
    """
    return html


# ==========================================
# 5. ENVÍO DE EMAIL
# ==========================================

def send_email(html_body, report_date):
    recipients_raw = os.getenv('REPORT_RECIPIENT', '')
    recipients = [r.strip() for r in recipients_raw.split(',') if r.strip()]
    if not recipients:
        raise ValueError("REPORT_RECIPIENT vacio o mal formado")
    subject = f"SunVolt | Reporte Diario {report_date.strftime('%d/%m/%Y')}"

    # Intentar Resend API primero (funciona en VPS donde SMTP está bloqueado)
    resend_key = os.getenv('RESEND_API_KEY', '').strip()
    if resend_key:
        resp = http_requests.post(
            'https://api.resend.com/emails',
            headers={'Authorization': f'Bearer {resend_key}', 'Content-Type': 'application/json'},
            json={
                'from': os.getenv('RESEND_FROM', 'SunVolt Reportes <reportes@sunvolt.com.co>'),
                'to': recipients,
                'subject': subject,
                'html': html_body,
            },
            timeout=30,
        )
        resp.raise_for_status()
        return

    # Fallback: Gmail SMTP (funciona en local/entornos sin bloqueo de puertos)
    smtp_user = os.getenv('SMTP_USER')
    smtp_pass = os.getenv('SMTP_PASSWORD')

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = f"SunVolt Reportes <{smtp_user}>"
    msg['To'] = ', '.join(recipients)
    msg.attach(MIMEText(html_body, 'html'))

    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, recipients, msg.as_string())
    except OSError:
        # Puerto 587 bloqueado, intentar 465
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, recipients, msg.as_string())


# ==========================================
# MAIN
# ==========================================

def main():
    now = datetime.now(COL_TZ)
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] Iniciando reporte diario...")

    load_config()

    # Reportar sobre ayer (el script corre a las 7 AM)
    report_date = (now - timedelta(days=1)).date()
    print(f"  Fecha del reporte: {report_date}")

    conn = get_db()
    try:
        data = fetch_report_data(conn, report_date)
    finally:
        conn.close()

    if data['df_daily'].empty:
        print(f"  AVISO: Sin datos de produccion para {report_date}. El email se envia de todas formas.")

    kpis = process_data(data)
    html = build_html(kpis)
    send_email(html, report_date)

    print(f"  Email enviado a {os.getenv('REPORT_RECIPIENT')}")
    print(f"  Gen dia: {kpis['gen_dia_real']:,.0f} kWh ({kpis['pr_dia']:.1f}%) | Gen mes: {kpis['gen_mes_real']:,.0f} kWh ({kpis['pr_mes']:.1f}%)")
    print(f"  Plantas operando: {kpis['plantas_operando']}/{kpis['total_plants']}")
    print(f"  Plantas offline: {len(kpis['offline'])}")


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"[{datetime.now(COL_TZ).strftime('%Y-%m-%d %H:%M:%S')}] FATAL: {e}")
        traceback.print_exc()
        sys.exit(1)
