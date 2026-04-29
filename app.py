import os
import psycopg2
import psycopg2.extras
import anthropic
import pandas as pd
import plotly.express as px
import streamlit as st
from datetime import date
from dotenv import load_dotenv

load_dotenv()


def get_secret(key, default=None):
    try:
        return st.secrets[key]
    except:
        return os.getenv(key, default)


st.set_page_config(
    page_title="Sistema de Inversión",
    page_icon="📊",
    layout="wide",
)

# ── Conexión ────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        host=get_secret("POSTGRES_HOST", "localhost"),
        port=int(get_secret("POSTGRES_PORT", 5433)),
        dbname=get_secret("POSTGRES_DB", "ndev25"),
        user=get_secret("POSTGRES_USER", "ndev"),
        password=get_secret("POSTGRES_PASSWORD", "ndev"),
    )

@st.cache_data(ttl=300)
def query(sql: str) -> pd.DataFrame:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    return pd.DataFrame(rows)


# ── Queries ─────────────────────────────────────────────────────────────────

SQL_MACRO = """
SELECT estado_macro, score_riesgo, confianza,
       n_verdes, n_amarillos, n_rojos,
       regla_disparada, ultima_actualizacion,
       desempleo, fed_funds, ipc_anual, core_anual,
       curva_10y2y, pib_trim, vix, expectativas_inf, nfci,
       s_desempleo, s_fed, s_ipc, s_core, s_curva, s_pib,
       s_vix, s_expectativas, s_nfci, s_ventas, s_permisos, s_credito
FROM macro.v_diagnostico
LIMIT 1
"""

SQL_MACRO_RAW = """
SELECT
    r.serie_id,
    ms.nombre,
    ms.unidad,
    r.valor,
    r.semaforo,
    r.nota_semaforo,
    r.fecha_dato
FROM macro.macro_raw r
JOIN macro.macro_series ms ON r.serie_id = ms.serie_id
WHERE (r.serie_id, r.fecha_dato) IN (
    SELECT serie_id, MAX(fecha_dato)
    FROM macro.macro_raw
    GROUP BY serie_id
)
ORDER BY r.serie_id
"""

SQL_MACRO_NOTA = """
SELECT resumen, riesgos, outlook,
       score_sentimiento, score_recesion, score_inflacion,
       generado_en
FROM macro.macro_notas_ai
ORDER BY generado_en DESC
LIMIT 1
"""

SQL_SECTOR = """
SELECT estado_macro, top_tickers_aligned, top_tickers_global,
       señal_rotacion, n_aligned, n_leading_strong,
       n_leading_weak, n_neutral, n_lagging, score_universo
FROM sector.v_sector_diagnostico
LIMIT 1
"""

SQL_SECTOR_RANKING = """
SELECT ticker, industria, sector_gics, estado,
       alineacion_macro, score_total, ret_3m, rsi_rs_semanal,
       score_momentum, score_volumen
FROM sector.v_sector_ranking
WHERE tipo = 'industria'
ORDER BY rank_total
"""

SQL_SECTOR_DIAG_TEC = """
SELECT score_defensivos, score_ciclicos, score_mixtos,
       top_3_lideres, top_3_rezagados,
       diagnostico_sector, estado_macro, coherencia, nota
FROM sector.sector_diagnostico_tecnico
ORDER BY fecha DESC
LIMIT 1
"""

SQL_SECTOR_NOTA = """
SELECT resumen, oportunidades, riesgos
FROM sector.sector_notas_ai
ORDER BY generado_en DESC
LIMIT 1
"""

SQL_SECTOR_GICS = """
SELECT ticker, industria, estado, alineacion_macro,
       rsi_rs_semanal, ret_3m, ret_1m, score_total
FROM sector.v_sector_ranking
WHERE tipo = 'sector'
  AND ticker IN ('XLK','XLV','XLF','XLI','XLE',
                 'XLP','XLY','XLC','XLB','XLRE','XLU')
ORDER BY score_total DESC
"""

SQL_SECTOR_TOP_BOTTOM = """
SELECT ticker, industria, rsi_rs_semanal, ret_3m, score_total
FROM sector.v_sector_ranking
ORDER BY score_total DESC
"""

SQL_ETF_SIGNAL = """
SELECT s.ticker, s.señal, s.score, s.score_tecnico,
       s.estado_macro, s.razon,
       e.nombre, e.tipo, e.industria,
       r.rsi_rs_semanal, r.ret_3m, r.ret_6m,
       r.rs_percentil, r.estado, r.alineacion_macro
FROM etf.signal s
JOIN sector.sector_etfs e ON e.ticker = s.ticker
JOIN sector.v_sector_ranking r ON r.ticker = s.ticker
WHERE s.snapshot_date = (SELECT MAX(snapshot_date) FROM etf.signal)
ORDER BY s.score DESC
"""

SQL_MICRO = """
SELECT ticker, sector, industry, market_cap_tier,
       quality_score, value_score, multifactor_score,
       multifactor_rank, multifactor_percentile,
       rsi_14_semanal, precio_vs_ma200,
       volume_ratio_20d, obv_slope,
       momentum_3m, momentum_6m, momentum_12m,
       altman_z_score, altman_zona,
       piotroski_score, piotroski_categoria,
       roic_tendencia, roic_signo, roic_r2, roic_confiable,
       deuda_tendencia, deuda_signo, deuda_r2, deuda_confiable,
       estado_macro, sector_etf, sector_alineado
FROM seleccion.enriquecimiento
WHERE snapshot_date = (SELECT MAX(snapshot_date)
                       FROM seleccion.enriquecimiento)
ORDER BY multifactor_rank
"""

SQL_DIRECCION = """
SELECT ticker, contexto, instrumento, direccion,
       flag_timing, target_position_size, notas_pre_trade,
       snapshot_date
FROM agente.trade_decision_direction
WHERE trade_status = 'active'
ORDER BY target_position_size DESC, ticker
"""

SQL_METRICAS_SISTEMA = """
SELECT
    (SELECT COUNT(*) FROM universos.stock_opciones_2026)                                    AS universo_inicial,
    (SELECT COUNT(*) FROM seleccion.universo
     WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM seleccion.universo))             AS pasan_calidad,
    (SELECT COUNT(*) FROM agente.decision
     WHERE trade_status = 'active'
       AND snapshot_date = (SELECT MAX(snapshot_date) FROM agente.decision))                AS señales_activas,
    (SELECT COUNT(*) FROM agente.top
     WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM agente.top))                     AS top_seleccion
"""

SQL_TOP_SELECCION = """
SELECT d.ticker, d.snapshot_date,
       d.sector, d.industry, d.market_cap_tier,
       d.contexto, d.instrumento, d.flag_timing,
       d.score_conviccion, d.rank_conviccion,
       d.target_position_size, d.trade_status,
       d.sector_alineado, d.estado_macro,
       e.quality_score, e.value_score,
       e.altman_z_score, e.piotroski_score,
       e.rsi_14_semanal, e.precio_vs_ma200,
       e.volume_ratio_20d
FROM agente.decision d
JOIN seleccion.enriquecimiento e
    ON  e.ticker = d.ticker
    AND e.snapshot_date = d.snapshot_date
WHERE d.snapshot_date = (SELECT MAX(snapshot_date) FROM agente.decision)
  AND d.trade_status = 'active'
ORDER BY d.rank_conviccion
"""

SQL_ESTRATEGIA_CARDS = """
SELECT
    t.ticker, t.rank_conviccion, t.score_conviccion,
    t.instrumento, t.contexto, t.flag_timing,
    t.sector_alineado, t.target_position_size,
    e.sector, e.industry, e.market_cap_tier,
    e.quality_score, e.value_score,
    e.rsi_14_semanal, e.precio_vs_ma200,
    e.volume_ratio_20d, e.momentum_3m,
    e.altman_z_score, e.altman_zona,
    e.piotroski_score, e.piotroski_categoria,
    e.roic_signo, e.deuda_signo
FROM agente.top t
JOIN seleccion.enriquecimiento e
    ON  e.ticker = t.ticker
    AND e.snapshot_date = t.snapshot_date
WHERE t.snapshot_date = (SELECT MAX(snapshot_date) FROM agente.top)
ORDER BY t.rank_conviccion
"""

SQL_MACRO_ESTADO = """
SELECT estado_macro, score_riesgo, vix
FROM macro.macro_diagnostico
ORDER BY calculado_en DESC LIMIT 1
"""

SQL_OPCIONES = """
SELECT o.ticker, o.snapshot_date,
       o.direccion, o.contexto, o.tendencia_fundamental,
       o.estado_macro, o.regimen_vix, o.vix,
       o.nivel_iv, o.iv_promedio, o.term_structure,
       o.liquidez, o.estrategia, o.delta_objetivo,
       o.put_strike, o.put_delta, o.put_theta,
       o.put_iv, o.put_dte,
       o.call_strike, o.call_delta, o.call_theta,
       o.sizing, o.trade_status, o.notas,
       e.quality_score, e.value_score,
       e.altman_z_score, e.piotroski_score,
       e.rsi_14_semanal, e.sector, e.industry,
       e.roic_signo, e.deuda_signo
FROM agente_opciones.trade_decision_opciones o
JOIN seleccion.enriquecimiento e
    ON  e.ticker = o.ticker
    AND e.snapshot_date = o.snapshot_date
WHERE o.snapshot_date = (SELECT MAX(snapshot_date)
                         FROM agente_opciones.trade_decision_opciones)
  AND o.trade_status = 'active'
ORDER BY o.sizing DESC
"""


# ── Helpers ─────────────────────────────────────────────────────────────────

ESTADO_COLOR = {
    "EXPANSION":   ("🟢", "#28a745"),
    "RECOVERY":    ("🟡", "#ffc107"),
    "SLOWDOWN":    ("🟡", "#ffc107"),
    "CONTRACTION": ("🔴", "#dc3545"),
}

def semaforo(estado: str) -> tuple[str, str]:
    return ESTADO_COLOR.get(estado.upper(), ("⚪", "#6c757d"))

SEMAFORO_BG = {
    "verde":    ("#057a55", "#f3faf7"),
    "amarillo": ("#c27803", "#fdf6b2"),
    "rojo":     ("#e02424", "#fdf2f2"),
}

def tarjeta_indicador(label: str, valor: str, estado: str,
                      nota: str = "", fecha: str = "") -> str:
    """Devuelve HTML de una tarjeta con color según estado del semáforo."""
    key = str(estado).lower() if estado is not None else ""
    border, bg = SEMAFORO_BG.get(key, ("#6b7280", "#f9fafb"))
    nota_html  = f'<div style="font-size:.75rem;color:#6b7280;margin-top:5px;">{nota}</div>' if nota else ""
    fecha_html = f'<div style="font-size:.7rem;color:#9ca3af;margin-top:3px;">{fecha}</div>' if fecha else ""
    return (
        f'<div style="background:{bg};border-left:5px solid {border};'
        f'padding:12px 14px;border-radius:6px;height:100%;">'
        f'<div style="font-size:.78rem;color:#6b7280;margin-bottom:4px;">{label}</div>'
        f'<div style="font-size:1.3rem;font-weight:700;color:{border};">{valor}</div>'
        f'{nota_html}{fecha_html}'
        f'</div>'
    )


# ── Página 1: MACRO ──────────────────────────────────────────────────────────

def _fmt(val, decimales=2, sufijo="") -> str:
    """Formatea un valor numérico o devuelve '—' si es nulo."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    return f"{float(val):.{decimales}f}{sufijo}"

def pagina_macro():
    st.title("Macro")

    macro    = query(SQL_MACRO)
    raw      = query(SQL_MACRO_RAW)
    nota     = query(SQL_MACRO_NOTA)

    if macro.empty:
        st.error("Sin datos en macro.v_diagnostico.")
        return

    row = macro.iloc[0]
    estado = str(row["estado_macro"])
    emoji, color = semaforo(estado)

    # ── Encabezado de estado ────────────────────────────────────────────────
    col_estado, col_riesgo, col_confianza, col_verdes, col_amarillos, col_rojos = st.columns(6)
    with col_estado:
        st.markdown(
            f'<div style="background:{color}22;border-left:6px solid {color};'
            f'padding:16px;border-radius:6px;">'
            f'<div style="font-size:1.8rem;font-weight:700;color:{color};">{emoji} {estado}</div>'
            f'<div style="color:#888;font-size:.8rem;">Estado macro</div></div>',
            unsafe_allow_html=True,
        )
    with col_riesgo:
        st.metric("Score de riesgo", f"{int(row['score_riesgo'])}/100")
    with col_confianza:
        confianza = row.get("confianza") or "—"
        st.metric("Confianza", confianza.capitalize())
    with col_verdes:
        st.metric("🟢 Verdes",    int(row["n_verdes"]    or 0))
    with col_amarillos:
        st.metric("🟡 Amarillos", int(row["n_amarillos"] or 0))
    with col_rojos:
        st.metric("🔴 Rojos",     int(row["n_rojos"]     or 0))

    fecha = pd.to_datetime(row["ultima_actualizacion"]).strftime("%d/%m/%Y %H:%M") if row.get("ultima_actualizacion") else "—"
    st.caption(f"Última actualización: {fecha}")
    if row.get("regla_disparada"):
        st.caption(f"Regla disparada: *{row['regla_disparada']}*")

    st.divider()

    # ── Semáforos desde macro_raw en grilla 4 columnas ──────────────────────
    st.subheader("Indicadores")

    if not raw.empty:
        # Formateo del valor según unidad
        def fmt_valor(val, unidad: str) -> str:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return "—"
            v = float(val)
            u = (unidad or "").strip().lower()
            if u == "%":
                return f"{v:.2f}%"
            if u == "pp":
                return f"{v:.3f} pp"
            if u == "índice":
                return f"{v:.4f}"
            if u == "miles":
                return f"{v:,.0f} K"
            if u == "m usd":
                return f"${v/1_000_000:.2f}T"
            if u == "puntos":
                return f"{v:.2f}"
            return f"{v:.4f}"

        series = raw.to_dict("records")
        for i in range(0, len(series), 4):
            fila = series[i:i+4]
            cols = st.columns(4)
            for col, s in zip(cols, fila):
                valor_fmt = fmt_valor(s["valor"], s["unidad"])
                fecha_fmt = pd.to_datetime(s["fecha_dato"]).strftime("%d/%m/%Y") if s.get("fecha_dato") else ""
                html = tarjeta_indicador(
                    label  = s["nombre"],
                    valor  = valor_fmt,
                    estado = s.get("semaforo") or "",
                    nota   = s.get("nota_semaforo") or "",
                    fecha  = fecha_fmt,
                )
                col.markdown(html, unsafe_allow_html=True)
            st.markdown("<div style='margin-bottom:10px'></div>", unsafe_allow_html=True)
    else:
        st.info("Sin datos en macro.macro_raw.")

    st.divider()

    # ── Nota AI ─────────────────────────────────────────────────────────────
    st.subheader("Nota AI")
    if not nota.empty:
        n = nota.iloc[0]
        fecha_nota = pd.to_datetime(n["generado_en"]).strftime("%d/%m/%Y %H:%M") if n.get("generado_en") else "—"

        c1, c2, c3, c4 = st.columns(4)
        c1.caption(f"Generado: {fecha_nota}")
        if n.get("score_sentimiento") is not None:
            c2.metric("Sentimiento",  f"{int(n['score_sentimiento'])}/10")
        if n.get("score_recesion") is not None:
            c3.metric("Riesgo recesión", f"{int(n['score_recesion'])}/10")
        if n.get("score_inflacion") is not None:
            c4.metric("Riesgo inflación", f"{int(n['score_inflacion'])}/10")

        if n.get("resumen"):
            st.markdown(f"**Resumen**\n\n{n['resumen']}")

        tab1, tab2, tab3 = st.tabs(["Outlook", "Riesgos", "Nota completa"])
        with tab1:
            st.write(n.get("outlook") or "—")
        with tab2:
            st.write(n.get("riesgos") or "—")
        with tab3:
            nota_df = query("SELECT nota_completa FROM macro.macro_notas_ai ORDER BY generado_en DESC LIMIT 1")
            if not nota_df.empty:
                st.write(nota_df.iloc[0]["nota_completa"] or "—")
    else:
        st.info("Sin notas AI disponibles.")


# ── Página 2: SECTORES ───────────────────────────────────────────────────────

SECTOR_NOMBRE = {
    "XLK":  "Tecnología",
    "XLV":  "Salud",
    "XLF":  "Financiero",
    "XLI":  "Industrial",
    "XLE":  "Energía",
    "XLP":  "Consumo básico",
    "XLY":  "Consumo discrecional",
    "XLC":  "Comunicaciones",
    "XLB":  "Materiales",
    "XLRE": "Inmobiliario",
    "XLU":  "Utilities",
}

ESTADO_SECTOR = {
    "LEADING_STRONG": ("🟢", "#f0fdf4", "#16a34a"),
    "LEADING_WEAK":   ("🟡", "#fefce8", "#ca8a04"),
    "NEUTRAL":        ("⬜", "#f9fafb", "#9ca3af"),
    "LAGGING":        ("🔴", "#fef2f2", "#dc2626"),
}

DIAG_BANNER = {
    "CONFIRMA_SLOWDOWN":    ("🔵", "El mercado busca refugio",              "#eff6ff", "#2563eb", "#1e3a8a"),
    "CONFIRMA_EXPANSION":   ("🟢", "El mercado está en modo ofensivo",      "#f0fdf4", "#16a34a", "#14532d"),
    "SEÑAL_MIXTA":          ("🟡", "Sin dirección clara en sectores",        "#fefce8", "#ca8a04", "#713f12"),
    "ROTACION_DEFENSIVA":   ("🟠", "Rotación hacia sectores defensivos",     "#fff7ed", "#ea580c", "#7c2d12"),
    "CONFIRMA_CONTRACTION": ("🔴", "El mercado está en contracción",         "#fef2f2", "#dc2626", "#7f1d1d"),
}

# (bg, color_texto, border)
SEÑAL_BADGE_STYLE = {
    "COMPRAR":          ("#166534", "#ffffff", "#166534"),
    "INTERESANTE":      ("#f0fdf4", "#166534", "#16a34a"),
    "MONITOREAR":       ("#fefce8", "#92400e", "#ca8a04"),
    "ESPERAR_PULLBACK": ("#fff7ed", "#c2410c", "#ea580c"),
    "EVITAR":           ("#fef2f2", "#991b1b", "#dc2626"),
    "NEUTRAL":          ("#f9fafb", "#6b7280", "#9ca3af"),
}

TIPO_LABEL = {
    "sector_gics":   "Sector",
    "industria":     "Temático",
    "commodity":     "Commodity",
    "internacional": "Internacional",
    "renta_fija":    "Renta Fija",
    "refugio":       "Refugio",
    "benchmark":     "Benchmark",
}


def _render_tabla_etfs(df: pd.DataFrame, key_prefix: str, con_filtros: bool = True):
    """Tabla completa de ETFs con filtros opcionales."""
    if df.empty:
        st.info("Sin datos de señales ETF.")
        return

    for col_num in ["score", "score_tecnico", "rsi_rs_semanal", "ret_3m", "ret_6m", "rs_percentil"]:
        if col_num in df.columns:
            df[col_num] = pd.to_numeric(df[col_num], errors="coerce")

    dff = df.copy()

    if con_filtros:
        fc1, fc2 = st.columns(2)
        with fc1:
            opts_tipo = ["Todos"] + sorted(df["tipo"].dropna().unique().tolist())
            f_tipo = st.selectbox("Tipo", opts_tipo, key=f"{key_prefix}_tipo")
        with fc2:
            opts_señal = ["Todas", "COMPRAR", "INTERESANTE", "MONITOREAR", "EVITAR", "ESPERAR_PULLBACK", "NEUTRAL"]
            f_señal = st.selectbox("Señal", opts_señal, key=f"{key_prefix}_señal")
        if f_tipo  != "Todos":  dff = dff[dff["tipo"]  == f_tipo]
        if f_señal != "Todas":  dff = dff[dff["señal"] == f_señal]

    st.caption(f"{len(dff)} ETF(s) mostrado(s)")

    # Encabezado
    h0, h1, h2, h3, h4, h5, h6 = st.columns([1.2, 3, 1.5, 2, 1.2, 1.5, 2])
    for col, txt in [(h0, "ETF"), (h1, "Nombre"), (h2, "Tipo"), (h3, "Señal"),
                     (h4, "Score"), (h5, "RSI"), (h6, "Ret 3M")]:
        col.markdown(f"**{txt}**")
    st.markdown("<hr style='margin:4px 0 6px;border-color:#e5e7eb;'>", unsafe_allow_html=True)

    for _, row in dff.iterrows():
        ticker  = str(row.get("ticker") or "")
        nombre  = str(row.get("nombre") or "—")[:28]
        tipo    = TIPO_LABEL.get(str(row.get("tipo") or ""), str(row.get("tipo") or "—"))
        señal   = str(row.get("señal") or "NEUTRAL")
        score   = row.get("score")
        rsi     = row.get("rsi_rs_semanal")
        ret3m   = row.get("ret_3m")

        bg_s, txt_s, brd_s = SEÑAL_BADGE_STYLE.get(señal, ("#f9fafb", "#6b7280", "#9ca3af"))
        score_f = f"{float(score):.1f}" if score is not None and not pd.isna(score) else "—"
        rsi_f   = f"{float(rsi):.0f}"   if rsi   is not None and not pd.isna(rsi)   else "—"
        ret_v   = float(ret3m) if ret3m is not None and not pd.isna(ret3m) else None
        ret_f   = f"{ret_v:+.1f}%" if ret_v is not None else "—"
        ret_col = "#057a55" if ret_v is not None and ret_v >= 0 else "#e02424"

        c0, c1, c2, c3, c4, c5, c6 = st.columns([1.2, 3, 1.5, 2, 1.2, 1.5, 2])
        c0.markdown(f"`{ticker}`")
        c1.markdown(nombre)
        c2.markdown(f'<span style="font-size:.8rem;color:#6b7280;">{tipo}</span>', unsafe_allow_html=True)
        c3.markdown(
            f'<span style="background:{bg_s};color:{txt_s};border:1px solid {brd_s};'
            f'padding:2px 8px;border-radius:10px;font-size:.78rem;font-weight:600;">{señal}</span>',
            unsafe_allow_html=True,
        )
        c4.markdown(f"**{score_f}**")
        c5.markdown(rsi_f)
        c6.markdown(
            f'<span style="color:{ret_col};font-weight:600;">{ret_f}</span>',
            unsafe_allow_html=True,
        )


def pagina_sectores():
    st.title("Sectores")

    diag_tec = query(SQL_SECTOR_DIAG_TEC)
    df_gics  = query(SQL_SECTOR_GICS)
    df_all   = query(SQL_SECTOR_TOP_BOTTOM)
    nota     = query(SQL_SECTOR_NOTA)
    df_etf   = query(SQL_ETF_SIGNAL)

    # ── Banner diagnóstico (siempre visible, encima de tabs) ─────────────────
    if not diag_tec.empty:
        dt     = diag_tec.iloc[0]
        diag_k = str(dt.get("diagnostico_sector") or "SEÑAL_MIXTA").upper()
        emoji, titulo, bg, border, texto_color = DIAG_BANNER.get(
            diag_k, ("🟡", diag_k, "#fefce8", "#ca8a04", "#713f12")
        )
        nota_txt = str(dt.get("nota") or "")
        st.markdown(
            f'<div style="background:{bg};border:2px solid {border};'
            f'border-radius:12px;padding:20px 28px;margin-bottom:16px;">'
            f'<div style="font-size:1.6rem;font-weight:800;color:{texto_color};">'
            f'{emoji} {titulo}</div>'
            f'<div style="font-size:1rem;color:{texto_color};margin-top:6px;opacity:.85;">'
            f'{nota_txt}</div></div>',
            unsafe_allow_html=True,
        )
        m1, m2, m3, m4 = st.columns(4)
        def _fmt(v, dec=1):
            return f"{float(v):.{dec}f}" if v is not None and not pd.isna(v) else "—"
        m1.metric("RSI Defensivos", _fmt(dt.get("score_defensivos")))
        m2.metric("RSI Cíclicos",   _fmt(dt.get("score_ciclicos")))
        m3.metric("RSI Mixtos",     _fmt(dt.get("score_mixtos")))
        coh = str(dt.get("coherencia") or "—")
        coh_emoji = {"ALTA": "✅", "MEDIA": "⚠️", "CONTRADICE": "❌"}.get(coh, "—")
        m4.metric("Coherencia macro", f"{coh_emoji} {coh}")
    else:
        st.info("Sin datos en sector.sector_diagnostico_tecnico.")

    st.divider()

    # ── Tabs principales ─────────────────────────────────────────────────────
    tab_gics, tab_señal, tab_todos = st.tabs(
        ["Sectores GICS", "Señales ETF", "Ver todos los ETFs"]
    )

    # ════════════════════════════════════════════════════════════════════════
    # TAB 1 — Sectores GICS
    # ════════════════════════════════════════════════════════════════════════
    with tab_gics:
        st.subheader("Los 11 sectores del mercado")

        if not df_gics.empty:
            for col_num in ["rsi_rs_semanal", "ret_3m", "ret_1m", "score_total"]:
                if col_num in df_gics.columns:
                    df_gics[col_num] = pd.to_numeric(df_gics[col_num], errors="coerce")

            h0, h1, h2, h3, h4, h5 = st.columns([3, 1.2, 2, 1.2, 1.5, 2])
            for col, txt in [(h0, "Sector"), (h1, "ETF"), (h2, "Estado"),
                              (h3, "RSI"), (h4, "Ret 3M"), (h5, "Alineación")]:
                col.markdown(f"**{txt}**")
            st.markdown("<hr style='margin:4px 0 8px;border-color:#e5e7eb;'>", unsafe_allow_html=True)

            for _, row in df_gics.iterrows():
                ticker  = str(row.get("ticker") or "")
                estado  = str(row.get("estado") or "NEUTRAL").upper()
                alin    = str(row.get("alineacion_macro") or "")
                rsi     = row.get("rsi_rs_semanal")
                ret3m   = row.get("ret_3m")
                nombre  = SECTOR_NOMBRE.get(ticker, ticker)

                icono_e, bg_e, color_e = ESTADO_SECTOR.get(estado, ("⬜", "#f9fafb", "#9ca3af"))
                rsi_f   = f"{float(rsi):.1f}" if rsi is not None and not pd.isna(rsi) else "—"
                ret3m_v = float(ret3m) if ret3m is not None and not pd.isna(ret3m) else None
                ret3m_f = f"{ret3m_v:+.1f}%" if ret3m_v is not None else "—"
                ret_col = "#057a55" if ret3m_v is not None and ret3m_v >= 0 else "#e02424"
                alin_txt = "✅ Alineado" if alin == "ALIGNED" else "⬜ Neutral"

                c0, c1, c2, c3, c4, c5 = st.columns([3, 1.2, 2, 1.2, 1.5, 2])
                c0.markdown(f"**{nombre}**")
                c1.markdown(f"`{ticker}`")
                c2.markdown(
                    f'<span style="background:{bg_e};color:{color_e};border:1px solid {color_e};'
                    f'padding:2px 10px;border-radius:12px;font-size:.8rem;font-weight:600;">'
                    f'{icono_e} {estado.replace("_", " ").title()}</span>',
                    unsafe_allow_html=True,
                )
                c3.markdown(rsi_f)
                c4.markdown(
                    f'<span style="color:{ret_col};font-weight:600;">{ret3m_f}</span>',
                    unsafe_allow_html=True,
                )
                c5.markdown(alin_txt)
        else:
            st.info("Sin datos en sector.v_sector_ranking.")

        st.divider()

        # Top 3 / Bottom 3
        if not df_all.empty:
            for col_num in ["rsi_rs_semanal", "ret_3m", "score_total"]:
                if col_num in df_all.columns:
                    df_all[col_num] = pd.to_numeric(df_all[col_num], errors="coerce")
            df_sorted = df_all.dropna(subset=["score_total"]).sort_values("score_total", ascending=False)
            top3    = df_sorted.head(3)
            bottom3 = df_sorted.tail(3).iloc[::-1]

            col_top, col_bot = st.columns(2)

            def _card_etf(col, titulo, df_sub):
                with col:
                    st.markdown(f"### {titulo}")
                    for _, row in df_sub.iterrows():
                        tk  = str(row.get("ticker") or "")
                        ind = str(row.get("industria") or "—")
                        rsi = row.get("rsi_rs_semanal")
                        r3m = row.get("ret_3m")
                        rsi_f   = f"{float(rsi):.1f}" if rsi is not None and not pd.isna(rsi) else "—"
                        r3m_v   = float(r3m) if r3m is not None and not pd.isna(r3m) else None
                        r3m_f   = f"{r3m_v:+.1f}%" if r3m_v is not None else "—"
                        r3m_col = "#057a55" if r3m_v is not None and r3m_v >= 0 else "#e02424"
                        with st.container(border=True):
                            st.markdown(f"**{tk}** — {ind[:35]}")
                            ca, cb = st.columns(2)
                            ca.markdown(f"RSI: **{rsi_f}**")
                            cb.markdown(
                                f'Ret 3M: <span style="color:{r3m_col};font-weight:700;">{r3m_f}</span>',
                                unsafe_allow_html=True,
                            )

            _card_etf(col_top, "🏆 Liderando esta semana", top3)
            _card_etf(col_bot, "⚠️ Rezagados esta semana", bottom3)

        st.divider()

        # Nota AI
        st.subheader("Nota sectorial AI")
        if not nota.empty:
            n = nota.iloc[0]
            if n.get("resumen"):
                st.markdown(n["resumen"])
            tab_op, tab_ri = st.tabs(["Oportunidades", "Riesgos"])
            with tab_op:
                st.write(n.get("oportunidades") or "—")
            with tab_ri:
                st.write(n.get("riesgos") or "—")
        else:
            st.info("Sin notas sectoriales disponibles.")

    # ════════════════════════════════════════════════════════════════════════
    # TAB 2 — Señales ETF
    # ════════════════════════════════════════════════════════════════════════
    with tab_señal:
        if df_etf.empty:
            st.info("Sin datos en etf.signal. Corré etf/etf_signal.py primero.")
        else:
            for col_num in ["score", "score_tecnico", "rsi_rs_semanal", "ret_3m", "ret_6m"]:
                if col_num in df_etf.columns:
                    df_etf[col_num] = pd.to_numeric(df_etf[col_num], errors="coerce")

            # Sección A — Métricas resumen
            n_total      = len(df_etf)
            n_positivos  = int(df_etf["señal"].isin(["COMPRAR", "INTERESANTE"]).sum())
            n_monitorear = int((df_etf["señal"] == "MONITOREAR").sum())
            n_evitar     = int(df_etf["señal"].isin(["EVITAR", "ESPERAR_PULLBACK"]).sum())

            ma, mb, mc, md = st.columns(4)
            ma.metric("ETFs analizados",  n_total)
            mb.metric("Interesantes",     n_positivos)
            mc.metric("Monitorear",       n_monitorear)
            md.metric("Evitar",           n_evitar)

            st.divider()

            # Sección B — Cards por señal (solo COMPRAR, INTERESANTE, MONITOREAR)
            ORDEN_CARDS = ["COMPRAR", "INTERESANTE", "MONITOREAR"]
            df_cards = df_etf[df_etf["señal"].isin(ORDEN_CARDS)].copy()

            if not df_cards.empty:
                cols_grid = st.columns(3)
                for i, (_, row) in enumerate(df_cards.iterrows()):
                    ticker  = str(row.get("ticker") or "")
                    nombre  = str(row.get("nombre") or ticker)
                    tipo    = TIPO_LABEL.get(str(row.get("tipo") or ""), "—")
                    señal   = str(row.get("señal") or "NEUTRAL")
                    score   = row.get("score")
                    rsi     = row.get("rsi_rs_semanal")
                    ret3m   = row.get("ret_3m")
                    alin    = str(row.get("alineacion_macro") or "")
                    razon   = str(row.get("razon") or "")

                    bg_s, txt_s, brd_s = SEÑAL_BADGE_STYLE.get(señal, ("#f9fafb", "#6b7280", "#9ca3af"))
                    score_v = float(score) if score is not None and not pd.isna(score) else 0.0
                    score_n = min(max(score_v / 100, 0), 1)
                    score_f = f"{score_v:.0f}"
                    rsi_f   = f"{float(rsi):.0f}" if rsi is not None and not pd.isna(rsi) else "—"
                    ret_v   = float(ret3m) if ret3m is not None and not pd.isna(ret3m) else None
                    ret_f   = f"{ret_v:+.1f}%" if ret_v is not None else "—"
                    ret_col = "#057a55" if ret_v is not None and ret_v >= 0 else "#e02424"
                    alin_txt = "✅ Alineado con macro" if alin == "ALIGNED" else "⬜ Sin alineación macro"

                    with cols_grid[i % 3]:
                        with st.container(border=True):
                            st.markdown(
                                f'<div style="font-size:1.15rem;font-weight:800;">{nombre[:30]}</div>'
                                f'<div style="font-size:.8rem;color:#6b7280;">{ticker} · {tipo}</div>',
                                unsafe_allow_html=True,
                            )
                            st.markdown(
                                f'<span style="background:{bg_s};color:{txt_s};border:1px solid {brd_s};'
                                f'padding:3px 12px;border-radius:12px;font-size:.82rem;font-weight:700;">'
                                f'{señal}</span>',
                                unsafe_allow_html=True,
                            )
                            st.progress(score_n, text=f"Convicción: {score_f}/100")
                            st.markdown(
                                f'RSI: **{rsi_f}** &nbsp;|&nbsp; '
                                f'Ret 3M: <span style="color:{ret_col};font-weight:600;">{ret_f}</span>',
                                unsafe_allow_html=True,
                            )
                            st.markdown(alin_txt)
                            if razon:
                                st.caption(razon)

            st.divider()

            # Sección C — Tabla completa expandible
            with st.expander("Ver análisis completo — 75 ETFs"):
                _render_tabla_etfs(df_etf.copy(), key_prefix="exp", con_filtros=False)

    # ════════════════════════════════════════════════════════════════════════
    # TAB 3 — Ver todos los ETFs
    # ════════════════════════════════════════════════════════════════════════
    with tab_todos:
        st.subheader("Todos los ETFs con señal")
        _render_tabla_etfs(df_etf.copy(), key_prefix="tab3", con_filtros=True)


# ── Página 3: MICRO ──────────────────────────────────────────────────────────

ALTMAN_ZONA_COLOR = {
    "safe":     ("#057a55", "#f3faf7"),
    "grey":     ("#c27803", "#fefce8"),
    "distress": ("#e02424", "#fef2f2"),
}

SIGNO_LABEL = {1: "↑", 0: "→", -1: "↓"}


def pagina_micro():
    st.title("Micro — Universo de trabajo")

    df = query(SQL_MICRO)

    if df.empty:
        st.info("Sin datos en seleccion.enriquecimiento.")
        return

    # Conversiones numéricas
    for col in ["quality_score", "value_score", "multifactor_score", "multifactor_rank",
                "multifactor_percentile", "rsi_14_semanal", "precio_vs_ma200",
                "volume_ratio_20d", "momentum_3m", "momentum_6m", "momentum_12m",
                "altman_z_score", "piotroski_score", "roic_signo", "deuda_signo"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # ── Métricas resumen ─────────────────────────────────────────────────────
    n_aligned       = int((df["sector_alineado"] == "ALIGNED").sum())
    n_roic_mejora   = int((df["roic_signo"] == 1).sum())
    n_deuda_baja    = int((df["deuda_signo"] == -1).sum())
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Empresas en universo", len(df))
    c2.metric("Sector ALIGNED",       n_aligned)
    c3.metric("ROIC mejorando",        n_roic_mejora)
    c4.metric("Deuda bajando",         n_deuda_baja)

    st.divider()

    # ── Filtros ──────────────────────────────────────────────────────────────
    ff1, ff2, ff3, ff4, ff5 = st.columns(5)
    with ff1:
        opts_sec = ["Todos"] + sorted(df["sector"].dropna().unique().tolist())
        f_sec = st.selectbox("Sector", opts_sec)
    with ff2:
        opts_cap = ["Todos"] + sorted(df["market_cap_tier"].dropna().unique().tolist())
        f_cap = st.selectbox("Market cap", opts_cap)
    with ff3:
        opts_az = ["Todos"] + sorted(df["altman_zona"].dropna().unique().tolist())
        f_az = st.selectbox("Altman zona", opts_az)
    with ff4:
        opts_pio = ["Todos"] + sorted(df["piotroski_categoria"].dropna().unique().tolist())
        f_pio = st.selectbox("Piotroski cat.", opts_pio)
    with ff5:
        opts_alin = ["Todos"] + sorted(df["sector_alineado"].dropna().unique().tolist())
        f_alin = st.selectbox("Alineado", opts_alin)

    dff = df.copy()
    if f_sec  != "Todos": dff = dff[dff["sector"]            == f_sec]
    if f_cap  != "Todos": dff = dff[dff["market_cap_tier"]   == f_cap]
    if f_az   != "Todos": dff = dff[dff["altman_zona"]       == f_az]
    if f_pio  != "Todos": dff = dff[dff["piotroski_categoria"] == f_pio]
    if f_alin != "Todos": dff = dff[dff["sector_alineado"]   == f_alin]

    st.caption(f"{len(dff)} empresa(s) mostrada(s)")

    # ── Tabla principal ──────────────────────────────────────────────────────
    def zona_badge(zona: str | None) -> str:
        if not zona:
            return "—"
        color, bg = ALTMAN_ZONA_COLOR.get(zona, ("#6b7280", "#f9fafb"))
        return f'<span style="background:{bg};color:{color};border:1px solid {color};padding:1px 8px;border-radius:10px;font-size:.72rem;font-weight:600;">{zona}</span>'

    for _, row in dff.iterrows():
        ticker   = str(row.get("ticker") or "")
        sector_s = str(row.get("sector") or "—")[:24]
        cap      = str(row.get("market_cap_tier") or "—")
        az_zona  = str(row.get("altman_zona") or "")
        alin     = str(row.get("sector_alineado") or "")
        rank_v   = row.get("multifactor_rank")
        multi_v  = row.get("multifactor_score")
        rank_f   = f"#{int(rank_v)}" if pd.notna(rank_v) else "—"
        multi_f  = f"{float(multi_v):.1f}" if pd.notna(multi_v) else "—"
        alin_ico = "✅" if alin == "ALIGNED" else "⬜"

        titulo = (
            f"{rank_f}  {ticker}  ·  {sector_s}  ·  {cap}  ·  "
            f"{alin_ico}  ·  Score {multi_f}"
        )

        with st.expander(titulo):
            az_v = row.get("altman_z_score")
            if pd.notna(az_v):
                az_color, az_bg = ALTMAN_ZONA_COLOR.get(az_zona, ("#6b7280", "#f9fafb"))
                st.markdown(
                    f'<div style="background:{az_bg};border-left:4px solid {az_color};'
                    f'padding:4px 10px;border-radius:4px;margin-bottom:8px;font-size:.8rem;">'
                    f'Altman Z: <strong>{float(az_v):.2f}</strong> — {zona_badge(az_zona)}</div>',
                    unsafe_allow_html=True,
                )

            col_a, col_b, col_c, col_d = st.columns(4)

            # Col A — Scores
            with col_a:
                st.markdown("**Scores**")
                qs = row.get("quality_score")
                vs = row.get("value_score")
                ms = row.get("multifactor_score")
                st.metric("Quality",     f"{float(qs):.1f}"  if pd.notna(qs) else "—")
                st.metric("Value",       f"{float(vs):.1f}"  if pd.notna(vs) else "—")
                st.metric("Multifactor", f"{float(ms):.1f}"  if pd.notna(ms) else "—")

            # Col B — Técnico
            with col_b:
                st.markdown("**Técnico**")
                rsi = row.get("rsi_14_semanal")
                ma  = row.get("precio_vs_ma200")
                m3  = row.get("momentum_3m")
                m6  = row.get("momentum_6m")
                if pd.notna(rsi):
                    rv = float(rsi)
                    rc = "#057a55" if 40 <= rv <= 65 else ("#e02424" if rv < 40 else "#c27803")
                    st.markdown(f'RSI 14s: <span style="color:{rc};font-weight:700;">{rv:.1f}</span>', unsafe_allow_html=True)
                else:
                    st.markdown("RSI 14s: —")
                if pd.notna(ma):
                    mv = float(ma); mc = "#057a55" if mv >= 0 else "#e02424"
                    st.markdown(f'vs MA200: <span style="color:{mc};font-weight:700;">{mv:+.2f}%</span>', unsafe_allow_html=True)
                else:
                    st.markdown("vs MA200: —")
                st.markdown(f"Mom 3m: **{f'{float(m3):.1f}%' if pd.notna(m3) else '—'}**")
                st.markdown(f"Mom 6m: **{f'{float(m6):.1f}%' if pd.notna(m6) else '—'}**")

            # Col C — Salud / signos
            with col_c:
                st.markdown("**Salud**")
                ps = row.get("piotroski_score")
                rs = row.get("roic_signo")
                rc_conf = row.get("roic_confiable")
                ds = row.get("deuda_signo")
                dc_conf = row.get("deuda_confiable")
                if pd.notna(ps):
                    pv = int(ps)
                    pc = "#057a55" if pv >= 7 else ("#c27803" if pv >= 4 else "#e02424")
                    st.markdown(f'Piotroski: <span style="color:{pc};font-weight:700;">{pv}</span>', unsafe_allow_html=True)
                else:
                    st.markdown("Piotroski: —")
                roic_lbl = SIGNO_LABEL.get(int(rs) if pd.notna(rs) else None, "—")
                deuda_lbl = SIGNO_LABEL.get(int(ds) if pd.notna(ds) else None, "—")
                conf_r = "✓" if rc_conf else "—"
                conf_d = "✓" if dc_conf else "—"
                st.markdown(f"ROIC: **{roic_lbl}** {conf_r}")
                st.markdown(f"Deuda: **{deuda_lbl}** {conf_d}")

            # Col D — Regresiones
            with col_d:
                st.markdown("**Regresiones**")
                for lbl, trend_col, r2_col, conf_col in [
                    ("ROIC",  "roic_tendencia",  "roic_r2",  "roic_confiable"),
                    ("Deuda", "deuda_tendencia", "deuda_r2", "deuda_confiable"),
                ]:
                    tv = row.get(trend_col)
                    r2 = row.get(r2_col)
                    cv = row.get(conf_col)
                    tv_f = f"{float(tv):+.4f}" if pd.notna(tv) else "—"
                    r2_f = f"R²={float(r2):.2f}" if pd.notna(r2) else ""
                    cv_f = " ✓" if cv else ""
                    st.markdown(f"{lbl}: **{tv_f}** {r2_f}{cv_f}")

            ind = str(row.get("industry") or "—")
            st.caption(f"{sector_s} · {ind} · {cap}")


# ── Página 4: ESTRATEGIA ─────────────────────────────────────────────────────

# Helpers de badges para esta página
def _badge(texto: str, color: str, bg: str) -> str:
    return (
        f'<span style="background:{bg};color:{color};border:1px solid {color};'
        f'padding:2px 10px;border-radius:12px;font-size:.75rem;font-weight:600;">'
        f'{texto}</span>'
    )

FLAG_TIMING_ICONO = {
    "tecnico_confirmado": "🟢",
    "pullback_comprable": "🟡",
    "macro_defensivo":    "🔵",
    "fundamental_only":   "⚪",
}

INSTRUMENTO_STYLE = {
    "stock":            ("#057a55", "#f3faf7", "Stock"),
    "cash_secured_put": ("#1d4ed8", "#eff6ff", "CSP"),
}

CONTEXTO_LABEL = {
    "structural_quality":  "Empresa de alta calidad",
    "solid_but_expensive": "Sólida pero cara",
    "improving":           "En proceso de mejora",
    "structural_neutral":  "Neutral estructural",
    "structural_risk":     "Riesgo estructural",
}

BANNER_CONFIG = {
    "SLOWDOWN": {
        "emoji": "🟡", "titulo": "El mercado está frenando",
        "bg": "#fefce8", "border": "#ca8a04", "texto_color": "#713f12",
        "descripcion": (
            "El sistema recomienda estrategias defensivas de ingreso. "
            "Vendemos puts sobre empresas sólidas para cobrar prima "
            "mientras esperamos mejor momento para comprar acciones."
        ),
    },
    "EXPANSION": {
        "emoji": "🟢", "titulo": "El mercado está creciendo",
        "bg": "#f0fdf4", "border": "#16a34a", "texto_color": "#14532d",
        "descripcion": (
            "El sistema recomienda comprar acciones de empresas "
            "de calidad con buen momentum técnico."
        ),
    },
    "CONTRACTION": {
        "emoji": "🔴", "titulo": "El mercado está contrayéndose",
        "bg": "#fef2f2", "border": "#dc2626", "texto_color": "#7f1d1d",
        "descripcion": (
            "El sistema recomienda máxima cautela. "
            "Solo estrategias muy defensivas."
        ),
    },
    "RECOVERY": {
        "emoji": "🔵", "titulo": "El mercado está recuperándose",
        "bg": "#eff6ff", "border": "#2563eb", "texto_color": "#1e3a8a",
        "descripcion": (
            "El sistema empieza a identificar oportunidades "
            "selectivas en empresas de calidad."
        ),
    },
}


def estrellas(score) -> str:
    if score is None or (isinstance(score, float) and pd.isna(score)):
        return "☆☆☆☆☆"
    s = float(score)
    if s >= 85: return "⭐⭐⭐⭐⭐"
    if s >= 75: return "⭐⭐⭐⭐☆"
    if s >= 60: return "⭐⭐⭐☆☆"
    if s >= 40: return "⭐⭐☆☆☆"
    if s >= 20: return "⭐☆☆☆☆"
    return "☆☆☆☆☆"


def estrellas_salud(altman_zona, piotroski_score) -> str:
    az = str(altman_zona or "").lower()
    ps = int(piotroski_score) if piotroski_score is not None and not (isinstance(piotroski_score, float) and pd.isna(piotroski_score)) else 0
    if az == "safe" and ps >= 7:  return "⭐⭐⭐⭐⭐"
    if az == "safe" and ps >= 5:  return "⭐⭐⭐⭐☆"
    if az == "grey" and ps >= 7:  return "⭐⭐⭐☆☆"
    if az == "grey":               return "⭐⭐☆☆☆"
    return "⭐☆☆☆☆"


def _render_vista_simple(df_cards: pd.DataFrame, macro_row):
    """Sección 1: banner + métricas. Sección 2: cards en grilla de 3."""

    estado_macro = str(macro_row.get("estado_macro") or "SLOWDOWN") if macro_row is not None else "SLOWDOWN"
    cfg = BANNER_CONFIG.get(estado_macro, BANNER_CONFIG["SLOWDOWN"])

    # ── Banner grande ────────────────────────────────────────────────────────
    st.markdown(
        f"""<div style="background:{cfg['bg']};border:2px solid {cfg['border']};
        border-radius:12px;padding:20px 28px;margin-bottom:16px;">
        <div style="font-size:1.6rem;font-weight:800;color:{cfg['texto_color']};">
            {cfg['emoji']} {cfg['titulo']}
        </div>
        <div style="font-size:1rem;color:{cfg['texto_color']};margin-top:6px;opacity:.85;">
            {cfg['descripcion']}
        </div></div>""",
        unsafe_allow_html=True,
    )

    # ── 3 métricas simples ───────────────────────────────────────────────────
    n_universo = query("SELECT COUNT(*) AS n FROM seleccion.universo WHERE snapshot_date=(SELECT MAX(snapshot_date) FROM seleccion.universo)")
    n_activas  = query("SELECT COUNT(*) AS n FROM agente.decision WHERE trade_status='active' AND snapshot_date=(SELECT MAX(snapshot_date) FROM agente.decision)")
    instr_dom  = query("SELECT instrumento, COUNT(*) AS n FROM agente.decision WHERE trade_status='active' AND snapshot_date=(SELECT MAX(snapshot_date) FROM agente.decision) GROUP BY instrumento ORDER BY n DESC LIMIT 1")

    n_univ  = int(n_universo.iloc[0]["n"]) if not n_universo.empty else 0
    n_act   = int(n_activas.iloc[0]["n"])  if not n_activas.empty  else 0
    dom_raw = str(instr_dom.iloc[0]["instrumento"]) if not instr_dom.empty else ""
    dom_lbl = "Venta de puts" if dom_raw == "cash_secured_put" else ("Compra de acciones" if dom_raw == "stock" else dom_raw)

    m1, m2, m3 = st.columns(3)
    m1.metric("Empresas analizadas", n_univ)
    m2.metric("Oportunidades activas", n_act)
    m3.metric("Estrategia dominante", dom_lbl)

    st.divider()

    if df_cards.empty:
        st.info("Sin empresas en agente.top.")
        return

    # ── Cards en grilla de 3 ─────────────────────────────────────────────────
    cols_grid = st.columns(3)

    for i, (_, row) in enumerate(df_cards.iterrows()):
        ticker    = str(row.get("ticker") or "")
        sector_s  = str(row.get("sector") or "—")
        industry  = str(row.get("industry") or "—")
        cap       = str(row.get("market_cap_tier") or "—")
        instrumen = str(row.get("instrumento") or "")
        timing    = str(row.get("flag_timing") or "")
        alin      = str(row.get("sector_alineado") or "")
        score     = row.get("score_conviccion")
        roic_s    = row.get("roic_signo")
        deuda_s   = row.get("deuda_signo")

        # Scores y salud para estrellas
        qs        = row.get("quality_score")
        vs        = row.get("value_score")
        az_zona   = str(row.get("altman_zona") or "")
        ps        = row.get("piotroski_score")

        # Score de convicción
        score_v    = float(score) if score is not None and not pd.isna(score) else 0
        score_norm = min(max(score_v / 100, 0), 1)
        score_f    = f"{score_v:.0f}/100"

        # Checkmarks
        ck_empresa = (roic_s is not None and not pd.isna(roic_s) and int(roic_s) == 1
                      and deuda_s is not None and not pd.isna(deuda_s) and int(deuda_s) == -1)
        ck_sector  = alin == "ALIGNED"
        ck_timing  = timing in ("tecnico_confirmado", "pullback_comprable")

        # Etiqueta instrumento
        if instrumen == "cash_secured_put":
            instr_label = "📋 Cash Secured Put"
        elif instrumen == "bull_put_spread":
            instr_label = "📋 Bull Put Spread"
        elif instrumen == "stock":
            instr_label = "📈 Stock"
        else:
            instr_label = f"📋 {instrumen.replace('_', ' ').title()}"

        with cols_grid[i % 3]:
            with st.container(border=True):
                # ── Header ──────────────────────────────────────────────────
                st.markdown(
                    f'<div style="font-size:1.25rem;font-weight:800;line-height:1.2;">{ticker}</div>'
                    f'<div style="color:#6b7280;font-size:.8rem;margin-bottom:2px;">'
                    f'{sector_s} · {cap}</div>',
                    unsafe_allow_html=True,
                )
                st.caption(industry[:40])

                # ── Estrategia ──────────────────────────────────────────────
                st.markdown(f"**{instr_label}**")

                # ── Estrellas ───────────────────────────────────────────────
                st.markdown(
                    f"Calidad:&nbsp;&nbsp; {estrellas(qs)}  \n"
                    f"Valor:&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; {estrellas(vs)}  \n"
                    f"Salud:&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; {estrellas_salud(az_zona, ps)}"
                )

                # ── Checkmarks ──────────────────────────────────────────────
                st.markdown(
                    ("✅" if ck_empresa else "⚠️") + " Mejorando estructuralmente  \n" +
                    ("✅" if ck_sector  else "⚠️") + " Sector alineado  \n" +
                    ("✅" if ck_timing  else "⚠️") + " Momento técnico favorable"
                )

                # ── Barra convicción ─────────────────────────────────────────
                st.progress(score_norm, text=f"Convicción: {score_f}")

                # ── Expander técnico ─────────────────────────────────────────
                with st.expander("Ver análisis completo ▼"):
                    ca, cb, cc = st.columns(3)

                    # Col A — FUNDAMENTAL
                    with ca:
                        st.markdown("**Fundamental**")
                        if qs is not None and not pd.isna(qs):
                            st.markdown(f"Calidad: **{float(qs):.0f}/100**")
                        if vs is not None and not pd.isna(vs):
                            st.markdown(f"Valor: **{float(vs):.0f}/100**")
                        ctx = str(row.get("contexto") or "")
                        if ctx:
                            st.markdown(f"*{CONTEXTO_LABEL.get(ctx, ctx)}*")
                        roic_ok  = roic_s is not None and not pd.isna(roic_s) and int(roic_s) == 1
                        deuda_ok = deuda_s is not None and not pd.isna(deuda_s) and int(deuda_s) == -1
                        st.markdown(
                            f'ROIC: <span style="color:{"#057a55" if roic_ok else "#e02424"};font-weight:600;">'
                            f'{"↑ Mejorando" if roic_ok else "↓ Deteriorando"}</span><br>'
                            f'Deuda: <span style="color:{"#057a55" if deuda_ok else "#e02424"};font-weight:600;">'
                            f'{"↓ Bajando" if deuda_ok else "↑ Subiendo"}</span>',
                            unsafe_allow_html=True,
                        )

                    # Col B — TÉCNICO
                    with cb:
                        st.markdown("**Técnico**")
                        rsi = row.get("rsi_14_semanal")
                        if rsi is not None and not pd.isna(rsi):
                            rsi_v   = float(rsi)
                            rsi_col = "#057a55" if 40 <= rsi_v <= 65 else ("#e02424" if rsi_v < 40 else "#c27803")
                            st.markdown(
                                f'RSI semanal: <span style="color:{rsi_col};font-weight:700;">{rsi_v:.1f}</span>',
                                unsafe_allow_html=True,
                            )
                        ma = row.get("precio_vs_ma200")
                        if ma is not None and not pd.isna(ma):
                            ma_v   = float(ma)
                            ma_col = "#057a55" if ma_v >= 0 else "#e02424"
                            st.markdown(
                                f'vs MA200: <span style="color:{ma_col};font-weight:700;">{ma_v:+.1f}%</span>',
                                unsafe_allow_html=True,
                            )
                        vol = row.get("volume_ratio_20d")
                        if vol is not None and not pd.isna(vol):
                            st.markdown(f"Volumen: **{float(vol):.1f}x** la media")
                        mom = row.get("momentum_3m")
                        if mom is not None and not pd.isna(mom):
                            st.markdown(f"Momentum 3M: **{float(mom):.1f}%**")

                    # Col C — SALUD
                    with cc:
                        st.markdown("**Salud**")
                        az = row.get("altman_z_score")
                        if az is not None and not pd.isna(az):
                            az_v = float(az)
                            if az_v >= 2.99:
                                st.markdown(f"🟢 Zona segura ({az_v:.1f})")
                            elif az_v >= 1.81:
                                st.markdown(f"🟡 Zona gris ({az_v:.1f})")
                            else:
                                st.markdown(f"🔴 Zona riesgo ({az_v:.1f})")
                        if ps is not None and not pd.isna(ps):
                            ps_v = int(ps)
                            if ps_v >= 7:
                                st.markdown(f"🟢 Piotroski fuerte ({ps_v}/9)")
                            elif ps_v >= 5:
                                st.markdown(f"🟡 Piotroski neutral ({ps_v}/9)")
                            else:
                                st.markdown(f"🔴 Piotroski débil ({ps_v}/9)")
                        roic_ok  = roic_s is not None and not pd.isna(roic_s) and int(roic_s) == 1
                        deuda_ok = deuda_s is not None and not pd.isna(deuda_s) and int(deuda_s) == -1
                        st.markdown(
                            f'ROIC: <span style="color:{"#057a55" if roic_ok else "#e02424"};font-weight:600;">'
                            f'{"↑ Mejorando" if roic_ok else "↓ Deteriorando"}</span><br>'
                            f'Deuda: <span style="color:{"#057a55" if deuda_ok else "#e02424"};font-weight:600;">'
                            f'{"↓ Bajando" if deuda_ok else "↑ Subiendo"}</span>',
                            unsafe_allow_html=True,
                        )


def _render_vista_tecnica(df: pd.DataFrame):
    """Vista técnica original con filtros y expanders."""

    if df.empty:
        st.info("Sin datos en agente.decision.")
        return

    for col_num in ["quality_score", "value_score", "score_conviccion",
                    "altman_z_score", "rsi_14_semanal", "precio_vs_ma200", "volume_ratio_20d"]:
        if col_num in df.columns:
            df[col_num] = pd.to_numeric(df[col_num], errors="coerce")
    for col_int in ["piotroski_score", "rank_conviccion"]:
        if col_int in df.columns:
            df[col_int] = pd.to_numeric(df[col_int], errors="coerce")

    n_stock   = int((df["instrumento"] == "stock").sum())
    n_csp     = int((df["instrumento"] == "cash_secured_put").sum())
    n_aligned = int((df["sector_alineado"] == "ALIGNED").sum())
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total seleccionadas", len(df))
    c2.metric("Stock",               n_stock)
    c3.metric("Cash-secured put",    n_csp)
    c4.metric("Sector alineado",     n_aligned)

    st.divider()

    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        opts_inst = ["Todos"] + sorted(df["instrumento"].dropna().unique().tolist())
        f_inst = st.selectbox("Instrumento", opts_inst, key="vt_inst")
    with fc2:
        opts_alin = ["Todos"] + sorted(df["sector_alineado"].dropna().unique().tolist())
        f_alin = st.selectbox("Sector alineado", opts_alin, key="vt_alin")
    with fc3:
        opts_cap = ["Todos"] + sorted(df["market_cap_tier"].dropna().unique().tolist())
        f_cap = st.selectbox("Market cap", opts_cap, key="vt_cap")

    dff = df.copy()
    if f_inst != "Todos": dff = dff[dff["instrumento"] == f_inst]
    if f_alin != "Todos": dff = dff[dff["sector_alineado"] == f_alin]
    if f_cap  != "Todos": dff = dff[dff["market_cap_tier"] == f_cap]

    st.caption(f"{len(dff)} empresa(s) mostrada(s)")

    for _, row in dff.iterrows():
        inst     = str(row.get("instrumento") or "")
        alin     = str(row.get("sector_alineado") or "")
        timing   = str(row.get("flag_timing") or "")
        ticker   = str(row.get("ticker") or "")
        rank     = row.get("rank_conviccion")
        score    = row.get("score_conviccion")
        pos_size = row.get("target_position_size")
        sector_s = str(row.get("sector") or "—")

        inst_color, inst_bg, inst_lbl = INSTRUMENTO_STYLE.get(inst, ("#6b7280", "#f9fafb", inst))
        alin_txt     = "✅ ALIGNED" if alin == "ALIGNED" else "⬜ NEUTRAL"
        timing_icono = FLAG_TIMING_ICONO.get(timing, "⚪")
        score_f      = f"{float(score):.0f}" if score is not None and not pd.isna(score) else "—"
        pos_f        = f"{float(pos_size):.2f}" if pos_size is not None and not pd.isna(pos_size) else "—"
        rank_f       = f"#{int(rank)}" if rank is not None and not pd.isna(rank) else "—"

        titulo = (
            f"{rank_f}  {ticker}  ·  {sector_s[:28]}  ·  "
            f"{inst_lbl}  ·  {timing_icono} {timing.replace('_',' ')}  ·  "
            f"Score {score_f}  ·  Size {pos_f}"
        )

        with st.expander(titulo):
            score_norm = min(max(float(score) / 100 if score is not None and not pd.isna(score) else 0, 0), 1)
            st.progress(score_norm, text=f"Score de convicción: {score_f}/100")
            st.markdown(
                _badge(inst_lbl, inst_color, inst_bg) + "&nbsp;&nbsp;" +
                _badge(alin_txt, "#057a55" if alin == "ALIGNED" else "#6b7280",
                       "#f3faf7" if alin == "ALIGNED" else "#f9fafb"),
                unsafe_allow_html=True,
            )
            st.markdown("<div style='margin-bottom:10px'></div>", unsafe_allow_html=True)

            col_a, col_b, col_c = st.columns(3)

            with col_a:
                st.markdown("**FUNDAMENTAL**")
                qs = row.get("quality_score")
                vs = row.get("value_score")
                st.metric("Quality score", f"{float(qs):.1f}" if qs is not None and not pd.isna(qs) else "—")
                st.metric("Value score",   f"{float(vs):.1f}" if vs is not None and not pd.isna(vs) else "—")
                ctx = str(row.get("contexto") or "")
                if ctx:
                    st.markdown(_badge(ctx.replace("_", " ").title(), "#4b5563", "#f3f4f6"), unsafe_allow_html=True)

            with col_b:
                st.markdown("**TÉCNICO**")
                rsi = row.get("rsi_14_semanal")
                if rsi is not None and not pd.isna(rsi):
                    rsi_v = float(rsi)
                    rsi_color = "#057a55" if 40 <= rsi_v <= 65 else ("#e02424" if rsi_v < 40 else "#c27803")
                    st.markdown(f'RSI semanal: <span style="color:{rsi_color};font-weight:700;">{rsi_v:.1f}</span>', unsafe_allow_html=True)
                ma = row.get("precio_vs_ma200")
                if ma is not None and not pd.isna(ma):
                    ma_v = float(ma)
                    ma_color = "#057a55" if ma_v >= 0 else "#e02424"
                    st.markdown(f'vs MA200: <span style="color:{ma_color};font-weight:700;">{ma_v:+.2f}%</span>', unsafe_allow_html=True)
                vol = row.get("volume_ratio_20d")
                vol_f = f"{float(vol):.2f}x" if vol is not None and not pd.isna(vol) else "—"
                st.markdown(f"Vol ratio 20d: **{vol_f}**")

            with col_c:
                st.markdown("**SALUD**")
                az = row.get("altman_z_score")
                if az is not None and not pd.isna(az):
                    az_v = float(az)
                    az_color = "#057a55" if az_v > 2.99 else ("#c27803" if az_v >= 1.81 else "#e02424")
                    st.markdown(f'Altman Z: <span style="color:{az_color};font-weight:700;">{az_v:.2f}</span>', unsafe_allow_html=True)
                ps = row.get("piotroski_score")
                if ps is not None and not pd.isna(ps):
                    ps_v = int(ps)
                    ps_color = "#057a55" if ps_v >= 7 else ("#c27803" if ps_v >= 5 else "#e02424")
                    st.markdown(f'Piotroski F: <span style="color:{ps_color};font-weight:700;">{ps_v}</span>', unsafe_allow_html=True)

            ind = str(row.get("industry") or "—")
            cap = str(row.get("market_cap_tier") or "—")
            st.caption(f"{sector_s} · {ind} · {cap}")


def pagina_estrategia():
    st.title("Estrategia")

    # Leer datos macro para el banner
    df_macro = query(SQL_MACRO_ESTADO)
    macro_row = df_macro.iloc[0].to_dict() if not df_macro.empty else None

    # Leer cards (agente.top + enriquecimiento)
    df_cards = query(SQL_ESTRATEGIA_CARDS)
    for col_num in ["quality_score", "value_score", "score_conviccion", "altman_z_score",
                    "rsi_14_semanal", "precio_vs_ma200", "volume_ratio_20d", "momentum_3m"]:
        if col_num in df_cards.columns:
            df_cards[col_num] = pd.to_numeric(df_cards[col_num], errors="coerce")
    for col_int in ["piotroski_score", "rank_conviccion", "roic_signo", "deuda_signo"]:
        if col_int in df_cards.columns:
            df_cards[col_int] = pd.to_numeric(df_cards[col_int], errors="coerce")

    # Leer datos para vista técnica
    df_tecnica = query(SQL_TOP_SELECCION)

    # Tabs
    tab_simple, tab_tecnica = st.tabs(["Vista simple", "Vista técnica"])

    with tab_simple:
        _render_vista_simple(df_cards, macro_row)

    with tab_tecnica:
        _render_vista_tecnica(df_tecnica)


# ── Página 5: ESTRATEGIA OPCIONES ────────────────────────────────────────────

ESTRATEGIA_STYLE = {
    "cash_secured_put": ("#057a55", "#f3faf7", "CSP"),
    "bull_put_spread":  ("#1d4ed8", "#eff6ff", "Bull Put"),
    "iron_condor":      ("#c27803", "#fefce8", "Iron Condor"),
    "jade_lizard":      ("#c2410c", "#fff7ed", "Jade Lizard"),
    "calendar_spread":  ("#7c3aed", "#f5f3ff", "Calendar"),
}
REGIMEN_VIX_COLOR = {
    "panico":       "#e02424",
    "elevado":      "#c27803",
    "normal":       "#057a55",
    "complacencia": "#6b7280",
}
TERM_ICONO = {
    "backwardation": "📉 Backwardation",
    "contango":      "📈 Contango",
    "flat":          "➡️ Flat",
}
LIQUIDEZ_ICONO = {
    "liquido":      "💧 Líquido",
    "semi_liquido": "⚠️ Semi-líquido",
}
TENDENCIA_COLOR = {
    "mejora_estructural": "#057a55",
    "mejora_parcial":     "#c27803",
    "deterioro":          "#e02424",
}

IV_COLOR = {
    "baja":  "#6b7280",
    "media": "#c27803",
    "alta":  "#057a55",
}


def pagina_opciones():
    st.title("Estrategia Opciones")

    df = query(SQL_OPCIONES)

    if df.empty:
        st.info("No hay estrategias activas.")
        return

    for col_num in ["iv_promedio", "put_strike", "put_delta", "put_theta",
                    "put_iv", "put_dte", "call_strike", "call_delta", "call_theta",
                    "sizing", "vix", "delta_objetivo",
                    "quality_score", "value_score", "altman_z_score", "piotroski_score",
                    "rsi_14_semanal"]:
        if col_num in df.columns:
            df[col_num] = pd.to_numeric(df[col_num], errors="coerce")

    # ── Sección 1: Métricas resumen (4 columnas) ─────────────────────────────
    n_total  = len(df)
    n_bps    = int((df["estrategia"] == "bull_put_spread").sum())
    n_csp    = int((df["estrategia"] == "cash_secured_put").sum())
    iv_mean  = df["iv_promedio"].mean()
    vix_row  = df.dropna(subset=["vix"]).iloc[0] if not df.dropna(subset=["vix"]).empty else None

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total estrategias activas", n_total)
    c2.metric("Bull Put Spread", n_bps, delta=f"CSP: {n_csp}", delta_color="off")

    if vix_row is not None:
        vix_v   = float(vix_row["vix"])
        regimen = str(vix_row.get("regimen_vix") or "").lower()
        reg_col = REGIMEN_VIX_COLOR.get(regimen, "#6b7280")
        c3.metric("VIX", f"{vix_v:.2f}")
        c3.markdown(
            f'<span style="background:{reg_col}22;color:{reg_col};'
            f'border:1px solid {reg_col};padding:2px 10px;border-radius:12px;'
            f'font-weight:700;font-size:.8rem;">{regimen.upper()}</span>',
            unsafe_allow_html=True,
        )
    else:
        c3.metric("VIX", "—")

    if not pd.isna(iv_mean):
        c4.metric("IV promedio universo", f"{iv_mean:.1%}")
    else:
        c4.metric("IV promedio universo", "—")

    st.divider()

    # ── Sección 2: Filtros ───────────────────────────────────────────────────
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        opts_est = ["Todas"] + sorted(df["estrategia"].dropna().unique().tolist())
        f_est    = st.selectbox("Estrategia", opts_est)
    with fc2:
        opts_iv  = ["Todas"] + sorted(df["nivel_iv"].dropna().unique().tolist())
        f_iv     = st.selectbox("Nivel IV", opts_iv)
    with fc3:
        opts_liq = ["Todas"] + sorted(df["liquidez"].dropna().unique().tolist())
        f_liq    = st.selectbox("Liquidez", opts_liq)

    dff = df.copy()
    if f_est != "Todas": dff = dff[dff["estrategia"] == f_est]
    if f_iv  != "Todas": dff = dff[dff["nivel_iv"]   == f_iv]
    if f_liq != "Todas": dff = dff[dff["liquidez"]   == f_liq]

    st.caption(f"{len(dff)} estrategia(s) mostrada(s)")

    # ── Sección 3: Tabla principal ────────────────────────────────────────────
    tabla_cols = ["ticker", "estrategia", "nivel_iv",
                  "put_strike", "put_dte", "put_delta", "put_theta", "sizing"]
    tabla = dff[tabla_cols].copy()
    tabla.columns = ["Ticker", "Estrategia", "Nivel IV",
                     "Strike", "DTE", "Delta", "Theta", "Sizing"]

    def color_estrategia(val):
        colores = {
            "cash_secured_put": "background-color: #d4edda; color: #155724",
            "bull_put_spread":  "background-color: #cce5ff; color: #004085",
            "iron_condor":      "background-color: #fff3cd; color: #856404",
            "jade_lizard":      "background-color: #ffe5d0; color: #7c3200",
            "calendar_spread":  "background-color: #e2d9f3; color: #432874",
        }
        return colores.get(val, "")

    def fmt_sizing_bar(val):
        if pd.isna(val):
            return ""
        pct = min(max(float(val), 0), 1)
        return f"{pct:.0%}"

    tabla_display = tabla.style.map(color_estrategia, subset=["Estrategia"]).format({
        "Strike": lambda v: f"${v:.2f}"  if pd.notna(v) else "—",
        "DTE":    lambda v: f"{int(v)}d" if pd.notna(v) else "—",
        "Delta":  lambda v: f"{v:.3f}"   if pd.notna(v) else "—",
        "Theta":  lambda v: f"{v:.4f}"   if pd.notna(v) else "—",
        "Sizing": fmt_sizing_bar,
    })
    st.dataframe(tabla_display, use_container_width=True, hide_index=True)

    st.divider()

    # ── Sección 4: Expanders por ticker ──────────────────────────────────────
    for _, row in dff.iterrows():
        est      = str(row.get("estrategia") or "")
        ticker   = str(row.get("ticker") or "")
        nivel_iv = str(row.get("nivel_iv") or "")
        strike   = row.get("put_strike")
        dte      = row.get("put_dte")
        delta_ob = row.get("delta_objetivo")
        sizing   = row.get("sizing")

        est_color, est_bg, est_lbl = ESTRATEGIA_STYLE.get(est, ("#6b7280", "#f9fafb", est))
        strike_s = f"${float(strike):.2f}" if pd.notna(strike) else "—"
        dte_s    = f"{int(dte)}d"          if pd.notna(dte)    else "—"
        delta_s  = f"{float(delta_ob):.2f}" if pd.notna(delta_ob) else "—"
        sizing_s = f"{float(sizing):.0%}"  if pd.notna(sizing) else "—"
        iv_col   = IV_COLOR.get(nivel_iv.lower(), "#6b7280")

        titulo = (
            f"{ticker}  ·  {est_lbl}  ·  "
            f"Strike {strike_s}  ·  DTE {dte_s}  ·  "
            f"Δ {delta_s}  ·  IV {nivel_iv}  ·  Size {sizing_s}"
        )

        with st.expander(titulo):

            # Badge estrategia
            st.markdown(
                f'<span style="background:{est_bg};color:{est_color};'
                f'border:1.5px solid {est_color};padding:4px 14px;border-radius:20px;'
                f'font-weight:700;font-size:.95rem;">{est_lbl}</span>',
                unsafe_allow_html=True,
            )
            st.markdown("<div style='margin-bottom:10px'></div>", unsafe_allow_html=True)

            col_a, col_b, col_c = st.columns(3)

            # ── Columna A: CONTRATO ──────────────────────────────────────────
            with col_a:
                st.markdown("**CONTRATO**")
                if pd.notna(strike):
                    st.metric("Put Strike", strike_s)
                st.markdown(f"Vencimiento en **{dte_s}**")
                st.markdown(f"Delta objetivo: **{delta_s}**")
                call_s = row.get("call_strike")
                if pd.notna(call_s):
                    st.markdown(f"Call Strike: **${float(call_s):.2f}**")

            # ── Columna B: VOLATILIDAD ───────────────────────────────────────
            with col_b:
                st.markdown("**VOLATILIDAD**")
                st.markdown(
                    f'Nivel IV: <span style="color:{iv_col};font-weight:700;">'
                    f'{nivel_iv.upper() if nivel_iv else "—"}</span>',
                    unsafe_allow_html=True,
                )
                iv_prom = row.get("iv_promedio")
                if pd.notna(iv_prom):
                    st.markdown(f"IV promedio: **{float(iv_prom):.1%}**")
                put_iv = row.get("put_iv")
                if pd.notna(put_iv):
                    st.markdown(f"IV contrato: **{float(put_iv):.1%}**")
                term = str(row.get("term_structure") or "").lower()
                if term:
                    st.markdown(TERM_ICONO.get(term, f"➡️ {term}"))
                regimen = str(row.get("regimen_vix") or "").lower()
                if regimen:
                    reg_col = REGIMEN_VIX_COLOR.get(regimen, "#6b7280")
                    st.markdown(
                        f'Régimen VIX: <span style="color:{reg_col};font-weight:700;">'
                        f'{regimen.upper()}</span>',
                        unsafe_allow_html=True,
                    )

            # ── Columna C: FUNDAMENTAL ───────────────────────────────────────
            with col_c:
                st.markdown("**FUNDAMENTAL**")

                qs = row.get("quality_score")
                vs = row.get("value_score")
                if pd.notna(qs): st.markdown(f"Quality score: **{float(qs):.1f}**")
                if pd.notna(vs): st.markdown(f"Value score: **{float(vs):.1f}**")

                az = row.get("altman_z_score")
                if pd.notna(az):
                    az_zona = "safe" if float(az) >= 2.99 else ("grey" if float(az) >= 1.81 else "distress")
                    az_color, az_bg = ALTMAN_ZONA_COLOR.get(az_zona, ("#6b7280", "#f9fafb"))
                    st.markdown(
                        f'Altman Z: <span style="background:{az_bg};color:{az_color};'
                        f'padding:1px 8px;border-radius:10px;font-weight:600;">'
                        f'{float(az):.2f} ({az_zona})</span>',
                        unsafe_allow_html=True,
                    )

                pio = row.get("piotroski_score")
                if pd.notna(pio):
                    pio_v   = int(pio)
                    pio_col = "#057a55" if pio_v >= 7 else ("#c27803" if pio_v >= 5 else "#e02424")
                    st.markdown(
                        f'Piotroski: <span style="color:{pio_col};font-weight:700;">'
                        f'{pio_v}/9</span>',
                        unsafe_allow_html=True,
                    )

                roic_s = row.get("roic_signo")
                if roic_s is not None and not pd.isna(roic_s):
                    roic_txt = "↑ ROIC mejora" if int(roic_s) == 1 else "↓ ROIC cae"
                    roic_col = "#057a55" if int(roic_s) == 1 else "#e02424"
                    st.markdown(
                        f'<span style="color:{roic_col};">{roic_txt}</span>',
                        unsafe_allow_html=True,
                    )

                tend = str(row.get("tendencia_fundamental") or "")
                if tend:
                    tend_col = TENDENCIA_COLOR.get(tend, "#6b7280")
                    tend_lbl = tend.replace("_", " ").title()
                    st.markdown(
                        f'Tendencia: <span style="color:{tend_col};font-weight:700;">'
                        f'{tend_lbl}</span>',
                        unsafe_allow_html=True,
                    )

            # ── Pie del expander ─────────────────────────────────────────────
            st.markdown("<div style='margin-top:8px'></div>", unsafe_allow_html=True)
            theta = row.get("put_theta")
            if pd.notna(theta):
                st.markdown(f"Prima diaria estimada: **${float(theta):.4f}**")

            sizing_norm = min(max(float(sizing) if pd.notna(sizing) else 0, 0), 1)
            st.progress(sizing_norm, text=f"Sizing: {sizing_s}")

            notas = row.get("notas")
            if notas:
                st.caption(f"📝 {notas}")


# ── Página 0: CÓMO FUNCIONA ──────────────────────────────────────────────────

def pagina_como_funciona():

    # ── Sección 1: Hero ──────────────────────────────────────────────────────
    st.title("Ciencia de Datos aplicada a la Bolsa de Valores")
    st.subheader("Análisis cuantitativo sistemático del mercado estadounidense")
    st.markdown(
        """
El mercado genera ruido constante. Aplicamos modelos matemáticos, regresiones lineales
y análisis técnico masivo sobre más de 3.000 empresas para separar la señal del ruido.

El resultado: reportes semanales con estrategias accionables — acciones directas y opciones
con prima positiva — fundamentadas en datos, no en intuición.
"""
    )

    # ── Propuestas de valor ──────────────────────────────────────────────────
    pv1, pv2, pv3 = st.columns(3)
    with pv1:
        with st.container(border=True):
            st.markdown("### 📊")
            st.markdown("**Análisis semanal actualizado**")
            st.markdown("Cada lunes el sistema procesa el mercado completo y genera señales frescas")
    with pv2:
        with st.container(border=True):
            st.markdown("### 🎯")
            st.markdown("**Estrategias accionables**")
            st.markdown("No solo análisis — señales concretas con el instrumento y el momento exacto")
    with pv3:
        with st.container(border=True):
            st.markdown("### 🤖")
            st.markdown("**Asistente con contexto real**")
            st.markdown("Preguntá en lenguaje natural y recibí respuestas basadas en datos reales del sistema")

    st.divider()

    # ── Sección 2: Pipeline visual ───────────────────────────────────────────
    CAPAS = [
        ("📊", "MACRO",      "Ciclo económico",       "FRED API · 15 indicadores · estado EXPANSION / SLOWDOWN / CONTRACTION / RECOVERY"),
        ("🔄", "SECTORES",   "Rotación sectorial",    "63 ETFs rankeados · alineación con el ciclo macro · señal de rotación"),
        ("🏢", "EMPRESAS",   "Calidad + Valor",       "~700 empresas filtradas · z-score Quality · percentil Value · Altman Z · Piotroski F"),
        ("🎯", "ESTRATEGIA", "Señales accionables",   "Dirección por ticker · instrumento óptimo · timing técnico · score de convicción"),
        ("📈", "OPCIONES",   "Premium y cobertura",   "Strikes por Greeks · IV relativa · estructura de term · estrategia por régimen"),
    ]

    cols = st.columns(11)   # 5 tarjetas + 4 flechas + 2 laterales de relleno
    col_idx = [1, 3, 5, 7, 9]
    arrow_idx = [2, 4, 6, 8]

    for ci, (icono, nombre, sub, desc) in zip(col_idx, CAPAS):
        with cols[ci]:
            st.markdown(
                f'<div style="border:1px solid #e5e7eb;border-radius:10px;padding:16px 12px;'
                f'text-align:center;height:100%;">'
                f'<div style="font-size:1.8rem;">{icono}</div>'
                f'<div style="font-weight:700;font-size:.95rem;margin:6px 0 2px;">{nombre}</div>'
                f'<div style="color:#374151;font-size:.82rem;font-weight:600;margin-bottom:6px;">{sub}</div>'
                f'<div style="color:#9ca3af;font-size:.72rem;line-height:1.4;">{desc}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
    for ai in arrow_idx:
        with cols[ai]:
            st.markdown(
                '<div style="text-align:center;padding-top:28px;font-size:1.4rem;color:#d1d5db;">→</div>',
                unsafe_allow_html=True,
            )

    st.divider()

    # ── Sección 3: Cómo leer cada página ────────────────────────────────────
    st.subheader("Cómo leer cada página")
    tabla_guia = pd.DataFrame([
        {
            "Página":          "Macro",
            "Qué muestra":     "El estado del ciclo económico basado en 15 indicadores de la Fed (FRED)",
            "Cómo interpretarlo": "Verde = expansión, condiciones favorables para activos de riesgo. Rojo = contracción, reducir exposición.",
        },
        {
            "Página":          "Sectores",
            "Qué muestra":     "Ranking de 63 ETFs sectoriales e industriales con su momentum relativo al S&P 500",
            "Cómo interpretarlo": "LEADING_STRONG = sectores con más fuerza relativa. ALIGNED = coinciden con lo que el ciclo macro favorece.",
        },
        {
            "Página":          "Micro",
            "Qué muestra":     "Las ~700 empresas que pasaron el filtro de salud financiera, con sus scores de calidad y valor",
            "Cómo interpretarlo": "Quality %ile alto = empresa de alta rentabilidad vs. sus pares. Piotroski ≥7 = sólida en los 9 criterios contables.",
        },
        {
            "Página":          "Estrategia",
            "Qué muestra":     "Las empresas con señal activa: instrumento recomendado, timing técnico y score de convicción",
            "Cómo interpretarlo": "Score alto + ALIGNED + tecnico_confirmado = mayor convicción. CSP = venta de put cubierta con efectivo.",
        },
        {
            "Página":          "Estrategia Opciones",
            "Qué muestra":     "La estrategia de opciones específica para cada ticker: strikes, delta, theta, DTE",
            "Cómo interpretarlo": "Estrategia elegida según IV, régimen de VIX y dirección. Mayor sizing = mayor convicción del sistema.",
        },
    ])
    st.table(tabla_guia)

    st.divider()

    # ── Sección 4: Glosario ──────────────────────────────────────────────────
    with st.expander("Ver glosario de términos"):
        GLOSARIO = [
            ("RSI (Relative Strength Index)",
             "Oscilador de momentum de 0 a 100. Por encima de 65 indica sobrecompra; por debajo de 40, sobreventa. "
             "Aquí se usa en versión semanal para filtrar ruido de corto plazo."),
            ("Altman Z-Score",
             "Modelo de predicción de quiebra basado en 5 ratios financieros. Z > 2.99 = zona segura (verde). "
             "1.81–2.99 = zona gris (amarillo). < 1.81 = zona de distress (rojo)."),
            ("Piotroski F-Score",
             "Puntaje de 0 a 9 que evalúa rentabilidad, apalancamiento y eficiencia operativa. "
             "≥7 = empresa financieramente sólida. <5 = señales de deterioro."),
            ("Cash Secured Put (CSP)",
             "Estrategia de opciones que consiste en vender una opción put respaldada por el efectivo necesario para comprar "
             "las acciones si se ejerce. Genera prima inmediata; ideal cuando se quiere entrar a un precio menor al actual."),
            ("ALIGNED",
             "Indica que el sector del ticker coincide con los sectores favorecidos por el estado macro actual. "
             "Un ticker ALIGNED tiene viento a favor del ciclo económico."),
            ("Score de convicción",
             "Puntuación de 0 a 100 que combina calidad fundamental, alineación sectorial, timing técnico y salud financiera. "
             "Determina el ranking y el tamaño de posición sugerido."),
            ("flag_timing",
             "Clasifica el momento técnico de entrada: tecnico_confirmado = precio sobre MA200 + RSI favorable; "
             "pullback_comprable = caída dentro de tendencia alcista; macro_defensivo = solo por fundamentos en entorno difícil."),
            ("MA200 (Media móvil de 200 ruedas)",
             "Indicador de tendencia de largo plazo. Precio por encima = tendencia alcista estructural. "
             "precio_vs_ma200 muestra el porcentaje de distancia respecto a esa media."),
        ]
        for termino, definicion in GLOSARIO:
            st.markdown(f"**{termino}**")
            st.markdown(f"{definicion}")
            st.markdown("---")

    st.divider()

    # ── Sección 5: Embudo de selección ──────────────────────────────────────
    st.subheader("Estado actual del sistema")
    metricas = query(SQL_METRICAS_SISTEMA)
    if not metricas.empty:
        m = metricas.iloc[0]
        n1 = int(m["universo_inicial"] or 0)
        n2 = int(m["pasan_calidad"]    or 0)
        n3 = int(m["señales_activas"]  or 0)
        n4 = int(m["top_seleccion"]    or 0)

        PASOS = [
            (n1, "Universo USA",             "universos.stock_opciones_2026"),
            (n2, "Filtro calidad de balance", "ROIC · D/E · NetDebt/EBITDA · FCF"),
            (n3, "Señal activa",             "Dirección + timing técnico"),
            (n4, "Mayor convicción",         "agente.top · score ≥ umbral"),
        ]

        # Fila de métricas
        cols_m = st.columns(4)
        for col, (n, label, detalle) in zip(cols_m, PASOS):
            col.metric(label, f"{n:,}".replace(",", "."))
            col.caption(detalle)

        # Embudo visual con caracteres unicode
        # Calcula anchos proporcionales (mínimo 4 chars, máximo 32)
        max_n = max(n1, 1)
        def barra(n: int, ancho_max: int = 32, min_ancho: int = 4) -> str:
            ancho = max(round(n / max_n * ancho_max), min_ancho)
            return "█" * ancho

        st.markdown("<div style='margin-top:18px'></div>", unsafe_allow_html=True)

        pasos_html = ""
        for i, (n, label, _) in enumerate(PASOS):
            barra_str = barra(n)
            pct = f"({n/n1:.0%})" if i > 0 else ""
            flecha = '<span style="color:#9ca3af;font-size:1.1rem;"> ▼ </span>' if i < len(PASOS) - 1 else ""
            pasos_html += (
                f'<div style="margin:2px 0;">'
                f'<span style="font-family:monospace;color:#1d4ed8;font-size:.95rem;">{barra_str}</span>'
                f'  <span style="font-size:.9rem;font-weight:600;">{n:,} empresas</span>'
                f'  <span style="color:#9ca3af;font-size:.85rem;">{label} {pct}</span>'
                f'</div>{flecha}'
            )

        st.markdown(pasos_html, unsafe_allow_html=True)

    st.divider()

    # ── Sección 6: Disclaimer ────────────────────────────────────────────────
    st.info(
        "**Aviso legal** — Este sistema es una herramienta de análisis cuantitativo para uso "
        "personal e informativo. No constituye asesoramiento financiero, recomendación de inversión "
        "ni oferta de compra o venta de valores. Toda decisión de inversión es responsabilidad "
        "exclusiva del usuario. Los resultados pasados no garantizan rendimientos futuros. "
        "Las opciones son instrumentos complejos que pueden implicar pérdida total del capital invertido."
    )


# ── Página 6: ASISTENTE ──────────────────────────────────────────────────────

SYSTEM_PROMPT = """
Sos un asistente especializado en análisis cuantitativo de inversiones. Trabajás con un sistema
multifactor que analiza más de 3.000 empresas USA y 75 ETFs en 5 capas:
MACRO → SECTORES → EMPRESAS → ESTRATEGIA → OPCIONES.

En cada consulta recibís:
1. El estado actual del sistema (contexto fijo)
2. Datos específicos de las empresas o ETFs que el usuario mencionó (contexto dinámico)

Usá SIEMPRE los datos del sistema para fundamentar.

Podés ayudar con:
- Estado general del mercado y del sistema
- Análisis de empresas específicas (datos de la DB)
- Análisis de ETFs y commodities
- Explicación de estrategias de opciones
- Interpretación de métricas del sistema

Reglas estrictas:
- Solo usá datos que estén explícitamente en el contexto
- Si no tenés datos de una empresa, decilo claramente
- Siempre aclará que no es asesoramiento financiero
- Sé conciso — máximo 4 párrafos
- Usá los datos reales del sistema para fundamentar
- No inventes datos ni señales
- Si el usuario pregunta algo fuera del scope de inversiones, redirigilo amablemente
"""


@st.cache_data(ttl=300)
def leer_contexto_sistema() -> str:
    """Lee el estado actual de la DB y lo devuelve como string formateado."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            cur.execute("""
                SELECT estado_macro, score_riesgo, confianza
                FROM macro.macro_diagnostico
                ORDER BY calculado_en DESC LIMIT 1
            """)
            macro = cur.fetchone() or {}

            cur.execute("""
                SELECT señal_rotacion, top_tickers_aligned,
                       top_tickers_global, score_universo, n_leading_strong
                FROM sector.v_sector_diagnostico
                LIMIT 1
            """)
            sector = cur.fetchone() or {}

            cur.execute("""
                SELECT COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE instrumento = 'stock') AS n_stock,
                       COUNT(*) FILTER (WHERE instrumento = 'cash_secured_put') AS n_csp,
                       MODE() WITHIN GROUP (ORDER BY flag_timing) AS timing_dominante
                FROM agente.decision
                WHERE trade_status = 'active'
                  AND snapshot_date = (SELECT MAX(snapshot_date)
                                       FROM agente.decision)
            """)
            señales = cur.fetchone() or {}

            cur.execute("""
                SELECT ticker, sector, instrumento,
                       score_conviccion, flag_timing,
                       sector_alineado
                FROM agente.top
                WHERE snapshot_date = (SELECT MAX(snapshot_date)
                                       FROM agente.top)
                ORDER BY rank_conviccion
                LIMIT 5
            """)
            top5 = cur.fetchall() or []

            cur.execute("""
                SELECT diagnostico_sector, coherencia, nota,
                       score_defensivos, score_ciclicos
                FROM sector.sector_diagnostico_tecnico
                ORDER BY fecha DESC LIMIT 1
            """)
            diag_tec = cur.fetchone() or {}

    # Armar tabla top 5
    lineas_top5 = []
    for r in top5:
        score = f"{float(r['score_conviccion']):.1f}" if r.get("score_conviccion") else "—"
        lineas_top5.append(
            f"  {r.get('ticker','?'):<6} | {str(r.get('sector',''))[:22]:<22} | "
            f"{str(r.get('instrumento','')):<18} | score {score} | "
            f"timing: {r.get('flag_timing','—')} | "
            f"alineado: {r.get('sector_alineado','—')}"
        )
    tabla_top5 = "\n".join(lineas_top5) if lineas_top5 else "  Sin datos"

    return f"""\
ESTADO DEL SISTEMA — {date.today().strftime('%d/%m/%Y')}

MACRO:
- Estado: {macro.get('estado_macro', '—')} | Score riesgo: {macro.get('score_riesgo', '—')}
- Confianza: {macro.get('confianza', '—')}

SECTORES:
- Señal rotación: {sector.get('señal_rotacion', '—')}
- Top alineados con macro: {sector.get('top_tickers_aligned', '—')}
- Score universo: {sector.get('score_universo', '—')}
- Leading strong: {sector.get('n_leading_strong', '—')}
- Diagnóstico técnico: {diag_tec.get('diagnostico_sector', '—')}
- Coherencia macro/sector: {diag_tec.get('coherencia', '—')}
- RSI Defensivos: {diag_tec.get('score_defensivos', '—')} | RSI Cíclicos: {diag_tec.get('score_ciclicos', '—')}
- Nota técnica: {diag_tec.get('nota', '—')}

SEÑALES ACTIVAS:
- Total: {señales.get('total', 0)} | Stock: {señales.get('n_stock', 0)} | CSP: {señales.get('n_csp', 0)}
- Timing dominante: {señales.get('timing_dominante', '—')}

TOP 5 EMPRESAS POR CONVICCIÓN:
{tabla_top5}
"""


def get_empresa_data(conn, ticker: str):
    ticker = ticker.upper().strip()

    sql_enriquecimiento = """
        SELECT
            e.ticker, e.sector, e.industry, e.market_cap_tier,
            e.quality_score, e.value_score,
            e.multifactor_score, e.multifactor_rank,
            e.rsi_14_semanal, e.precio_vs_ma200,
            e.volume_ratio_20d, e.momentum_3m,
            e.momentum_6m, e.momentum_12m,
            e.altman_z_score, e.altman_zona,
            e.piotroski_score, e.piotroski_categoria,
            e.roic_signo, e.roic_confiable,
            e.deuda_signo, e.deuda_confiable,
            e.sector_alineado, e.estado_macro,
            d.contexto, d.instrumento, d.flag_timing,
            d.score_conviccion, d.rank_conviccion,
            d.target_position_size, d.tendencia_fundamental,
            o.estrategia, o.put_strike, o.put_delta,
            o.put_theta, o.put_iv, o.put_dte,
            o.nivel_iv, o.sizing AS sizing_opciones
        FROM seleccion.enriquecimiento e
        LEFT JOIN agente.decision d
            ON d.ticker = e.ticker
            AND d.snapshot_date = e.snapshot_date
        LEFT JOIN agente_opciones.trade_decision_opciones o
            ON o.ticker = e.ticker
            AND o.snapshot_date = e.snapshot_date
        WHERE e.ticker = %s
          AND e.snapshot_date = (SELECT MAX(snapshot_date)
                                  FROM seleccion.enriquecimiento)
    """

    sql_fundamentales = """
        SELECT
            r.operating_profit_margin,
            r.free_cash_flow_operating_cash_flow_ratio,
            r.interest_coverage_ratio,
            r.debt_to_equity_ratio,
            r.price_to_earnings_ratio,
            r.price_to_book_ratio,
            r.price_to_free_cash_flow_ratio,
            r.free_cash_flow_per_share,
            k.roic,
            k.roe,
            k.roa,
            k.income_quality,
            k.ev_to_ebitda,
            k.net_debt_to_ebitda,
            k.fcf_yield,
            k.market_cap
        FROM ingest.ratios_ttm r
        JOIN ingest.keymetrics k
            ON k.ticker = r.ticker
            AND k.fecha_consulta = (SELECT MAX(fecha_consulta)
                                    FROM ingest.keymetrics)
        WHERE r.ticker = %s
          AND r.fecha_consulta = (SELECT MAX(fecha_consulta)
                                  FROM ingest.ratios_ttm)
    """

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql_enriquecimiento, (ticker,))
        row_enriq = cur.fetchone()
        cur.execute(sql_fundamentales, (ticker,))
        row_fund = cur.fetchone()

    # Empresa fuera del universo filtrado — usar solo fundamentales
    if not row_enriq and not row_fund:
        return None

    result = dict(row_enriq) if row_enriq else {"ticker": ticker}
    if row_fund:
        result.update(dict(row_fund))
    return result


def get_etf_data(conn, ticker: str):
    ticker = ticker.upper().strip()
    sql = """
        SELECT s.ticker, s.señal, s.score,
               s.score_tecnico, s.razon, s.estado_macro,
               e.nombre, e.tipo, e.industria,
               r.rsi_rs_semanal, r.ret_3m, r.ret_6m,
               r.rs_percentil, r.estado, r.alineacion_macro,
               r.score_total
        FROM etf.signal s
        JOIN sector.sector_etfs e ON e.ticker = s.ticker
        JOIN sector.v_sector_ranking r ON r.ticker = s.ticker
        WHERE s.ticker = %s
          AND s.snapshot_date = (SELECT MAX(snapshot_date) FROM etf.signal)
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (ticker,))
        row = cur.fetchone()
    return dict(row) if row else None


def get_opciones_data(conn, ticker: str) -> list[dict]:
    ticker = ticker.upper().strip()
    sql = """
        SELECT ticker, opcion, contract_type,
               strike, vto, dte, delta, theta,
               iv, oi, volume, close_price
        FROM agente_opciones.contratos_raw
        WHERE ticker = %s
          AND fecha = (SELECT MAX(fecha) FROM agente_opciones.contratos_raw)
          AND oi >= 5
        ORDER BY ABS(theta) DESC
        LIMIT 10
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (ticker,))
        return [dict(r) for r in cur.fetchall()]


def enriquecer_contexto_con_ticker(conn, pregunta: str, contexto_base: str) -> str:
    import re
    tickers_mencionados = re.findall(r'\b[A-Z]{2,5}\b', pregunta)

    contexto_extra = ""
    for ticker in tickers_mencionados:
        # Intentar como empresa
        data = get_empresa_data(conn, ticker)
        if data:
            ma200 = data.get("precio_vs_ma200")
            ma200_f = f"{float(ma200):.1f}%" if ma200 is not None else "N/A"
            en_universo = data.get("quality_score") is not None
            contexto_extra += f"""
DATOS DEL SISTEMA PARA {ticker}{' (fuera del universo filtrado — solo fundamentales TTM)' if not en_universo else ''}:
  Sector: {data.get('sector', 'N/D')} | {data.get('industry', 'N/D')}
  Quality: {data.get('quality_score', 'N/D')} | Value: {data.get('value_score', 'N/D')}
  Score convicción: {data.get('score_conviccion', 'N/D')}
  Instrumento: {data.get('instrumento', 'N/D')}
  RSI semanal: {data.get('rsi_14_semanal', 'N/D')} | MA200: {ma200_f}
  Altman: {data.get('altman_z_score', 'N/D')} ({data.get('altman_zona', 'N/D')})
  Piotroski: {data.get('piotroski_score', 'N/D')}
  ROIC: {'↑ mejorando' if data.get('roic_signo') == 1 else '↓ deteriorando' if data.get('roic_signo') is not None else 'N/D'}
  Deuda: {'↓ bajando' if data.get('deuda_signo') == -1 else '↑ subiendo' if data.get('deuda_signo') is not None else 'N/D'}
  Tendencia: {data.get('tendencia_fundamental', 'N/D')}
  Opciones: {data.get('estrategia', 'N/D')} | Strike: {data.get('put_strike', 'N/D')}
  MÉTRICAS FUNDAMENTALES TTM:
  ROIC: {data.get('roic', 'N/D')}
  ROE: {data.get('roe', 'N/D')}
  Margen operativo: {data.get('operating_profit_margin', 'N/D')}
  P/E: {data.get('price_to_earnings_ratio', 'N/D')}
  P/FCF: {data.get('price_to_free_cash_flow_ratio', 'N/D')}
  EV/EBITDA: {data.get('ev_to_ebitda', 'N/D')}
  Net Debt/EBITDA: {data.get('net_debt_to_ebitda', 'N/D')}
  FCF/share: {data.get('free_cash_flow_per_share', 'N/D')}
  Income quality: {data.get('income_quality', 'N/D')}
  Market cap: {data.get('market_cap', 'N/D')}
"""
            opciones = get_opciones_data(conn, ticker)
            if opciones:
                contexto_extra += (
                    f"  Contratos disponibles: {len(opciones)} | "
                    f"Mejor theta: {opciones[0].get('theta')}\n"
                )
            continue

        # Intentar como ETF
        etf = get_etf_data(conn, ticker)
        if etf:
            ret3m = etf.get("ret_3m")
            ret3m_f = f"{float(ret3m):.1f}%" if ret3m is not None else "N/A"
            contexto_extra += f"""
DATOS DEL SISTEMA PARA ETF {ticker}:
  Nombre: {etf.get('nombre')} | Tipo: {etf.get('tipo')}
  Señal: {etf.get('señal')} | Score: {etf.get('score')}
  RSI: {etf.get('rsi_rs_semanal')} | Ret 3M: {ret3m_f}
  Alineación macro: {etf.get('alineacion_macro')}
  Razón: {etf.get('razon')}
"""

    if contexto_extra:
        return contexto_base + "\n\nDATOS ESPECÍFICOS CONSULTADOS:\n" + contexto_extra
    return contexto_base


def llamar_claude_chat(
    system: str,
    contexto: str,
    pregunta: str,
    historial: list[dict],
) -> str:
    """Llama a Claude con historial completo y contexto del sistema."""
    cliente = anthropic.Anthropic(api_key=get_secret("ANTHROPIC_API_KEY"))

    # Construir mensajes: historial previo + pregunta actual con contexto
    messages = []
    for msg in historial:
        messages.append({"role": msg["role"], "content": msg["content"]})

    messages.append({
        "role": "user",
        "content": f"Contexto actual del sistema:\n{contexto}\n\nPregunta: {pregunta}",
    })

    respuesta = cliente.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        system=system,
        messages=messages,
    )
    return respuesta.content[0].text


MENSAJE_BIENVENIDA = """
👋 Hola. Soy el asistente del sistema de inversión cuantitativo.

Analizamos más de 3.000 empresas y 75 ETFs cada semana usando modelos matemáticos, multifactores, regresiones lineales y análisis fundamental y técnico masivo.

Podés preguntarme, por ejemplo, sobre:

🌍 **Contexto macroeconómico**
- "¿En qué fase del ciclo estamos?"
- "¿Cómo está la inflación y qué implica para el mercado?"
- "¿Qué dice el desempleo sobre la economía?"

🏢 **Empresas y métricas financieras**
- "¿Cuál es el ROIC de TSLA?"
- "¿Cómo está AMD en el sistema?"
- "¿Qué oportunidades hay para invertir a largo plazo?"
- "Explicame el moat de AAPL"

📈 **ETFs, sectores y commodities**
- "¿Qué ETFs favorece el sistema esta semana?"
- "¿Cómo está el oro en este contexto?"
- "¿Qué sectores lideran y cuáles evitar?"

🎯 **Estrategias de inversión**
- "¿Qué oportunidades concretas hay esta semana?"
- "Explicame cómo funciona un cash secured put"
- "¿Cuándo conviene bull put spread vs CSP?"
- "¿Qué significa delta -0.34 en una opción?"

---
⚠️ *Este asistente no constituye asesoramiento financiero. Verificá siempre con un profesional antes de operar.*
"""


def pagina_asistente():
    st.title("💬 Asistente de Inversiones")
    st.markdown("""
### 📊 Tu analista de inversiones cuantitativo
Análisis basado en datos de más de **3.000 empresas USA** y **75 ETFs** — actualizado cada semana.
""")

    if "ANTHROPIC_API_KEY" not in os.environ or not os.environ["ANTHROPIC_API_KEY"]:
        st.error("Falta la variable ANTHROPIC_API_KEY en el archivo .env.")
        return

    # Inicializar historial con mensaje de bienvenida
    if "chat_historial" not in st.session_state:
        st.session_state.chat_historial = [
            {"role": "assistant", "content": MENSAJE_BIENVENIDA}
        ]

    # Mostrar historial
    for msg in st.session_state.chat_historial:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # Input del usuario
    if pregunta := st.chat_input("Ej: ¿Cuáles son las mejores empresas esta semana?"):

        with st.chat_message("user"):
            st.markdown(pregunta)

        contexto_base = leer_contexto_sistema()

        with st.chat_message("assistant"):
            # DEBUG TEMPORAL — borrar después
            api_key = get_secret("ANTHROPIC_API_KEY")
            st.warning(f"Key empieza con: {api_key[:15] if api_key else 'NO ENCONTRADA'}")

            with st.spinner("Analizando el sistema..."):
                try:
                    conn = get_conn()
                    contexto = enriquecer_contexto_con_ticker(conn, pregunta, contexto_base)
                    conn.close()
                    respuesta = llamar_claude_chat(
                        system=SYSTEM_PROMPT,
                        contexto=contexto,
                        pregunta=pregunta,
                        historial=st.session_state.chat_historial,
                    )
                except anthropic.APIError as e:
                    respuesta = f"Error al contactar la API de Claude: {e}"
                except Exception as e:
                    respuesta = f"Error consultando la base de datos: {e}"

            st.markdown(respuesta)

        st.session_state.chat_historial.append({"role": "user",      "content": pregunta})
        st.session_state.chat_historial.append({"role": "assistant", "content": respuesta})

    # Botón limpiar historial
    if len(st.session_state.chat_historial) > 1:
        if st.button("🗑️ Limpiar conversación"):
            st.session_state.chat_historial = [
                {"role": "assistant", "content": MENSAJE_BIENVENIDA}
            ]
            st.rerun()


# ── Página 7: TRACK RECORD ───────────────────────────────────────────────────

SQL_TR_METRICAS = """
SELECT pnl_total_usd, win_rate, sharpe, max_drawdown_pct,
       n_trades, n_wins, n_losses, n_open,
       pnl_promedio_usd, pnl_mejor_trade, pnl_peor_trade,
       pnl_stock, pnl_opciones, win_rate_stock, win_rate_opciones,
       fecha_calculo
FROM cartera.metricas_resumen
WHERE periodo = 'total'
ORDER BY fecha_calculo DESC LIMIT 1
"""

SQL_TR_TRADES = """
SELECT ticker, instrumento, estrategia,
       fecha_entrada, precio_entrada,
       fecha_salida, precio_salida,
       pnl_usd, pnl_pct, resultado,
       estado_macro, flag_timing, score_conviccion
FROM cartera.trade_results
ORDER BY fecha_entrada DESC
LIMIT 20
"""

SQL_TR_EQUITY = """
SELECT fecha, retorno_acum_pct, spy_retorno_acum, alpha_acumulado
FROM cartera.vs_benchmark
ORDER BY fecha ASC
"""

SQL_TR_POR_INSTRUMENTO = """
SELECT instrumento,
       COUNT(*) AS n_trades,
       SUM(pnl_usd) AS pnl_total,
       ROUND(AVG(CASE WHEN resultado = 'win' THEN 1.0 ELSE 0.0 END) * 100, 1) AS win_rate
FROM cartera.trade_results
WHERE resultado != 'open'
GROUP BY instrumento
ORDER BY pnl_total DESC
"""


def pagina_track_record():
    st.title("📊 Track Record")
    st.caption("Performance del sistema — IBKR Paper Trading · Actualización semanal")

    metricas = query(SQL_TR_METRICAS)
    trades   = query(SQL_TR_TRADES)
    equity   = query(SQL_TR_EQUITY)
    por_inst = query(SQL_TR_POR_INSTRUMENTO)

    hay_datos = not metricas.empty or not trades.empty

    # ── Sección 1: Header / estado ───────────────────────────────────────────
    if not hay_datos:
        st.info(
            "📊 **Track record en construcción.** "
            "El sistema comenzó a registrar operaciones recientemente. "
            "Los resultados se actualizan semanalmente desde IBKR Paper Trading. "
            "Esta página mostrará la equity curve, win rate, Sharpe y detalle de trades "
            "una vez que haya posiciones cerradas."
        )
        st.divider()
        st.caption(
            "Track record generado desde IBKR Paper Trading. "
            "Actualización semanal via Flex Query. "
            "No constituye asesoramiento financiero."
        )
        return

    # ── Sección 2: Métricas globales ─────────────────────────────────────────
    m = metricas.iloc[0]

    def _fv(campo, decimales=2):
        v = m.get(campo)
        return float(v) if v is not None and not pd.isna(v) else None

    pnl     = _fv("pnl_total_usd")
    wr      = _fv("win_rate")
    sharpe  = _fv("sharpe")
    mdd     = _fv("max_drawdown_pct")

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        pnl_str = f"${pnl:+,.2f}" if pnl is not None else "—"
        st.metric("P&L Total", pnl_str)
        if pnl is not None:
            color = "#057a55" if pnl >= 0 else "#e02424"
            st.markdown(f'<span style="color:{color};font-weight:600;">'
                        f'{"▲" if pnl >= 0 else "▼"} {"Positivo" if pnl >= 0 else "Negativo"}'
                        f'</span>', unsafe_allow_html=True)

    with c2:
        wr_str = f"{wr * 100:.1f}%" if wr is not None else "—"
        st.metric("Win Rate", wr_str)
        if wr is not None:
            color = "#057a55" if wr >= 0.60 else "#c27803"
            st.markdown(f'<span style="color:{color};font-size:.82rem;">'
                        f'{"≥ 60% ✓" if wr >= 0.60 else "< 60%"}</span>',
                        unsafe_allow_html=True)

    with c3:
        sharpe_str = f"{sharpe:.2f}" if sharpe is not None else "—"
        st.metric("Sharpe Ratio", sharpe_str)
        if sharpe is not None:
            color = "#057a55" if sharpe >= 1 else "#c27803"
            st.markdown(f'<span style="color:{color};font-size:.82rem;">'
                        f'{"≥ 1.0 ✓" if sharpe >= 1 else "< 1.0"}</span>',
                        unsafe_allow_html=True)

    with c4:
        mdd_str = f"{mdd:.2f}%" if mdd is not None else "—"
        st.metric("Max Drawdown", mdd_str)

    # Submétrica de trades
    n_t = int(m.get("n_trades") or 0)
    n_w = int(m.get("n_wins")   or 0)
    n_l = int(m.get("n_losses") or 0)
    n_o = int(m.get("n_open")   or 0)
    st.caption(f"Trades totales: {n_t}  ·  Wins: {n_w}  ·  Losses: {n_l}  ·  Open: {n_o}")

    st.divider()

    # ── Sección 3: Equity curve ──────────────────────────────────────────────
    st.subheader("Retorno acumulado vs SPY")
    if equity.empty:
        st.warning(
            "El gráfico de performance se generará cuando haya "
            "al menos 2 semanas de trades."
        )
    else:
        for col_num in ["retorno_acum_pct", "spy_retorno_acum", "alpha_acumulado"]:
            equity[col_num] = pd.to_numeric(equity[col_num], errors="coerce")
        equity["fecha"] = pd.to_datetime(equity["fecha"])

        fig = px.line(
            equity,
            x="fecha",
            y=["retorno_acum_pct", "spy_retorno_acum"],
            labels={
                "value": "Retorno acumulado (%)",
                "fecha": "Fecha",
                "variable": "Serie",
            },
            color_discrete_map={
                "retorno_acum_pct":  "#057a55",
                "spy_retorno_acum":  "#9ca3af",
            },
        )
        fig.update_traces(selector={"name": "retorno_acum_pct"}, name="Sistema")
        fig.update_traces(selector={"name": "spy_retorno_acum"}, name="SPY")

        # Área sombreada del alpha
        alpha_pos = equity[equity["alpha_acumulado"] >= 0]
        alpha_neg = equity[equity["alpha_acumulado"] < 0]
        if not alpha_pos.empty:
            fig.add_traces(px.area(
                alpha_pos, x="fecha", y="alpha_acumulado",
                color_discrete_sequence=["#057a5540"],
            ).data)
        if not alpha_neg.empty:
            fig.add_traces(px.area(
                alpha_neg, x="fecha", y="alpha_acumulado",
                color_discrete_sequence=["#e0242440"],
            ).data)

        fig.update_layout(
            plot_bgcolor="#ffffff",
            paper_bgcolor="#ffffff",
            xaxis=dict(gridcolor="#f3f4f6"),
            yaxis=dict(gridcolor="#f3f4f6", ticksuffix="%"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(l=20, r=20, t=20, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── Sección 4: Performance por instrumento ───────────────────────────────
    st.subheader("Performance por instrumento")
    if por_inst.empty:
        st.info("Sin trades cerrados todavía.")
    else:
        # Agrupar opciones multi-pata bajo una categoría
        def _grupo(inst: str) -> str:
            inst = (inst or "").lower()
            if inst == "stock":
                return "stock"
            if inst == "cash_secured_put":
                return "cash_secured_put"
            return "opciones"

        por_inst["grupo"] = por_inst["instrumento"].apply(_grupo)
        agrupado = (
            por_inst.groupby("grupo")
            .agg(n_trades=("n_trades", "sum"),
                 pnl_total=("pnl_total", "sum"),
                 win_rate=("win_rate", "mean"))
            .reset_index()
        )

        LABELS = {
            "stock":            ("Acciones",         "#057a55", "#f3faf7"),
            "cash_secured_put": ("Cash-Secured Put",  "#1d4ed8", "#eff6ff"),
            "opciones":         ("Opciones multi-pata","#7c3aed", "#f5f3ff"),
        }
        ORDEN = ["stock", "cash_secured_put", "opciones"]
        cols_inst = st.columns(3)

        for col, grupo in zip(cols_inst, ORDEN):
            fila = agrupado[agrupado["grupo"] == grupo]
            lbl, brd, bg = LABELS[grupo]
            with col:
                if fila.empty:
                    st.markdown(
                        f'<div style="border:1px solid #e5e7eb;border-radius:8px;'
                        f'padding:16px;text-align:center;color:#9ca3af;">'
                        f'<b>{lbl}</b><br>Sin trades</div>',
                        unsafe_allow_html=True,
                    )
                else:
                    r        = fila.iloc[0]
                    pnl_v    = float(r["pnl_total"] or 0)
                    wr_v     = float(r["win_rate"]  or 0)
                    pnl_color = "#057a55" if pnl_v >= 0 else "#e02424"
                    st.markdown(
                        f'<div style="background:{bg};border:1px solid {brd};'
                        f'border-radius:8px;padding:16px;">'
                        f'<div style="font-weight:700;color:{brd};margin-bottom:8px;">{lbl}</div>'
                        f'<div style="font-size:.85rem;">Trades: <b>{int(r["n_trades"])}</b></div>'
                        f'<div style="font-size:.85rem;">P&L: '
                        f'<b style="color:{pnl_color};">${pnl_v:+,.2f}</b></div>'
                        f'<div style="font-size:.85rem;">Win rate: <b>{wr_v:.1f}%</b></div>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )

    st.divider()

    # ── Sección 5: Trades recientes ──────────────────────────────────────────
    st.subheader("Trades recientes")
    if trades.empty:
        st.info("Sin trades registrados todavía.")
    else:
        for col_num in ["pnl_usd", "pnl_pct", "score_conviccion", "precio_entrada", "precio_salida"]:
            trades[col_num] = pd.to_numeric(trades[col_num], errors="coerce")

        RESULTADO_BG = {"win": "#f3faf7", "loss": "#fdf2f2", "open": "#f9fafb"}
        RESULTADO_COLOR = {"win": "#057a55", "loss": "#e02424", "open": "#6b7280"}

        for _, row in trades.iterrows():
            res       = str(row.get("resultado") or "open")
            bg        = RESULTADO_BG.get(res, "#f9fafb")
            res_color = RESULTADO_COLOR.get(res, "#6b7280")

            pnl_v   = row.get("pnl_usd")
            pnl_pct = row.get("pnl_pct")
            pnl_str = f"${float(pnl_v):+,.2f}" if pnl_v is not None and not pd.isna(pnl_v) else "—"
            pct_str = f"({float(pnl_pct):+.1f}%)" if pnl_pct is not None and not pd.isna(pnl_pct) else ""
            pnl_color = "#057a55" if (pnl_v is not None and not pd.isna(pnl_v) and float(pnl_v) >= 0) else "#e02424"

            score   = row.get("score_conviccion")
            score_s = f"{float(score):.0f}" if score is not None and not pd.isna(score) else "—"
            macro   = str(row.get("estado_macro") or "—")
            timing  = str(row.get("flag_timing")  or "—").replace("_", " ")
            inst    = str(row.get("instrumento")  or "—")
            ticker  = str(row.get("ticker")       or "—")
            f_ent   = str(row.get("fecha_entrada") or "—")
            f_sal   = str(row.get("fecha_salida")  or "abierto")

            st.markdown(
                f'<div style="background:{bg};border:1px solid #e5e7eb;border-radius:6px;'
                f'padding:10px 14px;margin-bottom:6px;display:flex;gap:16px;align-items:center;">'
                f'<span style="font-weight:700;font-size:1rem;min-width:52px;">{ticker}</span>'
                f'<span style="color:#6b7280;font-size:.82rem;min-width:60px;">{f_ent}</span>'
                f'<span style="font-size:.82rem;min-width:110px;">{inst}</span>'
                f'<span style="font-weight:700;color:{pnl_color};min-width:90px;">{pnl_str} {pct_str}</span>'
                f'<span style="background:{bg};color:{res_color};border:1px solid {res_color};'
                f'padding:1px 8px;border-radius:10px;font-size:.72rem;font-weight:700;">{res.upper()}</span>'
                f'<span style="color:#9ca3af;font-size:.78rem;">{macro} · {timing} · score {score_s}</span>'
                f'<span style="color:#9ca3af;font-size:.75rem;margin-left:auto;">salida: {f_sal}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )

    st.divider()

    # ── Sección 6: Nota al pie ───────────────────────────────────────────────
    st.caption(
        "Track record generado desde IBKR Paper Trading. "
        "Actualización semanal via Flex Query. "
        "No constituye asesoramiento financiero."
    )


# ── Navegación ───────────────────────────────────────────────────────────────

PAGINAS = {
    "💬 Asistente":          pagina_asistente,
    "📊 Cómo funciona":      pagina_como_funciona,
    "🌍 Macro":              pagina_macro,
    "🔄 Sectores":           pagina_sectores,
    "🏢 Micro":              pagina_micro,
    "🎯 Estrategia":         pagina_estrategia,
    "📈 Estrategia Opciones": pagina_opciones,
    "📊 Track Record":       pagina_track_record,
}

GRUPOS_SIDEBAR = [
    ["💬 Asistente"],
    ["📊 Cómo funciona"],
    ["🌍 Macro", "🔄 Sectores", "🏢 Micro", "🎯 Estrategia", "📈 Estrategia Opciones"],
    ["📊 Track Record"],
]

with st.sidebar:
    st.title("Sistema de Inversión")
    st.divider()

    if "pagina_actual" not in st.session_state:
        st.session_state.pagina_actual = "💬 Asistente"

    for i, grupo in enumerate(GRUPOS_SIDEBAR):
        for nombre in grupo:
            if st.button(nombre, key=f"nav_{nombre}", use_container_width=True,
                         type="primary" if st.session_state.pagina_actual == nombre else "secondary"):
                st.session_state.pagina_actual = nombre
                st.rerun()
        if i < len(GRUPOS_SIDEBAR) - 1:
            st.sidebar.markdown("---")

PAGINAS[st.session_state.pagina_actual]()
