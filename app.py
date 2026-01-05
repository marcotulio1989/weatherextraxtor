#!/usr/bin/env python3
"""
Weather Extractor - Open-Meteo (Multi-Model)
Extrai dados meteorológicos e marinhos com menor intervalo possível (15 min).
Compara múltiplos modelos: ECMWF, ICON, GFS, etc.
"""

import requests
import pandas as pd
import datetime
import os
import argparse
import pytz
import time
from jinja2 import Template


# --------------------------------------------------------------------------
# MODELOS DISPONÍVEIS
# --------------------------------------------------------------------------

# Modelos que suportam minutely_15 na Forecast API
WEATHER_MODELS = [
    "ecmwf_ifs025",      # ECMWF IFS 0.25° (Europa) - mais preciso
    "icon_seamless",     # ICON (Alemanha) - excelente para Europa/Atlântico
    "gfs_seamless",      # GFS (EUA) - global
    "meteofrance_seamless",  # Météo-France
    "jma_seamless",      # JMA (Japão)
]

# --------------------------------------------------------------------------
# VARIÁVEIS DISPONÍVEIS (minutely_15)
# --------------------------------------------------------------------------

# Variáveis principais para comparação entre modelos
CORE_VARS = [
    # Vento (principal para comparação)
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    # Pressão
    "pressure_msl",
    # Condição
    "weather_code",
    # Temperatura
    "temperature_2m",
    # Precipitação
    "precipitation",
]

# Variáveis extras (só modelo padrão, não variam muito entre modelos)
EXTRA_VARS = [
    "relative_humidity_2m",
    "dew_point_2m",
    "apparent_temperature",
    "rain",
    "snowfall",
    "snow_depth",
    "surface_pressure",
    "cloud_cover",
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high",
    "visibility",
    "evapotranspiration",
    "wind_speed_80m",
    "wind_direction_80m",
    "shortwave_radiation",
    "direct_radiation",
    "diffuse_radiation",
    "direct_normal_irradiance",
    "global_tilted_irradiance",
    "terrestrial_radiation",
    "cape",
    "lifted_index",
    "convective_inhibition",
    "sunshine_duration",
    "lightning_potential",
]

# Marine API - dividido em grupos menores para evitar timeout
MARINE_VARS_WAVES = [
    "wave_height",
    "wave_direction",
    "wave_period",
]

MARINE_VARS_WIND_WAVES = [
    "wind_wave_height",
    "wind_wave_direction",
    "wind_wave_period",
]

MARINE_VARS_SWELL = [
    "swell_wave_height",
    "swell_wave_direction",
    "swell_wave_period",
    "swell_wave_peak_period",
]

# Lista completa (sem correntes oceânicas - não disponível para todas as regiões)
MARINE_VARS = MARINE_VARS_WAVES + MARINE_VARS_WIND_WAVES + MARINE_VARS_SWELL


def baixar_dados(url, params, nome_etapa, chave="minutely_15", delay=2):
    """Baixa dados de uma API Open-Meteo e retorna DataFrame."""
    print(f"[{nome_etapa}] Solicitando dados...")
    
    # Aguarda entre requisições para não sobrecarregar a API
    time.sleep(delay)
    
    try:
        response = requests.get(url, params=params, timeout=120)
        response.raise_for_status()
        dados = response.json()

        if chave in dados:
            df = pd.DataFrame(dados[chave])
            print(f"    ✓ {len(df)} registros, {len(df.columns)} colunas")
            return df
        else:
            print(f"    ✗ Chave '{chave}' não encontrada.")
            return pd.DataFrame()

    except requests.exceptions.HTTPError as e:
        print(f"    ✗ HTTP Error: {e}")
        try:
            err = e.response.json()
            if "reason" in err:
                print(f"      Motivo: {err['reason']}")
        except Exception:
            pass
        return pd.DataFrame()
    except Exception as e:
        print(f"    ✗ Erro: {e}")
        return pd.DataFrame()


def run(lat, lon, timezone, outdir=None, forecast_days=3, past_hours=24, future_hours=24, generate_html=False):
    """Executa a extração completa com múltiplos modelos."""
    DATA_HOJE = datetime.date.today()
    NOME_ARQUIVO = f"OpenMeteo_MULTIMODEL_{DATA_HOJE}.csv"
    if outdir:
        os.makedirs(outdir, exist_ok=True)
        NOME_ARQUIVO = os.path.join(outdir, NOME_ARQUIVO)

    # Calcular período de tempo: 24h antes e 24h depois
    tz = pytz.timezone(timezone)
    agora = datetime.datetime.now(tz)
    inicio = agora - datetime.timedelta(hours=past_hours)
    fim = agora + datetime.timedelta(hours=future_hours)
    
    # Parâmetros de data para API
    start_date = inicio.strftime("%Y-%m-%d")
    end_date = fim.strftime("%Y-%m-%d")

    print("=" * 70)
    print("    OPEN-METEO DATA EXTRACTOR - MULTI-MODEL COMPARISON")
    print("=" * 70)
    print(f"  Alvo       : Lat {lat} / Lon {lon}")
    print(f"  Timezone   : {timezone}")
    print(f"  Intervalo  : 15 minutos")
    print(f"  Período    : {inicio.strftime('%Y-%m-%d %H:%M')} até {fim.strftime('%Y-%m-%d %H:%M')}")
    print(f"  Modelos    : {', '.join(WEATHER_MODELS)}")
    print("=" * 70)

    all_dfs = []

    # -------------------------------------------------------------------------
    # ETAPA 1: MARINE API (Dados de Mar)
    # -------------------------------------------------------------------------
    url_marine = "https://marine-api.open-meteo.com/v1/marine"
    params_marine = {
        "latitude": lat,
        "longitude": lon,
        "timezone": timezone,
        "start_date": start_date,
        "end_date": end_date,
        "minutely_15": ",".join(MARINE_VARS),
    }
    df_marine = baixar_dados(url_marine, params_marine, "1. Marine API")
    if not df_marine.empty:
        all_dfs.append(df_marine)

    # -------------------------------------------------------------------------
    # ETAPA 2: FORECAST API - TODOS OS MODELOS EM UMA ÚNICA REQUISIÇÃO
    # -------------------------------------------------------------------------
    url_forecast = "https://api.open-meteo.com/v1/forecast"
    
    params_multimodel = {
        "latitude": lat,
        "longitude": lon,
        "timezone": timezone,
        "start_date": start_date,
        "end_date": end_date,
        "models": ",".join(WEATHER_MODELS),
        "minutely_15": ",".join(CORE_VARS),
    }
    df_multimodel = baixar_dados(
        url_forecast, params_multimodel, "2. Forecast (Multi-Model)"
    )
    if not df_multimodel.empty:
        all_dfs.append(df_multimodel)

    # -------------------------------------------------------------------------
    # ETAPA 3: FORECAST API - EXTRAS (modelo padrão, variáveis adicionais)
    # -------------------------------------------------------------------------
    params_extras = {
        "latitude": lat,
        "longitude": lon,
        "timezone": timezone,
        "start_date": start_date,
        "end_date": end_date,
        "minutely_15": ",".join(EXTRA_VARS),
    }
    df_extras = baixar_dados(url_forecast, params_extras, "3. Forecast Extras")
    if not df_extras.empty:
        # Renomear para indicar modelo padrão
        cols_rename = {
            col: f"{col}_best_match" for col in df_extras.columns if col != "time"
        }
        df_extras.rename(columns=cols_rename, inplace=True)
        all_dfs.append(df_extras)

    # -------------------------------------------------------------------------
    # ETAPA FINAL: Dados horários para variáveis sem minutely_15
    # -------------------------------------------------------------------------
    # Algumas variáveis só existem em hourly, vamos buscar também
    hourly_vars = [
        "soil_temperature_0cm",
        "soil_moisture_0_to_1cm",
        "uv_index",
        "uv_index_clear_sky",
        "is_day",
        "freezing_level_height",
    ]
    params_hourly = {
        "latitude": lat,
        "longitude": lon,
        "timezone": timezone,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(hourly_vars),
    }
    df_hourly = baixar_dados(
        url_forecast, params_hourly, "4. Hourly Extras", chave="hourly"
    )
    # Não mesclamos hourly com minutely_15 (intervalos diferentes)
    # Salvamos separado se necessário

    # -------------------------------------------------------------------------
    # CONSOLIDAÇÃO
    # -------------------------------------------------------------------------
    print("-" * 70)
    print("Consolidando bases de dados...")

    if len(all_dfs) == 0:
        print("ERRO: Nenhuma base retornou dados. Abortando.")
        return 1

    # Merge sequencial por 'time'
    df_final = all_dfs[0]
    for df in all_dfs[1:]:
        df_final = pd.merge(df_final, df, on="time", how="outer")

    # Ordenar por tempo
    df_final.sort_values("time", inplace=True)
    df_final.reset_index(drop=True, inplace=True)

    # Filtrar para ±24h exatas
    df_final['time_dt'] = pd.to_datetime(df_final['time'])
    inicio_filtro = agora - datetime.timedelta(hours=past_hours)
    fim_filtro = agora + datetime.timedelta(hours=future_hours)
    
    # Converter para naive datetime para comparação
    inicio_naive = inicio_filtro.replace(tzinfo=None)
    fim_naive = fim_filtro.replace(tzinfo=None)
    
    df_final = df_final[(df_final['time_dt'] >= inicio_naive) & (df_final['time_dt'] <= fim_naive)]
    df_final = df_final.drop(columns=['time_dt'])
    df_final.reset_index(drop=True, inplace=True)
    
    print(f"Filtrado para período: {inicio_filtro.strftime('%Y-%m-%d %H:%M')} até {fim_filtro.strftime('%Y-%m-%d %H:%M')}")

    # Salvar
    try:
        df_final.to_csv(NOME_ARQUIVO, index=False)
        caminho_completo = os.path.abspath(NOME_ARQUIVO)

        print()
        print("=" * 70)
        print("  SUCESSO!")
        print("=" * 70)
        print(f"  Arquivo     : {NOME_ARQUIVO}")
        print(f"  Caminho     : {caminho_completo}")
        print(f"  Linhas      : {len(df_final)} (intervalos de 15 min)")
        print(f"  Colunas     : {len(df_final.columns)}")
        print("=" * 70)
        print()
        
        # Mostrar colunas por categoria
        cols = list(df_final.columns)
        print("COLUNAS DISPONÍVEIS:")
        print("-" * 70)
        
        # Agrupar por modelo
        models_found = set()
        for col in cols:
            for model in WEATHER_MODELS:
                if model in col:
                    models_found.add(model)
        
        print("\n[MARINE - Dados de Mar]")
        marine_cols = [c for c in cols if any(v in c for v in MARINE_VARS)]
        for c in marine_cols[:12]:  # limitar output
            print(f"  • {c}")
        
        for model in sorted(models_found):
            print(f"\n[{model.upper()}]")
            model_cols = [c for c in cols if model in c]
            for c in model_cols[:8]:
                print(f"  • {c}")
            if len(model_cols) > 8:
                print(f"  ... e mais {len(model_cols) - 8} colunas")
        
        extras_cols = [c for c in cols if "best_match" in c]
        if extras_cols:
            print("\n[BEST_MATCH - Variáveis Extras]")
            for c in extras_cols[:10]:
                print(f"  • {c}")
            if len(extras_cols) > 10:
                print(f"  ... e mais {len(extras_cols) - 10} colunas")
        
        print()
        
        # Gerar HTML se solicitado
        if generate_html:
            html_file = gerar_html(df_final, agora, timezone, lat, lon, outdir)
            print(f"  HTML gerado: {html_file}")
        
        return 0

    except Exception as e:
        import traceback
        print(f"Erro ao salvar CSV: {e}")
        traceback.print_exc()
        return 1


def gerar_html(df, agora, timezone, lat, lon, outdir=None):
    """Gera um arquivo HTML com a tabela de dados, destacando a hora atual."""
    
    # Identificar a linha mais próxima da hora atual
    df['time_dt'] = pd.to_datetime(df['time'])
    hora_atual_str = agora.strftime("%Y-%m-%dT%H")
    
    # Selecionar apenas colunas principais para exibição (para não ficar muito pesado)
    main_cols = ['time']
    
    # Adicionar colunas de ondas
    for col in df.columns:
        if any(v in col for v in ['wave_height', 'wave_direction', 'wave_period', 'wind_speed', 'wind_direction', 'wind_gusts', 'temperature', 'pressure']):
            if col not in main_cols and len(main_cols) < 25:
                main_cols.append(col)
    
    df_display = df[main_cols].copy()
    
    # Marcar linha atual
    df_display['is_current'] = df_display['time'].str.startswith(hora_atual_str)
    
    html_template = """
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="refresh" content="3600">
    <title>Weather Extractor - Bacia de Campos</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
            min-height: 100vh;
            color: #e0e0e0;
            padding: 20px;
        }
        .container {
            max-width: 100%;
            margin: 0 auto;
        }
        header {
            text-align: center;
            padding: 30px;
            background: rgba(255,255,255,0.05);
            border-radius: 15px;
            margin-bottom: 30px;
            backdrop-filter: blur(10px);
        }
        h1 {
            font-size: 2.5em;
            color: #00d4ff;
            text-shadow: 0 0 20px rgba(0,212,255,0.5);
            margin-bottom: 10px;
        }
        .info {
            display: flex;
            justify-content: center;
            gap: 30px;
            flex-wrap: wrap;
            margin-top: 15px;
        }
        .info-item {
            background: rgba(0,212,255,0.1);
            padding: 10px 20px;
            border-radius: 25px;
            border: 1px solid rgba(0,212,255,0.3);
        }
        .info-item strong {
            color: #00d4ff;
        }
        .update-time {
            background: linear-gradient(90deg, #ff6b6b, #ffa502);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            font-size: 1.2em;
            margin-top: 15px;
        }
        .table-container {
            overflow-x: auto;
            background: rgba(255,255,255,0.03);
            border-radius: 15px;
            padding: 20px;
            backdrop-filter: blur(10px);
        }
        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.85em;
        }
        th {
            background: linear-gradient(180deg, #00d4ff 0%, #0099cc 100%);
            color: #1a1a2e;
            padding: 12px 8px;
            text-align: left;
            font-weight: 600;
            position: sticky;
            top: 0;
            white-space: nowrap;
        }
        td {
            padding: 10px 8px;
            border-bottom: 1px solid rgba(255,255,255,0.05);
            white-space: nowrap;
        }
        tr:hover {
            background: rgba(0,212,255,0.1);
        }
        .current-row {
            background: linear-gradient(90deg, rgba(255,107,107,0.4), rgba(255,165,2,0.4)) !important;
            animation: pulse 2s infinite;
            font-weight: bold;
        }
        .current-row td {
            color: #fff;
            border-bottom: 2px solid #ff6b6b;
        }
        @keyframes pulse {
            0%, 100% { box-shadow: 0 0 10px rgba(255,107,107,0.5); }
            50% { box-shadow: 0 0 25px rgba(255,107,107,0.8); }
        }
        .past-row {
            opacity: 0.6;
        }
        .legend {
            display: flex;
            justify-content: center;
            gap: 30px;
            margin: 20px 0;
            flex-wrap: wrap;
        }
        .legend-item {
            display: flex;
            align-items: center;
            gap: 8px;
        }
        .legend-color {
            width: 20px;
            height: 20px;
            border-radius: 4px;
        }
        .legend-current {
            background: linear-gradient(90deg, rgba(255,107,107,0.8), rgba(255,165,2,0.8));
        }
        .legend-past {
            background: rgba(255,255,255,0.3);
        }
        .legend-future {
            background: rgba(0,212,255,0.5);
        }
        footer {
            text-align: center;
            padding: 20px;
            margin-top: 30px;
            color: #888;
        }
        .download-btn {
            display: inline-block;
            background: linear-gradient(90deg, #00d4ff, #0099cc);
            color: #1a1a2e;
            padding: 12px 30px;
            border-radius: 25px;
            text-decoration: none;
            font-weight: bold;
            margin: 10px;
            transition: transform 0.3s, box-shadow 0.3s;
        }
        .download-btn:hover {
            transform: translateY(-3px);
            box-shadow: 0 10px 30px rgba(0,212,255,0.4);
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🌊 Weather Extractor</h1>
            <p>Dados Meteorológicos e Marinhos - Bacia de Campos</p>
            <div class="info">
                <div class="info-item"><strong>📍 Latitude:</strong> {{ lat }}</div>
                <div class="info-item"><strong>📍 Longitude:</strong> {{ lon }}</div>
                <div class="info-item"><strong>🌐 Timezone:</strong> {{ timezone }}</div>
                <div class="info-item"><strong>📊 Registros:</strong> {{ total_rows }}</div>
            </div>
            <p class="update-time">🕐 Última atualização: {{ update_time }}</p>
            <p style="margin-top: 15px;">
                <a href="OpenMeteo_MULTIMODEL_{{ date }}.csv" class="download-btn">📥 Download CSV</a>
            </p>
        </header>
        
        <div class="legend">
            <div class="legend-item">
                <div class="legend-color legend-current"></div>
                <span>Hora Atual</span>
            </div>
            <div class="legend-item">
                <div class="legend-color legend-past"></div>
                <span>Passado (24h)</span>
            </div>
            <div class="legend-item">
                <div class="legend-color legend-future"></div>
                <span>Futuro (24h)</span>
            </div>
        </div>
        
        <div class="table-container">
            <table>
                <thead>
                    <tr>
                        {% for col in columns %}
                        <th>{{ col }}</th>
                        {% endfor %}
                    </tr>
                </thead>
                <tbody>
                    {% for row in rows %}
                    <tr class="{{ row.row_class }}">
                        {% for val in row.cells %}
                        <td>{{ val }}</td>
                        {% endfor %}
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
        
        <footer>
            <p>Dados obtidos via <a href="https://open-meteo.com/" style="color: #00d4ff;">Open-Meteo API</a></p>
            <p>Atualização automática a cada hora</p>
        </footer>
    </div>
</body>
</html>
    """
    
    # Preparar dados para o template
    columns = list(main_cols)
    rows = []
    
    agora_str = agora.strftime("%Y-%m-%dT%H:%M")
    
    for _, row in df_display.iterrows():
        time_str = row['time']
        is_current = row['is_current']
        
        # Determinar classe da linha
        if is_current:
            row_class = 'current-row'
        elif time_str < agora_str:
            row_class = 'past-row'
        else:
            row_class = ''
        
        values = []
        for col in main_cols:
            val = row[col]
            if pd.isna(val):
                values.append('-')
            elif isinstance(val, float):
                values.append(f'{val:.2f}')
            else:
                values.append(str(val))
        
        rows.append({'row_class': row_class, 'cells': values})
    
    template = Template(html_template)
    html_content = template.render(
        lat=lat,
        lon=lon,
        timezone=timezone,
        total_rows=len(df),
        update_time=agora.strftime("%Y-%m-%d %H:%M:%S"),
        date=agora.strftime("%Y-%m-%d"),
        columns=columns,
        rows=rows
    )
    
    # Salvar HTML
    html_file = "index.html"
    if outdir:
        html_file = os.path.join(outdir, html_file)
    
    with open(html_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    return html_file


def parse_args():
    parser = argparse.ArgumentParser(
        description="Extrator meteorológico/marinho Open-Meteo - Multi-Model (15 min)."
    )
    parser.add_argument("--lat", type=float, default=-22.46, help="Latitude alvo")
    parser.add_argument("--lon", type=float, default=-40.54, help="Longitude alvo")
    parser.add_argument(
        "--timezone", type=str, default="America/Sao_Paulo", help="Timezone"
    )
    parser.add_argument("--outdir", type=str, default=None, help="Pasta de saída")
    parser.add_argument(
        "--days", type=int, default=3, help="Dias de previsão (1-16)"
    )
    parser.add_argument(
        "--past-hours", type=int, default=24, help="Horas passadas para buscar"
    )
    parser.add_argument(
        "--future-hours", type=int, default=24, help="Horas futuras para buscar"
    )
    parser.add_argument(
        "--html", action="store_true", help="Gerar arquivo HTML com visualização"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    exit_code = run(
        args.lat, 
        args.lon, 
        args.timezone, 
        args.outdir, 
        args.days,
        args.past_hours,
        args.future_hours,
        args.html
    )
    raise SystemExit(exit_code)
