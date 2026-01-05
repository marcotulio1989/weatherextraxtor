#!/usr/bin/env python3
"""
Weather Extractor - Open-Meteo (Multi-Model)
Extrai dados meteorológicos e marinhos com menor intervalo possível (15 min).
Compara múltiplos modelos: ECMWF, ICON, GFS, etc.
Dashboard de monitoramento em tempo real.
"""

import requests
import pandas as pd
import datetime
import os
import argparse
import pytz
import time
import json
from jinja2 import Template, Environment, FileSystemLoader

# Import CPTEC/INPE extractor
try:
    from cptec_extractor import CPTECExtractor, CONDICOES_CPTEC
    CPTEC_AVAILABLE = True
except ImportError:
    CPTEC_AVAILABLE = False
    CONDICOES_CPTEC = {}


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


def run(lat, lon, timezone, outdir=None, forecast_days=3, past_hours=24, future_hours=24, generate_html=False, cidade_cptec=None):
    """Executa a extração completa com múltiplos modelos."""
    # Calcular período de tempo: 24h antes e 24h depois
    tz = pytz.timezone(timezone)
    agora = datetime.datetime.now(tz)
    
    # Nome do arquivo com data e hora de extração
    DATA_HORA = agora.strftime("%Y-%m-%d_%H%M")
    NOME_ARQUIVO = f"OpenMeteo_MULTIMODEL_{DATA_HORA}.csv"
    if outdir:
        os.makedirs(outdir, exist_ok=True)
        NOME_ARQUIVO = os.path.join(outdir, NOME_ARQUIVO)

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
            html_file = gerar_html(df_final, agora, timezone, lat, lon, outdir, NOME_ARQUIVO, cidade_cptec)
            print(f"  HTML gerado: {html_file}")
        
        return 0

    except Exception as e:
        import traceback
        print(f"Erro ao salvar CSV: {e}")
        traceback.print_exc()
        return 1


def get_wind_direction_text(degrees):
    """Converte graus em texto de direção do vento."""
    if degrees is None or pd.isna(degrees):
        return "N/A"
    directions = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", 
                  "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    idx = int((degrees + 11.25) / 22.5) % 16
    return directions[idx]


def get_safe_value(df, col, idx, default=0):
    """Obtém valor seguro de um DataFrame."""
    try:
        if col in df.columns:
            val = df.iloc[idx][col]
            if pd.notna(val):
                return round(float(val), 2)
    except:
        pass
    return default


def buscar_dados_cptec(cidade_nome="Campos dos Goytacazes"):
    """Busca dados do CPTEC/INPE para uma cidade."""
    if not CPTEC_AVAILABLE:
        return None
    
    try:
        extractor = CPTECExtractor()
        
        # Buscar cidade
        cidades = extractor.buscar_cidade(cidade_nome)
        if not cidades:
            print(f"[CPTEC] Cidade '{cidade_nome}' não encontrada")
            return None
        
        codigo = int(cidades[0].get('id'))
        nome = cidades[0].get('nome')
        estado = cidades[0].get('estado')
        
        # Previsão 7 dias
        previsao = extractor.previsao_cidade_7dias(codigo)
        
        if not previsao or not previsao.get('previsao'):
            print(f"[CPTEC] Sem previsão para {nome}")
            return None
        
        # Formatar dados
        dados_cptec = {
            'cidade': nome,
            'estado': estado,
            'atualizado': previsao.get('atualizado', ''),
            'previsao': []
        }
        
        for dia in previsao.get('previsao', []):
            condicao_cod = dia.get('condicao', '')
            condicao_desc = CONDICOES_CPTEC.get(condicao_cod, condicao_cod)
            
            dados_cptec['previsao'].append({
                'data': dia.get('data', ''),
                'min': dia.get('min', ''),
                'max': dia.get('max', ''),
                'condicao': condicao_cod,
                'condicao_desc': condicao_desc,
                'iuv': dia.get('iuv', '0')
            })
        
        print(f"[CPTEC] ✓ {len(dados_cptec['previsao'])} dias de previsão para {nome}/{estado}")
        return dados_cptec
        
    except Exception as e:
        print(f"[CPTEC] Erro: {e}")
        return None


def gerar_html(df, agora, timezone, lat, lon, outdir=None, csv_filename=None, cidade_cptec=None):
    """Gera um dashboard HTML interativo com gráficos e monitoramento em tempo real."""
    
    # Buscar dados CPTEC se cidade especificada
    dados_cptec = None
    if cidade_cptec:
        dados_cptec = buscar_dados_cptec(cidade_cptec)
    
    # Listar todos os CSVs disponíveis na pasta
    csv_files = []
    if outdir and os.path.exists(outdir):
        for f in sorted(os.listdir(outdir), reverse=True):
            if f.endswith('.csv') and f.startswith('OpenMeteo_MULTIMODEL_'):
                csv_files.append(f)
    
    # Preparar dados
    df = df.copy()
    df['time_dt'] = pd.to_datetime(df['time'])
    hora_atual_str = agora.strftime("%Y-%m-%dT%H")
    
    # Encontrar índice mais próximo da hora atual
    agora_naive = agora.replace(tzinfo=None)
    df['time_diff'] = abs(df['time_dt'] - agora_naive)
    current_idx = df['time_diff'].idxmin()
    
    # Selecionar todas as colunas (exceto auxiliares) para a tabela completa
    exclude_cols = {'time_dt', 'time_diff'}
    main_cols = ['time']
    for col in df.columns:
        if col not in exclude_cols and col != 'time':
            main_cols.append(col)
    
    df_display = df[main_cols].copy()
    df_display['is_current'] = df_display['time'].str.startswith(hora_atual_str)
    
    # Preparar dados para gráficos (amostrar a cada 4 pontos = 1h para reduzir dados)
    df_chart = df.iloc[::4].reset_index(drop=True)
    chart_current_idx = len(df_chart[df_chart['time_dt'] <= agora_naive]) - 1
    
    # Preparar JSON para Chart.js
    # NOTA: Os dados do CSV já estão em knots, não precisa conversão
    def safe_list(series):
        return [round(float(x), 2) if pd.notna(x) else None for x in series]
    
    chart_data = {
        'time': df_chart['time'].str.replace('T', ' ').str[:16].tolist(),
        # Ondas (Marine API - único modelo)
        'wave_height': safe_list(df_chart.get('wave_height', pd.Series())),
        'wave_direction': safe_list(df_chart.get('wave_direction', pd.Series())),
        'wave_period': safe_list(df_chart.get('wave_period', pd.Series())),
        'swell_height': safe_list(df_chart.get('swell_wave_height', pd.Series())),
        'swell_direction': safe_list(df_chart.get('swell_wave_direction', pd.Series())),
        'swell_period': safe_list(df_chart.get('swell_wave_period', pd.Series())),
        'swell_peak_period': safe_list(df_chart.get('swell_wave_peak_period', pd.Series())),
        'wind_wave_height': safe_list(df_chart.get('wind_wave_height', pd.Series())),
        'wind_wave_direction': safe_list(df_chart.get('wind_wave_direction', pd.Series())),
        'wind_wave_period': safe_list(df_chart.get('wind_wave_period', pd.Series())),

        # Instabilidade/Severidade
        'cape': safe_list(df_chart.get('cape_best_match', pd.Series())),
        'lifted_index': safe_list(df_chart.get('lifted_index_best_match', pd.Series())),
        'cin': safe_list(df_chart.get('convective_inhibition_best_match', pd.Series())),
        'lightning': safe_list(df_chart.get('lightning_potential_best_match', pd.Series())),
        'sunshine': safe_list(df_chart.get('sunshine_duration_best_match', pd.Series())),
        'visibility': safe_list(df_chart.get('visibility_best_match', pd.Series())),

        # Radiação
        'shortwave': safe_list(df_chart.get('shortwave_radiation_best_match', pd.Series())),
        'direct_rad': safe_list(df_chart.get('direct_radiation_best_match', pd.Series())),
        'diffuse_rad': safe_list(df_chart.get('diffuse_radiation_best_match', pd.Series())),
        'dni': safe_list(df_chart.get('direct_normal_irradiance_best_match', pd.Series())),
        'gti': safe_list(df_chart.get('global_tilted_irradiance_best_match', pd.Series())),
        'terrestrial_rad': safe_list(df_chart.get('terrestrial_radiation_best_match', pd.Series())),
        
        # Cobertura de Nuvens
        'cloud_cover': safe_list(df_chart.get('cloud_cover_best_match', pd.Series())),
        'cloud_cover_low': safe_list(df_chart.get('cloud_cover_low_best_match', pd.Series())),
        'cloud_cover_mid': safe_list(df_chart.get('cloud_cover_mid_best_match', pd.Series())),
        'cloud_cover_high': safe_list(df_chart.get('cloud_cover_high_best_match', pd.Series())),
        
        # Umidade e Precipitação
        'humidity': safe_list(df_chart.get('relative_humidity_2m_best_match', pd.Series())),
        'dew_point': safe_list(df_chart.get('dew_point_2m_best_match', pd.Series())),
        'apparent_temp': safe_list(df_chart.get('apparent_temperature_best_match', pd.Series())),
        'precipitation': safe_list(df_chart.get('precipitation_best_match', pd.Series())),
        'rain': safe_list(df_chart.get('rain_best_match', pd.Series())),
        'snowfall': safe_list(df_chart.get('snowfall_best_match', pd.Series())),
        'snow_depth': safe_list(df_chart.get('snow_depth_best_match', pd.Series())),
        'evapotranspiration': safe_list(df_chart.get('evapotranspiration_best_match', pd.Series())),
        
        # Pressão de Superfície
        'surface_pressure': safe_list(df_chart.get('surface_pressure_best_match', pd.Series())),
        
        # Vento 80m (já em knots no CSV)
        'wind_speed_80m': safe_list(df_chart.get('wind_speed_80m_best_match', pd.Series())),
        'wind_dir_80m': safe_list(df_chart.get('wind_direction_80m_best_match', pd.Series())),
        
        # Weather Code
        'weather_code': safe_list(df_chart.get('weather_code_best_match', pd.Series())),
        
        # Velocidade do Vento - TODOS OS MODELOS (já em knots no CSV)
        'wind_speed_ecmwf': safe_list(df_chart.get('wind_speed_10m_ecmwf_ifs025', pd.Series())),
        'wind_speed_icon': safe_list(df_chart.get('wind_speed_10m_icon_seamless', pd.Series())),
        'wind_speed_gfs': safe_list(df_chart.get('wind_speed_10m_gfs_seamless', pd.Series())),
        'wind_speed_meteofrance': safe_list(df_chart.get('wind_speed_10m_meteofrance_seamless', pd.Series())),
        'wind_speed_jma': safe_list(df_chart.get('wind_speed_10m_jma_seamless', pd.Series())),
        
        # Direção do Vento - TODOS OS MODELOS
        'wind_dir_ecmwf': safe_list(df_chart.get('wind_direction_10m_ecmwf_ifs025', pd.Series())),
        'wind_dir_icon': safe_list(df_chart.get('wind_direction_10m_icon_seamless', pd.Series())),
        'wind_dir_gfs': safe_list(df_chart.get('wind_direction_10m_gfs_seamless', pd.Series())),
        'wind_dir_meteofrance': safe_list(df_chart.get('wind_direction_10m_meteofrance_seamless', pd.Series())),
        'wind_dir_jma': safe_list(df_chart.get('wind_direction_10m_jma_seamless', pd.Series())),
        
        # Rajadas - TODOS OS MODELOS (já em knots no CSV)
        'wind_gusts_ecmwf': safe_list(df_chart.get('wind_gusts_10m_ecmwf_ifs025', pd.Series())),
        'wind_gusts_icon': safe_list(df_chart.get('wind_gusts_10m_icon_seamless', pd.Series())),
        'wind_gusts_gfs': safe_list(df_chart.get('wind_gusts_10m_gfs_seamless', pd.Series())),
        'wind_gusts_meteofrance': safe_list(df_chart.get('wind_gusts_10m_meteofrance_seamless', pd.Series())),
        'wind_gusts_jma': safe_list(df_chart.get('wind_gusts_10m_jma_seamless', pd.Series())),
        
        # Temperatura - TODOS OS MODELOS
        'temp_ecmwf': safe_list(df_chart.get('temperature_2m_ecmwf_ifs025', pd.Series())),
        'temp_icon': safe_list(df_chart.get('temperature_2m_icon_seamless', pd.Series())),
        'temp_gfs': safe_list(df_chart.get('temperature_2m_gfs_seamless', pd.Series())),
        'temp_meteofrance': safe_list(df_chart.get('temperature_2m_meteofrance_seamless', pd.Series())),
        'temp_jma': safe_list(df_chart.get('temperature_2m_jma_seamless', pd.Series())),
        
        # Pressão - TODOS OS MODELOS
        'pressure_ecmwf': safe_list(df_chart.get('pressure_msl_ecmwf_ifs025', pd.Series())),
        'pressure_icon': safe_list(df_chart.get('pressure_msl_icon_seamless', pd.Series())),
        'pressure_gfs': safe_list(df_chart.get('pressure_msl_gfs_seamless', pd.Series())),
        'pressure_meteofrance': safe_list(df_chart.get('pressure_msl_meteofrance_seamless', pd.Series())),
        'pressure_jma': safe_list(df_chart.get('pressure_msl_jma_seamless', pd.Series())),
        
        # Aliases para compatibilidade (já em knots no CSV)
        'wind_speed': safe_list(df_chart.get('wind_speed_10m_ecmwf_ifs025', pd.Series())),
        'wind_gusts': safe_list(df_chart.get('wind_gusts_10m_ecmwf_ifs025', pd.Series())),
        'wind_direction': safe_list(df_chart.get('wind_direction_10m_ecmwf_ifs025', pd.Series())),
        'wind_ecmwf': safe_list(df_chart.get('wind_speed_10m_ecmwf_ifs025', pd.Series())),
        'wind_icon': safe_list(df_chart.get('wind_speed_10m_icon_seamless', pd.Series())),
        'wind_gfs': safe_list(df_chart.get('wind_speed_10m_gfs_seamless', pd.Series())),
        'wind_meteofrance': safe_list(df_chart.get('wind_speed_10m_meteofrance_seamless', pd.Series())),
        'wind_jma': safe_list(df_chart.get('wind_speed_10m_jma_seamless', pd.Series())),
    }
    
    # Valores atuais
    current_wave_height = get_safe_value(df, 'wave_height', current_idx, 0)
    current_wave_direction = get_safe_value(df, 'wave_direction', current_idx, 0)
    current_wave_period = get_safe_value(df, 'wave_period', current_idx, 0)
    current_swell_height = get_safe_value(df, 'swell_wave_height', current_idx, 0)
    current_swell_direction = get_safe_value(df, 'swell_wave_direction', current_idx, 0)
    current_swell_period = get_safe_value(df, 'swell_wave_period', current_idx, 0)
    current_wind_wave_height = get_safe_value(df, 'wind_wave_height', current_idx, 0)
    current_wind_wave_direction = get_safe_value(df, 'wind_wave_direction', current_idx, 0)
    
    # Fator de conversão para knots
    KMH_TO_KT = 0.539957
    
    current_wind_speed = round(get_safe_value(df, 'wind_speed_10m_ecmwf_ifs025', current_idx, 0) * KMH_TO_KT, 1)
    current_wind_gusts = round(get_safe_value(df, 'wind_gusts_10m_ecmwf_ifs025', current_idx, 0) * KMH_TO_KT, 1)
    current_wind_direction = get_safe_value(df, 'wind_direction_10m_ecmwf_ifs025', current_idx, 0)
    current_temperature = get_safe_value(df, 'temperature_2m_ecmwf_ifs025', current_idx, 0)
    current_pressure = get_safe_value(df, 'pressure_msl_ecmwf_ifs025', current_idx, 0)
    current_apparent_temp = get_safe_value(df, 'apparent_temperature_best_match', current_idx, current_temperature)

    # Valores atuais (instabilidade / radiação)
    current_cape = get_safe_value(df, 'cape_best_match', current_idx, None)
    current_lifted_index = get_safe_value(df, 'lifted_index_best_match', current_idx, None)
    current_cin = get_safe_value(df, 'convective_inhibition_best_match', current_idx, None)
    current_lightning = get_safe_value(df, 'lightning_potential_best_match', current_idx, None)
    current_dni = get_safe_value(df, 'direct_normal_irradiance_best_match', current_idx, None)
    current_shortwave = get_safe_value(df, 'shortwave_radiation_best_match', current_idx, None)
    
    # Valores por modelo (em KNOTS)
    wind_ecmwf = round(get_safe_value(df, 'wind_speed_10m_ecmwf_ifs025', current_idx, 0) * KMH_TO_KT, 1)
    wind_icon = round(get_safe_value(df, 'wind_speed_10m_icon_seamless', current_idx, 0) * KMH_TO_KT, 1)
    wind_gfs = round(get_safe_value(df, 'wind_speed_10m_gfs_seamless', current_idx, 0) * KMH_TO_KT, 1)
    wind_meteofrance = round(get_safe_value(df, 'wind_speed_10m_meteofrance_seamless', current_idx, 0) * KMH_TO_KT, 1)
    wind_jma = round(get_safe_value(df, 'wind_speed_10m_jma_seamless', current_idx, 0) * KMH_TO_KT, 1)
    
    # Calcular tendências (comparar com 1h atrás) - em KNOTS
    prev_idx = max(0, current_idx - 4)
    prev_wave = get_safe_value(df, 'wave_height', prev_idx, current_wave_height)
    prev_wind = get_safe_value(df, 'wind_speed_10m_ecmwf_ifs025', prev_idx, 0) * KMH_TO_KT

    wave_diff = current_wave_height - prev_wave
    wind_diff = current_wind_speed - prev_wind
    
    if wave_diff > 0.1:
        wave_trend_class, wave_trend_icon, wave_trend_text = 'trend-up', '↑', f'+{wave_diff:.1f}m'
    elif wave_diff < -0.1:
        wave_trend_class, wave_trend_icon, wave_trend_text = 'trend-down', '↓', f'{wave_diff:.1f}m'
    else:
        wave_trend_class, wave_trend_icon, wave_trend_text = 'trend-stable', '→', 'Estável'
    
    if wind_diff > 1:  # 1 kt threshold
        wind_trend_class, wind_trend_icon, wind_trend_text = 'trend-up', '↑', f'+{wind_diff:.1f}kt'
    elif wind_diff < -1:
        wind_trend_class, wind_trend_icon, wind_trend_text = 'trend-down', '↓', f'{wind_diff:.1f}kt'
    else:
        wind_trend_class, wind_trend_icon, wind_trend_text = 'trend-stable', '→', 'Estável'
    
    wind_direction_text = get_wind_direction_text(current_wind_direction)
    
    # Preparar linhas da tabela
    columns = list(main_cols)
    rows = []
    agora_str = agora.strftime("%Y-%m-%dT%H:%M")
    
    for idx, row in df_display.iterrows():
        time_str = row['time']
        is_current = row['is_current']
        
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
    
    # Carregar template
    template_path = os.path.join(os.path.dirname(__file__), 'templates', 'dashboard.html')
    
    if os.path.exists(template_path):
        with open(template_path, 'r', encoding='utf-8') as f:
            html_template = f.read()
    else:
        # Fallback para template inline simples
        html_template = get_fallback_template()
    
    csv_basename = os.path.basename(csv_filename) if csv_filename else ""
    
    template = Template(html_template)
    html_content = template.render(
        lat=lat,
        lon=lon,
        timezone=timezone,
        current_time=agora.strftime("%H:%M"),
        update_time=agora.strftime("%Y-%m-%d %H:%M:%S"),
        csv_filename=csv_basename,
        csv_files=csv_files,
        columns=columns,
        rows=rows,
        chart_data_json=json.dumps(chart_data),
        current_time_index=chart_current_idx,
        # Valores atuais
        current_wave_height=current_wave_height,
        current_wave_direction=int(current_wave_direction),
        current_wave_period=current_wave_period,
        current_swell_height=current_swell_height,
        current_swell_direction=int(current_swell_direction),
        current_swell_period=current_swell_period,
        current_wind_wave_height=current_wind_wave_height,
        current_wind_wave_direction=int(current_wind_wave_direction),
        current_wind_speed=current_wind_speed,
        current_wind_gusts=current_wind_gusts,
        current_wind_direction=int(current_wind_direction),
        current_temperature=current_temperature,
        current_pressure=current_pressure,
        current_apparent_temp=current_apparent_temp,
        current_cape=current_cape,
        current_lifted_index=current_lifted_index,
        current_cin=current_cin,
        current_lightning=current_lightning,
        current_dni=current_dni,
        current_shortwave=current_shortwave,
        wind_direction_text=wind_direction_text,
        # Tendências
        wave_trend_class=wave_trend_class,
        wave_trend_icon=wave_trend_icon,
        wave_trend_text=wave_trend_text,
        wind_trend_class=wind_trend_class,
        wind_trend_icon=wind_trend_icon,
        wind_trend_text=wind_trend_text,
        # Modelos
        wind_ecmwf=wind_ecmwf,
        wind_icon=wind_icon,
        wind_gfs=wind_gfs,
        wind_meteofrance=wind_meteofrance,
        wind_jma=wind_jma,
        # CPTEC/INPE
        cptec_data=dados_cptec,
        cptec_json=json.dumps(dados_cptec) if dados_cptec else 'null',
    )
    
    # Salvar HTML
    html_file = "index.html"
    if outdir:
        html_file = os.path.join(outdir, html_file)
    
    with open(html_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    return html_file


def get_fallback_template():
    """Template HTML de fallback caso o arquivo não exista."""
    return """
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <title>Weather Monitor</title>
    <meta http-equiv="refresh" content="900">
    <style>
        body { font-family: Arial; background: #1a1a2e; color: #e0e0e0; padding: 20px; }
        h1 { color: #00d4ff; }
        table { width: 100%; border-collapse: collapse; }
        th { background: #00d4ff; color: #1a1a2e; padding: 10px; }
        td { padding: 8px; border-bottom: 1px solid #333; }
        .current-row { background: rgba(255,107,107,0.4); }
        .past-row { opacity: 0.6; }
    </style>
</head>
<body>
    <h1>🌊 Weather Monitor - {{ lat }}, {{ lon }}</h1>
    <p>Atualizado: {{ update_time }}</p>
    <table>
        <thead><tr>{% for col in columns %}<th>{{ col }}</th>{% endfor %}</tr></thead>
        <tbody>
            {% for row in rows %}
            <tr class="{{ row.row_class }}">{% for val in row.cells %}<td>{{ val }}</td>{% endfor %}</tr>
            {% endfor %}
        </tbody>
    </table>
</body>
</html>
"""


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
    parser.add_argument(
        "--cidade", type=str, default=None, help="Cidade para buscar dados CPTEC/INPE (Brasil)"
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
        args.html,
        args.cidade
    )
    raise SystemExit(exit_code)
