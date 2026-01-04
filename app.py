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

# Marine API - todas as variáveis suportadas em minutely_15
MARINE_VARS = [
    "wave_height",
    "wave_direction",
    "wave_period",
    "wind_wave_height",
    "wind_wave_direction",
    "wind_wave_period",
    "swell_wave_height",
    "swell_wave_direction",
    "swell_wave_period",
    "swell_wave_peak_period",
    "ocean_current_velocity",
    "ocean_current_direction",
]


def baixar_dados(url, params, nome_etapa, chave="minutely_15"):
    """Baixa dados de uma API Open-Meteo e retorna DataFrame."""
    print(f"[{nome_etapa}] Solicitando dados...")
    try:
        response = requests.get(url, params=params, timeout=60)
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


def run(lat, lon, timezone, outdir=None, forecast_days=3):
    """Executa a extração completa com múltiplos modelos."""
    DATA_HOJE = datetime.date.today()
    NOME_ARQUIVO = f"OpenMeteo_MULTIMODEL_{DATA_HOJE}.csv"
    if outdir:
        os.makedirs(outdir, exist_ok=True)
        NOME_ARQUIVO = os.path.join(outdir, NOME_ARQUIVO)

    print("=" * 70)
    print("    OPEN-METEO DATA EXTRACTOR - MULTI-MODEL COMPARISON")
    print("=" * 70)
    print(f"  Alvo       : Lat {lat} / Lon {lon}")
    print(f"  Timezone   : {timezone}")
    print(f"  Intervalo  : 15 minutos")
    print(f"  Previsão   : {forecast_days} dias")
    print(f"  Modelos    : {', '.join(WEATHER_MODELS)}")
    print("=" * 70)

    all_dfs = []

    # -------------------------------------------------------------------------
    # ETAPA 1: MARINE API (Dados de Mar) - modelo único
    # -------------------------------------------------------------------------
    url_marine = "https://marine-api.open-meteo.com/v1/marine"
    params_marine = {
        "latitude": lat,
        "longitude": lon,
        "timezone": timezone,
        "forecast_days": forecast_days,
        "minutely_15": ",".join(MARINE_VARS),
    }
    df_marine = baixar_dados(url_marine, params_marine, "1/4 Marine API")
    if not df_marine.empty:
        all_dfs.append(df_marine)

    # -------------------------------------------------------------------------
    # ETAPA 2: FORECAST API - MULTI-MODEL (variáveis core)
    # -------------------------------------------------------------------------
    url_forecast = "https://api.open-meteo.com/v1/forecast"
    params_multimodel = {
        "latitude": lat,
        "longitude": lon,
        "timezone": timezone,
        "forecast_days": forecast_days,
        "models": ",".join(WEATHER_MODELS),
        "minutely_15": ",".join(CORE_VARS),
    }
    df_multimodel = baixar_dados(
        url_forecast, params_multimodel, "2/4 Forecast Multi-Model"
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
        "forecast_days": forecast_days,
        "minutely_15": ",".join(EXTRA_VARS),
    }
    df_extras = baixar_dados(url_forecast, params_extras, "3/4 Forecast Extras")
    if not df_extras.empty:
        # Renomear para indicar modelo padrão
        cols_rename = {
            col: f"{col}_best_match" for col in df_extras.columns if col != "time"
        }
        df_extras.rename(columns=cols_rename, inplace=True)
        all_dfs.append(df_extras)

    # -------------------------------------------------------------------------
    # ETAPA 4: Dados horários para variáveis sem minutely_15
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
        "forecast_days": forecast_days,
        "hourly": ",".join(hourly_vars),
    }
    df_hourly = baixar_dados(
        url_forecast, params_hourly, "4/4 Hourly Extras", chave="hourly"
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
        return 0

    except Exception as e:
        print(f"Erro ao salvar CSV: {e}")
        return 1


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
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    exit_code = run(args.lat, args.lon, args.timezone, args.outdir, args.days)
    raise SystemExit(exit_code)
