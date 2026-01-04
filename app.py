#!/usr/bin/env python3
"""
Weather Extractor - Open-Meteo
Extrai dados meteorológicos e marinhos com menor intervalo possível (15 min).
"""

import requests
import pandas as pd
import datetime
import os
import argparse


# --------------------------------------------------------------------------
# VARIÁVEIS DISPONÍVEIS (minutely_15)
# --------------------------------------------------------------------------

# Forecast API - todas as variáveis suportadas em minutely_15
FORECAST_VARS = [
    # Temperatura e umidade
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "apparent_temperature",
    # Precipitação
    "precipitation",
    "rain",
    "snowfall",
    "snow_depth",
    # Condição
    "weather_code",
    # Pressão
    "pressure_msl",
    "surface_pressure",
    # Nuvens e visibilidade
    "cloud_cover",
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high",
    "visibility",
    # Evapotranspiração
    "evapotranspiration",
    # Vento
    "wind_speed_10m",
    "wind_speed_80m",
    "wind_direction_10m",
    "wind_direction_80m",
    "wind_gusts_10m",
    # Radiação solar
    "shortwave_radiation",
    "direct_radiation",
    "diffuse_radiation",
    "direct_normal_irradiance",
    "global_tilted_irradiance",
    "terrestrial_radiation",
    # Convecção / Tempestades
    "cape",
    "lifted_index",
    "convective_inhibition",
    # Outros
    "sunshine_duration",
    "lightning_potential",
]

# Marine API - todas as variáveis suportadas em minutely_15
MARINE_VARS = [
    # Ondas totais
    "wave_height",
    "wave_direction",
    "wave_period",
    # Mar de vento
    "wind_wave_height",
    "wind_wave_direction",
    "wind_wave_period",
    # Swell (ondulação de fundo)
    "swell_wave_height",
    "swell_wave_direction",
    "swell_wave_period",
    "swell_wave_peak_period",
    # Corrente oceânica
    "ocean_current_velocity",
    "ocean_current_direction",
]


def baixar_dados(url, params, nome_etapa, chave="minutely_15"):
    """Baixa dados de uma API Open-Meteo e retorna DataFrame."""
    print(f"[{nome_etapa}] Solicitando dados...")
    try:
        response = requests.get(url, params=params, timeout=30)
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
        # Mostra detalhes do erro se disponíveis
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
    """Executa a extração completa."""
    DATA_HOJE = datetime.date.today()
    NOME_ARQUIVO = f"OpenMeteo_FULL_{DATA_HOJE}.csv"
    if outdir:
        os.makedirs(outdir, exist_ok=True)
        NOME_ARQUIVO = os.path.join(outdir, NOME_ARQUIVO)

    print("=" * 70)
    print("       OPEN-METEO DATA EXTRACTOR - FULL VARIABLES")
    print("=" * 70)
    print(f"  Alvo       : Lat {lat} / Lon {lon}")
    print(f"  Timezone   : {timezone}")
    print(f"  Intervalo  : 15 minutos")
    print(f"  Previsão   : {forecast_days} dias")
    print("=" * 70)

    # -------------------------------------------------------------------------
    # ETAPA 1: MARINE API (Dados de Mar)
    # -------------------------------------------------------------------------
    url_marine = "https://marine-api.open-meteo.com/v1/marine"
    params_marine = {
        "latitude": lat,
        "longitude": lon,
        "timezone": timezone,
        "forecast_days": forecast_days,
        "minutely_15": ",".join(MARINE_VARS),
    }
    df_marine = baixar_dados(url_marine, params_marine, "1/2 Marine API")

    # -------------------------------------------------------------------------
    # ETAPA 2: FORECAST API (Atmosfera completa)
    # -------------------------------------------------------------------------
    url_forecast = "https://api.open-meteo.com/v1/forecast"
    params_forecast = {
        "latitude": lat,
        "longitude": lon,
        "timezone": timezone,
        "forecast_days": forecast_days,
        "minutely_15": ",".join(FORECAST_VARS),
    }
    df_forecast = baixar_dados(url_forecast, params_forecast, "2/2 Forecast API")

    # -------------------------------------------------------------------------
    # CONSOLIDAÇÃO
    # -------------------------------------------------------------------------
    print("-" * 70)
    print("Consolidando bases de dados...")

    dfs = []
    if not df_marine.empty:
        dfs.append(df_marine)
    if not df_forecast.empty:
        dfs.append(df_forecast)

    if len(dfs) == 0:
        print("ERRO: Nenhuma base retornou dados. Abortando.")
        return 1

    if len(dfs) == 1:
        df_final = dfs[0]
    else:
        df_final = pd.merge(df_marine, df_forecast, on="time", how="outer")

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
        print("Colunas disponíveis:")
        for i, col in enumerate(df_final.columns, 1):
            print(f"  {i:2}. {col}")
        print()
        return 0

    except Exception as e:
        print(f"Erro ao salvar CSV: {e}")
        return 1


def parse_args():
    parser = argparse.ArgumentParser(
        description="Extrator meteorológico/marinho Open-Meteo (15 min)."
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
