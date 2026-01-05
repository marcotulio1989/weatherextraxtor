#!/usr/bin/env python3
"""
Weather Extractor Unificado
Extrai dados de múltiplas fontes meteorológicas:
- Open-Meteo (ECMWF, ICON, GFS, Météo-France, JMA)
- CPTEC/INPE (Brasil)
- GFS NOAA (GRIB2)

Gera dashboard HTML e arquivos CSV consolidados.
"""

import os
import sys
import argparse
import datetime
import pandas as pd
import xarray as xr


def extrair_openmeteo(lat: float, lon: float, outdir: str, html: bool = True, cidade: str = None):
    """Extrai dados do Open-Meteo (multi-modelo)."""
    print("\n" + "=" * 60)
    print("  📡 OPEN-METEO (Multi-Modelo)")
    print("=" * 60)
    
    try:
        from app import run
        return run(
            lat=lat,
            lon=lon,
            timezone="America/Sao_Paulo",
            outdir=outdir,
            forecast_days=3,
            past_hours=24,
            future_hours=24,
            generate_html=html,
            cidade_cptec=cidade
        )
    except Exception as e:
        print(f"  ❌ Erro Open-Meteo: {e}")
        return 1


def extrair_cptec(cidade: str, outdir: str):
    """Extrai dados do CPTEC/INPE."""
    print("\n" + "=" * 60)
    print("  🇧🇷 CPTEC/INPE (Brasil)")
    print("=" * 60)
    
    try:
        from cptec_extractor import extrair_para_csv
        filepath = extrair_para_csv(cidade, dias=7, outdir=outdir)
        return filepath
    except Exception as e:
        print(f"  ❌ Erro CPTEC: {e}")
        return None


def extrair_gfs_grib(lat: float, lon: float, outdir: str):
    """Baixa dados GFS em formato GRIB2."""
    print("\n" + "=" * 60)
    print("  🌐 GFS NOAA (GRIB2)")
    print("=" * 60)
    
    try:
        from gfs_extractor import GFSExtractor
        extractor = GFSExtractor()
        files = extractor.download_grib2(
            lat=lat,
            lon=lon,
            variables=['TMP', 'UGRD', 'VGRD', 'RH', 'PRMSL', 'GUST'],
            levels=['surface', '2_m_above_ground', '10_m_above_ground', 'mean_sea_level'],
            forecast_hours=[0, 3, 6, 12, 24, 48, 72],
            outdir=outdir
        )
        return files
    except Exception as e:
        print(f"  ❌ Erro GFS: {e}")
        return []


def processar_grib_para_csv(grib_files: list, lat: float, lon: float, outdir: str):
    """Processa arquivos GRIB2 e extrai série temporal para um ponto."""
    print("\n" + "=" * 60)
    print("  📊 Processando GRIB2 → CSV")
    print("=" * 60)
    
    if not grib_files:
        print("  ⚠️ Nenhum arquivo GRIB2 para processar")
        return None
    
    try:
        records = []
        
        # Converter longitude para 0-360
        lon_360 = lon + 360 if lon < 0 else lon
        
        for grib_file in sorted(grib_files):
            try:
                ds = xr.open_dataset(grib_file, engine='cfgrib')
                
                # Selecionar ponto mais próximo
                ds_point = ds.sel(latitude=lat, longitude=lon_360, method='nearest')
                
                # Extrair dados
                time_val = pd.Timestamp(ds_point.time.values)
                step_val = pd.Timedelta(ds_point.step.values)
                valid_time = time_val + step_val
                
                record = {
                    'time': valid_time.isoformat(),
                    'forecast_time': time_val.isoformat(),
                    'step_hours': step_val.total_seconds() / 3600,
                }
                
                # Extrair variáveis disponíveis
                for var in ds_point.data_vars:
                    val = float(ds_point[var].values)
                    
                    # Converter unidades
                    if var in ['t', 't2m']:
                        val = val - 273.15  # Kelvin → Celsius
                        record[f'{var}_celsius'] = round(val, 2)
                    elif var in ['prmsl']:
                        val = val / 100  # Pa → hPa
                        record[f'{var}_hpa'] = round(val, 2)
                    elif var in ['gust']:
                        val = val * 3.6  # m/s → km/h
                        record[f'{var}_kmh'] = round(val, 2)
                    else:
                        record[var] = round(val, 2)
                
                records.append(record)
                print(f"  ✓ {os.path.basename(grib_file)}: {valid_time}")
                
                ds.close()
                
            except Exception as e:
                print(f"  ⚠️ Erro em {grib_file}: {e}")
        
        if records:
            df = pd.DataFrame(records)
            df = df.sort_values('time')
            
            # Salvar CSV
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M")
            filename = f"GFS_timeseries_{timestamp}.csv"
            filepath = os.path.join(outdir, filename)
            
            df.to_csv(filepath, index=False)
            print(f"\n  ✅ Série temporal salva: {filepath}")
            print(f"     {len(df)} registros, {len(df.columns)} colunas")
            
            return filepath
        
        return None
        
    except ImportError:
        print("  ⚠️ xarray/cfgrib não instalados. Execute: pip install xarray cfgrib eccodes")
        return None
    except Exception as e:
        print(f"  ❌ Erro ao processar GRIB: {e}")
        return None


def consolidar_dados(outdir: str, lat: float, lon: float):
    """Consolida todos os dados em um único arquivo."""
    print("\n" + "=" * 60)
    print("  📦 Consolidando Dados")
    print("=" * 60)
    
    arquivos = {
        'openmeteo': [],
        'cptec': [],
        'gfs': []
    }
    
    # Listar arquivos gerados
    if os.path.exists(outdir):
        for f in os.listdir(outdir):
            if f.endswith('.csv'):
                if 'OpenMeteo' in f or 'MULTIMODEL' in f:
                    arquivos['openmeteo'].append(f)
                elif 'CPTEC' in f:
                    arquivos['cptec'].append(f)
                elif 'GFS' in f:
                    arquivos['gfs'].append(f)
    
    print(f"\n  Arquivos encontrados:")
    for fonte, lista in arquivos.items():
        print(f"    {fonte.upper()}: {len(lista)} arquivo(s)")
        for f in lista[-3:]:  # Mostrar os 3 mais recentes
            print(f"      • {f}")
    
    # Criar resumo
    resumo = {
        'timestamp': datetime.datetime.now().isoformat(),
        'coordenadas': {'lat': lat, 'lon': lon},
        'arquivos': arquivos,
        'total_arquivos': sum(len(v) for v in arquivos.values())
    }
    
    # Salvar resumo em JSON
    import json
    resumo_file = os.path.join(outdir, 'resumo_extracao.json')
    with open(resumo_file, 'w', encoding='utf-8') as f:
        json.dump(resumo, f, indent=2, ensure_ascii=False)
    
    print(f"\n  ✅ Resumo salvo: {resumo_file}")
    
    return resumo


def main():
    parser = argparse.ArgumentParser(
        description="Extrator Unificado de Dados Meteorológicos",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Fontes de Dados:
  Open-Meteo    : ECMWF, ICON, GFS, Météo-France, JMA (ondas, vento, temperatura)
  CPTEC/INPE    : Previsão brasileira oficial (temperatura, condição)
  GFS NOAA      : Dados brutos GRIB2 do Global Forecast System

Exemplos:
  # Extração completa para Bacia de Campos
  python unified_extractor.py --all --cidade "Campos dos Goytacazes"
  
  # Apenas Open-Meteo com dashboard
  python unified_extractor.py --openmeteo --html
  
  # Apenas CPTEC
  python unified_extractor.py --cptec --cidade "Rio de Janeiro"
  
  # Apenas GFS GRIB2
  python unified_extractor.py --gfs-grib
        """
    )
    
    # Coordenadas
    parser.add_argument("--lat", type=float, default=-22.46, help="Latitude (default: -22.46 Bacia de Campos)")
    parser.add_argument("--lon", type=float, default=-40.54, help="Longitude (default: -40.54 Bacia de Campos)")
    parser.add_argument("--outdir", type=str, default="output", help="Diretório de saída")
    parser.add_argument("--cidade", type=str, default=None, help="Cidade para CPTEC (ex: 'Campos dos Goytacazes')")
    
    # Fontes
    parser.add_argument("--all", action="store_true", help="Extrair de todas as fontes")
    parser.add_argument("--openmeteo", action="store_true", help="Extrair Open-Meteo (multi-modelo)")
    parser.add_argument("--cptec", action="store_true", help="Extrair CPTEC/INPE")
    parser.add_argument("--gfs-grib", action="store_true", help="Baixar GFS GRIB2")
    parser.add_argument("--process-grib", action="store_true", help="Processar GRIB2 → CSV")
    
    # Opções
    parser.add_argument("--html", action="store_true", help="Gerar dashboard HTML")
    parser.add_argument("--no-consolidate", action="store_true", help="Não consolidar dados")
    
    args = parser.parse_args()
    
    # Se nenhuma fonte especificada, mostrar ajuda
    if not any([args.all, args.openmeteo, args.cptec, args.gfs_grib]):
        parser.print_help()
        print("\n⚠️  Use --all para extrair de todas as fontes ou especifique fontes individuais.")
        return 1
    
    # Criar diretório de saída
    os.makedirs(args.outdir, exist_ok=True)
    
    print("=" * 60)
    print("  🌤️ WEATHER EXTRACTOR UNIFICADO")
    print("=" * 60)
    print(f"  Coordenadas: {args.lat}, {args.lon}")
    print(f"  Saída: {args.outdir}/")
    if args.cidade:
        print(f"  Cidade CPTEC: {args.cidade}")
    
    resultados = {}
    grib_files = []
    
    # Open-Meteo
    if args.all or args.openmeteo:
        result = extrair_openmeteo(args.lat, args.lon, args.outdir, args.html, args.cidade)
        resultados['openmeteo'] = 'OK' if result == 0 else 'ERRO'
    
    # CPTEC
    if (args.all or args.cptec) and args.cidade:
        filepath = extrair_cptec(args.cidade, args.outdir)
        resultados['cptec'] = 'OK' if filepath else 'ERRO'
    elif args.cptec and not args.cidade:
        print("\n⚠️  CPTEC requer --cidade. Use: --cidade 'Nome da Cidade'")
        resultados['cptec'] = 'SKIP'
    
    # GFS GRIB2
    if args.all or args.gfs_grib:
        grib_files = extrair_gfs_grib(args.lat, args.lon, args.outdir)
        resultados['gfs_grib'] = 'OK' if grib_files else 'ERRO'
    
    # Processar GRIB2
    if (args.all or args.process_grib) and grib_files:
        csv_file = processar_grib_para_csv(grib_files, args.lat, args.lon, args.outdir)
        resultados['grib_csv'] = 'OK' if csv_file else 'ERRO'
    
    # Consolidar
    if not args.no_consolidate:
        consolidar_dados(args.outdir, args.lat, args.lon)
    
    # Resumo final
    print("\n" + "=" * 60)
    print("  📋 RESUMO")
    print("=" * 60)
    for fonte, status in resultados.items():
        emoji = "✅" if status == "OK" else "❌" if status == "ERRO" else "⏭️"
        print(f"  {emoji} {fonte.upper()}: {status}")
    
    print(f"\n  📂 Arquivos em: {os.path.abspath(args.outdir)}/")
    
    # Mostrar arquivos gerados
    if os.path.exists(args.outdir):
        print("\n  Arquivos gerados:")
        for f in sorted(os.listdir(args.outdir))[-10:]:
            size = os.path.getsize(os.path.join(args.outdir, f))
            print(f"    • {f} ({size:,} bytes)")
    
    print("\n" + "=" * 60)
    
    return 0 if all(v != 'ERRO' for v in resultados.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
