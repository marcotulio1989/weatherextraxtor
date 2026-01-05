#!/usr/bin/env python3
"""
GFS Data Extractor - NOAA NOMADS
Extrai dados do Global Forecast System (GFS) diretamente do NOAA.
Resolução: 0.25° (~28km)
Atualização: 4x ao dia (00Z, 06Z, 12Z, 18Z)
"""

import requests
import os
import datetime
import pandas as pd
from typing import Optional, List, Dict, Any

# URLs base
NOMADS_BASE = "https://nomads.ncep.noaa.gov"
GFS_FILTER_URL = f"{NOMADS_BASE}/cgi-bin/filter_gfs_0p25.pl"
GFS_DATA_URL = f"{NOMADS_BASE}/pub/data/nccf/com/gfs/prod"

# Variáveis disponíveis no GFS
GFS_VARIABLES = {
    # Atmosféricas
    'TMP': 'Temperatura',
    'UGRD': 'Componente U do Vento',
    'VGRD': 'Componente V do Vento',
    'RH': 'Umidade Relativa',
    'PRMSL': 'Pressão ao Nível do Mar',
    'APCP': 'Precipitação Acumulada',
    'TCDC': 'Cobertura de Nuvens',
    'CAPE': 'CAPE',
    'CIN': 'CIN',
    'GUST': 'Rajada de Vento',
    'VIS': 'Visibilidade',
    # Ondas (GFS Wave)
    'HTSGW': 'Altura Significativa de Onda',
    'WVDIR': 'Direção de Onda',
    'WVPER': 'Período de Onda',
    'SWELL': 'Altura de Swell',
    'SWDIR': 'Direção de Swell',
    'SWPER': 'Período de Swell',
}

# Níveis
GFS_LEVELS = {
    'surface': 'Superfície',
    '2_m_above_ground': '2m acima do solo',
    '10_m_above_ground': '10m acima do solo',
    '80_m_above_ground': '80m acima do solo',
    'mean_sea_level': 'Nível do Mar',
    'entire_atmosphere': 'Atmosfera Inteira',
}


class GFSExtractor:
    """Extrator de dados GFS do NOAA NOMADS."""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WeatherExtractor/1.0 (Python)'
        })
    
    def _get_latest_run(self) -> tuple:
        """
        Determina o ciclo GFS mais recente disponível.
        GFS roda às 00Z, 06Z, 12Z, 18Z.
        Dados ficam disponíveis ~4h após o início do ciclo.
        
        Returns:
            (date_str, cycle): Ex: ('20260105', '00')
        """
        now_utc = datetime.datetime.utcnow()
        
        # Ciclos disponíveis (com 4h de atraso)
        available_hour = now_utc.hour - 4
        
        if available_hour >= 18:
            cycle = '18'
            date = now_utc.date()
        elif available_hour >= 12:
            cycle = '12'
            date = now_utc.date()
        elif available_hour >= 6:
            cycle = '06'
            date = now_utc.date()
        elif available_hour >= 0:
            cycle = '00'
            date = now_utc.date()
        else:
            # Usar ciclo do dia anterior
            cycle = '18'
            date = now_utc.date() - datetime.timedelta(days=1)
        
        return date.strftime('%Y%m%d'), cycle
    
    def list_available_files(self, date: str = None, cycle: str = None) -> List[str]:
        """
        Lista arquivos GFS disponíveis para um ciclo.
        
        Args:
            date: Data no formato YYYYMMDD
            cycle: Ciclo (00, 06, 12, 18)
            
        Returns:
            Lista de arquivos disponíveis
        """
        if not date or not cycle:
            date, cycle = self._get_latest_run()
        
        url = f"{GFS_DATA_URL}/gfs.{date}/{cycle}/atmos/"
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Parsear HTML para extrair nomes de arquivos
            import re
            files = re.findall(r'gfs\.t\d{2}z\.pgrb2\.0p25\.f\d{3}', response.text)
            return sorted(set(files))
        except Exception as e:
            print(f"Erro ao listar arquivos: {e}")
            return []
    
    def download_grib2(
        self,
        lat: float,
        lon: float,
        variables: List[str] = None,
        levels: List[str] = None,
        forecast_hours: List[int] = None,
        date: str = None,
        cycle: str = None,
        outdir: str = "."
    ) -> str:
        """
        Baixa dados GFS em formato GRIB2 usando o filtro do NOMADS.
        Permite selecionar apenas as variáveis e região de interesse.
        
        Args:
            lat, lon: Coordenadas do ponto de interesse
            variables: Lista de variáveis (ex: ['TMP', 'UGRD', 'VGRD'])
            levels: Lista de níveis (ex: ['surface', '2_m_above_ground'])
            forecast_hours: Horas de previsão (ex: [0, 3, 6, 12, 24])
            date: Data YYYYMMDD
            cycle: Ciclo (00, 06, 12, 18)
            outdir: Diretório de saída
            
        Returns:
            Caminho do arquivo baixado
        """
        if not date or not cycle:
            date, cycle = self._get_latest_run()
        
        if not variables:
            variables = ['TMP', 'UGRD', 'VGRD', 'RH', 'PRMSL', 'GUST']
        
        if not levels:
            levels = ['surface', '2_m_above_ground', '10_m_above_ground', 'mean_sea_level']
        
        if not forecast_hours:
            forecast_hours = [0, 3, 6, 12, 24, 48, 72]
        
        # Área de interesse (subregião)
        margin = 2.0  # graus
        left_lon = lon - margin
        right_lon = lon + margin
        top_lat = lat + margin
        bottom_lat = lat - margin
        
        # Ajustar longitude para 0-360 se necessário
        if left_lon < 0:
            left_lon += 360
        if right_lon < 0:
            right_lon += 360
        
        os.makedirs(outdir, exist_ok=True)
        downloaded_files = []
        
        for fhr in forecast_hours:
            fhr_str = f"f{fhr:03d}"
            filename = f"gfs.t{cycle}z.pgrb2.0p25.{fhr_str}"
            
            params = {
                'file': filename,
                'dir': f'/gfs.{date}/{cycle}/atmos',
                'subregion': '',
                'leftlon': left_lon,
                'rightlon': right_lon,
                'toplat': top_lat,
                'bottomlat': bottom_lat,
            }
            
            # Adicionar variáveis
            for var in variables:
                params[f'var_{var}'] = 'on'
            
            # Adicionar níveis
            for lev in levels:
                params[f'lev_{lev}'] = 'on'
            
            try:
                print(f"[GFS] Baixando {filename}...")
                response = self.session.get(GFS_FILTER_URL, params=params, timeout=120)
                
                if response.status_code == 200 and len(response.content) > 100:
                    outfile = os.path.join(outdir, f"GFS_{date}_{cycle}z_{fhr_str}.grib2")
                    with open(outfile, 'wb') as f:
                        f.write(response.content)
                    print(f"    ✓ Salvo: {outfile} ({len(response.content)} bytes)")
                    downloaded_files.append(outfile)
                else:
                    print(f"    ✗ Erro: Status {response.status_code}")
            
            except Exception as e:
                print(f"    ✗ Erro: {e}")
        
        return downloaded_files
    
    def get_point_forecast_json(
        self,
        lat: float,
        lon: float,
        date: str = None,
        cycle: str = None
    ) -> Dict[str, Any]:
        """
        Obtém previsão para um ponto usando OpenDAP/NetCDF subset.
        Retorna dados em formato JSON-like.
        
        Args:
            lat, lon: Coordenadas
            date, cycle: Data e ciclo GFS
            
        Returns:
            Dicionário com dados de previsão
        """
        if not date or not cycle:
            date, cycle = self._get_latest_run()
        
        # Usar o serviço TDS (THREDDS) da NOAA para subset
        # Alternativa: usar Open-Meteo que já processa GFS
        
        # Por simplicidade, vamos usar a API Open-Meteo com modelo GFS
        # já que ela fornece acesso fácil aos dados GFS processados
        
        url = "https://api.open-meteo.com/v1/gfs"
        
        params = {
            'latitude': lat,
            'longitude': lon,
            'hourly': ','.join([
                'temperature_2m',
                'relative_humidity_2m',
                'wind_speed_10m',
                'wind_direction_10m',
                'wind_gusts_10m',
                'pressure_msl',
                'precipitation',
                'weather_code',
                'cloud_cover',
                'visibility',
                'cape',
            ]),
            'wind_speed_unit': 'kmh',
            'timezone': 'America/Sao_Paulo',
            'forecast_days': 7,
        }
        
        try:
            response = self.session.get(url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            return {
                'modelo': 'GFS (Global Forecast System)',
                'fonte': 'NOAA/NCEP via Open-Meteo',
                'latitude': data.get('latitude'),
                'longitude': data.get('longitude'),
                'timezone': data.get('timezone'),
                'data': data.get('hourly', {}),
            }
        
        except Exception as e:
            print(f"Erro ao obter dados GFS: {e}")
            return None
    
    def get_gfs_marine(
        self,
        lat: float,
        lon: float,
    ) -> Dict[str, Any]:
        """
        Obtém dados marinhos do GFS Wave.
        
        Args:
            lat, lon: Coordenadas
            
        Returns:
            Dicionário com dados de ondas
        """
        # GFS Wave via Open-Meteo Marine API
        url = "https://marine-api.open-meteo.com/v1/marine"
        
        params = {
            'latitude': lat,
            'longitude': lon,
            'hourly': ','.join([
                'wave_height',
                'wave_direction',
                'wave_period',
                'wind_wave_height',
                'wind_wave_direction',
                'wind_wave_period',
                'swell_wave_height',
                'swell_wave_direction',
                'swell_wave_period',
            ]),
            'forecast_days': 7,
        }
        
        try:
            response = self.session.get(url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            return {
                'modelo': 'GFS Wave / NOAA WAVEWATCH III',
                'fonte': 'NOAA/NCEP',
                'latitude': data.get('latitude'),
                'longitude': data.get('longitude'),
                'data': data.get('hourly', {}),
            }
        
        except Exception as e:
            print(f"Erro ao obter dados marinhos: {e}")
            return None


def demo_gfs():
    """Demonstração do extrator GFS."""
    extractor = GFSExtractor()
    
    print("=" * 60)
    print("  GFS Data Extractor - Demo")
    print("=" * 60)
    
    # Verificar ciclo mais recente
    date, cycle = extractor._get_latest_run()
    print(f"\n📅 Ciclo GFS mais recente: {date} {cycle}Z")
    
    # Listar arquivos disponíveis
    print("\n📂 Arquivos disponíveis:")
    files = extractor.list_available_files(date, cycle)
    if files:
        print(f"   {len(files)} arquivos encontrados")
        for f in files[:5]:
            print(f"   • {f}")
        if len(files) > 5:
            print(f"   ... e mais {len(files) - 5} arquivos")
    
    # Coordenadas da Bacia de Campos
    lat, lon = -22.46, -40.54
    
    # Obter previsão em JSON
    print(f"\n🌡️ Previsão GFS para {lat}, {lon}...")
    forecast = extractor.get_point_forecast_json(lat, lon)
    
    if forecast and forecast.get('data'):
        data = forecast['data']
        times = data.get('time', [])
        temps = data.get('temperature_2m', [])
        winds = data.get('wind_speed_10m', [])
        
        print(f"   Modelo: {forecast['modelo']}")
        print(f"   {len(times)} timestamps de previsão")
        
        print(f"\n   📊 Primeiras 6 horas:")
        for i in range(min(6, len(times))):
            t = times[i] if i < len(times) else '-'
            temp = temps[i] if i < len(temps) else '-'
            wind = winds[i] if i < len(winds) else '-'
            print(f"   {t}: Temp {temp}°C, Vento {wind} km/h")
    
    # Dados marinhos
    print(f"\n🌊 Dados marinhos GFS Wave...")
    marine = extractor.get_gfs_marine(lat, lon)
    
    if marine and marine.get('data'):
        data = marine['data']
        times = data.get('time', [])
        waves = data.get('wave_height', [])
        
        print(f"   Modelo: {marine['modelo']}")
        print(f"   {len(times)} timestamps")
        
        print(f"\n   🌊 Primeiras 6 horas:")
        for i in range(min(6, len(times))):
            t = times[i] if i < len(times) else '-'
            wave = waves[i] if i < len(waves) else '-'
            print(f"   {t}: Onda {wave}m")
    
    print("\n" + "=" * 60)


def extrair_gfs_csv(lat: float, lon: float, outdir: str = ".") -> str:
    """
    Extrai dados GFS e salva em CSV.
    
    Args:
        lat, lon: Coordenadas
        outdir: Diretório de saída
        
    Returns:
        Caminho do arquivo CSV
    """
    extractor = GFSExtractor()
    
    print(f"🌡️ Obtendo dados GFS para {lat}, {lon}...")
    
    # Dados atmosféricos
    atmos = extractor.get_point_forecast_json(lat, lon)
    
    # Dados marinhos
    marine = extractor.get_gfs_marine(lat, lon)
    
    if not atmos or not atmos.get('data'):
        raise ValueError("Não foi possível obter dados GFS")
    
    # Converter para DataFrame
    df_atmos = pd.DataFrame(atmos['data'])
    
    if marine and marine.get('data'):
        df_marine = pd.DataFrame(marine['data'])
        # Merge por tempo
        df = df_atmos.merge(df_marine, on='time', how='left', suffixes=('', '_marine'))
    else:
        df = df_atmos
    
    # Adicionar metadados
    df['modelo'] = 'GFS'
    df['fonte'] = 'NOAA/NCEP'
    df['latitude'] = lat
    df['longitude'] = lon
    
    # Salvar
    os.makedirs(outdir, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M")
    filename = f"GFS_{timestamp}.csv"
    filepath = os.path.join(outdir, filename)
    
    df.to_csv(filepath, index=False)
    print(f"✅ Dados GFS salvos em: {filepath}")
    print(f"   {len(df)} linhas, {len(df.columns)} colunas")
    
    return filepath


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Extrator de dados GFS (NOAA/NCEP)")
    parser.add_argument("--lat", type=float, default=-22.46, help="Latitude")
    parser.add_argument("--lon", type=float, default=-40.54, help="Longitude")
    parser.add_argument("--outdir", type=str, default="output", help="Diretório de saída")
    parser.add_argument("--demo", action="store_true", help="Executar demonstração")
    parser.add_argument("--csv", action="store_true", help="Exportar para CSV")
    parser.add_argument("--grib", action="store_true", help="Baixar arquivos GRIB2")
    
    args = parser.parse_args()
    
    if args.demo:
        demo_gfs()
    elif args.grib:
        extractor = GFSExtractor()
        extractor.download_grib2(args.lat, args.lon, outdir=args.outdir)
    elif args.csv:
        extrair_gfs_csv(args.lat, args.lon, args.outdir)
    else:
        print("Uso do GFS Extractor:")
        print("  --demo              Executar demonstração")
        print("  --csv               Exportar para CSV")
        print("  --grib              Baixar arquivos GRIB2")
        print("  --lat LAT           Latitude (default: -22.46)")
        print("  --lon LON           Longitude (default: -40.54)")
        print("  --outdir DIR        Diretório de saída")
        print("")
        print("Exemplos:")
        print("  python gfs_extractor.py --demo")
        print("  python gfs_extractor.py --csv --lat -22.46 --lon -40.54")
        print("  python gfs_extractor.py --grib --outdir output")
