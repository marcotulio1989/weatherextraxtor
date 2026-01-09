"""
=============================================================================
GOES-R DERIVED MOTION WINDS (DMW) EXTRACTOR
=============================================================================
Baixa dados NetCDF de ventos derivados de movimento do GOES-16/18/19.
Os dados DMW são derivados do movimento das nuvens e vapor d'água entre
imagens consecutivas do satélite.

Fonte: AWS S3 - noaa-goes16/ABI-L2-DMWF/ (Full Disk)
                 noaa-goes18/ABI-L2-DMWF/
                 noaa-goes19/ABI-L2-DMWF/

Níveis de pressão aproximados:
- Baixo (Low-Level): 700-1000 hPa (abaixo de ~3km) - nuvens cumulus
- Médio (Mid-Level): 400-700 hPa (~3-7km) - nuvens altocumulus
- Alto (High-Level): 100-400 hPa (acima de ~7km) - cirrus

Documentação: https://www.goes-r.gov/products/baseline-derived-motion-winds.html
=============================================================================
"""

import os
import json
import boto3
from datetime import datetime, timedelta, timezone
import numpy as np
from botocore import UNSIGNED
from botocore.config import Config

try:
    import xarray as xr
    HAS_XARRAY = True
except ImportError:
    HAS_XARRAY = False
    print("⚠️ xarray não disponível - funcionalidade limitada")

try:
    import netCDF4 as nc
    HAS_NETCDF4 = True
except ImportError:
    HAS_NETCDF4 = False
    print("⚠️ netCDF4 não disponível")


# =========================================================================
# CONFIGURAÇÃO
# =========================================================================

# Diretório para cache de dados
CACHE_DIR = "/workspaces/weatherextraxtor/data/dmw"
os.makedirs(CACHE_DIR, exist_ok=True)

# S3 Bucket NOAA (acesso público)
S3_CONFIG = Config(signature_version=UNSIGNED)

# Satellites disponíveis
SATELLITES = {
    "goes16": {"bucket": "noaa-goes16", "name": "GOES-16 (Backup)"},
    "goes18": {"bucket": "noaa-goes18", "name": "GOES-18 (West)"},
    "goes19": {"bucket": "noaa-goes19", "name": "GOES-19 (East)"},
}

# Produtos DMW disponíveis
DMW_PRODUCTS = {
    "DMWF": "Full Disk",
    "DMWC": "CONUS (EUA)",
    "DMWVF": "Full Disk - Vapor d'Água",
}

# Níveis de altitude baseados em pressão (hPa)
ALTITUDE_LEVELS = {
    "low": {"min_pressure": 700, "max_pressure": 1100, "name": "Baixo Nível (<3km)"},
    "mid": {"min_pressure": 400, "max_pressure": 700, "name": "Nível Médio (3-7km)"},
    "high": {"min_pressure": 100, "max_pressure": 400, "name": "Alto Nível (>7km)"},
    "all": {"min_pressure": 100, "max_pressure": 1100, "name": "Todos os níveis"},
}


class DMWExtractor:
    """Extrator de Derived Motion Winds do GOES-R."""
    
    def __init__(self, satellite="goes16"):
        """
        Inicializa o extrator.
        
        Args:
            satellite: goes16, goes18, ou goes19
        """
        if satellite not in SATELLITES:
            raise ValueError(f"Satélite inválido: {satellite}. Use: {list(SATELLITES.keys())}")
        
        self.satellite = satellite
        self.bucket_name = SATELLITES[satellite]["bucket"]
        self.s3 = boto3.client('s3', config=S3_CONFIG)
        
        print(f"🛰️ DMW Extractor inicializado para {SATELLITES[satellite]['name']}")
    
    def list_available_files(self, product="DMWF", hours_back=6):
        """
        Lista arquivos DMW disponíveis nas últimas horas.
        
        Args:
            product: DMWF (Full Disk), DMWC (CONUS), ou DMWM (Mesoscale)
            hours_back: Quantas horas no passado buscar
            
        Returns:
            Lista de dicionários com informações dos arquivos
        """
        now = datetime.now(timezone.utc)
        files = []
        
        for hours_ago in range(hours_back):
            dt = now - timedelta(hours=hours_ago)
            prefix = f"ABI-L2-{product}/{dt.year}/{dt.strftime('%j')}/{dt.strftime('%H')}/"
            
            try:
                response = self.s3.list_objects_v2(
                    Bucket=self.bucket_name,
                    Prefix=prefix,
                    MaxKeys=100
                )
                
                if 'Contents' in response:
                    for obj in response['Contents']:
                        key = obj['Key']
                        if key.endswith('.nc'):
                            # Extrair informações do nome do arquivo
                            filename = os.path.basename(key)
                            parts = filename.split('_')
                            
                            # Parse da data/hora do scan
                            for part in parts:
                                if part.startswith('s'):
                                    scan_time = datetime.strptime(part[1:14], '%Y%j%H%M%S')
                                    break
                            else:
                                continue
                            
                            files.append({
                                'key': key,
                                'filename': filename,
                                'size_mb': obj['Size'] / 1024 / 1024,
                                'scan_time': scan_time,
                                'bucket': self.bucket_name
                            })
                            
            except Exception as e:
                print(f"⚠️ Erro ao listar {prefix}: {e}")
        
        # Ordenar por data (mais recente primeiro)
        files.sort(key=lambda x: x['scan_time'], reverse=True)
        
        return files
    
    def download_latest(self, product="DMWF", use_cache=True):
        """
        Baixa o arquivo DMW mais recente.
        
        Args:
            product: DMWF, DMWC, ou DMWM
            use_cache: Se True, usa arquivo em cache se recente
            
        Returns:
            Caminho do arquivo baixado
        """
        # Verificar cache
        cache_file = os.path.join(CACHE_DIR, f"{self.satellite}_{product}_latest.nc")
        cache_meta = cache_file + ".meta"
        
        if use_cache and os.path.exists(cache_file) and os.path.exists(cache_meta):
            # Verificar se cache é recente (menos de 15 minutos)
            with open(cache_meta, 'r') as f:
                meta = json.load(f)
            cache_time = datetime.fromisoformat(meta['download_time'].replace('Z', '+00:00'))
            if cache_time.tzinfo is None:
                cache_time = cache_time.replace(tzinfo=timezone.utc)
            if (datetime.now(timezone.utc) - cache_time).total_seconds() < 900:
                print(f"📦 Usando cache: {cache_file}")
                return cache_file
        
        # Buscar arquivo mais recente
        print(f"🔍 Buscando arquivos DMW mais recentes...")
        files = self.list_available_files(product=product, hours_back=3)
        
        if not files:
            raise FileNotFoundError(f"Nenhum arquivo DMW encontrado para {self.satellite}/{product}")
        
        latest = files[0]
        print(f"📥 Baixando: {latest['filename']} ({latest['size_mb']:.1f} MB)")
        
        # Download
        self.s3.download_file(
            self.bucket_name,
            latest['key'],
            cache_file
        )
        
        # Salvar metadados
        with open(cache_meta, 'w') as f:
            json.dump({
                'download_time': datetime.now(timezone.utc).isoformat(),
                'filename': latest['filename'],
                'scan_time': latest['scan_time'].isoformat(),
                'size_mb': latest['size_mb']
            }, f)
        
        print(f"✅ Download completo: {cache_file}")
        return cache_file
    
    def extract_winds(self, nc_file, level="low", bbox=None):
        """
        Extrai dados de vento do arquivo NetCDF.
        
        Args:
            nc_file: Caminho do arquivo NetCDF
            level: low, mid, high, ou all
            bbox: Dicionário com lat_min, lat_max, lon_min, lon_max (opcional)
            
        Returns:
            Dicionário com dados de vento formatados para JSON
        """
        if not HAS_XARRAY:
            raise ImportError("xarray é necessário para extrair dados")
        
        if level not in ALTITUDE_LEVELS:
            level = "low"
        
        level_info = ALTITUDE_LEVELS[level]
        
        print(f"📊 Extraindo ventos: {level_info['name']}")
        
        # Abrir arquivo
        ds = xr.open_dataset(nc_file)
        
        # Variáveis principais do DMW:
        # - wind_speed: Velocidade do vento (m/s)
        # - wind_direction: Direção do vento (graus, de onde vem)
        # - lat: Latitude
        # - lon: Longitude
        # - pressure: Nível de pressão (hPa)
        # - DQF: Data Quality Flag
        
        # Extrair dados
        try:
            # Obter coordenadas e dados
            lat = ds['lat'].values
            lon = ds['lon'].values
            pressure = ds['pressure'].values
            speed = ds['wind_speed'].values
            direction = ds['wind_direction'].values
            dqf = ds['DQF'].values if 'DQF' in ds else np.zeros_like(speed)
            
        except KeyError as e:
            print(f"⚠️ Variável não encontrada: {e}")
            print(f"📋 Variáveis disponíveis: {list(ds.data_vars)}")
            ds.close()
            raise
        
        ds.close()
        
        # Filtrar por nível de altitude (pressão)
        mask_level = (pressure >= level_info['min_pressure']) & (pressure <= level_info['max_pressure'])
        
        # Filtrar por qualidade (DQF = 0 é melhor qualidade)
        mask_quality = dqf <= 1
        
        # Filtrar por bbox se fornecido
        if bbox:
            mask_bbox = (
                (lat >= bbox['lat_min']) & (lat <= bbox['lat_max']) &
                (lon >= bbox['lon_min']) & (lon <= bbox['lon_max'])
            )
            mask = mask_level & mask_quality & mask_bbox
        else:
            mask = mask_level & mask_quality
        
        # Aplicar máscara
        lat_filtered = lat[mask]
        lon_filtered = lon[mask]
        speed_filtered = speed[mask]
        direction_filtered = direction[mask]
        pressure_filtered = pressure[mask]
        dqf_filtered = dqf[mask]
        
        # Converter velocidade de m/s para knots
        speed_knots = speed_filtered * 1.94384
        
        # Calcular componentes U e V para plotagem
        # Direção meteorológica: de onde vem o vento
        dir_rad = np.radians(direction_filtered)
        u = -speed_filtered * np.sin(dir_rad)  # componente leste-oeste
        v = -speed_filtered * np.cos(dir_rad)  # componente norte-sul
        
        # Subsamplear para não sobrecarregar (máx ~500 pontos)
        n_points = len(lat_filtered)
        if n_points > 500:
            step = n_points // 500
            indices = np.arange(0, n_points, step)
            lat_filtered = lat_filtered[indices]
            lon_filtered = lon_filtered[indices]
            speed_filtered = speed_filtered[indices]
            speed_knots = speed_knots[indices]
            direction_filtered = direction_filtered[indices]
            pressure_filtered = pressure_filtered[indices]
            dqf_filtered = dqf_filtered[indices]
            u = u[indices]
            v = v[indices]
            n_points = len(lat_filtered)
        
        # Formatar para JSON
        winds = []
        for i in range(n_points):
            if np.isnan(speed_filtered[i]) or np.isnan(direction_filtered[i]):
                continue
            
            winds.append({
                'lat': float(lat_filtered[i]),
                'lon': float(lon_filtered[i]),
                'speed_ms': float(speed_filtered[i]),
                'speed_kt': float(speed_knots[i]),
                'direction': float(direction_filtered[i]),
                'pressure_hpa': float(pressure_filtered[i]),
                'quality_flag': int(dqf_filtered[i]),
                'u': float(u[i]),
                'v': float(v[i])
            })
        
        print(f"✅ Extraídos {len(winds)} vetores de vento")
        
        return {
            'satellite': self.satellite,
            'level': level,
            'level_name': level_info['name'],
            'count': len(winds),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'winds': winds
        }
    
    def get_winds_json(self, product="DMWF", level="low", bbox=None):
        """
        Baixa dados mais recentes e retorna JSON formatado.
        
        Args:
            product: DMWF, DMWC, ou DMWM
            level: low, mid, high, ou all
            bbox: Bounding box opcional
            
        Returns:
            JSON string com dados de vento
        """
        nc_file = self.download_latest(product=product)
        winds = self.extract_winds(nc_file, level=level, bbox=bbox)
        return json.dumps(winds)


def get_dmw_data(satellite="goes16", level="low", bbox=None):
    """
    Função de conveniência para obter dados DMW.
    
    Args:
        satellite: goes16, goes18, ou goes19
        level: low, mid, high, ou all
        bbox: Dicionário com lat_min, lat_max, lon_min, lon_max
        
    Returns:
        Dicionário com dados de vento
    """
    extractor = DMWExtractor(satellite=satellite)
    nc_file = extractor.download_latest()
    return extractor.extract_winds(nc_file, level=level, bbox=bbox)


# =========================================================================
# TESTE
# =========================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("GOES-R DERIVED MOTION WINDS (DMW) EXTRACTOR")
    print("=" * 70)
    
    # Testar listagem
    extractor = DMWExtractor(satellite="goes16")
    
    print("\n📋 Arquivos disponíveis:")
    files = extractor.list_available_files(hours_back=2)
    for f in files[:5]:
        print(f"  - {f['scan_time']} | {f['size_mb']:.1f} MB | {f['filename']}")
    
    if files:
        # Baixar mais recente
        print("\n📥 Baixando arquivo mais recente...")
        nc_file = extractor.download_latest()
        
        # Extrair ventos de baixa altitude
        print("\n🌬️ Extraindo ventos de baixa altitude...")
        bbox = {
            'lat_min': -35,
            'lat_max': -10,
            'lon_min': -55,
            'lon_max': -30
        }
        winds = extractor.extract_winds(nc_file, level="low", bbox=bbox)
        
        print(f"\n📊 Resultados:")
        print(f"   Satélite: {winds['satellite']}")
        print(f"   Nível: {winds['level_name']}")
        print(f"   Vetores: {winds['count']}")
        
        if winds['winds']:
            sample = winds['winds'][0]
            print(f"\n   Exemplo: {sample['lat']:.2f}°, {sample['lon']:.2f}°")
            print(f"            Vel: {sample['speed_kt']:.1f} kt, Dir: {sample['direction']:.0f}°")
            print(f"            Pressão: {sample['pressure_hpa']:.0f} hPa")
    else:
        print("❌ Nenhum arquivo encontrado")
