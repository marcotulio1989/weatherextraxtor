"""
=============================================================================
GOES-R LEVEL 2 PRODUCTS EXTRACTOR
=============================================================================
Extrai dados de produtos L2 do GOES-16/18/19 para uma área específica.

Produtos suportados:
- DSIF: Índices de Estabilidade (CAPE, LI, K-Index, Total Totals, Showalter)
- ACHAF: Altura do Topo da Nuvem (Cloud Top Height)
- ACTPF: Fase do Topo da Nuvem (Cloud Top Phase)
- CTPF: Pressão do Topo da Nuvem (Cloud Top Pressure)
- ACMF: Máscara de Céu Limpo (Clear Sky Mask)
- TPWF: Água Precipitável Total (Total Precipitable Water)
- SSTF: Temperatura da Superfície do Mar (Sea Surface Temperature)
- CODF: Profundidade Óptica da Nuvem (Cloud Optical Depth)

Fonte: AWS S3 - noaa-goes16/ABI-L2-*/
                 noaa-goes18/ABI-L2-*/
                 noaa-goes19/ABI-L2-*/
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
    print("⚠️ xarray não disponível")

# =========================================================================
# CONFIGURAÇÃO
# =========================================================================

CACHE_DIR = "/workspaces/weatherextraxtor/data/goes_l2"
os.makedirs(CACHE_DIR, exist_ok=True)

S3_CONFIG = Config(signature_version=UNSIGNED)

SATELLITES = {
    "goes16": {"bucket": "noaa-goes16", "name": "GOES-16 (Backup)"},
    "goes18": {"bucket": "noaa-goes18", "name": "GOES-18 (West)"},
    "goes19": {"bucket": "noaa-goes19", "name": "GOES-19 (East)"},
}

# Produtos L2 e suas variáveis principais
L2_PRODUCTS = {
    "DSIF": {
        "name": "Stability Indices",
        "description": "Índices de instabilidade atmosférica",
        "variables": {
            "CAPE": {"unit": "J/kg", "description": "Convective Available Potential Energy"},
            "LI": {"unit": "K", "description": "Lifted Index"},
            "KI": {"unit": "", "description": "K-Index"},
            "TT": {"unit": "", "description": "Total Totals Index"},
            "SI": {"unit": "", "description": "Showalter Index"}
        },
        "dqf_var": "DQF_Overall"
    },
    "ACHAF": {
        "name": "Cloud Top Height",
        "description": "Altura do topo da nuvem",
        "variables": {
            "HT": {"unit": "km", "description": "Cloud Top Height"}
        },
        "dqf_var": "DQF"
    },
    "ACTPF": {
        "name": "Cloud Top Phase",
        "description": "Fase do topo da nuvem",
        "variables": {
            "Phase": {"unit": "", "description": "Cloud Phase (0=clear, 1=water, 2=supercooled, 3=mixed, 4=ice, 5=uncertain)"}
        },
        "dqf_var": "DQF",
        "phase_labels": {0: "Clear", 1: "Water", 2: "Supercooled", 3: "Mixed", 4: "Ice", 5: "Uncertain"}
    },
    "CTPF": {
        "name": "Cloud Top Pressure",
        "description": "Pressão do topo da nuvem",
        "variables": {
            "PRES": {"unit": "hPa", "description": "Cloud Top Pressure"}
        },
        "dqf_var": "DQF"
    },
    "ACMF": {
        "name": "Clear Sky Mask",
        "description": "Máscara de céu limpo",
        "variables": {
            "BCM": {"unit": "", "description": "Binary Cloud Mask (0=cloud, 1=clear)"}
        },
        "dqf_var": "DQF"
    },
    "TPWF": {
        "name": "Total Precipitable Water",
        "description": "Água precipitável total",
        "variables": {
            "TPW": {"unit": "mm", "description": "Total Precipitable Water"}
        },
        "dqf_var": "DQF_Overall"
    },
    "SSTF": {
        "name": "Sea Surface Temperature",
        "description": "Temperatura da superfície do mar",
        "variables": {
            "SST": {"unit": "K", "description": "Sea Surface Temperature"}
        },
        "dqf_var": "DQF"
    },
    "CODF": {
        "name": "Cloud Optical Depth",
        "description": "Profundidade óptica da nuvem",
        "variables": {
            "COD": {"unit": "", "description": "Cloud Optical Depth"}
        },
        "dqf_var": "DQF"
    },
}


class GOESL2Extractor:
    """Extrator de produtos L2 do GOES-R."""
    
    def __init__(self, satellite="goes19"):
        if satellite not in SATELLITES:
            raise ValueError(f"Satélite inválido: {satellite}")
        
        self.satellite = satellite
        self.bucket_name = SATELLITES[satellite]["bucket"]
        self.s3 = boto3.client('s3', config=S3_CONFIG)
        
        # Cache de transformadores por produto (cada um pode ter resolução diferente)
        self._transformers = {}
        
        print(f"🛰️ GOES L2 Extractor inicializado para {SATELLITES[satellite]['name']}")
    
    def list_available_files(self, product="DSIF", hours_back=3):
        """Lista arquivos disponíveis para um produto."""
        now = datetime.now(timezone.utc)
        files = []
        
        for hours_ago in range(hours_back):
            dt = now - timedelta(hours=hours_ago)
            prefix = f"ABI-L2-{product}/{dt.year}/{dt.strftime('%j')}/{dt.strftime('%H')}/"
            
            try:
                response = self.s3.list_objects_v2(
                    Bucket=self.bucket_name,
                    Prefix=prefix,
                    MaxKeys=50
                )
                
                if 'Contents' in response:
                    for obj in response['Contents']:
                        key = obj['Key']
                        if key.endswith('.nc'):
                            filename = os.path.basename(key)
                            
                            # Parse scan time
                            for part in filename.split('_'):
                                if part.startswith('s'):
                                    try:
                                        scan_time = datetime.strptime(part[1:14], '%Y%j%H%M%S')
                                        scan_time = scan_time.replace(tzinfo=timezone.utc)
                                        break
                                    except:
                                        continue
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
        
        files.sort(key=lambda x: x['scan_time'], reverse=True)
        return files
    
    def download_latest(self, product="DSIF", use_cache=True, max_age_minutes=15):
        """Baixa o arquivo mais recente de um produto."""
        cache_file = os.path.join(CACHE_DIR, f"{self.satellite}_{product}_latest.nc")
        cache_meta = cache_file + ".meta"
        
        if use_cache and os.path.exists(cache_file) and os.path.exists(cache_meta):
            with open(cache_meta, 'r') as f:
                meta = json.load(f)
            cache_time = datetime.fromisoformat(meta['download_time'].replace('Z', '+00:00'))
            if cache_time.tzinfo is None:
                cache_time = cache_time.replace(tzinfo=timezone.utc)
            if (datetime.now(timezone.utc) - cache_time).total_seconds() < max_age_minutes * 60:
                print(f"📦 Usando cache: {product}")
                return cache_file, meta.get('scan_time')
        
        print(f"🔍 Buscando arquivos {product}...")
        files = self.list_available_files(product=product, hours_back=3)
        
        if not files:
            raise FileNotFoundError(f"Nenhum arquivo encontrado para {product}")
        
        latest = files[0]
        print(f"📥 Baixando: {latest['filename']} ({latest['size_mb']:.1f} MB)")
        
        self.s3.download_file(
            self.bucket_name,
            latest['key'],
            cache_file
        )
        
        with open(cache_meta, 'w') as f:
            json.dump({
                'download_time': datetime.now(timezone.utc).isoformat(),
                'filename': latest['filename'],
                'scan_time': latest['scan_time'].isoformat(),
                'size_mb': latest['size_mb'],
                'product': product
            }, f)
        
        print(f"✅ Download completo: {product}")
        return cache_file, latest['scan_time'].isoformat()
    
    def _get_transformer(self, ds):
        """Cria transformador de coordenadas para o dataset."""
        from pyproj import Proj, Transformer
        
        proj = ds['goes_imager_projection']
        sat_height = float(proj.attrs['perspective_point_height'])
        sat_lon = float(proj.attrs['longitude_of_projection_origin'])
        sweep = proj.attrs['sweep_angle_axis']
        
        geos = Proj(proj='geos', h=sat_height, lon_0=sat_lon, sweep=sweep)
        transformer = Transformer.from_proj(
            Proj('epsg:4326'),
            geos,
            always_xy=True
        )
        
        return transformer, sat_height
    
    def extract_for_points(self, nc_file, product, grid_points):
        """
        Extrai dados de um produto L2 para pontos específicos.
        
        Args:
            nc_file: Caminho do arquivo NetCDF
            product: Código do produto (DSIF, ACHAF, etc.)
            grid_points: Lista de dicts com lat, lon
            
        Returns:
            Lista de dicts com dados para cada ponto
        """
        if product not in L2_PRODUCTS:
            raise ValueError(f"Produto não suportado: {product}")
        
        product_info = L2_PRODUCTS[product]
        print(f"📊 Extraindo {product_info['name']}...")
        
        ds = xr.open_dataset(nc_file)
        
        # Obter transformador e coordenadas
        transformer, sat_height = self._get_transformer(ds)
        x = ds['x'].values * sat_height
        y = ds['y'].values * sat_height
        
        results = []
        
        for point in grid_points:
            lat, lon = point['lat'], point['lon']
            
            try:
                # Converter lat/lon para coordenadas geoestacionárias
                geos_x, geos_y = transformer.transform(lon, lat)
                
                # Encontrar índices mais próximos
                xi = np.argmin(np.abs(x - geos_x))
                yi = np.argmin(np.abs(y - geos_y))
                
                point_data = {'lat': lat, 'lon': lon}
                has_valid_data = False
                
                # Extrair cada variável
                for var_name, var_info in product_info['variables'].items():
                    if var_name in ds.data_vars:
                        try:
                            val = float(ds[var_name].values[yi, xi])
                            # Incluir 0 como válido, mas excluir NaN e valores de fill (-999, etc)
                            if not np.isnan(val) and val > -900:
                                point_data[var_name] = round(val, 2)
                                has_valid_data = True
                        except (IndexError, ValueError):
                            pass
                
                # Extrair DQF se disponível
                dqf_var = product_info.get('dqf_var')
                if dqf_var and dqf_var in ds.data_vars:
                    try:
                        dqf = int(ds[dqf_var].values[yi, xi])
                        if dqf >= 0:
                            point_data['DQF'] = dqf
                    except:
                        pass
                
                # Adicionar se tiver dados válidos
                if has_valid_data:
                    results.append(point_data)
                    
            except Exception as e:
                continue
        
        ds.close()
        print(f"   ✅ {len(results)} pontos extraídos")
        return results
    
    def extract_all_products(self, grid_points, products=None):
        """
        Extrai todos os produtos L2 para os pontos de grade.
        
        Args:
            grid_points: Pontos de grade [{lat, lon}, ...]
            products: Lista de produtos (None = principais)
            
        Returns:
            Dict com dados de todos os produtos
        """
        if products is None:
            products = ["DSIF", "ACHAF", "ACTPF", "TPWF", "ACMF"]
        
        all_data = {
            'satellite': self.satellite,
            'satellite_name': SATELLITES[self.satellite]['name'],
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'grid_points_count': len(grid_points),
            'products': {}
        }
        
        for product in products:
            try:
                nc_file, scan_time = self.download_latest(product=product)
                data = self.extract_for_points(nc_file, product, grid_points)
                
                all_data['products'][product] = {
                    'name': L2_PRODUCTS[product]['name'],
                    'description': L2_PRODUCTS[product]['description'],
                    'scan_time': scan_time,
                    'variables': L2_PRODUCTS[product]['variables'],
                    'count': len(data),
                    'data': data
                }
            except Exception as e:
                print(f"⚠️ Erro ao extrair {product}: {e}")
                all_data['products'][product] = {'error': str(e)}
        
        return all_data
    
    def create_merged_grid(self, grid_points, products=None):
        """
        Cria uma grade unificada com todos os produtos L2.
        Cada ponto terá todos os dados disponíveis.
        
        Returns:
            Lista de dicts com todos os dados por ponto
        """
        all_data = self.extract_all_products(grid_points, products)
        
        # Criar dicionário por coordenada
        merged = {}
        
        for product, prod_data in all_data['products'].items():
            if 'data' not in prod_data:
                continue
                
            for point in prod_data['data']:
                key = (point['lat'], point['lon'])
                
                if key not in merged:
                    merged[key] = {'lat': point['lat'], 'lon': point['lon']}
                
                # Copiar todos os campos exceto lat/lon
                for field, value in point.items():
                    if field not in ['lat', 'lon']:
                        # Prefixar com produto para evitar conflitos
                        merged[key][f"{product}_{field}"] = value
        
        return {
            'satellite': all_data['satellite'],
            'timestamp': all_data['timestamp'],
            'points': list(merged.values())
        }


def generate_grid_points(center_lat, center_lon, radius_nm=100, step_nm=25):
    """
    Gera pontos de grade ao redor de um centro.
    
    Args:
        center_lat, center_lon: Centro da grade
        radius_nm: Raio em milhas náuticas
        step_nm: Espaçamento em milhas náuticas
        
    Returns:
        Lista de dicts com lat, lon
    """
    # Converter NM para graus (1 grau ≈ 60 NM)
    radius_deg = radius_nm / 60
    step_deg = step_nm / 60
    
    points = []
    
    lat = center_lat - radius_deg
    while lat <= center_lat + radius_deg:
        lon = center_lon - radius_deg
        while lon <= center_lon + radius_deg:
            # Verificar se está dentro do raio
            dist = np.sqrt((lat - center_lat)**2 + (lon - center_lon)**2)
            if dist <= radius_deg:
                points.append({'lat': lat, 'lon': lon})
            lon += step_deg
        lat += step_deg
    
    return points


# =========================================================================
# TESTE
# =========================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("GOES-R LEVEL 2 PRODUCTS EXTRACTOR")
    print("=" * 70)
    
    # Posição do navio
    SHIP_LAT = -22.5
    SHIP_LON = -40.5
    RADIUS_NM = 100
    
    # Gerar grade de pontos (mesma do modelo de vento)
    print(f"\n📍 Gerando grade ao redor do navio ({SHIP_LAT}, {SHIP_LON})...")
    grid_points = generate_grid_points(SHIP_LAT, SHIP_LON, radius_nm=RADIUS_NM, step_nm=25)
    print(f"   {len(grid_points)} pontos de grade gerados")
    
    # Extrair todos os produtos
    print()
    extractor = GOESL2Extractor(satellite="goes19")
    
    # Produtos a extrair
    products = ["DSIF", "ACHAF", "ACTPF", "TPWF"]
    
    all_data = extractor.extract_all_products(grid_points, products=products)
    
    # Resumo
    print()
    print("=" * 70)
    print("RESUMO DA EXTRAÇÃO")
    print("=" * 70)
    
    for product, prod_data in all_data['products'].items():
        if 'error' in prod_data:
            print(f"❌ {product}: {prod_data['error']}")
        else:
            print(f"✅ {product}: {prod_data['count']} pontos")
            if prod_data['data']:
                sample = prod_data['data'][0]
                vars_str = ", ".join([f"{k}={v}" for k, v in sample.items() if k not in ['lat', 'lon', 'DQF']])
                print(f"   Amostra: {vars_str}")
    
    # Salvar resultado
    output_file = os.path.join(CACHE_DIR, 'l2_products_latest.json')
    with open(output_file, 'w') as f:
        json.dump(all_data, f, indent=2)
    print(f"\n💾 Dados salvos em: {output_file}")
    
    # Também salvar na pasta docs
    docs_output = '/workspaces/weatherextraxtor/docs/goes_l2_latest.json'
    with open(docs_output, 'w') as f:
        json.dump(all_data, f, indent=2)
    print(f"💾 Dados salvos em: {docs_output}")
