"""
=============================================================================
SCATTEROMETER DATA FETCHER
=============================================================================
Script Python para baixar dados de satélites escaterômetros de múltiplas fontes.

SATÉLITES ESCATERÔMETROS DISPONÍVEIS:
-------------------------------------
ATIVOS:
  - ASCAT (MetOp-A, B, C) - EUMETSAT/Copernicus
  - CYGNSS (8 satélites) - NASA
  - HY-2A, HY-2B, HY-2C - China (NSOAS)
  - ScatSat-1 - ISRO (Índia)
  
HISTÓRICOS:
  - QuikSCAT (1999-2009) - NASA
  - RapidScat (2014-2016) - NASA/ISS
  - SeaWinds - NASA
  - ERS-1, ERS-2 - ESA

COMO USAR:
----------
1. Instale dependências:
   pip install requests pandas netCDF4 xarray copernicusmarine

2. Configure suas credenciais:
   - Copernicus: https://data.marine.copernicus.eu/register
   - NASA: https://urs.earthdata.nasa.gov/users/new

3. Execute:
   python scatterometer_fetcher.py
=============================================================================
"""

import os
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# =============================================================================
# CONFIGURAÇÃO
# =============================================================================

# Posição do navio (Bacia de Campos)
LAT_NAVIO = -22.50
LON_NAVIO = -40.50

# Área de interesse (bounding box)
BBOX = {
    'lat_min': LAT_NAVIO - 5,
    'lat_max': LAT_NAVIO + 5,
    'lon_min': LON_NAVIO - 5,
    'lon_max': LON_NAVIO + 5
}

# Diretório de saída
OUTPUT_DIR = Path("docs/scatterometer_data")

# Credenciais (podem ser configuradas via variáveis de ambiente)
CREDENTIALS = {
    'copernicus': {
        'username': os.environ.get('CMEMS_USER', ''),
        'password': os.environ.get('CMEMS_PASS', '')
    },
    'nasa': {
        'username': os.environ.get('NASA_USER', ''),
        'password': os.environ.get('NASA_PASS', '')
    }
}

# =============================================================================
# CLASSE BASE PARA FONTES DE DADOS
# =============================================================================

class ScatterometerSource:
    """Classe base para fontes de dados de escaterômetros"""
    
    def __init__(self, name):
        self.name = name
        self.data = []
        
    def fetch(self, bbox, start_time, end_time):
        raise NotImplementedError
        
    def to_dataframe(self):
        return pd.DataFrame(self.data)
        
    def save_csv(self, filename):
        df = self.to_dataframe()
        if not df.empty:
            df.to_csv(filename, index=False)
            print(f"✅ Salvo: {filename}")
        else:
            print(f"⚠️ Sem dados para salvar: {self.name}")


# =============================================================================
# FONTE 1: NOAA ERDDAP (GRATUITO - SEM API KEY)
# =============================================================================

class NOAAErddapSource(ScatterometerSource):
    """
    NOAA ERDDAP - Dados ASCAT gratuitos
    https://coastwatch.pfeg.noaa.gov/erddap/
    
    Datasets disponíveis:
    - erdQAwind1day: ASCAT Daily wind
    - erdQAwind3day: ASCAT 3-day composite
    - erdQAwind8day: ASCAT 8-day composite
    - erdQAwindmday: ASCAT Monthly
    """
    
    BASE_URL = "https://coastwatch.pfeg.noaa.gov/erddap/griddap"
    
    DATASETS = {
        'ascat_daily': 'erdQAwind1day',
        'ascat_3day': 'erdQAwind3day', 
        'ascat_8day': 'erdQAwind8day',
        'ascat_monthly': 'erdQAwindmday'
    }
    
    def __init__(self):
        super().__init__("NOAA ERDDAP ASCAT")
        
    def fetch(self, bbox, start_time=None, end_time=None, dataset='ascat_daily'):
        """
        Buscar dados ASCAT do NOAA ERDDAP
        
        Args:
            bbox: dict com lat_min, lat_max, lon_min, lon_max
            start_time: datetime de início (default: 24h atrás)
            end_time: datetime de fim (default: agora)
            dataset: qual dataset usar
        """
        if end_time is None:
            end_time = datetime.utcnow()
        if start_time is None:
            start_time = end_time - timedelta(days=1)
            
        dataset_id = self.DATASETS.get(dataset, 'erdQAwind1day')
        
        # Formato de tempo para ERDDAP
        start_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        # Construir URL
        url = (
            f"{self.BASE_URL}/{dataset_id}.json?"
            f"x_wind[({start_str}):1:({end_str})]"
            f"[({bbox['lat_max']}):1:({bbox['lat_min']})]"
            f"[({bbox['lon_min']}):1:({bbox['lon_max']})]"
            f",y_wind[({start_str}):1:({end_str})]"
            f"[({bbox['lat_max']}):1:({bbox['lat_min']})]"
            f"[({bbox['lon_min']}):1:({bbox['lon_max']})]"
        )
        
        print(f"📡 Buscando: {self.name}")
        print(f"   Dataset: {dataset_id}")
        print(f"   Período: {start_str} a {end_str}")
        
        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            self._parse_erddap_response(data)
            
            print(f"   ✅ {len(self.data)} pontos recebidos")
            
        except requests.exceptions.HTTPError as e:
            print(f"   ❌ Erro HTTP: {e}")
            print(f"   Tentando dataset alternativo...")
            self._fetch_alternative(bbox)
            
        except Exception as e:
            print(f"   ❌ Erro: {e}")
            
        return self.data
    
    def _parse_erddap_response(self, data):
        """Parsear resposta JSON do ERDDAP"""
        if 'table' not in data or 'rows' not in data['table']:
            return
            
        for row in data['table']['rows']:
            # row = [time, lat, lon, x_wind, y_wind]
            if len(row) >= 5 and row[3] is not None and row[4] is not None:
                u = row[3]  # componente x (m/s)
                v = row[4]  # componente y (m/s)
                
                # Calcular velocidade e direção
                speed_ms = (u**2 + v**2)**0.5
                speed_kn = speed_ms * 1.94384  # m/s para knots
                direction = (180 + 180/3.14159 * 
                           (3.14159 + (0 if u == 0 else 
                            3.14159/2 - (u/abs(u)) * 
                            (3.14159/2 - abs((v/(abs(u)+abs(v))) * 3.14159/2))
                           if abs(u) < abs(v) else
                            (u/abs(u)) * abs((u/(abs(u)+abs(v))) * 3.14159/2)))) % 360
                
                # Correção para cálculo de direção
                import math
                direction = (math.degrees(math.atan2(-u, -v)) + 360) % 360
                
                self.data.append({
                    'timestamp': row[0],
                    'latitude': row[1],
                    'longitude': row[2],
                    'u_wind_ms': u,
                    'v_wind_ms': v,
                    'wind_speed_ms': speed_ms,
                    'wind_speed_kn': speed_kn,
                    'wind_direction': direction,
                    'source': 'NOAA_ASCAT'
                })
                
    def _fetch_alternative(self, bbox):
        """Buscar de fonte alternativa (Open-Meteo)"""
        print("   🔄 Usando Open-Meteo como fallback...")
        
        # Criar grid de pontos
        lats = []
        lons = []
        step = 0.5
        
        lat = bbox['lat_min']
        while lat <= bbox['lat_max']:
            lats.append(round(lat, 2))
            lat += step
            
        lon = bbox['lon_min']
        while lon <= bbox['lon_max']:
            lons.append(round(lon, 2))
            lon += step
            
        # Criar todas as combinações
        query_lats = []
        query_lons = []
        for la in lats:
            for lo in lons:
                query_lats.append(la)
                query_lons.append(lo)
                
        url = (
            f"https://api.open-meteo.com/v1/forecast?"
            f"latitude={','.join(map(str, query_lats))}&"
            f"longitude={','.join(map(str, query_lons))}&"
            f"current_weather=true&windspeed_unit=kn"
        )
        
        try:
            response = requests.get(url, timeout=30)
            data = response.json()
            
            if isinstance(data, list):
                for point in data:
                    if 'current_weather' in point:
                        w = point['current_weather']
                        self.data.append({
                            'timestamp': datetime.utcnow().isoformat(),
                            'latitude': point['latitude'],
                            'longitude': point['longitude'],
                            'wind_speed_kn': w['windspeed'],
                            'wind_direction': w['winddirection'],
                            'source': 'OpenMeteo'
                        })
                        
            print(f"   ✅ {len(self.data)} pontos do Open-Meteo")
            
        except Exception as e:
            print(f"   ❌ Fallback falhou: {e}")


# =============================================================================
# FONTE 2: COPERNICUS MARINE SERVICE
# =============================================================================

class CopernicusSource(ScatterometerSource):
    """
    Copernicus Marine Service (CMEMS)
    https://data.marine.copernicus.eu/
    
    Datasets de vento:
    - WIND_GLO_PHY_L4_NRT_012_004: Global wind L4 NRT
    - WIND_GLO_PHY_L3_NRT_012_002: ASCAT Level 3 NRT
    - WIND_GLO_PHY_L3_MY_012_003: ASCAT Level 3 reprocessado
    
    Requer registro gratuito em:
    https://data.marine.copernicus.eu/register
    """
    
    DATASETS = {
        'global_wind_l4': 'WIND_GLO_PHY_L4_NRT_012_004',
        'ascat_l3_nrt': 'WIND_GLO_PHY_L3_NRT_012_002',
        'ascat_l3_my': 'WIND_GLO_PHY_L3_MY_012_003'
    }
    
    def __init__(self, username=None, password=None):
        super().__init__("Copernicus Marine")
        self.username = username or CREDENTIALS['copernicus']['username']
        self.password = password or CREDENTIALS['copernicus']['password']
        
    def fetch(self, bbox, start_time=None, end_time=None, dataset='global_wind_l4'):
        """
        Para usar Copernicus Marine, é necessário o cliente Python oficial.
        
        Instalação:
            pip install copernicusmarine
            
        Configuração:
            copernicusmarine login
            
        Uso via Python:
            import copernicusmarine
            copernicusmarine.subset(
                dataset_id="WIND_GLO_PHY_L4_NRT_012_004",
                variables=["eastward_wind", "northward_wind"],
                minimum_longitude=-45,
                maximum_longitude=-35,
                minimum_latitude=-27,
                maximum_latitude=-17,
                start_datetime="2024-01-01T00:00:00",
                end_datetime="2024-01-02T00:00:00"
            )
        """
        
        if not self.username or not self.password:
            print(f"⚠️ {self.name}: Credenciais não configuradas")
            print("   Registre-se em: https://data.marine.copernicus.eu/register")
            print("   Depois configure:")
            print("   export CMEMS_USER='seu_usuario'")
            print("   export CMEMS_PASS='sua_senha'")
            return []
            
        print(f"📡 Buscando: {self.name}")
        print(f"   Dataset: {self.DATASETS.get(dataset)}")
        
        try:
            # Tentar usar o cliente oficial
            import copernicusmarine
            
            if end_time is None:
                end_time = datetime.utcnow()
            if start_time is None:
                start_time = end_time - timedelta(days=1)
                
            result = copernicusmarine.subset(
                dataset_id=self.DATASETS.get(dataset),
                variables=["eastward_wind", "northward_wind"],
                minimum_longitude=bbox['lon_min'],
                maximum_longitude=bbox['lon_max'],
                minimum_latitude=bbox['lat_min'],
                maximum_latitude=bbox['lat_max'],
                start_datetime=start_time.strftime('%Y-%m-%dT%H:%M:%S'),
                end_datetime=end_time.strftime('%Y-%m-%dT%H:%M:%S'),
                output_directory=str(OUTPUT_DIR),
                force_download=True
            )
            
            print(f"   ✅ Dados salvos em: {OUTPUT_DIR}")
            
        except ImportError:
            print("   ❌ Cliente copernicusmarine não instalado")
            print("   Execute: pip install copernicusmarine")
            self._print_manual_instructions(bbox, dataset)
            
        except Exception as e:
            print(f"   ❌ Erro: {e}")
            
        return self.data
        
    def _print_manual_instructions(self, bbox, dataset):
        """Instruções para download manual"""
        dataset_id = self.DATASETS.get(dataset)
        print(f"""
   📋 INSTRUÇÕES MANUAIS:
   
   1. Instale o cliente:
      pip install copernicusmarine
      
   2. Faça login:
      copernicusmarine login
      
   3. Baixe os dados:
      copernicusmarine subset \\
        --dataset-id {dataset_id} \\
        --variable eastward_wind \\
        --variable northward_wind \\
        --minimum-longitude {bbox['lon_min']} \\
        --maximum-longitude {bbox['lon_max']} \\
        --minimum-latitude {bbox['lat_min']} \\
        --maximum-latitude {bbox['lat_max']}
        """)


# =============================================================================
# FONTE 3: NASA EARTHDATA (CYGNSS, QuikSCAT, etc)
# =============================================================================

class NASAEarthdataSource(ScatterometerSource):
    """
    NASA Earthdata - Vários satélites
    https://earthdata.nasa.gov/
    
    Satélites disponíveis:
    - CYGNSS: Constelação de 8 satélites (ativo)
    - QuikSCAT: 1999-2009 (histórico)
    - RapidScat: 2014-2016 (histórico)
    
    Requer registro gratuito em:
    https://urs.earthdata.nasa.gov/users/new
    """
    
    CMR_URL = "https://cmr.earthdata.nasa.gov/search/granules.json"
    
    COLLECTIONS = {
        'cygnss_l2': 'C2036882048-POCLOUD',  # CYGNSS Level 2
        'quikscat_l2b': 'C1243477366-PODAAC',  # QuikSCAT L2B
        'rapidscat_l2b': 'C1243522156-PODAAC'  # RapidScat L2B
    }
    
    def __init__(self, username=None, password=None):
        super().__init__("NASA Earthdata")
        self.username = username or CREDENTIALS['nasa']['username']
        self.password = password or CREDENTIALS['nasa']['password']
        
    def fetch(self, bbox, start_time=None, end_time=None, collection='cygnss_l2'):
        """
        Buscar granules disponíveis no NASA CMR
        """
        
        if not self.username or not self.password:
            print(f"⚠️ {self.name}: Credenciais não configuradas")
            print("   Registre-se em: https://urs.earthdata.nasa.gov/users/new")
            print("   Depois configure:")
            print("   export NASA_USER='seu_usuario'")
            print("   export NASA_PASS='sua_senha'")
            return []
            
        if end_time is None:
            end_time = datetime.utcnow()
        if start_time is None:
            start_time = end_time - timedelta(days=1)
            
        print(f"📡 Buscando: {self.name}")
        print(f"   Collection: {collection}")
        
        # Buscar granules via CMR
        params = {
            'collection_concept_id': self.COLLECTIONS.get(collection),
            'temporal': f"{start_time.strftime('%Y-%m-%dT%H:%M:%SZ')},{end_time.strftime('%Y-%m-%dT%H:%M:%SZ')}",
            'bounding_box': f"{bbox['lon_min']},{bbox['lat_min']},{bbox['lon_max']},{bbox['lat_max']}",
            'page_size': 100
        }
        
        try:
            response = requests.get(self.CMR_URL, params=params, timeout=30)
            data = response.json()
            
            granules = data.get('feed', {}).get('entry', [])
            print(f"   📦 {len(granules)} granules encontrados")
            
            for g in granules[:5]:  # Mostrar primeiros 5
                print(f"      - {g.get('title', 'N/A')}")
                
            if granules:
                print(f"\n   Para baixar, use o Earthdata Search:")
                print(f"   https://search.earthdata.nasa.gov")
                
        except Exception as e:
            print(f"   ❌ Erro: {e}")
            
        return self.data


# =============================================================================
# FONTE 4: KNMI ASCAT (Holanda)
# =============================================================================

class KNMISource(ScatterometerSource):
    """
    KNMI Scatterometer - Processamento holandês do ASCAT
    https://scatterometer.knmi.nl/
    
    Dados disponíveis:
    - ASCAT 12.5km coastal
    - ASCAT 25km  
    - ASCAT Soil Moisture
    
    Acesso gratuito via FTP/HTTP
    """
    
    BASE_URL = "https://scatterometer.knmi.nl"
    
    def __init__(self):
        super().__init__("KNMI ASCAT")
        
    def fetch(self, bbox, start_time=None, end_time=None):
        """
        KNMI oferece dados via catálogo HTTP/FTP
        """
        print(f"📡 Buscando: {self.name}")
        print(f"   URL: {self.BASE_URL}")
        print(f"   ℹ️ KNMI requer acesso manual via catálogo")
        print(f"""
   📋 INSTRUÇÕES:
   
   1. Acesse: https://scatterometer.knmi.nl/qscat_prod/
   
   2. Navegue até os dados desejados:
      - ascat_[a/b/c]_coastal/  (12.5km resolução)
      - ascat_[a/b/c]_szf/      (25km resolução)
      
   3. Baixe arquivos NetCDF (.nc)
   
   4. Processe com xarray:
      import xarray as xr
      ds = xr.open_dataset('arquivo.nc')
        """)
        
        return self.data


# =============================================================================
# PROCESSADOR DE DADOS NETCDF
# =============================================================================

class NetCDFProcessor:
    """Processar arquivos NetCDF de escaterômetros"""
    
    @staticmethod
    def process_ascat(filepath):
        """Processar arquivo ASCAT NetCDF"""
        try:
            import xarray as xr
            
            ds = xr.open_dataset(filepath)
            print(f"📄 Processando: {filepath}")
            print(f"   Variáveis: {list(ds.data_vars)}")
            
            # Extrair componentes de vento
            # Nomes podem variar: wind_speed, eastward_wind, etc
            speed_var = None
            dir_var = None
            
            for var in ds.data_vars:
                if 'speed' in var.lower():
                    speed_var = var
                elif 'dir' in var.lower():
                    dir_var = var
                    
            if speed_var:
                print(f"   Velocidade: {speed_var}")
            if dir_var:
                print(f"   Direção: {dir_var}")
                
            return ds
            
        except ImportError:
            print("❌ xarray não instalado. Execute: pip install xarray netCDF4")
        except Exception as e:
            print(f"❌ Erro processando {filepath}: {e}")
            
        return None


# =============================================================================
# FUNÇÃO PRINCIPAL
# =============================================================================

def main():
    """Função principal"""
    print("=" * 70)
    print("SCATTEROMETER DATA FETCHER")
    print("=" * 70)
    print(f"\n📍 Posição do navio: {LAT_NAVIO}°, {LON_NAVIO}°")
    print(f"📍 Área de busca: {BBOX}")
    print()
    
    # Criar diretório de saída
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Lista de fontes
    sources = [
        NOAAErddapSource(),
        CopernicusSource(),
        NASAEarthdataSource(),
        KNMISource()
    ]
    
    all_data = []
    
    for source in sources:
        print("-" * 70)
        data = source.fetch(BBOX)
        if data:
            all_data.extend(data)
            
            # Salvar CSV individual
            timestamp = datetime.now().strftime('%Y%m%d_%H%M')
            filename = OUTPUT_DIR / f"{source.name.replace(' ', '_')}_{timestamp}.csv"
            source.save_csv(filename)
            
        print()
    
    # Salvar CSV combinado
    if all_data:
        combined_df = pd.DataFrame(all_data)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        combined_file = OUTPUT_DIR / f"combined_scatterometer_{timestamp}.csv"
        combined_df.to_csv(combined_file, index=False)
        print(f"\n✅ Dados combinados salvos: {combined_file}")
        print(f"   Total de pontos: {len(all_data)}")
    
    print("\n" + "=" * 70)
    print("RESUMO DE FONTES DISPONÍVEIS:")
    print("=" * 70)
    print("""
┌─────────────────────┬────────────┬──────────────┬─────────────────────┐
│ Fonte               │ Status     │ API Key      │ Satélites           │
├─────────────────────┼────────────┼──────────────┼─────────────────────┤
│ NOAA ERDDAP         │ ✅ Grátis  │ Não precisa  │ ASCAT MetOp-A/B/C   │
│ Open-Meteo          │ ✅ Grátis  │ Não precisa  │ Modelos (fallback)  │
│ Copernicus Marine   │ ✅ Grátis  │ Registro     │ ASCAT L3/L4         │
│ NASA Earthdata      │ ✅ Grátis  │ Registro     │ CYGNSS, QuikSCAT    │
│ KNMI                │ ✅ Grátis  │ Não precisa  │ ASCAT processado    │
└─────────────────────┴────────────┴──────────────┴─────────────────────┘
    """)
    
    print("\n📋 PARA CONFIGURAR CREDENCIAIS:")
    print("-" * 70)
    print("""
# Copernicus Marine:
export CMEMS_USER='seu_usuario'
export CMEMS_PASS='sua_senha'

# NASA Earthdata:
export NASA_USER='seu_usuario'  
export NASA_PASS='sua_senha'
    """)


if __name__ == "__main__":
    main()
