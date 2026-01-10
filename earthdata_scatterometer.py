"""
=============================================================================
EARTHDATA SCATTEROMETER EXTRACTOR
=============================================================================
Extrator de dados de ventos de satélites escaterômetros usando a biblioteca
earthdata da NASA.

SATÉLITES SCATTERÔMETROS ATIVOS DISPONÍVEIS:
---------------------------------------------
1. CYGNSS (Cyclone Global Navigation Satellite System)
   - Constelação de 8 micro-satélites
   - Usa reflexão de GPS para medir ventos oceânicos
   - Resolução temporal: ~7 horas de revisita
   - Ideal para ciclones tropicais
   
2. ASCAT (Advanced Scatterometer) - MetOp-B/C
   - Operado pela EUMETSAT, disponível via PO.DAAC
   - Resolução: 12.5km e 25km
   - Cobertura global diária
   
3. RapidScat (2014-2016) - HISTÓRICO
4. QuikSCAT (1999-2009) - HISTÓRICO

CREDENCIAIS:
------------
Requer registro no NASA Earthdata: https://urs.earthdata.nasa.gov/users/new
Configure suas credenciais no arquivo ~/.netrc:
    machine urs.earthdata.nasa.gov
        login SEU_USUARIO
        password SUA_SENHA

Ou via variáveis de ambiente:
    export EARTHDATA_USERNAME=seu_usuario
    export EARTHDATA_PASSWORD=sua_senha

=============================================================================
"""

import os
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Any
import tempfile

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Diretório de saída
OUTPUT_DIR = Path("docs/scatterometer_data")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Área de interesse padrão (Bacia de Campos)
DEFAULT_BBOX = {
    'lat_min': -35.0,
    'lat_max': -5.0,
    'lon_min': -55.0,
    'lon_max': -25.0
}


class EarthdataScatterometer:
    """
    Classe para extrair dados de ventos de scatterômetros via NASA Earthdata.
    
    Usa a biblioteca 'earthdata' para autenticação e busca de dados.
    """
    
    # Coleções de dados de scatterômetros no CMR (Common Metadata Repository)
    COLLECTIONS = {
        # ASCAT MetOp-C Coastal (12.5km) - Mais recente e alta resolução
        'ascat_c_coastal': {
            'short_name': 'ASCATC-L2-Coastal',
            'version': '',
            'description': 'ASCAT MetOp-C L2 Coastal 12.5km',
            'provider': 'POCLOUD',
            'variables': ['wind_speed', 'wind_dir'],
            'delay_hours': 3
        },
        # ASCAT MetOp-B Coastal (12.5km)
        'ascat_b_coastal': {
            'short_name': 'ASCATB-L2-Coastal',
            'version': '',
            'description': 'ASCAT MetOp-B L2 Coastal 12.5km',
            'provider': 'POCLOUD',
            'variables': ['wind_speed', 'wind_dir'],
            'delay_hours': 3
        },
        # ASCAT MetOp-C 25km
        'ascat_c_25km': {
            'short_name': 'ASCATC-L2-25km',
            'version': '',
            'description': 'ASCAT MetOp-C L2 25km',
            'provider': 'POCLOUD',
            'variables': ['wind_speed', 'wind_dir'],
            'delay_hours': 3
        },
        # ASCAT MetOp-B 25km
        'ascat_b_25km': {
            'short_name': 'ASCATB-L2-25km',
            'version': '',
            'description': 'ASCAT MetOp-B L2 25km',
            'provider': 'POCLOUD',
            'variables': ['wind_speed', 'wind_dir'],
            'delay_hours': 3
        },
        # ASCAT MetOp-A 25km
        'ascat_a_25km': {
            'short_name': 'ASCATA-L2-25km',
            'version': '',
            'description': 'ASCAT MetOp-A L2 25km',
            'provider': 'POCLOUD',
            'variables': ['wind_speed', 'wind_dir'],
            'delay_hours': 3
        },
        # CCMP Winds - Produto combinado L4 (6h)
        'ccmp_winds': {
            'short_name': 'CCMP_WINDS_10M6HR_L4_V3.1',
            'version': '3.1',
            'description': 'CCMP Combined Wind 10m 6-hourly L4',
            'provider': 'POCLOUD',
            'variables': ['uwnd', 'vwnd'],
            'delay_hours': 24
        }
    }
    
    def __init__(self, username: Optional[str] = None, password: Optional[str] = None):
        """
        Inicializa o extrator.
        
        Args:
            username: Usuário do NASA Earthdata (opcional, usa variável de ambiente)
            password: Senha do NASA Earthdata (opcional, usa variável de ambiente)
        """
        self.username = username or os.environ.get('EARTHDATA_USERNAME', '')
        self.password = password or os.environ.get('EARTHDATA_PASSWORD', '')
        self.auth = None
        self.session = None
        self._earthdata_available = False
        
        # Tentar importar earthdata
        try:
            import earthaccess
            self._earthdata_available = True
            logger.info("✅ Biblioteca earthaccess disponível")
        except ImportError:
            logger.warning("⚠️ Biblioteca earthaccess não instalada. Execute: pip install earthaccess")
            self._earthdata_available = False
    
    def authenticate(self) -> bool:
        """
        Autentica com o NASA Earthdata.
        
        Returns:
            True se autenticação bem sucedida, False caso contrário
        """
        if not self._earthdata_available:
            logger.error("❌ Biblioteca earthaccess não disponível")
            return False
        
        try:
            import earthaccess
            
            # Tentar autenticar
            if self.username and self.password:
                logger.info(f"🔑 Autenticando com usuário: {self.username}")
                self.auth = earthaccess.login(strategy='environment')
            else:
                # Tentar usar .netrc ou cache persistente
                logger.info("🔑 Tentando autenticar via .netrc ou cache...")
                try:
                    self.auth = earthaccess.login(persist=True)
                except Exception:
                    # Se falhar, tentar sem persistência
                    self.auth = earthaccess.login()
            
            if self.auth:
                logger.info("✅ Autenticação bem sucedida!")
                return True
            else:
                logger.error("❌ Falha na autenticação")
                return False
                
        except Exception as e:
            logger.error(f"❌ Erro na autenticação: {e}")
            return False
    
    def search_granules(self, 
                       collection_key: str = 'cygnss_l2_nrt',
                       bbox: Optional[Dict] = None,
                       start_time: Optional[datetime] = None,
                       end_time: Optional[datetime] = None,
                       max_results: int = 50) -> List[Any]:
        """
        Busca granules (arquivos) disponíveis.
        
        Args:
            collection_key: Chave da coleção em COLLECTIONS
            bbox: Área de interesse {'lat_min', 'lat_max', 'lon_min', 'lon_max'}
            start_time: Início do período (default: 24h atrás)
            end_time: Fim do período (default: agora)
            max_results: Número máximo de resultados
            
        Returns:
            Lista de granules encontrados
        """
        if not self._earthdata_available:
            return []
        
        try:
            import earthaccess
            
            # Obter info da coleção
            collection = self.COLLECTIONS.get(collection_key)
            if not collection:
                logger.error(f"❌ Coleção não encontrada: {collection_key}")
                return []
            
            # Definir período
            if end_time is None:
                end_time = datetime.utcnow()
            if start_time is None:
                delay = timedelta(hours=collection.get('delay_hours', 3))
                start_time = end_time - timedelta(hours=48)  # Últimas 48h
                end_time = end_time - delay  # Descontar atraso de processamento
            
            # Definir bbox
            if bbox is None:
                bbox = DEFAULT_BBOX
            
            logger.info(f"📡 Buscando granules: {collection['short_name']}")
            logger.info(f"   Período: {start_time.isoformat()} a {end_time.isoformat()}")
            logger.info(f"   Área: lat {bbox['lat_min']} a {bbox['lat_max']}, lon {bbox['lon_min']} a {bbox['lon_max']}")
            
            # Buscar granules usando earthaccess
            results = earthaccess.search_data(
                short_name=collection['short_name'],
                temporal=(start_time, end_time),
                bounding_box=(
                    bbox['lon_min'], 
                    bbox['lat_min'], 
                    bbox['lon_max'], 
                    bbox['lat_max']
                ),
                count=max_results
            )
            
            logger.info(f"   📦 {len(results)} granules encontrados")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Erro na busca: {e}")
            return []
    
    def download_and_process(self, 
                            granules: List,
                            bbox: Optional[Dict] = None,
                            output_format: str = 'json') -> Optional[Dict]:
        """
        Baixa e processa granules para extrair dados de vento.
        
        Args:
            granules: Lista de granules do search_granules
            bbox: Área de interesse para filtrar os dados
            output_format: 'json' ou 'csv'
            
        Returns:
            Dicionário com dados processados ou None
        """
        if not granules:
            logger.warning("⚠️ Nenhum granule para processar")
            return None
        
        if not self._earthdata_available:
            return None
        
        if bbox is None:
            bbox = DEFAULT_BBOX
        
        try:
            import earthaccess
            import xarray as xr
            import numpy as np
            
            all_winds = []
            
            # Criar diretório temporário para downloads
            with tempfile.TemporaryDirectory() as tmpdir:
                # Baixar granules usando earthaccess
                logger.info(f"📥 Baixando {len(granules)} granules...")
                
                downloaded_files = earthaccess.download(
                    granules,
                    local_path=tmpdir
                )
                
                logger.info(f"   ✅ {len(downloaded_files)} arquivos baixados")
                
                # Processar cada arquivo
                for filepath in downloaded_files:
                    try:
                        ds = xr.open_dataset(filepath)
                        winds = self._extract_winds_from_dataset(ds, bbox)
                        all_winds.extend(winds)
                        ds.close()
                    except Exception as e:
                        logger.warning(f"   ⚠️ Erro processando {filepath}: {e}")
            
            if not all_winds:
                logger.warning("⚠️ Nenhum dado de vento extraído")
                return None
            
            # Compilar resultado
            result = {
                'timestamp': datetime.utcnow().isoformat(),
                'source': 'NASA_Earthdata_Scatterometer',
                'satellites': list(set(w.get('satellite', 'Unknown') for w in all_winds)),
                'total_points': len(all_winds),
                'winds': all_winds
            }
            
            logger.info(f"   ✅ {len(all_winds)} pontos de vento extraídos")
            
            return result
            
        except ImportError as e:
            logger.error(f"❌ Dependência faltando: {e}")
            logger.error("   Execute: pip install xarray netCDF4 h5netcdf")
            return None
        except Exception as e:
            logger.error(f"❌ Erro no download/processamento: {e}")
            return None
    
    def _extract_winds_from_dataset(self, ds, bbox: Optional[Dict] = None) -> List[Dict]:
        """
        Extrai dados de vento de um dataset xarray.
        
        Suporta formatos ASCAT (NetCDF do PO.DAAC).
        
        Args:
            ds: Dataset xarray
            bbox: Área de interesse para filtrar {'lat_min', 'lat_max', 'lon_min', 'lon_max'}
        """
        import numpy as np
        
        if bbox is None:
            bbox = DEFAULT_BBOX
        
        winds = []
        var_names = list(ds.data_vars)
        
        logger.debug(f"   Variáveis no dataset: {var_names}")
        
        # Formato ASCAT (NetCDF L2)
        # Variáveis: lat, lon, wind_speed, wind_dir
        if 'wind_speed' in var_names and 'wind_dir' in var_names:
            try:
                # Obter coordenadas
                if 'lat' in ds.coords:
                    lats = ds['lat'].values
                    lons = ds['lon'].values
                elif 'lat' in ds.data_vars:
                    lats = ds['lat'].values
                    lons = ds['lon'].values
                else:
                    logger.warning("   ⚠️ Coordenadas lat/lon não encontradas")
                    return winds
                
                speeds = ds['wind_speed'].values
                directions = ds['wind_dir'].values
                
                # Flatten arrays se necessário
                lats = lats.flatten()
                lons = lons.flatten()
                speeds = speeds.flatten()
                directions = directions.flatten()
                
                logger.debug(f"   Shape após flatten: {len(speeds)} pontos")
                
                # Filtrar dados válidos (remover NaN, valores extremos e fora da bbox)
                valid_count = 0
                for i in range(len(speeds)):
                    # Verificar se é válido
                    if not np.isfinite(speeds[i]) or speeds[i] <= 0 or speeds[i] > 100:
                        continue
                    if not np.isfinite(lats[i]) or not np.isfinite(lons[i]):
                        continue
                    if not np.isfinite(directions[i]):
                        continue
                    
                    # Criar ponto de vento
                    lat = float(lats[i])
                    lon = float(lons[i])
                    
                    # Converter longitude de 0-360 para -180 a +180
                    if lon > 180:
                        lon = lon - 360
                    
                    # Filtrar por bbox
                    if lat < bbox['lat_min'] or lat > bbox['lat_max']:
                        continue
                    if lon < bbox['lon_min'] or lon > bbox['lon_max']:
                        continue
                    
                    # Criar ponto de vento
                    speed_ms = float(speeds[i])
                    dir_deg = float(directions[i])
                    
                    winds.append({
                        'lat': lat,
                        'lon': lon,
                        'speed_ms': speed_ms,
                        'speed_kt': speed_ms * 1.94384,
                        'direction': dir_deg,
                        'satellite': 'ASCAT',
                        'source': 'ASCAT_L2'
                    })
                    valid_count += 1
                
                logger.debug(f"   {valid_count} pontos válidos na bbox")
                
            except Exception as e:
                logger.warning(f"   ⚠️ Erro extraindo dados ASCAT: {e}")
        
        # Formato CCMP (u/v components)
        elif 'uwnd' in var_names and 'vwnd' in var_names:
            try:
                u = ds['uwnd'].values.flatten()
                v = ds['vwnd'].values.flatten()
                
                if 'latitude' in ds.coords:
                    lats = ds['latitude'].values
                    lons = ds['longitude'].values
                else:
                    lats = ds['lat'].values
                    lons = ds['lon'].values
                
                # Expandir grid para match com u/v
                if lats.ndim == 1:
                    lons_grid, lats_grid = np.meshgrid(lons, lats)
                    lats = lats_grid.flatten()
                    lons = lons_grid.flatten()
                else:
                    lats = lats.flatten()
                    lons = lons.flatten()
                
                for i in range(len(u)):
                    if not np.isfinite(u[i]) or not np.isfinite(v[i]):
                        continue
                    
                    speed_ms = np.sqrt(u[i]**2 + v[i]**2)
                    direction = (np.degrees(np.arctan2(-u[i], -v[i])) + 360) % 360
                    
                    winds.append({
                        'lat': float(lats[i]),
                        'lon': float(lons[i]),
                        'speed_ms': float(speed_ms),
                        'speed_kt': float(speed_ms * 1.94384),
                        'direction': float(direction),
                        'satellite': 'CCMP',
                        'source': 'CCMP_L4'
                    })
                    
            except Exception as e:
                logger.warning(f"   ⚠️ Erro extraindo dados CCMP: {e}")
        
        return winds
    
    def fetch_latest(self, 
                    satellites: List[str] = None,
                    bbox: Optional[Dict] = None) -> Dict:
        """
        Busca os dados mais recentes de múltiplos satélites.
        
        Args:
            satellites: Lista de coleções a buscar (default: ASCAT B e C coastal)
            bbox: Área de interesse
            
        Returns:
            Dicionário com todos os dados combinados
        """
        if satellites is None:
            # Usar ASCAT MetOp-B e C que são os mais atualizados
            satellites = ['ascat_c_coastal', 'ascat_b_coastal']
        
        if bbox is None:
            bbox = DEFAULT_BBOX
        
        logger.info("=" * 60)
        logger.info("🛰️ EARTHDATA SCATTEROMETER - Buscando dados mais recentes")
        logger.info("=" * 60)
        
        # Autenticar se necessário
        if not self.auth:
            if not self.authenticate():
                return self._fallback_to_erddap(bbox)
        
        all_winds = []
        satellites_found = []
        
        for sat_key in satellites:
            logger.info(f"\n📡 Buscando: {sat_key}")
            
            # Buscar granules
            granules = self.search_granules(sat_key, bbox)
            
            if granules:
                # Baixar e processar
                result = self.download_and_process(granules[:5], bbox=bbox)  # Limitar a 5 mais recentes
                
                if result and result.get('winds'):
                    all_winds.extend(result['winds'])
                    satellites_found.extend(result.get('satellites', []))
        
        if not all_winds:
            logger.warning("⚠️ Nenhum dado obtido via Earthdata, usando fallback...")
            return self._fallback_to_erddap(bbox)
        
        # Compilar resultado final
        result = {
            'timestamp': datetime.utcnow().isoformat(),
            'source': 'NASA_Earthdata',
            'satellites': list(set(satellites_found)),
            'bbox': bbox,
            'total_points': len(all_winds),
            'winds': all_winds
        }
        
        # Salvar cache JSON
        self._save_cache(result)
        
        logger.info(f"\n✅ Total: {len(all_winds)} pontos de vento de {len(set(satellites_found))} satélites")
        
        return result
    
    def _fallback_to_erddap(self, bbox: Dict) -> Dict:
        """
        Fallback para NOAA ERDDAP quando earthdata não está disponível.
        """
        logger.info("🔄 Usando NOAA ERDDAP como fallback...")
        
        try:
            from scatterometer_fetcher import NOAAErddapSource
            
            source = NOAAErddapSource()
            source.fetch(bbox)
            
            winds = []
            for row in source.data:
                winds.append({
                    'lat': row.get('latitude'),
                    'lon': row.get('longitude'),
                    'speed_ms': row.get('wind_speed_ms', 0),
                    'speed_kt': row.get('wind_speed_kn', 0),
                    'direction': row.get('wind_direction'),
                    'satellite': 'ASCAT',
                    'source': 'NOAA_ERDDAP'
                })
            
            result = {
                'timestamp': datetime.utcnow().isoformat(),
                'source': 'NOAA_ERDDAP_Fallback',
                'satellites': ['ASCAT-MetOp'],
                'bbox': bbox,
                'total_points': len(winds),
                'winds': winds
            }
            
            self._save_cache(result)
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Fallback também falhou: {e}")
            return {
                'timestamp': datetime.utcnow().isoformat(),
                'source': 'ERROR',
                'error': str(e),
                'winds': []
            }
    
    def _save_cache(self, data: Dict, filename: str = 'scatterometer_latest.json'):
        """Salva dados em cache JSON."""
        try:
            output_path = Path("docs") / filename
            with open(output_path, 'w') as f:
                json.dump(data, f, indent=2)
            logger.info(f"💾 Cache salvo: {output_path}")
        except Exception as e:
            logger.warning(f"⚠️ Erro salvando cache: {e}")
    
    def get_available_collections(self) -> Dict:
        """Retorna lista de coleções disponíveis."""
        return {
            key: {
                'name': val['short_name'],
                'description': val['description'],
                'variables': val['variables'],
                'delay_hours': val['delay_hours']
            }
            for key, val in self.COLLECTIONS.items()
        }


def main():
    """Função principal para teste."""
    print("=" * 70)
    print("EARTHDATA SCATTEROMETER EXTRACTOR")
    print("=" * 70)
    
    # Verificar credenciais
    username = os.environ.get('EARTHDATA_USERNAME', '')
    if not username:
        print("\n⚠️ CREDENCIAIS NÃO CONFIGURADAS")
        print("Configure suas credenciais do NASA Earthdata:")
        print("  export EARTHDATA_USERNAME='seu_usuario'")
        print("  export EARTHDATA_PASSWORD='sua_senha'")
        print("\nOu crie o arquivo ~/.netrc com:")
        print("  machine urs.earthdata.nasa.gov")
        print("      login seu_usuario")
        print("      password sua_senha")
        print("\nRegistro gratuito: https://urs.earthdata.nasa.gov/users/new")
    
    # Criar extrator
    extractor = EarthdataScatterometer()
    
    # Listar coleções disponíveis
    print("\n📋 COLEÇÕES DISPONÍVEIS:")
    print("-" * 70)
    for key, info in extractor.get_available_collections().items():
        print(f"  {key}:")
        print(f"    Nome: {info['name']}")
        print(f"    Descrição: {info['description']}")
        print(f"    Variáveis: {', '.join(info['variables'])}")
        print(f"    Atraso típico: {info['delay_hours']}h")
        print()
    
    # Buscar dados
    print("\n🛰️ BUSCANDO DADOS...")
    print("-" * 70)
    
    result = extractor.fetch_latest()
    
    if result.get('winds'):
        print(f"\n✅ SUCESSO!")
        print(f"   Fonte: {result.get('source')}")
        print(f"   Satélites: {', '.join(result.get('satellites', []))}")
        print(f"   Total de pontos: {result.get('total_points')}")
        
        # Mostrar alguns exemplos
        print("\n📍 AMOSTRA DE DADOS:")
        for w in result['winds'][:5]:
            dir_str = f"{w['direction']:.0f}°" if w.get('direction') else "N/A"
            print(f"   {w['lat']:.2f}°, {w['lon']:.2f}° → {w['speed_kt']:.1f} kt, dir {dir_str} ({w['satellite']})")
    else:
        print(f"\n⚠️ Nenhum dado obtido")
        if result.get('error'):
            print(f"   Erro: {result['error']}")


if __name__ == "__main__":
    main()
