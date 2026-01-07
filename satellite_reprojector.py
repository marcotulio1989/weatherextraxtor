#!/usr/bin/env python3
# type: ignore
"""
=============================================================================
SATELLITE IMAGE REPROJECTOR - Reprojeção ÚNICA e correta
=============================================================================
Baixa imagens GOES-19/16 em projeção geoestacionária e reprojeta para
EPSG:4326 (lat/lon) ou EPSG:3857 (Web Mercator) usando:
- GDAL (rasterio + pyproj)
- Cartopy
- SatPy

A reprojeção é feita UMA SÓ VEZ no backend. O Leaflet recebe a imagem
já reprojetada com bounds em lat/lon.
=============================================================================
"""

import requests
import numpy as np
from PIL import Image  # type: ignore
from io import BytesIO
import json
from datetime import datetime, timezone
import os
import sys
import math
from enum import Enum
from abc import ABC, abstractmethod

# =========================================================================
# CONFIGURAÇÃO
# =========================================================================
SHIP_LAT = -22.50
SHIP_LON = -40.50
RADIUS_NM = 100

RADIUS_LAT_DEG = RADIUS_NM / 60
RADIUS_LON_DEG = RADIUS_NM / (60 * math.cos(math.radians(SHIP_LAT)))

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'docs')

# =========================================================================
# PARÂMETROS DO GOES-19/16 (Projeção Geoestacionária)
# =========================================================================
# Valores oficiais da NOAA (GOES-R PUG - Product User's Guide)
GOES_PARAMS = {
    "sat_lon": -75.2,           # Longitude do satélite (graus)
    "sat_height": 35786023.0,   # Altura do satélite (metros)
    "r_eq": 6378137.0,          # Raio equatorial da Terra (metros)
    "r_pol": 6356752.31414,     # Raio polar da Terra (metros)
    
    # Scan angles da imagem SSA (radianos) - calibrados empiricamente
    "scan_x_min": -0.08365403,
    "scan_x_max": 0.13199896,
    "scan_y_min": -0.14002762,
    "scan_y_max": 0.04996900,
    
    # Dimensões da imagem SSA original
    "img_width": 7200,
    "img_height": 4320,
}

# URLs dos satélites
SATELLITES = {
    "goes19": {
        "name": "GOES-19",
        "url": "https://cdn.star.nesdis.noaa.gov/GOES19/ABI/SECTOR/ssa/13/latest.jpg",
    },
    "goes16": {
        "name": "GOES-16", 
        "url": "https://cdn.star.nesdis.noaa.gov/GOES16/ABI/SECTOR/ssa/13/latest.jpg",
    }
}


class ReprojectionMethod(Enum):
    """Métodos de reprojeção disponíveis."""
    MANUAL = "manual"       # Matemática pura (sem dependências)
    GDAL = "gdal"           # rasterio + pyproj
    CARTOPY = "cartopy"     # cartopy + matplotlib
    SATPY = "satpy"         # satpy (mais preciso)


# =========================================================================
# FUNÇÕES DE PROJEÇÃO GEOESTACIONÁRIA (Matemática)
# =========================================================================
def latlon_to_scan(lat, lon, params=GOES_PARAMS):
    """
    Converte lat/lon para coordenadas de scan (radianos) na projeção geoestacionária.
    Baseado no GOES-R Product User's Guide (PUG).
    """
    H = params["sat_height"]
    r_eq = params["r_eq"]
    r_pol = params["r_pol"]
    lon_0 = params["sat_lon"]
    
    lat_rad = np.radians(lat)
    lon_rad = np.radians(lon - lon_0)
    
    # Raio geocêntrico (considerando Terra elipsoidal)
    e2 = 1 - (r_pol / r_eq) ** 2
    r_c = r_eq / np.sqrt(1 + e2 / (1 - e2) * np.sin(lat_rad)**2)
    
    # Coordenadas geocêntricas
    s_x = H + r_eq - r_c * np.cos(lat_rad) * np.cos(lon_rad)
    s_y = -r_c * np.cos(lat_rad) * np.sin(lon_rad)
    s_z = r_c * (r_pol/r_eq)**2 * np.sin(lat_rad)
    
    # Distância do satélite
    s_n = np.sqrt(s_x**2 + s_y**2 + s_z**2)
    
    # Scan angles (radianos)
    x_rad = np.arcsin(-s_y / s_n)
    y_rad = np.arctan(s_z / s_x)
    
    return x_rad, y_rad


def scan_to_latlon(x_rad, y_rad, params=GOES_PARAMS):
    """
    Converte coordenadas de scan (radianos) para lat/lon.
    Baseado no GOES-R Product User's Guide (PUG).
    
    Retorna (lat, lon) em graus, ou (NaN, NaN) se fora da Terra.
    """
    H = params["sat_height"]
    r_eq = params["r_eq"]
    r_pol = params["r_pol"]
    lon_0 = params["sat_lon"]
    
    # Coeficientes para encontrar distância
    a = np.sin(x_rad)**2 + np.cos(x_rad)**2 * (
        np.cos(y_rad)**2 + (r_eq/r_pol)**2 * np.sin(y_rad)**2
    )
    b = -2 * (H + r_eq) * np.cos(x_rad) * np.cos(y_rad)
    c = (H + r_eq)**2 - r_eq**2
    
    discriminant = b**2 - 4*a*c
    
    # Pontos fora da Terra
    if np.any(discriminant < 0):
        if np.isscalar(discriminant):
            return np.nan, np.nan
        else:
            lat = np.full_like(x_rad, np.nan, dtype=float)
            lon = np.full_like(x_rad, np.nan, dtype=float)
            valid = discriminant >= 0
            
            # Processar apenas pontos válidos
            if np.any(valid):
                rs = (-b[valid] - np.sqrt(discriminant[valid])) / (2*a[valid])
                
                sx = rs * np.cos(x_rad[valid]) * np.cos(y_rad[valid])
                sy = -rs * np.sin(x_rad[valid])
                sz = rs * np.cos(x_rad[valid]) * np.sin(y_rad[valid])
                
                lat[valid] = np.degrees(np.arctan((r_eq/r_pol)**2 * sz / np.sqrt((H + r_eq - sx)**2 + sy**2)))
                lon[valid] = lon_0 - np.degrees(np.arctan(sy / (H + r_eq - sx)))
            
            return lat, lon
    
    rs = (-b - np.sqrt(discriminant)) / (2*a)
    
    sx = rs * np.cos(x_rad) * np.cos(y_rad)
    sy = -rs * np.sin(x_rad)
    sz = rs * np.cos(x_rad) * np.sin(y_rad)
    
    lat = np.degrees(np.arctan((r_eq/r_pol)**2 * sz / np.sqrt((H + r_eq - sx)**2 + sy**2)))
    lon = lon_0 - np.degrees(np.arctan(sy / (H + r_eq - sx)))
    
    return lat, lon


def scan_to_pixel(x_rad, y_rad, params=GOES_PARAMS):
    """Converte scan angles para coordenadas de pixel na imagem SSA."""
    res_x = (params["scan_x_max"] - params["scan_x_min"]) / params["img_width"]
    res_y = (params["scan_y_max"] - params["scan_y_min"]) / params["img_height"]
    
    x = (x_rad - params["scan_x_min"]) / res_x
    y = (params["scan_y_max"] - y_rad) / res_y  # Y invertido (topo = menor y)
    
    return x, y


def pixel_to_scan(x, y, params=GOES_PARAMS):
    """Converte pixel para scan angles."""
    res_x = (params["scan_x_max"] - params["scan_x_min"]) / params["img_width"]
    res_y = (params["scan_y_max"] - params["scan_y_min"]) / params["img_height"]
    
    x_rad = params["scan_x_min"] + x * res_x
    y_rad = params["scan_y_max"] - y * res_y
    
    return x_rad, y_rad


# =========================================================================
# CLASSES DE REPROJEÇÃO
# =========================================================================
class BaseReprojector(ABC):
    """Classe base para reprojetores."""
    
    def __init__(self, name):
        self.name = name
    
    @abstractmethod
    def reproject(self, image_data, center_lat, center_lon, 
                  radius_lat, radius_lon, output_size=512):
        """
        Reprojeta imagem geoestacionária para lat/lon (EPSG:4326).
        
        Args:
            image_data: bytes da imagem original
            center_lat, center_lon: centro da área de interesse
            radius_lat, radius_lon: raio em graus
            output_size: tamanho da imagem de saída
            
        Returns:
            (PIL.Image, bounds_dict) ou (None, None) em caso de erro
        """
        pass


class ManualReprojector(BaseReprojector):
    """Reprojeção usando matemática pura (sem dependências extras)."""
    
    def __init__(self):
        super().__init__("Manual (Matemática Pura)")
    
    def reproject(self, image_data, center_lat, center_lon,
                  radius_lat, radius_lon, output_size=512):
        """
        Reprojeta usando amostragem reversa:
        1. Para cada pixel de saída (em lat/lon), 
        2. Calcular a posição correspondente na imagem geoestacionária,
        3. Amostrar o valor.
        """
        try:
            # Abrir imagem original
            src_img = Image.open(BytesIO(image_data))
            src_array = np.array(src_img)
            src_height, src_width = src_array.shape[:2]
            
            print(f"📐 Imagem fonte: {src_width}x{src_height}")
            
            # Bounds de saída
            lat_min = center_lat - radius_lat
            lat_max = center_lat + radius_lat
            lon_min = center_lon - radius_lon
            lon_max = center_lon + radius_lon
            
            bounds = {
                "lat_min": lat_min, "lat_max": lat_max,
                "lon_min": lon_min, "lon_max": lon_max
            }
            
            # Grid de coordenadas de saída
            lats = np.linspace(lat_max, lat_min, output_size)  # Norte para Sul
            lons = np.linspace(lon_min, lon_max, output_size)  # Oeste para Leste
            
            lon_grid, lat_grid = np.meshgrid(lons, lats)
            
            # Converter lat/lon para scan angles
            x_rad, y_rad = latlon_to_scan(lat_grid, lon_grid)
            
            # Converter scan angles para pixels na imagem fonte
            src_x, src_y = scan_to_pixel(x_rad, y_rad)
            
            # Arredondar e clipar
            src_x = np.clip(np.round(src_x).astype(int), 0, src_width - 1)
            src_y = np.clip(np.round(src_y).astype(int), 0, src_height - 1)
            
            # Criar imagem de saída
            if len(src_array.shape) == 3:
                # Imagem colorida
                out_array = src_array[src_y, src_x]
            else:
                # Grayscale
                out_array = src_array[src_y, src_x]
            
            out_img = Image.fromarray(out_array)
            
            print(f"✅ Reprojetado (Manual): {output_size}x{output_size}")
            return out_img, bounds
            
        except Exception as e:
            print(f"❌ Erro na reprojeção manual: {e}")
            import traceback
            traceback.print_exc()
            return None, None


class GDALReprojector(BaseReprojector):
    """Reprojeção usando rasterio + pyproj (GDAL bindings)."""
    
    def __init__(self):
        super().__init__("GDAL (rasterio + pyproj)")
        self.available = self._check_available()
    
    def _check_available(self):
        try:
            import rasterio
            from rasterio.warp import reproject, Resampling
            from rasterio.crs import CRS
            from rasterio.transform import from_bounds
            import pyproj
            return True
        except ImportError:
            return False
    
    def reproject(self, image_data, center_lat, center_lon,
                  radius_lat, radius_lon, output_size=512):
        if not self.available:
            print("❌ rasterio/pyproj não disponível. Instale com: pip install rasterio pyproj")
            return None, None
        
        try:
            from rasterio.warp import reproject, Resampling
            from rasterio.crs import CRS
            from rasterio.transform import from_bounds
            import rasterio
            import tempfile
            
            # Abrir imagem original
            src_img = Image.open(BytesIO(image_data))
            src_array = np.array(src_img)
            
            if len(src_array.shape) == 2:
                src_array = np.expand_dims(src_array, axis=0)
            else:
                # (H, W, C) -> (C, H, W)
                src_array = np.moveaxis(src_array, -1, 0)
            
            src_height, src_width = src_array.shape[1], src_array.shape[2]
            
            # Definir CRS da projeção geoestacionária GOES
            # Formato PROJ4
            goes_proj = (
                f"+proj=geos +h={GOES_PARAMS['sat_height']} "
                f"+lon_0={GOES_PARAMS['sat_lon']} "
                f"+x_0=0 +y_0=0 "
                f"+a={GOES_PARAMS['r_eq']} +b={GOES_PARAMS['r_pol']} "
                f"+units=m +sweep=x +no_defs"
            )
            
            # Bounds em metros (scan angles * height)
            H = GOES_PARAMS["sat_height"]
            src_left = np.tan(GOES_PARAMS["scan_x_min"]) * H
            src_right = np.tan(GOES_PARAMS["scan_x_max"]) * H
            src_bottom = np.tan(GOES_PARAMS["scan_y_min"]) * H
            src_top = np.tan(GOES_PARAMS["scan_y_max"]) * H
            
            src_transform = from_bounds(src_left, src_bottom, src_right, src_top, 
                                        src_width, src_height)
            
            # CRS de destino (EPSG:4326 - lat/lon)
            dst_crs = CRS.from_epsg(4326)
            
            # Bounds de saída
            lat_min = center_lat - radius_lat
            lat_max = center_lat + radius_lat
            lon_min = center_lon - radius_lon
            lon_max = center_lon + radius_lon
            
            dst_transform = from_bounds(lon_min, lat_min, lon_max, lat_max,
                                        output_size, output_size)
            
            # Array de destino
            dst_array = np.zeros((src_array.shape[0], output_size, output_size), 
                                 dtype=src_array.dtype)
            
            # Reprojetar cada banda
            for i in range(src_array.shape[0]):
                reproject(
                    source=src_array[i],
                    destination=dst_array[i],
                    src_transform=src_transform,
                    src_crs=CRS.from_proj4(goes_proj),
                    dst_transform=dst_transform,
                    dst_crs=dst_crs,
                    resampling=Resampling.bilinear
                )
            
            # Converter de volta para (H, W, C) se necessário
            if dst_array.shape[0] == 1:
                out_array = dst_array[0]
            else:
                out_array = np.moveaxis(dst_array, 0, -1)
            
            out_img = Image.fromarray(out_array.astype(np.uint8))
            
            bounds = {
                "lat_min": lat_min, "lat_max": lat_max,
                "lon_min": lon_min, "lon_max": lon_max
            }
            
            print(f"✅ Reprojetado (GDAL/rasterio): {output_size}x{output_size}")
            return out_img, bounds
            
        except Exception as e:
            print(f"❌ Erro na reprojeção GDAL: {e}")
            import traceback
            traceback.print_exc()
            return None, None


class CartopyReprojector(BaseReprojector):
    """Reprojeção usando Cartopy."""
    
    def __init__(self):
        super().__init__("Cartopy")
        self.available = self._check_available()
    
    def _check_available(self):
        try:
            import cartopy.crs as ccrs
            import matplotlib.pyplot as plt
            return True
        except ImportError:
            return False
    
    def reproject(self, image_data, center_lat, center_lon,
                  radius_lat, radius_lon, output_size=512):
        if not self.available:
            print("❌ Cartopy não disponível. Instale com: pip install cartopy")
            return None, None
        
        try:
            import cartopy.crs as ccrs
            import matplotlib.pyplot as plt
            from matplotlib.backends.backend_agg import FigureCanvasAgg
            
            # Abrir imagem original
            src_img = Image.open(BytesIO(image_data))
            src_array = np.array(src_img)
            
            # Definir projeção geoestacionária
            goes_crs = ccrs.Geostationary(
                central_longitude=GOES_PARAMS["sat_lon"],
                satellite_height=GOES_PARAMS["sat_height"],
                sweep_axis='x'
            )
            
            # Bounds em metros
            H = GOES_PARAMS["sat_height"]
            src_left = np.tan(GOES_PARAMS["scan_x_min"]) * H
            src_right = np.tan(GOES_PARAMS["scan_x_max"]) * H
            src_bottom = np.tan(GOES_PARAMS["scan_y_min"]) * H
            src_top = np.tan(GOES_PARAMS["scan_y_max"]) * H
            
            # Bounds de saída
            lat_min = center_lat - radius_lat
            lat_max = center_lat + radius_lat
            lon_min = center_lon - radius_lon
            lon_max = center_lon + radius_lon
            
            # Criar figura sem margens
            dpi = 100
            fig_size = output_size / dpi
            
            fig = plt.figure(figsize=(fig_size, fig_size), dpi=dpi)
            ax = fig.add_axes([0, 0, 1, 1], projection=ccrs.PlateCarree())
            ax.set_extent([lon_min, lon_max, lat_min, lat_max], crs=ccrs.PlateCarree())
            
            # Plotar imagem
            ax.imshow(src_array, origin='upper', 
                     extent=[src_left, src_right, src_bottom, src_top],
                     transform=goes_crs,
                     interpolation='bilinear')
            
            ax.axis('off')
            
            # Renderizar para array
            canvas = FigureCanvasAgg(fig)
            canvas.draw()
            
            # Converter para array
            buf = canvas.buffer_rgba()
            out_array = np.asarray(buf)
            
            plt.close(fig)
            
            # Converter RGBA para RGB ou manter
            out_img = Image.fromarray(out_array)
            
            bounds = {
                "lat_min": lat_min, "lat_max": lat_max,
                "lon_min": lon_min, "lon_max": lon_max
            }
            
            print(f"✅ Reprojetado (Cartopy): {out_img.size[0]}x{out_img.size[1]}")
            return out_img, bounds
            
        except Exception as e:
            print(f"❌ Erro na reprojeção Cartopy: {e}")
            import traceback
            traceback.print_exc()
            return None, None


class SatPyReprojector(BaseReprojector):
    """Reprojeção usando SatPy (mais preciso para dados de satélite)."""
    
    def __init__(self):
        super().__init__("SatPy")
        self.available = self._check_available()
    
    def _check_available(self):
        try:
            import satpy
            from pyresample import create_area_def
            return True
        except ImportError:
            return False
    
    def reproject(self, image_data, center_lat, center_lon,
                  radius_lat, radius_lon, output_size=512):
        if not self.available:
            print("❌ SatPy não disponível. Instale com: pip install satpy pyresample")
            return None, None
        
        try:
            from pyresample import create_area_def
            from pyresample.geometry import AreaDefinition
            from pyresample import kd_tree
            import xarray as xr
            
            # Abrir imagem original
            src_img = Image.open(BytesIO(image_data))
            src_array = np.array(src_img)
            src_height, src_width = src_array.shape[:2]
            
            # Criar grid de lat/lon para imagem fonte
            y_indices = np.arange(src_height)
            x_indices = np.arange(src_width)
            x_grid, y_grid = np.meshgrid(x_indices, y_indices)
            
            # Converter pixels para scan angles
            x_rad, y_rad = pixel_to_scan(x_grid, y_grid)
            
            # Converter scan angles para lat/lon
            src_lats, src_lons = scan_to_latlon(x_rad, y_rad)
            
            # Bounds de saída
            lat_min = center_lat - radius_lat
            lat_max = center_lat + radius_lat
            lon_min = center_lon - radius_lon
            lon_max = center_lon + radius_lon
            
            # Definir área de destino
            dst_area = create_area_def(
                'target_area',
                {'proj': 'latlong', 'datum': 'WGS84'},
                area_extent=[lon_min, lat_min, lon_max, lat_max],
                shape=(output_size, output_size)
            )
            
            # Definir área fonte (SwathDefinition)
            from pyresample.geometry import SwathDefinition
            src_swath = SwathDefinition(lons=src_lons, lats=src_lats)
            
            # Reamostragem
            if len(src_array.shape) == 3:
                # Colorida
                out_channels = []
                for c in range(src_array.shape[2]):
                    resampled = kd_tree.resample_nearest(
                        src_swath, src_array[:, :, c],
                        dst_area, radius_of_influence=50000,
                        fill_value=0
                    )
                    out_channels.append(resampled)
                out_array = np.stack(out_channels, axis=-1)
            else:
                # Grayscale
                out_array = kd_tree.resample_nearest(
                    src_swath, src_array,
                    dst_area, radius_of_influence=50000,
                    fill_value=0
                )
            
            out_img = Image.fromarray(out_array.astype(np.uint8))
            
            bounds = {
                "lat_min": lat_min, "lat_max": lat_max,
                "lon_min": lon_min, "lon_max": lon_max
            }
            
            print(f"✅ Reprojetado (SatPy): {output_size}x{output_size}")
            return out_img, bounds
            
        except Exception as e:
            print(f"❌ Erro na reprojeção SatPy: {e}")
            import traceback
            traceback.print_exc()
            return None, None


# =========================================================================
# FACTORY E UTILITÁRIOS
# =========================================================================
def get_reprojector(method: ReprojectionMethod) -> BaseReprojector:
    """Retorna o reprojetor apropriado para o método escolhido."""
    reprojectors = {
        ReprojectionMethod.MANUAL: ManualReprojector,
        ReprojectionMethod.GDAL: GDALReprojector,
        ReprojectionMethod.CARTOPY: CartopyReprojector,
        ReprojectionMethod.SATPY: SatPyReprojector,
    }
    return reprojectors[method]()


def check_available_methods():
    """Verifica quais métodos de reprojeção estão disponíveis."""
    methods = {}
    for method in ReprojectionMethod:
        reprojector = get_reprojector(method)
        if method == ReprojectionMethod.MANUAL:
            methods[method.value] = True
        else:
            methods[method.value] = reprojector.available
    return methods


def download_satellite_image(sat_id):
    """Baixa a imagem de um satélite."""
    sat_config = SATELLITES.get(sat_id)
    if not sat_config:
        return None, None
    
    url = sat_config["url"]
    name = sat_config["name"]
    
    try:
        print(f"📥 Baixando {name}: {url}")
        response = requests.get(url, timeout=120)
        if response.status_code == 200:
            print(f"✅ Download: {len(response.content) / 1024 / 1024:.2f} MB")
            
            # Horário da imagem
            last_modified = response.headers.get('last-modified')
            image_time = None
            if last_modified:
                from email.utils import parsedate_to_datetime
                try:
                    image_time = parsedate_to_datetime(last_modified)
                except:
                    pass
            
            return response.content, image_time
    except Exception as e:
        print(f"❌ Erro: {e}")
    
    return None, None


def apply_circular_mask(img):
    """Aplica máscara circular na imagem."""
    if img.mode != 'RGBA':
        img = img.convert('RGBA')
    
    width, height = img.size
    mask = Image.new('L', (width, height), 0)
    center_x, center_y = width // 2, height // 2
    radius = min(width, height) // 2
    
    y_indices, x_indices = np.ogrid[:height, :width]
    distances = np.sqrt((x_indices - center_x)**2 + (y_indices - center_y)**2)
    
    mask_array = np.zeros((height, width), dtype=np.uint8)
    mask_array[distances <= radius] = 255
    mask_array[(distances > radius) & (distances <= radius + 2)] = (
        255 * (1 - (distances[(distances > radius) & (distances <= radius + 2)] - radius) / 2)
    ).astype(np.uint8)
    
    mask = Image.fromarray(mask_array, mode='L')
    img.putalpha(mask)
    
    return img


def process_satellite(sat_id, method=ReprojectionMethod.MANUAL, apply_mask=True):
    """Processa um satélite com reprojeção."""
    sat_config = SATELLITES.get(sat_id)
    if not sat_config:
        return False
    
    name = sat_config["name"]
    print(f"\n{'='*60}")
    print(f"🛰️ Processando {name}")
    print(f"📐 Método: {method.value}")
    print(f"{'='*60}")
    
    # Baixar imagem
    raw_data, image_time = download_satellite_image(sat_id)
    if raw_data is None:
        return False
    
    # Reprojetar
    reprojector = get_reprojector(method)
    print(f"🔧 Usando: {reprojector.name}")
    
    processed_img, bounds = reprojector.reproject(
        raw_data,
        SHIP_LAT, SHIP_LON,
        RADIUS_LAT_DEG, RADIUS_LON_DEG,
        output_size=512
    )
    
    if processed_img is None:
        print(f"❌ Falha na reprojeção")
        return False
    
    # Aplicar máscara circular
    if apply_mask:
        processed_img = apply_circular_mask(processed_img)
        print(f"⭕ Máscara circular aplicada")
    
    # Salvar PNG
    method_suffix = f"_{method.value}" if method != ReprojectionMethod.MANUAL else ""
    png_path = os.path.join(OUTPUT_DIR, f'satellite_{sat_id}{method_suffix}.png')
    processed_img.save(png_path, 'PNG')
    print(f"💾 PNG: {png_path}")
    
    # Salvar JSON
    now = datetime.now(tz=timezone.utc)
    json_data = {
        "satellite_id": sat_id,
        "satellite_name": name,
        "reprojection_method": method.value,
        "processed_at": now.isoformat(),
        "image_time": image_time.isoformat() if image_time else None,
        "image_time_utc": image_time.strftime('%Y-%m-%d %H:%M:%S UTC') if image_time else "Desconhecido",
        "source": sat_config["url"],
        "ship": {
            "lat": SHIP_LAT,
            "lon": SHIP_LON,
            "radius_nm": RADIUS_NM
        },
        "bounds": bounds,
        "leaflet_bounds": [
            [bounds["lat_min"], bounds["lon_min"]],
            [bounds["lat_max"], bounds["lon_max"]]
        ],
        "image_url": f"satellite_{sat_id}{method_suffix}.png",
        "resolution": 512,
        "note": "Imagem reprojetada para EPSG:4326 (lat/lon). Pronta para uso direto no Leaflet."
    }
    
    json_path = os.path.join(OUTPUT_DIR, f'satellite_{sat_id}{method_suffix}.json')
    with open(json_path, 'w') as f:
        json.dump(json_data, f, indent=2)
    print(f"💾 JSON: {json_path}")
    
    return True


def main():
    """Processa satélites com todos os métodos disponíveis."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Reprojetor de Imagens de Satélite')
    parser.add_argument('--method', '-m', 
                        choices=[m.value for m in ReprojectionMethod],
                        default='manual',
                        help='Método de reprojeção')
    parser.add_argument('--satellite', '-s',
                        choices=list(SATELLITES.keys()),
                        default='goes19',
                        help='Satélite')
    parser.add_argument('--check', '-c', action='store_true',
                        help='Verificar métodos disponíveis')
    
    args = parser.parse_args()
    
    if args.check:
        print("\n🔍 Métodos de Reprojeção Disponíveis:")
        print("="*40)
        available = check_available_methods()
        for method, is_available in available.items():
            status = "✅ Disponível" if is_available else "❌ Não instalado"
            print(f"  {method}: {status}")
        print()
        return
    
    method = ReprojectionMethod(args.method)
    process_satellite(args.satellite, method)


if __name__ == '__main__':
    main()
