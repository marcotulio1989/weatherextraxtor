#!/usr/bin/env python3
"""
=============================================================================
SATELLITE IMAGE PROCESSOR - Multi-satellite
=============================================================================
Baixa imagens GOES-19 e GOES-16, recorta área de 100NM ao redor do navio,
e gera arquivos PNG + JSON para cada satélite.
=============================================================================
"""

import requests
import numpy as np
from PIL import Image
from io import BytesIO
import json
from datetime import datetime, timezone
import os
import sys
import shutil

# =========================================================================
# CONFIGURAÇÃO DO NAVIO
# =========================================================================
SHIP_LAT = -22.50
SHIP_LON = -40.50
RADIUS_NM = 100  # Milhas náuticas

# Converter NM para graus (1 grau ≈ 60 NM)
RADIUS_DEG = RADIUS_NM / 60

# Diretório de saída
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'docs')

print(f"🛰️ Satellite Processor (Multi-Satellite)")
print(f"📍 Navio: {SHIP_LAT}°, {SHIP_LON}°")
print(f"📏 Raio: {RADIUS_NM} NM ({RADIUS_DEG:.2f}°)")

# =========================================================================
# CONFIGURAÇÃO DOS SATÉLITES
# =========================================================================
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

# Bounds do setor SSA (South America) - comum para ambos
SSA_BOUNDS = {
    "lat_min": -60.0,
    "lat_max": 15.0,
    "lon_min": -110.0,
    "lon_max": -25.0,
}


def download_satellite_image(sat_id):
    """Baixa a imagem de um satélite específico."""
    sat_config = SATELLITES.get(sat_id)
    if not sat_config:
        print(f"❌ Satélite desconhecido: {sat_id}")
        return None, None
    
    url = sat_config["url"]
    name = sat_config["name"]
    
    try:
        print(f"📥 Baixando {name}: {url}")
        response = requests.get(url, timeout=120, stream=True)
        if response.status_code == 200:
            total_size = int(response.headers.get('content-length', 0))
            print(f"📦 Tamanho: {total_size / 1024 / 1024:.2f} MB")
            
            # Pegar horário real da imagem do header Last-Modified
            last_modified = response.headers.get('last-modified', None)
            image_time = None
            if last_modified:
                from email.utils import parsedate_to_datetime
                try:
                    image_time = parsedate_to_datetime(last_modified)
                    print(f"📅 Imagem capturada: {image_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                except:
                    pass
            
            # Download com progresso
            data = b''
            downloaded = 0
            for chunk in response.iter_content(chunk_size=1024*1024):
                data += chunk
                downloaded += len(chunk)
                pct = (downloaded / total_size * 100) if total_size > 0 else 0
                print(f"   {pct:.0f}% ({downloaded / 1024 / 1024:.1f} MB)", end='\r')
            
            print(f"\n✅ Download completo: {len(data) / 1024 / 1024:.2f} MB")
            return data, image_time
    except Exception as e:
        print(f"❌ Erro: {e}")
    
    return None, None


def latlon_to_pixel(lat, lon, img_width, img_height, bounds):
    """Converte lat/lon para coordenadas de pixel."""
    x_norm = (lon - bounds["lon_min"]) / (bounds["lon_max"] - bounds["lon_min"])
    y_norm = (bounds["lat_max"] - lat) / (bounds["lat_max"] - bounds["lat_min"])
    
    x = int(x_norm * img_width)
    y = int(y_norm * img_height)
    
    return x, y


def pixel_to_latlon(x, y, img_width, img_height, bounds):
    """Converte pixel para lat/lon."""
    x_norm = x / img_width
    y_norm = y / img_height
    
    lon = bounds["lon_min"] + x_norm * (bounds["lon_max"] - bounds["lon_min"])
    lat = bounds["lat_max"] - y_norm * (bounds["lat_max"] - bounds["lat_min"])
    
    return lat, lon


def extract_region(image_data, center_lat, center_lon, radius_deg, output_size=512):
    """Extrai região centrada no navio."""
    try:
        img = Image.open(BytesIO(image_data))
        img_width, img_height = img.size
        print(f"📐 Imagem original: {img_width}x{img_height}")
        
        # Bounds da região de interesse
        roi_bounds = {
            "lat_min": center_lat - radius_deg,
            "lat_max": center_lat + radius_deg,
            "lon_min": center_lon - radius_deg,
            "lon_max": center_lon + radius_deg,
        }
        
        # Converter corners para pixels
        x1, y1 = latlon_to_pixel(roi_bounds["lat_max"], roi_bounds["lon_min"], 
                                  img_width, img_height, SSA_BOUNDS)
        x2, y2 = latlon_to_pixel(roi_bounds["lat_min"], roi_bounds["lon_max"], 
                                  img_width, img_height, SSA_BOUNDS)
        
        # Garantir dentro da imagem
        x1 = max(0, min(x1, img_width - 1))
        x2 = max(0, min(x2, img_width - 1))
        y1 = max(0, min(y1, img_height - 1))
        y2 = max(0, min(y2, img_height - 1))
        
        print(f"📍 ROI pixels: ({x1}, {y1}) a ({x2}, {y2})")
        print(f"📍 ROI size: {x2-x1}x{y2-y1}")
        
        # Recortar
        cropped = img.crop((x1, y1, x2, y2))
        print(f"✂️ Recortado: {cropped.size}")
        
        # Redimensionar
        resized = cropped.resize((output_size, output_size), Image.Resampling.LANCZOS)
        
        return resized, roi_bounds
        
    except Exception as e:
        print(f"❌ Erro ao processar: {e}")
        import traceback
        traceback.print_exc()
        return None, None


def image_to_points(img, bounds, step=8):
    """Converte imagem para array de pontos com coordenadas."""
    if img.mode != 'L':
        img = img.convert('L')
    
    width, height = img.size
    pixels = np.array(img)
    
    points = []
    
    for y in range(0, height, step):
        for x in range(0, width, step):
            lat, lon = pixel_to_latlon(x, y, width, height, bounds)
            value = int(pixels[y, x])
            
            points.append({
                "lat": round(lat, 4),
                "lon": round(lon, 4),
                "v": 255 - value
            })
    
    return points


def process_satellite(sat_id):
    """Processa um satélite e salva os arquivos."""
    sat_config = SATELLITES.get(sat_id)
    if not sat_config:
        return False
    
    name = sat_config["name"]
    print(f"\n{'='*60}")
    print(f"🛰️ Processando {name}")
    print(f"{'='*60}")
    
    # Baixar imagem
    raw_data, image_time = download_satellite_image(sat_id)
    if raw_data is None:
        print(f"❌ Falha ao baixar {name}")
        return False
    
    # Processar
    processed_img, bounds = extract_region(
        raw_data, 
        SHIP_LAT, 
        SHIP_LON, 
        RADIUS_DEG,
        output_size=512
    )
    
    if processed_img is None:
        print(f"❌ Falha ao processar {name}")
        return False
    
    # Salvar PNG
    png_path = os.path.join(OUTPUT_DIR, f'satellite_{sat_id}.png')
    processed_img.save(png_path, 'PNG')
    print(f"💾 Salvo: {png_path}")
    
    # Gerar pontos para JSON
    points = image_to_points(processed_img, bounds, step=8)
    
    # Timestamps
    now = datetime.now(tz=timezone.utc)
    
    # Salvar JSON
    json_data = {
        "satellite_id": sat_id,
        "satellite_name": name,
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
        "image_url": f"satellite_{sat_id}.png",
        "resolution": 512,
        "points_count": len(points),
        "points": points
    }
    
    json_path = os.path.join(OUTPUT_DIR, f'satellite_{sat_id}.json')
    with open(json_path, 'w') as f:
        json.dump(json_data, f)
    print(f"💾 Salvo: {json_path}")
    
    print(f"✅ {name} processado!")
    print(f"   PNG: {os.path.getsize(png_path) / 1024:.1f} KB")
    print(f"   JSON: {os.path.getsize(json_path) / 1024:.1f} KB ({len(points)} pontos)")
    if image_time:
        print(f"   🛰️ Horário da imagem: {image_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    
    return True


def create_satellite_index():
    """Cria um índice JSON com todos os satélites disponíveis."""
    index = {
        "satellites": [],
        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
        "ship": {
            "lat": SHIP_LAT,
            "lon": SHIP_LON,
            "radius_nm": RADIUS_NM
        }
    }
    
    for sat_id, sat_config in SATELLITES.items():
        json_path = os.path.join(OUTPUT_DIR, f'satellite_{sat_id}.json')
        if os.path.exists(json_path):
            with open(json_path, 'r') as f:
                data = json.load(f)
            index["satellites"].append({
                "id": sat_id,
                "name": sat_config["name"],
                "image_url": f"satellite_{sat_id}.png",
                "data_url": f"satellite_{sat_id}.json",
                "image_time_utc": data.get("image_time_utc", "Desconhecido")
            })
    
    index_path = os.path.join(OUTPUT_DIR, 'satellite_index.json')
    with open(index_path, 'w') as f:
        json.dump(index, f, indent=2)
    print(f"\n💾 Índice salvo: {index_path}")
    
    return index


def main():
    """Processa todos os satélites."""
    print("\n" + "="*60)
    print("🛰️ PROCESSAMENTO MULTI-SATÉLITE")
    print("="*60)
    
    success_count = 0
    
    for sat_id in SATELLITES.keys():
        if process_satellite(sat_id):
            success_count += 1
    
    # Criar índice
    index = create_satellite_index()
    
    print("\n" + "="*60)
    print(f"✅ CONCLUÍDO: {success_count}/{len(SATELLITES)} satélites processados")
    print("="*60)
    
    # Manter compatibilidade - copiar GOES-19 como default
    goes19_png = os.path.join(OUTPUT_DIR, 'satellite_goes19.png')
    goes19_json = os.path.join(OUTPUT_DIR, 'satellite_goes19.json')
    default_png = os.path.join(OUTPUT_DIR, 'satellite_current.png')
    default_json = os.path.join(OUTPUT_DIR, 'satellite_data.json')
    
    if os.path.exists(goes19_png):
        shutil.copy(goes19_png, default_png)
        shutil.copy(goes19_json, default_json)
        print(f"📋 Copiado GOES-19 como default")


if __name__ == '__main__':
    main()
