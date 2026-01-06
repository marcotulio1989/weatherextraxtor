#!/usr/bin/env python3
"""
=============================================================================
SATELLITE IMAGE PROCESSOR - Script para gerar arquivo estático
=============================================================================
Baixa imagem GOES-19, recorta área de 150NM ao redor do navio,
e gera arquivo PNG + JSON para uso no frontend.
=============================================================================
"""

import requests
import numpy as np
from PIL import Image
from io import BytesIO
import json
from datetime import datetime
import os
import sys

# =========================================================================
# CONFIGURAÇÃO DO NAVIO
# =========================================================================
SHIP_LAT = -22.50
SHIP_LON = -40.50
RADIUS_NM = 150  # Milhas náuticas

# Converter NM para graus (1 grau ≈ 60 NM)
RADIUS_DEG = RADIUS_NM / 60

# Diretório de saída
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'docs')

print(f"🛰️ Satellite Processor")
print(f"📍 Navio: {SHIP_LAT}°, {SHIP_LON}°")
print(f"📏 Raio: {RADIUS_NM} NM ({RADIUS_DEG:.2f}°)")

# =========================================================================
# PARÂMETROS DA PROJEÇÃO GOES
# =========================================================================
# Setor SSA (South America) bounds aproximados
SSA_BOUNDS = {
    "lat_min": -60.0,
    "lat_max": 15.0,
    "lon_min": -110.0,
    "lon_max": -25.0,
}


def download_goes_image():
    """Baixa a imagem GOES-19 mais recente."""
    urls = [
        "https://cdn.star.nesdis.noaa.gov/GOES19/ABI/SECTOR/ssa/13/latest.jpg",
        "https://cdn.star.nesdis.noaa.gov/GOES16/ABI/SECTOR/ssa/13/latest.jpg",
    ]
    
    for url in urls:
        try:
            print(f"📥 Baixando: {url}")
            response = requests.get(url, timeout=120, stream=True)
            if response.status_code == 200:
                total_size = int(response.headers.get('content-length', 0))
                print(f"📦 Tamanho: {total_size / 1024 / 1024:.2f} MB")
                
                # Download com progresso
                data = b''
                downloaded = 0
                for chunk in response.iter_content(chunk_size=1024*1024):
                    data += chunk
                    downloaded += len(chunk)
                    pct = (downloaded / total_size * 100) if total_size > 0 else 0
                    print(f"   {pct:.0f}% ({downloaded / 1024 / 1024:.1f} MB)", end='\r')
                
                print(f"\n✅ Download completo: {len(data) / 1024 / 1024:.2f} MB")
                return data, url
        except Exception as e:
            print(f"❌ Erro: {e}")
            continue
    
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
            
            # Valor IR: 0=quente (superfície), 255=frio (nuvem alta)
            # Inverter para: 255=quente, 0=frio
            points.append({
                "lat": round(lat, 4),
                "lon": round(lon, 4),
                "v": 255 - value  # Invertido para visualização
            })
    
    return points


def main():
    """Processa e salva imagem de satélite."""
    
    # Baixar imagem
    raw_data, source_url = download_goes_image()
    if raw_data is None:
        print("❌ Falha ao baixar imagem")
        sys.exit(1)
    
    # Processar
    processed_img, bounds = extract_region(
        raw_data, 
        SHIP_LAT, 
        SHIP_LON, 
        RADIUS_DEG,
        output_size=512
    )
    
    if processed_img is None:
        print("❌ Falha ao processar imagem")
        sys.exit(1)
    
    # Salvar PNG
    png_path = os.path.join(OUTPUT_DIR, 'satellite_current.png')
    processed_img.save(png_path, 'PNG')
    print(f"💾 Salvo: {png_path}")
    
    # Gerar pontos para JSON
    points = image_to_points(processed_img, bounds, step=8)
    
    # Salvar JSON
    json_data = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "source": source_url,
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
        "image_url": "satellite_current.png",
        "resolution": 512,
        "points_count": len(points),
        "points": points
    }
    
    json_path = os.path.join(OUTPUT_DIR, 'satellite_data.json')
    with open(json_path, 'w') as f:
        json.dump(json_data, f)
    print(f"💾 Salvo: {json_path}")
    
    print(f"\n✅ Processamento completo!")
    print(f"   PNG: {os.path.getsize(png_path) / 1024:.1f} KB")
    print(f"   JSON: {os.path.getsize(json_path) / 1024:.1f} KB ({len(points)} pontos)")
    print(f"   Bounds: {bounds}")


if __name__ == '__main__':
    main()
