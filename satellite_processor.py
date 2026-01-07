"""
=============================================================================
SATELLITE IMAGE PROCESSOR
=============================================================================
Baixa imagem GOES-19, reprojeta de Geoestacionário para Mercator,
recorta área de 150NM ao redor do navio e serve via Flask.
=============================================================================
"""

from flask import Flask, jsonify, Response, send_file
from flask_cors import CORS
import requests
import numpy as np
from PIL import Image
from io import BytesIO
import math
from datetime import datetime
import json

app = Flask(__name__)
CORS(app)

# =========================================================================
# CONFIGURAÇÃO DO NAVIO
# =========================================================================
SHIP_LAT = -22.50
SHIP_LON = -40.50
RADIUS_NM = 150  # Milhas náuticas

# Converter NM para graus (1 grau ≈ 60 NM)
RADIUS_DEG = RADIUS_NM / 60

# Bounding box da área de interesse
BBOX = {
    "lat_min": SHIP_LAT - RADIUS_DEG,
    "lat_max": SHIP_LAT + RADIUS_DEG,
    "lon_min": SHIP_LON - RADIUS_DEG,
    "lon_max": SHIP_LON + RADIUS_DEG,
}

print(f"🛰️ Satellite Processor")
print(f"📍 Navio: {SHIP_LAT}°, {SHIP_LON}°")
print(f"📏 Raio: {RADIUS_NM} NM ({RADIUS_DEG:.2f}°)")
print(f"📦 BBOX: {BBOX}")

# =========================================================================
# BOUNDS DO SETOR SSA (South America) - GOES-19
# =========================================================================
# Bounds calculados empiricamente a partir da imagem JPG do CDN STAR/NOAA
# A imagem SSA é reprojetada para coordenadas geográficas (lat/lon)
# Dimensões: 7200 x 4320 pixels
# Aspect ratio: 1.6667 (125°/75° = 7200/4320)

SSA_BOUNDS = {
    "lat_north": -12.0,     # Norte: 12°S (inclui Salvador, Lima)
    "lat_south": -55.0,     # Sul: 55°S (inclui Ushuaia/Terra do Fogo)
    "lon_west": -82.0,      # Oeste: 82°W (Oceano Pacífico, inclui Lima)
    "lon_east": -10.33,     # Leste: ~10°W (Oceano Atlântico)
}

# Dimensões da imagem SSA original
SSA_WIDTH = 7200
SSA_HEIGHT = 4320

# Resolução em graus por pixel
SSA_RES_LAT = (SSA_BOUNDS["lat_north"] - SSA_BOUNDS["lat_south"]) / SSA_HEIGHT  # ~0.0169°/px
SSA_RES_LON = (SSA_BOUNDS["lon_east"] - SSA_BOUNDS["lon_west"]) / SSA_WIDTH     # ~0.0169°/px

print(f"📐 SSA Bounds: Lat [{SSA_BOUNDS['lat_south']}° a {SSA_BOUNDS['lat_north']}°]")
print(f"              Lon [{SSA_BOUNDS['lon_west']}° a {SSA_BOUNDS['lon_east']}°]")
print(f"📏 Resolução: {SSA_RES_LAT:.4f}°/px x {SSA_RES_LON:.4f}°/px (~1.9 km/px)")


def download_goes_image():
    """Baixa a imagem GOES-19 mais recente."""
    urls = [
        "https://cdn.star.nesdis.noaa.gov/GOES19/ABI/SECTOR/ssa/13/latest.jpg",
        "https://cdn.star.nesdis.noaa.gov/GOES16/ABI/SECTOR/ssa/13/latest.jpg",
    ]
    
    for url in urls:
        try:
            print(f"📥 Baixando: {url}")
            response = requests.get(url, timeout=60)
            if response.status_code == 200:
                print(f"✅ Download OK: {len(response.content) / 1024 / 1024:.2f} MB")
                return response.content, url
        except Exception as e:
            print(f"❌ Erro: {e}")
            continue
    
    return None, None


def latlon_to_pixel(lat, lon, img_width, img_height, bounds):
    """
    Converte lat/lon para coordenadas de pixel na imagem SSA.
    A imagem SSA usa projeção equirectangular (lat/lon linear).
    
    Args:
        lat: Latitude em graus (-58 a 15 para SSA)
        lon: Longitude em graus (-116.83 a 4.83 para SSA)
        img_width: Largura da imagem em pixels
        img_height: Altura da imagem em pixels
        bounds: Dicionário com lat_north, lat_south, lon_west, lon_east
    
    Returns:
        (x, y): Coordenadas do pixel
    """
    # Usar nomes corretos das chaves
    lon_west = bounds.get("lon_west", bounds.get("lon_min"))
    lon_east = bounds.get("lon_east", bounds.get("lon_max"))
    lat_north = bounds.get("lat_north", bounds.get("lat_max"))
    lat_south = bounds.get("lat_south", bounds.get("lat_min"))
    
    # Normalizar para 0-1
    x_norm = (lon - lon_west) / (lon_east - lon_west)
    y_norm = (lat_north - lat) / (lat_north - lat_south)
    
    # Converter para pixels
    x = int(x_norm * img_width)
    y = int(y_norm * img_height)
    
    return x, y


def pixel_to_latlon(x, y, img_width, img_height, bounds):
    """
    Converte coordenadas de pixel para lat/lon.
    A imagem SSA usa projeção equirectangular (lat/lon linear).
    
    Args:
        x, y: Coordenadas do pixel
        img_width: Largura da imagem em pixels
        img_height: Altura da imagem em pixels
        bounds: Dicionário com lat_north, lat_south, lon_west, lon_east
    
    Returns:
        (lat, lon): Coordenadas geográficas em graus
    """
    # Usar nomes corretos das chaves
    lon_west = bounds.get("lon_west", bounds.get("lon_min"))
    lon_east = bounds.get("lon_east", bounds.get("lon_max"))
    lat_north = bounds.get("lat_north", bounds.get("lat_max"))
    lat_south = bounds.get("lat_south", bounds.get("lat_min"))
    
    x_norm = x / img_width
    y_norm = y / img_height
    
    lon = lon_west + x_norm * (lon_east - lon_west)
    lat = lat_north - y_norm * (lat_north - lat_south)
    
    return lat, lon


def extract_region(image_data, center_lat, center_lon, radius_deg, output_size=512):
    """
    Extrai uma região da imagem centrada em lat/lon com raio em graus.
    Retorna imagem recortada e reprojetada.
    """
    try:
        # Abrir imagem
        img = Image.open(BytesIO(image_data))
        img_width, img_height = img.size
        print(f"📐 Imagem original: {img_width}x{img_height}")
        
        # Calcular bounds da região de interesse
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
        
        # Garantir que está dentro da imagem
        x1 = max(0, min(x1, img_width - 1))
        x2 = max(0, min(x2, img_width - 1))
        y1 = max(0, min(y1, img_height - 1))
        y2 = max(0, min(y2, img_height - 1))
        
        print(f"📍 ROI pixels: ({x1}, {y1}) a ({x2}, {y2})")
        
        # Recortar
        cropped = img.crop((x1, y1, x2, y2))
        print(f"✂️ Recortado: {cropped.size}")
        
        # Redimensionar para tamanho fixo
        resized = cropped.resize((output_size, output_size), Image.Resampling.LANCZOS)
        
        return resized, roi_bounds
        
    except Exception as e:
        print(f"❌ Erro ao processar: {e}")
        return None, None


def image_to_json(img, bounds, step=4):
    """
    Converte imagem para JSON com coordenadas por pixel.
    Retorna array de pontos com lat, lon, e valor (0-255).
    """
    # Converter para grayscale se necessário
    if img.mode != 'L':
        img = img.convert('L')
    
    width, height = img.size
    pixels = np.array(img)
    
    points = []
    
    # Amostrar a cada N pixels para reduzir tamanho
    for y in range(0, height, step):
        for x in range(0, width, step):
            lat, lon = pixel_to_latlon(x, y, width, height, bounds)
            value = int(pixels[y, x])
            
            points.append({
                "lat": round(lat, 4),
                "lon": round(lon, 4),
                "v": value  # 0-255, onde menor = mais frio (nuvem alta)
            })
    
    return points


# =========================================================================
# CACHE
# =========================================================================
image_cache = {
    "data": None,
    "timestamp": None,
    "bounds": None,
    "json_data": None
}

CACHE_DURATION = 600  # 10 minutos


def get_processed_image():
    """Obtém imagem processada, usando cache se disponível."""
    now = datetime.now()
    
    # Verificar cache
    if (image_cache["data"] is not None and 
        image_cache["timestamp"] is not None and
        (now - image_cache["timestamp"]).total_seconds() < CACHE_DURATION):
        print("📦 Usando cache")
        return image_cache["data"], image_cache["bounds"], image_cache["json_data"]
    
    # Baixar nova imagem
    raw_data, source_url = download_goes_image()
    if raw_data is None:
        return None, None, None
    
    # Processar
    processed_img, bounds = extract_region(
        raw_data, 
        SHIP_LAT, 
        SHIP_LON, 
        RADIUS_DEG,
        output_size=512
    )
    
    if processed_img is None:
        return None, None, None
    
    # Converter para bytes
    buffer = BytesIO()
    processed_img.save(buffer, format='PNG')
    img_bytes = buffer.getvalue()
    
    # Gerar JSON
    json_data = image_to_json(processed_img, bounds, step=4)
    
    # Atualizar cache
    image_cache["data"] = img_bytes
    image_cache["timestamp"] = now
    image_cache["bounds"] = bounds
    image_cache["json_data"] = json_data
    
    print(f"✅ Imagem processada: {len(img_bytes)} bytes, {len(json_data)} pontos")
    
    return img_bytes, bounds, json_data


# =========================================================================
# API ENDPOINTS
# =========================================================================

@app.route('/api/satellite/image')
def get_satellite_image():
    """Retorna imagem PNG processada."""
    img_bytes, bounds, _ = get_processed_image()
    
    if img_bytes is None:
        return jsonify({"error": "Falha ao processar imagem"}), 500
    
    return Response(img_bytes, mimetype='image/png')


@app.route('/api/satellite/data')
def get_satellite_data():
    """Retorna dados JSON com coordenadas por pixel."""
    img_bytes, bounds, json_data = get_processed_image()
    
    if json_data is None:
        return jsonify({"error": "Falha ao processar imagem"}), 500
    
    return jsonify({
        "success": True,
        "timestamp": datetime.now().isoformat(),
        "ship": {
            "lat": SHIP_LAT,
            "lon": SHIP_LON,
            "radius_nm": RADIUS_NM
        },
        "bounds": bounds,
        "resolution": 512,
        "points_count": len(json_data),
        "points": json_data
    })


@app.route('/api/satellite/info')
def get_satellite_info():
    """Retorna informações sobre a imagem disponível."""
    _, bounds, json_data = get_processed_image()
    
    return jsonify({
        "success": True,
        "timestamp": datetime.now().isoformat(),
        "ship": {
            "lat": SHIP_LAT,
            "lon": SHIP_LON,
            "radius_nm": RADIUS_NM
        },
        "bounds": bounds,
        "image_url": "/api/satellite/image",
        "data_url": "/api/satellite/data",
        "points_count": len(json_data) if json_data else 0,
        "cache_duration_sec": CACHE_DURATION
    })


@app.route('/api/satellite/overlay')
def get_overlay_info():
    """
    Retorna informações para usar como ImageOverlay no Leaflet.
    """
    _, bounds, _ = get_processed_image()
    
    if bounds is None:
        return jsonify({"error": "Imagem não disponível"}), 500
    
    # Bounds no formato [[sw_lat, sw_lon], [ne_lat, ne_lon]]
    leaflet_bounds = [
        [bounds["lat_min"], bounds["lon_min"]],
        [bounds["lat_max"], bounds["lon_max"]]
    ]
    
    return jsonify({
        "success": True,
        "image_url": "/api/satellite/image",
        "bounds": leaflet_bounds,
        "timestamp": datetime.now().isoformat()
    })


@app.route('/')
def index():
    """Página de status."""
    return f"""
    <html>
    <head><title>Satellite Processor</title></head>
    <body style="background:#111;color:#0f0;font-family:monospace;padding:20px;">
        <h1>🛰️ Satellite Image Processor</h1>
        <p>Navio: {SHIP_LAT}°, {SHIP_LON}°</p>
        <p>Raio: {RADIUS_NM} NM ({RADIUS_DEG:.2f}°)</p>
        <hr>
        <h2>Endpoints:</h2>
        <ul>
            <li><a href="/api/satellite/info">/api/satellite/info</a> - Informações</li>
            <li><a href="/api/satellite/image">/api/satellite/image</a> - Imagem PNG</li>
            <li><a href="/api/satellite/data">/api/satellite/data</a> - Dados JSON</li>
            <li><a href="/api/satellite/overlay">/api/satellite/overlay</a> - Info para Leaflet</li>
        </ul>
        <hr>
        <h2>Preview:</h2>
        <img src="/api/satellite/image" style="max-width:512px; border:1px solid #0f0;">
    </body>
    </html>
    """


if __name__ == '__main__':
    print("\n" + "="*60)
    print("🛰️ SATELLITE IMAGE PROCESSOR")
    print("="*60)
    app.run(host='0.0.0.0', port=5001, debug=True)
