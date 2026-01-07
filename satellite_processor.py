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
# PARÂMETROS DA PROJEÇÃO GEOESTACIONÁRIA GOES-19
# =========================================================================
# Valores extraídos do NetCDF oficial da NOAA (ABI L2 CMI)
# A imagem SSA usa projeção geoestacionária, NÃO lat/lon linear!

GOES_PARAMS = {
    "H": 35786023.0,           # Altura do satélite em metros
    "r_eq": 6378137.0,         # Raio equatorial da Terra em metros
    "r_pol": 6356752.31414,    # Raio polar da Terra em metros
    "lon_0": -75.0,            # Longitude do ponto sub-satélite em graus
}

# Scan angles da imagem SSA (em radianos)
# Calculados usando conversão lat/lon -> scan angle:
# Topo: -12° lat -> y ≈ -0.036
# Base: -70° lat -> y ≈ -0.147 (depois é espaço)
SSA_SCAN_ANGLES = {
    "x_min": -0.035,    # Oeste (radianos)
    "x_max": 0.125,     # Leste (radianos)
    "y_min": -0.147,    # Sul - ~70°S (radianos)
    "y_max": -0.036,    # Norte - ~12°S (radianos)
}

# Dimensões da imagem SSA original
SSA_WIDTH = 7200
SSA_HEIGHT = 4320

# Resolução em radianos por pixel
SSA_RES_X = (SSA_SCAN_ANGLES["x_max"] - SSA_SCAN_ANGLES["x_min"]) / SSA_WIDTH
SSA_RES_Y = (SSA_SCAN_ANGLES["y_max"] - SSA_SCAN_ANGLES["y_min"]) / SSA_HEIGHT

print(f"🛰️ GOES-19 Geostationary Projection")
print(f"📐 Scan Angles: X [{SSA_SCAN_ANGLES['x_min']:.3f}, {SSA_SCAN_ANGLES['x_max']:.3f}] rad")
print(f"              Y [{SSA_SCAN_ANGLES['y_min']:.3f}, {SSA_SCAN_ANGLES['y_max']:.3f}] rad")
print(f"📏 Resolução: {np.degrees(SSA_RES_X)*3600:.2f} arcsec/px x {np.degrees(SSA_RES_Y)*3600:.2f} arcsec/px")


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


def latlon_to_scan(lat, lon):
    """
    Converte lat/lon para coordenadas de scan (radianos) da projeção geoestacionária.
    Baseado no GOES-R Product User's Guide (PUG).
    
    Args:
        lat: Latitude em graus
        lon: Longitude em graus
        
    Returns:
        (x_rad, y_rad): Coordenadas de scan em radianos
    """
    H = GOES_PARAMS["H"]
    r_eq = GOES_PARAMS["r_eq"]
    r_pol = GOES_PARAMS["r_pol"]
    lon_0 = GOES_PARAMS["lon_0"]
    
    lat_rad = np.radians(lat)
    lon_rad = np.radians(lon - lon_0)
    
    # Raio geocêntrico
    r_c = r_eq / np.sqrt(1 + ((r_eq**2 - r_pol**2) / r_pol**2) * np.sin(lat_rad)**2)
    
    # Coordenadas geocêntricas
    s_x = (H + r_eq) - r_c * np.cos(lat_rad) * np.cos(lon_rad)
    s_y = -r_c * np.cos(lat_rad) * np.sin(lon_rad)
    s_z = r_c * (r_pol/r_eq)**2 * np.sin(lat_rad)
    
    # Scan angles
    x_rad = np.arcsin(-s_y / np.sqrt(s_x**2 + s_y**2 + s_z**2))
    y_rad = np.arctan(s_z / s_x)
    
    return x_rad, y_rad


def scan_to_latlon(x_rad, y_rad):
    """
    Converte coordenadas de scan (radianos) para lat/lon.
    Baseado no GOES-R Product User's Guide (PUG).
    
    Args:
        x_rad, y_rad: Coordenadas de scan em radianos
        
    Returns:
        (lat, lon): Coordenadas em graus, ou (NaN, NaN) se fora da Terra
    """
    H = GOES_PARAMS["H"]
    r_eq = GOES_PARAMS["r_eq"]
    r_pol = GOES_PARAMS["r_pol"]
    lon_0 = GOES_PARAMS["lon_0"]
    
    # Calcular discriminante para verificar se o ponto está na Terra
    a = np.sin(x_rad)**2 + np.cos(x_rad)**2 * (np.cos(y_rad)**2 + (r_eq/r_pol)**2 * np.sin(y_rad)**2)
    b = -2 * (H + r_eq) * np.cos(x_rad) * np.cos(y_rad)
    c = (H + r_eq)**2 - r_eq**2
    
    discriminant = b**2 - 4*a*c
    
    if discriminant < 0:
        return np.nan, np.nan  # Ponto está no espaço
    
    # Distância do satélite ao ponto
    rs = (-b - np.sqrt(discriminant)) / (2*a)
    
    # Coordenadas do ponto
    sx = rs * np.cos(x_rad) * np.cos(y_rad)
    sy = -rs * np.sin(x_rad)
    sz = rs * np.cos(x_rad) * np.sin(y_rad)
    
    # Latitude geodésica
    lat = np.degrees(np.arctan((r_eq/r_pol)**2 * sz / np.sqrt((H + r_eq - sx)**2 + sy**2)))
    
    # Longitude
    lon = lon_0 - np.degrees(np.arctan(sy / (H + r_eq - sx)))
    
    return lat, lon


def latlon_to_pixel(lat, lon, img_width=SSA_WIDTH, img_height=SSA_HEIGHT, bounds=None):
    """
    Converte lat/lon para coordenadas de pixel na imagem SSA.
    Usa projeção geoestacionária (scan angles em radianos).
    
    Args:
        lat: Latitude em graus
        lon: Longitude em graus
        img_width: Largura da imagem em pixels
        img_height: Altura da imagem em pixels
        bounds: Ignorado (mantido por compatibilidade)
    
    Returns:
        (x, y): Coordenadas do pixel
    """
    # Converter lat/lon para scan angles
    x_rad, y_rad = latlon_to_scan(lat, lon)
    
    # Converter scan angles para pixels
    # X: linear de x_min para x_max
    x = int((x_rad - SSA_SCAN_ANGLES["x_min"]) / SSA_RES_X)
    # Y: y_max é menos negativo (topo), y_min é mais negativo (base)
    # Pixel 0 = topo (y_max), Pixel H = base (y_min)
    y = int((SSA_SCAN_ANGLES["y_max"] - y_rad) / SSA_RES_Y)
    
    return x, y


def pixel_to_latlon(x, y, img_width=SSA_WIDTH, img_height=SSA_HEIGHT, bounds=None):
    """
    Converte coordenadas de pixel para lat/lon.
    Usa projeção geoestacionária (scan angles em radianos).
    
    Args:
        x, y: Coordenadas do pixel
        img_width: Largura da imagem em pixels
        img_height: Altura da imagem em pixels
        bounds: Ignorado (mantido por compatibilidade)
    
    Returns:
        (lat, lon): Coordenadas geográficas em graus, ou (NaN, NaN) se no espaço
    """
    # Converter pixels para scan angles
    x_rad = SSA_SCAN_ANGLES["x_min"] + x * SSA_RES_X
    y_rad = SSA_SCAN_ANGLES["y_max"] - y * SSA_RES_Y
    
    # Converter scan angles para lat/lon
    lat, lon = scan_to_latlon(x_rad, y_rad)
    
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
        
        # Converter corners para pixels (usando projeção geoestacionária)
        x1, y1 = latlon_to_pixel(roi_bounds["lat_max"], roi_bounds["lon_min"])
        x2, y2 = latlon_to_pixel(roi_bounds["lat_min"], roi_bounds["lon_max"])
        
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
