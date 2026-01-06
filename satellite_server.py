"""
=============================================================================
SATELLITE IMAGE SERVER
=============================================================================
Servidor que baixa, processa e serve imagens de satélite para a área do navio.
Usa GOES-16 via NOAA AWS ou RAMMB SLIDER.
=============================================================================
"""

from flask import Flask, jsonify, send_file, Response
from flask_cors import CORS
import requests
from io import BytesIO
from PIL import Image
import base64
from datetime import datetime, timedelta
import json
import os

app = Flask(__name__)
CORS(app)

# =========================================================================
# CONFIGURAÇÃO DO NAVIO
# =========================================================================
CONFIG = {
    "ship_name": "Deepwater Aquila",
    "ship_lat": -22.50,
    "ship_lon": -40.50,
    "radius_nm": 150,  # Raio em milhas náuticas
}

# Converter NM para graus (1 grau ≈ 60 NM)
RADIUS_DEG = CONFIG["radius_nm"] / 60

# Bounding box da área de interesse
BBOX = {
    "lat_min": CONFIG["ship_lat"] - RADIUS_DEG,
    "lat_max": CONFIG["ship_lat"] + RADIUS_DEG,
    "lon_min": CONFIG["ship_lon"] - RADIUS_DEG,
    "lon_max": CONFIG["ship_lon"] + RADIUS_DEG,
}

print(f"🛰️ Satellite Server - {CONFIG['ship_name']}")
print(f"📍 Posição: {CONFIG['ship_lat']}°, {CONFIG['ship_lon']}°")
print(f"📏 Raio: {CONFIG['radius_nm']} NM ({RADIUS_DEG:.2f}°)")
print(f"📦 Área: {BBOX['lat_min']:.2f}° a {BBOX['lat_max']:.2f}° lat")
print(f"        {BBOX['lon_min']:.2f}° a {BBOX['lon_max']:.2f}° lon")


# =========================================================================
# FONTE 1: GOES-16 via SLIDER (RAMMB/CIRA)
# =========================================================================
def get_goes16_slider_url():
    """
    RAMMB SLIDER - Imagens GOES-16 em tempo real.
    Band 13 = Infrared (Clean Longwave Window)
    """
    now = datetime.utcnow()
    
    # SLIDER atualiza a cada 10-15 minutos, pegar imagem de 20 min atrás
    target_time = now - timedelta(minutes=20)
    
    year = target_time.strftime("%Y")
    doy = target_time.strftime("%j")  # Day of year
    hour = target_time.strftime("%H")
    
    # Arredondar para 10 minutos
    minute = (target_time.minute // 10) * 10
    minute_str = f"{minute:02d}"
    
    # URL do SLIDER para Full Disk
    # Setor: full_disk, conus, mesoscale
    url = f"https://rammb-slider.cira.colostate.edu/data/imagery/{year}/{doy}/goes-16---full_disk/band_13/{hour}{minute_str}/00_00.png"
    
    return url, target_time


def get_goes16_fulldisk():
    """
    Baixa imagem GOES-16 Full Disk e recorta para área do navio.
    """
    try:
        # Tentar várias abordagens
        
        # 1. NOAA GOES Image Viewer (mais confiável)
        # https://www.star.nesdis.noaa.gov/GOES/
        
        # URL direta para imagem recente do GOES-16 Band 13 (IR)
        # Cobertura: América do Sul
        urls_to_try = [
            # NOAA STAR - América do Sul IR
            "https://cdn.star.nesdis.noaa.gov/GOES16/ABI/SECTOR/ssa/13/latest.jpg",
            # Full Disk IR
            "https://cdn.star.nesdis.noaa.gov/GOES16/ABI/FD/13/latest.jpg",
        ]
        
        for url in urls_to_try:
            try:
                print(f"🔄 Tentando: {url}")
                response = requests.get(url, timeout=30)
                if response.status_code == 200:
                    print(f"✅ Sucesso: {len(response.content)} bytes")
                    return response.content, url
            except Exception as e:
                print(f"❌ Falhou: {e}")
                continue
        
        return None, None
        
    except Exception as e:
        print(f"❌ Erro ao baixar GOES-16: {e}")
        return None, None


# =========================================================================
# FONTE 2: INPE/CPTEC (Brasil)
# =========================================================================
def get_inpe_satellite():
    """
    INPE - Imagens de satélite para América do Sul.
    GOES-16 processado pelo CPTEC.
    """
    try:
        # INPE CPTEC Satellite
        # http://satelite.cptec.inpe.br/
        
        urls = [
            # Imagem IR realçada América do Sul
            "http://satelite.cptec.inpe.br/repositoriogoes/goes16/goes16_web/ams_ret_ch13_cpt/",
        ]
        
        # O INPE organiza por data/hora
        now = datetime.utcnow()
        date_str = now.strftime("%Y%m%d")
        
        # Listar arquivos disponíveis seria necessário
        # Por enquanto, usar a fonte NOAA que é mais direta
        
        return None, None
        
    except Exception as e:
        print(f"❌ Erro INPE: {e}")
        return None, None


# =========================================================================
# API ENDPOINTS
# =========================================================================

@app.route('/api/satellite/latest', methods=['GET'])
def get_latest_satellite():
    """
    Retorna a imagem de satélite mais recente para a área do navio.
    """
    image_data, source_url = get_goes16_fulldisk()
    
    if image_data:
        # Converter para base64
        img_base64 = base64.b64encode(image_data).decode('utf-8')
        
        return jsonify({
            "success": True,
            "timestamp": datetime.utcnow().isoformat(),
            "source": source_url,
            "image_base64": img_base64,
            "content_type": "image/jpeg",
            "bbox": BBOX,
            "ship": {
                "lat": CONFIG["ship_lat"],
                "lon": CONFIG["ship_lon"],
                "name": CONFIG["ship_name"]
            }
        })
    else:
        return jsonify({
            "success": False,
            "error": "Não foi possível obter imagem de satélite",
            "timestamp": datetime.utcnow().isoformat()
        }), 500


@app.route('/api/satellite/image', methods=['GET'])
def get_satellite_image():
    """
    Retorna a imagem diretamente (para uso como src de img).
    """
    image_data, _ = get_goes16_fulldisk()
    
    if image_data:
        return Response(image_data, mimetype='image/jpeg')
    else:
        return "Imagem não disponível", 500


@app.route('/api/satellite/tiles/<int:z>/<int:x>/<int:y>.png', methods=['GET'])
def get_satellite_tile(z, x, y):
    """
    Serve tiles de satélite para uso com Leaflet.
    Faz proxy para RainViewer ou outra fonte.
    """
    try:
        # Buscar path atual do RainViewer
        api_url = "https://api.rainviewer.com/public/weather-maps.json"
        response = requests.get(api_url, timeout=10)
        data = response.json()
        
        if data.get('satellite', {}).get('infrared'):
            latest = data['satellite']['infrared'][-1]
            path = latest['path']
            
            # Buscar tile
            tile_url = f"https://tilecache.rainviewer.com{path}/256/{z}/{x}/{y}/0/0_0.png"
            tile_response = requests.get(tile_url, timeout=10)
            
            if tile_response.status_code == 200:
                return Response(tile_response.content, mimetype='image/png')
        
        # Tile transparente se não disponível
        return Response(b'', mimetype='image/png'), 204
        
    except Exception as e:
        print(f"Erro tile: {e}")
        return Response(b'', mimetype='image/png'), 204


@app.route('/api/satellite/overlay', methods=['GET'])
def get_satellite_overlay():
    """
    Retorna informações para criar overlay no Leaflet.
    Inclui URL do tile e bounds.
    """
    try:
        # Buscar dados do RainViewer
        api_url = "https://api.rainviewer.com/public/weather-maps.json"
        response = requests.get(api_url, timeout=10)
        data = response.json()
        
        result = {
            "success": True,
            "timestamp": datetime.utcnow().isoformat(),
            "ship": {
                "lat": CONFIG["ship_lat"],
                "lon": CONFIG["ship_lon"],
                "name": CONFIG["ship_name"],
                "radius_nm": CONFIG["radius_nm"]
            },
            "bbox": BBOX,
            "sources": []
        }
        
        # RainViewer infrared
        if data.get('satellite', {}).get('infrared'):
            latest = data['satellite']['infrared'][-1]
            result["sources"].append({
                "name": "RainViewer IR",
                "type": "tiles",
                "url_template": f"https://tilecache.rainviewer.com{latest['path']}/256/{{z}}/{{x}}/{{y}}/0/0_0.png",
                "timestamp": datetime.fromtimestamp(latest['time']).isoformat(),
                "attribution": "RainViewer"
            })
        
        # GOES-16 direct image
        result["sources"].append({
            "name": "GOES-16 IR (América do Sul)",
            "type": "image",
            "url": "https://cdn.star.nesdis.noaa.gov/GOES16/ABI/SECTOR/ssa/13/latest.jpg",
            "attribution": "NOAA GOES-16"
        })
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route('/api/config', methods=['GET'])
def get_config():
    """Retorna configuração atual."""
    return jsonify({
        "ship": CONFIG,
        "bbox": BBOX,
        "radius_deg": RADIUS_DEG
    })


@app.route('/')
def index():
    """Página de status."""
    return f"""
    <html>
    <head><title>Satellite Server</title></head>
    <body style="background:#111;color:#0f0;font-family:monospace;padding:20px;">
        <h1>🛰️ Satellite Image Server</h1>
        <p>Navio: {CONFIG['ship_name']}</p>
        <p>Posição: {CONFIG['ship_lat']}°, {CONFIG['ship_lon']}°</p>
        <p>Raio: {CONFIG['radius_nm']} NM</p>
        <hr>
        <h2>Endpoints:</h2>
        <ul>
            <li><a href="/api/config">/api/config</a> - Configuração</li>
            <li><a href="/api/satellite/overlay">/api/satellite/overlay</a> - Info para overlay</li>
            <li><a href="/api/satellite/image">/api/satellite/image</a> - Imagem direta</li>
            <li><a href="/api/satellite/latest">/api/satellite/latest</a> - JSON com base64</li>
        </ul>
    </body>
    </html>
    """


if __name__ == '__main__':
    print("\n" + "="*60)
    print("🛰️ SATELLITE IMAGE SERVER")
    print("="*60)
    print(f"Iniciando servidor na porta 5001...")
    print(f"Acesse: http://localhost:5001")
    print("="*60 + "\n")
    
    app.run(host='0.0.0.0', port=5001, debug=True)
