"""
Servidor HTTP com proxy para dados de escaterômetro e DMW (Derived Motion Winds).
Resolve o problema de CORS ao buscar dados do NOAA ERDDAP.
"""

import http.server
import socketserver
import json
import urllib.request
import urllib.error
from urllib.parse import urlparse, parse_qs
import os
import sys

PORT = 8080

# Adicionar path para imports
sys.path.insert(0, '/workspaces/weatherextraxtor')

# Cache global para DMW
DMW_CACHE = {
    'data': None,
    'timestamp': None,
    'satellite': None,
    'level': None
}

class ProxyHandler(http.server.SimpleHTTPRequestHandler):
    
    def do_GET(self):
        parsed = urlparse(self.path)
        
        # Redirecionar raiz para o monitor
        if parsed.path == '/' or parsed.path == '':
            self.send_response(302)
            self.send_header('Location', '/docs/full_disk_viewer.html')
            self.end_headers()
            return
        
        # Proxy para NOAA ERDDAP
        if parsed.path == '/api/noaa':
            self.proxy_noaa(parsed.query)
            return
        
        # API para dados DMW (Derived Motion Winds)
        if parsed.path == '/api/dmw':
            self.get_dmw_data(parsed.query)
            return
        
        # API para listar arquivos DMW disponíveis
        if parsed.path == '/api/dmw/list':
            self.list_dmw_files(parsed.query)
            return
        
        # API para dados L2 (CAPE, LI, Cloud Height, TPW)
        if parsed.path == '/api/l2':
            self.get_l2_data(parsed.query)
            return
        
        # Servir arquivos estáticos normalmente
        super().do_GET()
    
    def proxy_noaa(self, query_string):
        """Faz proxy para NOAA ERDDAP e retorna dados com headers CORS"""
        params = parse_qs(query_string)
        
        # Pegar a URL do ERDDAP
        erddap_url = params.get('url', [''])[0]
        
        if not erddap_url:
            self.send_error(400, 'URL parameter required')
            return
        
        try:
            # Fazer request para NOAA
            req = urllib.request.Request(
                erddap_url,
                headers={'User-Agent': 'WeatherExtractor/1.0'}
            )
            
            with urllib.request.urlopen(req, timeout=30) as response:
                data = response.read()
                
                # Enviar resposta com CORS
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(data)
                
        except urllib.error.HTTPError as e:
            self.send_response(e.code)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({
                'error': f'NOAA returned {e.code}',
                'reason': str(e.reason)
            }).encode())
            
        except urllib.error.URLError as e:
            self.send_response(502)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({
                'error': 'Failed to connect to NOAA',
                'reason': str(e.reason)
            }).encode())
            
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({
                'error': str(e)
            }).encode())
    
    def do_OPTIONS(self):
        """Handle CORS preflight"""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', '*')
        self.end_headers()
    
    def get_dmw_data(self, query_string):
        """Retorna dados de Derived Motion Winds (DMW) do GOES."""
        global DMW_CACHE
        
        params = parse_qs(query_string)
        satellite = params.get('satellite', ['goes16'])[0]
        level = params.get('level', ['low'])[0]
        
        # Bbox opcional
        bbox = None
        if 'lat_min' in params:
            try:
                bbox = {
                    'lat_min': float(params.get('lat_min', [-35])[0]),
                    'lat_max': float(params.get('lat_max', [-10])[0]),
                    'lon_min': float(params.get('lon_min', [-55])[0]),
                    'lon_max': float(params.get('lon_max', [-30])[0])
                }
            except ValueError:
                pass
        
        try:
            from dmw_extractor import DMWExtractor
            from datetime import datetime, timedelta
            
            # Verificar cache (válido por 10 minutos)
            cache_valid = False
            if DMW_CACHE['data'] and DMW_CACHE['timestamp']:
                age = (datetime.utcnow() - DMW_CACHE['timestamp']).total_seconds()
                if age < 600 and DMW_CACHE['satellite'] == satellite and DMW_CACHE['level'] == level:
                    cache_valid = True
            
            if cache_valid:
                print(f"📦 DMW: Usando cache ({satellite}/{level})")
                data = DMW_CACHE['data']
            else:
                print(f"🌬️ DMW: Baixando dados ({satellite}/{level})")
                extractor = DMWExtractor(satellite=satellite)
                nc_file = extractor.download_latest()
                data = extractor.extract_winds(nc_file, level=level, bbox=bbox)
                
                # Atualizar cache
                DMW_CACHE['data'] = data
                DMW_CACHE['timestamp'] = datetime.utcnow()
                DMW_CACHE['satellite'] = satellite
                DMW_CACHE['level'] = level
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Cache-Control', 'max-age=600')
            self.end_headers()
            self.wfile.write(json.dumps(data).encode())
            
        except ImportError as e:
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({
                'error': 'Módulo DMW não disponível',
                'details': str(e)
            }).encode())
            
        except Exception as e:
            import traceback
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({
                'error': str(e),
                'traceback': traceback.format_exc()
            }).encode())
    
    def list_dmw_files(self, query_string):
        """Lista arquivos DMW disponíveis."""
        params = parse_qs(query_string)
        satellite = params.get('satellite', ['goes16'])[0]
        hours = int(params.get('hours', ['6'])[0])
        
        try:
            from dmw_extractor import DMWExtractor
            
            extractor = DMWExtractor(satellite=satellite)
            files = extractor.list_available_files(hours_back=hours)
            
            # Formatar para JSON
            files_json = [{
                'filename': f['filename'],
                'scan_time': f['scan_time'].isoformat(),
                'size_mb': round(f['size_mb'], 2)
            } for f in files[:20]]  # Limitar a 20 resultados
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({
                'satellite': satellite,
                'count': len(files_json),
                'files': files_json
            }).encode())
            
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({
                'error': str(e)
            }).encode())
    
    def get_l2_data(self, query_string):
        """Retorna dados L2 do GOES (CAPE, LI, Cloud Height, TPW)."""
        params = parse_qs(query_string)
        satellite = params.get('satellite', ['goes19'])[0]
        products_str = params.get('products', ['DSIF,ACHAF,ACTPF,TPWF'])[0]
        products = products_str.split(',')
        
        # Posição central e raio
        center_lat = float(params.get('lat', ['-22.5'])[0])
        center_lon = float(params.get('lon', ['-40.5'])[0])
        radius_nm = float(params.get('radius', ['100'])[0])
        
        try:
            from goes_l2_extractor import GOESL2Extractor, generate_grid_points
            
            print(f"📊 L2: Extraindo dados ({satellite}/{products_str})")
            
            # Gerar grade de pontos - step menor = mais pontos
            grid_points = generate_grid_points(center_lat, center_lon, radius_nm=radius_nm, step_nm=15)
            
            extractor = GOESL2Extractor(satellite=satellite)
            data = extractor.extract_all_products(grid_points, products=products)
            
            # Salvar também no arquivo JSON para uso offline
            import json
            output_file = '/workspaces/weatherextraxtor/docs/goes_l2_latest.json'
            with open(output_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Cache-Control', 'max-age=600')
            self.end_headers()
            self.wfile.write(json.dumps(data).encode())
            
        except ImportError as e:
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({
                'error': 'Módulo L2 não disponível',
                'details': str(e)
            }).encode())
            
        except Exception as e:
            import traceback
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({
                'error': str(e),
                'traceback': traceback.format_exc()
            }).encode())

def run():
    os.chdir('/workspaces/weatherextraxtor')
    
    with socketserver.TCPServer(("", PORT), ProxyHandler) as httpd:
        print(f"🌊 Servidor rodando em http://localhost:{PORT}")
        print(f"📡 Proxy NOAA: http://localhost:{PORT}/api/noaa?url=...")
        print(f"🌬️ DMW API: http://localhost:{PORT}/api/dmw?satellite=goes19&level=low")
        print(f"📊 L2 API: http://localhost:{PORT}/api/l2?satellite=goes19&products=DSIF,TPWF")
        print(f"🌐 Viewer: http://localhost:{PORT}/docs/full_disk_viewer.html")
        httpd.serve_forever()

if __name__ == '__main__':
    run()
