"""
Servidor HTTP com proxy para dados de escaterômetro.
Resolve o problema de CORS ao buscar dados do NOAA ERDDAP.
"""

import http.server
import socketserver
import json
import urllib.request
import urllib.error
from urllib.parse import urlparse, parse_qs
import os

PORT = 8080

class ProxyHandler(http.server.SimpleHTTPRequestHandler):
    
    def do_GET(self):
        parsed = urlparse(self.path)
        
        # Redirecionar raiz para o monitor
        if parsed.path == '/' or parsed.path == '':
            self.send_response(302)
            self.send_header('Location', '/scatterometer_monitor.html')
            self.end_headers()
            return
        
        # Proxy para NOAA ERDDAP
        if parsed.path == '/api/noaa':
            self.proxy_noaa(parsed.query)
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

def run():
    os.chdir('/workspaces/weatherextraxtor')
    
    with socketserver.TCPServer(("", PORT), ProxyHandler) as httpd:
        print(f"🌊 Servidor rodando em http://localhost:{PORT}")
        print(f"📡 Proxy NOAA: http://localhost:{PORT}/api/noaa?url=...")
        print(f"🌐 Monitor: http://localhost:{PORT}/scatterometer_monitor.html")
        httpd.serve_forever()

if __name__ == '__main__':
    run()
