#!/usr/bin/env python3
"""
CPTEC/INPE Data Extractor
Extrai dados de previsão meteorológica do CPTEC/INPE (Centro de Previsão de Tempo e Estudos Climáticos)
Fonte de dados meteorológicos brasileiros oficiais.
"""

import requests
import pandas as pd
import datetime
import os
import json
import xml.etree.ElementTree as ET
from typing import Optional, Dict, List

# ==============================================================================
# APIs DISPONÍVEIS DO CPTEC/INPE
# ==============================================================================

# API de Previsão por Cidade (XML oficial)
CPTEC_API_BASE = "http://servicos.cptec.inpe.br/XML"


class CPTECExtractor:
    """Extrator de dados do CPTEC/INPE usando API XML oficial"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WeatherExtractor/1.0'
        })
    
    def buscar_cidade(self, nome: str) -> List[Dict]:
        """
        Busca código de cidade pelo nome.
        
        Args:
            nome: Nome da cidade
            
        Returns:
            Lista de cidades encontradas com código e nome
        """
        try:
            url = f"{CPTEC_API_BASE}/listaCidades"
            params = {"city": nome}
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            # Parse XML
            root = ET.fromstring(response.content)
            cidades = []
            for cidade in root.findall('.//cidade'):
                cidades.append({
                    'id': cidade.find('id').text if cidade.find('id') is not None else None,
                    'nome': cidade.find('nome').text if cidade.find('nome') is not None else None,
                    'estado': cidade.find('uf').text if cidade.find('uf') is not None else None
                })
            return cidades
        except Exception as e:
            print(f"Erro ao buscar cidade: {e}")
            return []
    
    def previsao_cidade_4dias(self, codigo_cidade: int) -> Dict:
        """
        Obtém previsão do tempo para 4 dias.
        
        Args:
            codigo_cidade: Código CPTEC da cidade
            
        Returns:
            Dados de previsão
        """
        try:
            url = f"{CPTEC_API_BASE}/cidade/{codigo_cidade}/previsao.xml"
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            root = ET.fromstring(response.content)
            
            resultado = {
                'cidade': root.find('nome').text if root.find('nome') is not None else None,
                'estado': root.find('uf').text if root.find('uf') is not None else None,
                'atualizado': root.find('atualizacao').text if root.find('atualizacao') is not None else None,
                'previsao': []
            }
            
            for prev in root.findall('.//previsao'):
                dia = {
                    'data': prev.find('dia').text if prev.find('dia') is not None else None,
                    'condicao': prev.find('tempo').text if prev.find('tempo') is not None else None,
                    'max': prev.find('maxima').text if prev.find('maxima') is not None else None,
                    'min': prev.find('minima').text if prev.find('minima') is not None else None,
                    'iuv': prev.find('iuv').text if prev.find('iuv') is not None else None,
                }
                resultado['previsao'].append(dia)
            
            return resultado
        except Exception as e:
            print(f"Erro ao obter previsão 4 dias: {e}")
            return {}
    
    def previsao_cidade_7dias(self, codigo_cidade: int) -> Dict:
        """
        Obtém previsão estendida de 7 dias.
        
        Args:
            codigo_cidade: Código CPTEC da cidade
            
        Returns:
            Dados de previsão
        """
        try:
            url = f"{CPTEC_API_BASE}/cidade/7dias/{codigo_cidade}/previsao.xml"
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            root = ET.fromstring(response.content)
            
            resultado = {
                'cidade': root.find('nome').text if root.find('nome') is not None else None,
                'estado': root.find('uf').text if root.find('uf') is not None else None,
                'atualizado': root.find('atualizacao').text if root.find('atualizacao') is not None else None,
                'previsao': []
            }
            
            for prev in root.findall('.//previsao'):
                dia = {
                    'data': prev.find('dia').text if prev.find('dia') is not None else None,
                    'condicao': prev.find('tempo').text if prev.find('tempo') is not None else None,
                    'max': prev.find('maxima').text if prev.find('maxima') is not None else None,
                    'min': prev.find('minima').text if prev.find('minima') is not None else None,
                    'iuv': prev.find('iuv').text if prev.find('iuv') is not None else None,
                }
                resultado['previsao'].append(dia)
            
            return resultado
        except Exception as e:
            print(f"Erro ao obter previsão 7 dias: {e}")
            return {}
    
    def ondas_cidade(self, codigo_cidade: int) -> Dict:
        """
        Obtém previsão de ondas para cidades litorâneas.
        
        Args:
            codigo_cidade: Código CPTEC da cidade
            
        Returns:
            Dados de previsão de ondas
        """
        try:
            url = f"{CPTEC_API_BASE}/cidade/{codigo_cidade}/todos/tempos/ondas.xml"
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            root = ET.fromstring(response.content)
            
            resultado = {
                'cidade': root.find('nome').text if root.find('nome') is not None else None,
                'estado': root.find('uf').text if root.find('uf') is not None else None,
                'atualizado': root.find('atualizacao').text if root.find('atualizacao') is not None else None,
                'ondas': []
            }
            
            for prev in root.findall('.//previsao'):
                dia_elem = prev.find('dia')
                if dia_elem is not None:
                    for periodo in ['manha', 'tarde', 'noite']:
                        periodo_elem = prev.find(periodo)
                        if periodo_elem is not None:
                            onda = {
                                'data': dia_elem.text,
                                'periodo': periodo,
                                'altura': periodo_elem.find('altura').text if periodo_elem.find('altura') is not None else None,
                                'direcao': periodo_elem.find('direcao').text if periodo_elem.find('direcao') is not None else None,
                                'vento': periodo_elem.find('vento').text if periodo_elem.find('vento') is not None else None,
                                'direcao_vento': periodo_elem.find('direcao_vento').text if periodo_elem.find('direcao_vento') is not None else None,
                            }
                            resultado['ondas'].append(onda)
            
            return resultado
        except Exception as e:
            print(f"Erro ao obter previsão de ondas: {e}")
            return {}
    
    def condicoes_capitais(self) -> List[Dict]:
        """
        Obtém condições atuais de todas as capitais brasileiras.
        
        Returns:
            Lista com dados de todas as capitais
        """
        try:
            url = f"{CPTEC_API_BASE}/capitais/condicoesAtuais.xml"
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            root = ET.fromstring(response.content)
            
            capitais = []
            for capital in root.findall('.//capital'):
                dados = {
                    'nome': capital.find('nome').text if capital.find('nome') is not None else None,
                    'uf': capital.find('uf').text if capital.find('uf') is not None else None,
                    'temp': capital.find('temp').text if capital.find('temp') is not None else None,
                    'umidade': capital.find('umidade').text if capital.find('umidade') is not None else None,
                    'pressao': capital.find('pressao').text if capital.find('pressao') is not None else None,
                    'vento_dir': capital.find('vento-dir').text if capital.find('vento-dir') is not None else None,
                    'vento_vel': capital.find('vento-vel').text if capital.find('vento-vel') is not None else None,
                    'condicao': capital.find('tempo').text if capital.find('tempo') is not None else None,
                    'atualizado': capital.find('atualizacao').text if capital.find('atualizacao') is not None else None,
                }
                capitais.append(dados)
            
            return capitais
        except Exception as e:
            print(f"Erro ao obter condições das capitais: {e}")
            return []


# Mapeamento de códigos de condição CPTEC
CONDICOES_CPTEC = {
    'ec': 'Encoberto com Chuvas Isoladas',
    'ci': 'Chuvas Isoladas',
    'c': 'Chuva',
    'in': 'Instável',
    'pp': 'Possibilidade de Pancadas de Chuva',
    'cm': 'Chuva pela Manhã',
    'cn': 'Chuva pela Noite',
    'pt': 'Pancadas de Chuva pela Tarde',
    'pm': 'Pancadas de Chuva pela Manhã',
    'np': 'Nublado e Pancadas de Chuva',
    'pc': 'Pancadas de Chuva',
    'pn': 'Parcialmente Nublado',
    'cv': 'Chuvisco',
    'ch': 'Chuvoso',
    'e': 'Encoberto',
    'n': 'Nublado',
    'cl': 'Céu Claro',
    'nv': 'Nevoeiro',
    'g': 'Geada',
    'ne': 'Neve',
    'nd': 'Não Definido',
    'pnt': 'Pancadas de Chuva pela Noite',
    'psc': 'Possibilidade de Chuva',
    'pcm': 'Possibilidade de Chuva pela Manhã',
    'pct': 'Possibilidade de Chuva pela Tarde',
    'pcn': 'Possibilidade de Chuva pela Noite',
    'npt': 'Nublado com Pancadas pela Tarde',
    'npn': 'Nublado com Pancadas pela Noite',
    'ncn': 'Nublado com Chuvas pela Noite',
    'nct': 'Nublado com Chuvas pela Tarde',
    'ncm': 'Nublado com Chuvas pela Manhã',
    'npm': 'Nublado com Pancadas pela Manhã',
    't': 'Tempestade',
    'ps': 'Predomínio de Sol',
    'vn': 'Variação de Nebulosidade',
}


def demo_cptec():
    """Demonstração de uso da API CPTEC"""
    extractor = CPTECExtractor()
    
    print("=" * 60)
    print("  CPTEC/INPE Data Extractor - Demo")
    print("=" * 60)
    
    # Buscar cidade
    print("\n📍 Buscando 'Campos'...")
    cidades = extractor.buscar_cidade("Campos")
    
    if cidades:
        print(f"   Encontradas {len(cidades)} cidade(s):")
        for c in cidades[:5]:  # Mostrar apenas 5
            print(f"   - {c.get('nome')}, {c.get('estado')} (código: {c.get('id')})")
        
        # Pegar primeira cidade
        codigo = cidades[0].get('id')
        
        # Previsão 4 dias
        print(f"\n🌤️ Previsão 4 dias para código {codigo}...")
        previsao = extractor.previsao_cidade_4dias(int(codigo))
        
        if previsao and previsao.get('previsao'):
            print(f"   Cidade: {previsao.get('cidade')}")
            print(f"   Estado: {previsao.get('estado')}")
            print(f"   Atualizado: {previsao.get('atualizado')}")
            
            print(f"\n   📅 Previsão:")
            for dia in previsao.get('previsao', []):
                condicao = CONDICOES_CPTEC.get(dia.get('condicao'), dia.get('condicao'))
                print(f"   {dia.get('data')}: {condicao} "
                      f"Min: {dia.get('min')}°C Max: {dia.get('max')}°C IUV: {dia.get('iuv')}")
        
        # Previsão 7 dias
        print(f"\n📆 Previsão 7 dias...")
        previsao7 = extractor.previsao_cidade_7dias(int(codigo))
        if previsao7 and previsao7.get('previsao'):
            print(f"   {len(previsao7.get('previsao', []))} dias disponíveis")
        
        # Ondas (se disponível)
        print(f"\n🌊 Previsão de ondas...")
        ondas = extractor.ondas_cidade(int(codigo))
        if ondas and ondas.get('ondas'):
            for onda in ondas.get('ondas', [])[:6]:
                print(f"   {onda.get('data')} ({onda.get('periodo')}): "
                      f"Altura {onda.get('altura')}m, Dir: {onda.get('direcao')}, "
                      f"Vento: {onda.get('vento')} km/h {onda.get('direcao_vento')}")
        else:
            print("   Dados de ondas não disponíveis para esta cidade")
    
    # Condições capitais
    print("\n🏛️ Condições atuais das capitais...")
    capitais = extractor.condicoes_capitais()
    if capitais:
        print(f"   {len(capitais)} capitais disponíveis:")
        for cap in capitais[:8]:
            condicao = CONDICOES_CPTEC.get(cap.get('condicao'), cap.get('condicao'))
            print(f"   - {cap.get('nome')}/{cap.get('uf')}: {condicao} "
                  f"Temp: {cap.get('temp')}°C Umid: {cap.get('umidade')}% "
                  f"Vento: {cap.get('vento_vel')} km/h {cap.get('vento_dir')}")
    
    print("\n" + "=" * 60)


def extrair_para_csv(cidade_nome: str, dias: int = 7, outdir: str = ".") -> str:
    """
    Extrai dados do CPTEC e salva em CSV.
    
    Args:
        cidade_nome: Nome da cidade
        dias: Dias de previsão (4 ou 7)
        outdir: Diretório de saída
        
    Returns:
        Caminho do arquivo CSV gerado
    """
    extractor = CPTECExtractor()
    
    # Buscar cidade
    print(f"📍 Buscando cidade '{cidade_nome}'...")
    cidades = extractor.buscar_cidade(cidade_nome)
    if not cidades:
        raise ValueError(f"Cidade '{cidade_nome}' não encontrada")
    
    codigo = int(cidades[0].get('id'))
    nome_cidade = cidades[0].get('nome')
    print(f"   Encontrada: {nome_cidade} (código: {codigo})")
    
    # Obter previsão
    print(f"🌤️ Obtendo previsão...")
    if dias <= 4:
        previsao = extractor.previsao_cidade_4dias(codigo)
    else:
        previsao = extractor.previsao_cidade_7dias(codigo)
    
    if not previsao or not previsao.get('previsao'):
        raise ValueError("Não foi possível obter previsão")
    
    # Converter para DataFrame
    dados = previsao.get('previsao', [])
    df = pd.DataFrame(dados)
    
    # Traduzir códigos de condição
    if 'condicao' in df.columns:
        df['condicao_descricao'] = df['condicao'].map(CONDICOES_CPTEC)
    
    # Obter ondas (se disponível)
    print(f"🌊 Obtendo dados de ondas...")
    ondas = extractor.ondas_cidade(codigo)
    if ondas and ondas.get('ondas'):
        ondas_df = pd.DataFrame(ondas.get('ondas', []))
        print(f"   {len(ondas_df)} registros de ondas")
        
        # Salvar ondas separadamente
        os.makedirs(outdir, exist_ok=True)
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M")
        ondas_file = os.path.join(outdir, f"CPTEC_ondas_{nome_cidade.replace(' ', '_')}_{timestamp}.csv")
        ondas_df.to_csv(ondas_file, index=False)
        print(f"✅ Ondas salvas em: {ondas_file}")
    
    # Adicionar metadados
    df['cidade'] = previsao.get('cidade')
    df['estado'] = previsao.get('estado')
    df['fonte'] = 'CPTEC/INPE'
    df['atualizado'] = previsao.get('atualizado')
    
    # Salvar previsão
    os.makedirs(outdir, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M")
    filename = f"CPTEC_{nome_cidade.replace(' ', '_')}_{timestamp}.csv"
    filepath = os.path.join(outdir, filename)
    
    df.to_csv(filepath, index=False)
    print(f"✅ Previsão salva em: {filepath}")
    
    return filepath


def extrair_capitais(outdir: str = ".") -> str:
    """
    Extrai condições atuais de todas as capitais.
    
    Args:
        outdir: Diretório de saída
        
    Returns:
        Caminho do arquivo CSV gerado
    """
    extractor = CPTECExtractor()
    
    print("🏛️ Obtendo condições das capitais...")
    capitais = extractor.condicoes_capitais()
    
    if not capitais:
        raise ValueError("Não foi possível obter dados das capitais")
    
    df = pd.DataFrame(capitais)
    
    # Traduzir códigos
    if 'condicao' in df.columns:
        df['condicao_descricao'] = df['condicao'].map(CONDICOES_CPTEC)
    
    df['fonte'] = 'CPTEC/INPE'
    
    os.makedirs(outdir, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M")
    filename = f"CPTEC_capitais_{timestamp}.csv"
    filepath = os.path.join(outdir, filename)
    
    df.to_csv(filepath, index=False)
    print(f"✅ Dados das capitais salvos em: {filepath}")
    print(f"   {len(df)} capitais")
    
    return filepath


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Extrator de dados CPTEC/INPE")
    parser.add_argument("--cidade", type=str, help="Nome da cidade")
    parser.add_argument("--dias", type=int, default=7, help="Dias de previsão (4 ou 7)")
    parser.add_argument("--outdir", type=str, default="output", help="Diretório de saída")
    parser.add_argument("--demo", action="store_true", help="Executar demonstração")
    parser.add_argument("--capitais", action="store_true", help="Extrair condições das capitais")
    
    args = parser.parse_args()
    
    if args.demo:
        demo_cptec()
    elif args.capitais:
        extrair_capitais(args.outdir)
    elif args.cidade:
        extrair_para_csv(args.cidade, args.dias, args.outdir)
    else:
        print("Uso do CPTEC Extractor:")
        print("  --demo              Executar demonstração")
        print("  --capitais          Extrair condições das capitais")
        print("  --cidade NOME       Extrair previsão para cidade")
        print("  --dias N            Dias de previsão (4 ou 7)")
        print("  --outdir DIR        Diretório de saída")
        print("")
        print("Exemplos:")
        print("  python cptec_extractor.py --demo")
        print("  python cptec_extractor.py --capitais --outdir output")
        print("  python cptec_extractor.py --cidade 'Rio de Janeiro' --dias 7")
