# Weather Extractor (Open-Meteo)

🌊 Utilitário para extrair dados meteorológicos e marinhos em intervalos de 15 minutos usando a API Open-Meteo.

## 🌐 Visualização Online

Acesse os dados em tempo real: **https://marcotulio1989.github.io/weatherextraxtor/**

Os dados são atualizados automaticamente **a cada hora** via GitHub Actions.

## 📦 Instalação

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 🚀 Uso

### Básico
```bash
python app.py --lat -22.46 --lon -40.54 --timezone America/Sao_Paulo
```

### Com geração de HTML (para GitHub Pages)
```bash
python app.py --outdir docs --html --past-hours 24 --future-hours 24
```

## ⚙️ Opções

| Opção | Descrição | Padrão |
|-------|-----------|--------|
| `--lat` | Latitude alvo | `-22.46` (Bacia de Campos) |
| `--lon` | Longitude alvo | `-40.54` (Bacia de Campos) |
| `--timezone` | Timezone | `America/Sao_Paulo` |
| `--outdir` | Pasta para salvar os arquivos | `.` |
| `--past-hours` | Horas no passado para buscar | `24` |
| `--future-hours` | Horas no futuro para buscar | `24` |
| `--html` | Gera arquivo HTML com visualização | `false` |

## 📊 Dados Coletados

### Modelos Meteorológicos
- ECMWF IFS 0.25° (Europa)
- ICON (Alemanha)
- GFS (EUA)
- Météo-France
- JMA (Japão)

### Variáveis Marinhas
- Altura, direção e período das ondas
- Ondas de vento e swell
- Velocidade e direção das correntes oceânicas

### Variáveis Atmosféricas
- Velocidade e direção do vento (10m, 80m)
- Pressão atmosférica
- Temperatura
- Precipitação
- Cobertura de nuvens
- Radiação solar
- E muito mais...

## 🔄 Atualização Automática

O repositório usa GitHub Actions para atualizar os dados a cada hora. O workflow:
1. Executa o script Python
2. Gera o CSV e o HTML
3. Faz commit e push automático
4. Deploy no GitHub Pages

## 📁 Estrutura

```
├── app.py              # Script principal
├── requirements.txt    # Dependências Python
├── docs/               # Arquivos para GitHub Pages
│   ├── index.html      # Visualização web
│   └── *.csv           # Dados extraídos
└── .github/workflows/  # GitHub Actions
    └── update-weather.yml
```

## 📄 Licença

MIT