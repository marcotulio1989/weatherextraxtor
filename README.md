# Weather Extractor (Open-Meteo)

Pequeno utilitário para extrair dados meteorológicos/marítimos em intervalos de 15 minutos usando a API Open-Meteo.

Instalação:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Uso:

```bash
python app.py --lat -22.46 --lon -40.54 --timezone America/Sao_Paulo
```

Opções:
- `--lat` e `--lon`: coordenadas alvo (padrões já apontam a Bacia de Campos)
- `--timezone`: timezone (padrão: `America/Sao_Paulo`)
- `--outdir`: pasta para salvar o CSV

O script gera um arquivo CSV com nome `Bacia_Campos_CONSOLIDADO_<DATA>.csv`.
# weatherextraxtor