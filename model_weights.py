#!/usr/bin/env python3
"""
=============================================================================
DYNAMIC MODEL WEIGHTS SYSTEM
=============================================================================
Sistema de pesos dinâmicos para modelos meteorológicos baseado em dados
de satélites escaterômetros (ASCAT, CYGNSS, etc).

CONCEITO:
---------
1. Quando um novo arquivo scatterometer é detectado, os pesos são RESETADOS
2. Peso inicial é calculado baseado na variação entre:
   - Valor SCAT atual vs Previsão do modelo para o mesmo horário
3. Fórmula do peso inicial:
   - Direção: peso = 1 - (|variação_dir| / 180)   → 0° = 1.0, ±180° = 0.0
   - Velocidade: peso = 1 - (|variação_vel| / 30) → 0kt = 1.0, 30kt = 0.0
   - Peso inicial = (peso_dir + peso_vel) / 2
4. Durante o ciclo, o peso é atualizado:
   - peso_novo = sqrt(peso_inicial_ciclo * peso_calculado_comparativo)
5. Quando chega um novo arquivo SCAT, o ciclo reinicia
=============================================================================
"""

import os
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict, field


def utcnow() -> datetime:
    """Retorna datetime atual em UTC (timezone-aware)."""
    return datetime.now(timezone.utc)


# =============================================================================
# CONFIGURAÇÕES
# =============================================================================

# Lista de modelos suportados
WEATHER_MODELS = [
    "ecmwf_ifs025",
    "icon_seamless",
    "gfs_seamless",
    "meteofrance_seamless",
    "jma_seamless",
]

# Limites para cálculo de peso
MAX_DIRECTION_VARIATION = 180.0  # graus (variação máxima = peso 0)
MAX_SPEED_VARIATION = 30.0       # knots (variação máxima = peso 0)

# Arquivo de persistência dos pesos
WEIGHTS_FILE = Path("docs/model_weights.json")
SCAT_FILE = Path("docs/scatterometer_latest.json")


# =============================================================================
# ESTRUTURAS DE DADOS
# =============================================================================

@dataclass
class ModelWeight:
    """Peso de um modelo individual."""
    model_name: str
    initial_weight: float = 1.0           # Peso inicial do ciclo (baseado no SCAT)
    current_weight: float = 1.0           # Peso atual (após atualizações)
    direction_error: float = 0.0          # Erro de direção acumulado
    speed_error: float = 0.0              # Erro de velocidade acumulado
    comparison_count: int = 0             # Número de comparações feitas
    last_updated: str = ""                # Timestamp da última atualização


@dataclass
class WeightsState:
    """Estado completo do sistema de pesos."""
    scat_timestamp: str = ""              # Timestamp do arquivo SCAT atual
    scat_file_hash: str = ""              # Hash do arquivo SCAT (para detectar mudanças)
    cycle_start: str = ""                 # Início do ciclo atual
    reference_speed_kt: float = 0.0       # Velocidade de referência do SCAT (média próxima ao navio)
    reference_direction: float = 0.0      # Direção de referência do SCAT
    weights: Dict[str, ModelWeight] = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        """Converte para dicionário serializável."""
        return {
            "scat_timestamp": self.scat_timestamp,
            "scat_file_hash": self.scat_file_hash,
            "cycle_start": self.cycle_start,
            "reference_speed_kt": self.reference_speed_kt,
            "reference_direction": self.reference_direction,
            "weights": {k: asdict(v) for k, v in self.weights.items()}
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "WeightsState":
        """Reconstrói a partir de dicionário."""
        state = cls()
        state.scat_timestamp = data.get("scat_timestamp", "")
        state.scat_file_hash = data.get("scat_file_hash", "")
        state.cycle_start = data.get("cycle_start", "")
        state.reference_speed_kt = data.get("reference_speed_kt", 0.0)
        state.reference_direction = data.get("reference_direction", 0.0)
        
        for model_name, weight_data in data.get("weights", {}).items():
            state.weights[model_name] = ModelWeight(**weight_data)
        
        return state


# =============================================================================
# FUNÇÕES DE CÁLCULO ANGULAR
# =============================================================================

def normalize_angle(angle: float) -> float:
    """
    Normaliza ângulo para o intervalo [0, 360).
    """
    angle = angle % 360
    if angle < 0:
        angle += 360
    return angle


def angular_difference(angle1: float, angle2: float) -> float:
    """
    Calcula a diferença angular entre dois ângulos em graus.
    Retorna valor entre -180 e +180 (negativo = esquerda, positivo = direita).
    
    Exemplo:
        angular_difference(350, 10) = 20  (10 está 20° à direita de 350)
        angular_difference(10, 350) = -20 (350 está 20° à esquerda de 10)
    """
    angle1 = normalize_angle(angle1)
    angle2 = normalize_angle(angle2)
    
    diff = angle2 - angle1
    
    # Normalizar para [-180, 180]
    if diff > 180:
        diff -= 360
    elif diff < -180:
        diff += 360
    
    return diff


def angular_difference_absolute(angle1: float, angle2: float) -> float:
    """
    Calcula a diferença angular absoluta (sempre positiva).
    Retorna valor entre 0 e 180.
    """
    return abs(angular_difference(angle1, angle2))


# =============================================================================
# FUNÇÕES DE PESO
# =============================================================================

def calculate_direction_weight(scat_dir: float, model_dir: float) -> Tuple[float, float]:
    """
    Calcula o peso baseado na variação de direção.
    
    Args:
        scat_dir: Direção medida pelo scatterometer (graus)
        model_dir: Direção prevista pelo modelo (graus)
    
    Returns:
        Tuple (peso, variação em graus)
        - peso: 1.0 se variação=0°, 0.0 se variação=±180°
        - variação: diferença angular com sinal (-180 a +180)
    """
    variation = angular_difference(scat_dir, model_dir)
    abs_variation = abs(variation)
    
    # Peso proporcional: 1 quando var=0, 0 quando |var|=180
    weight = max(0.0, 1.0 - (abs_variation / MAX_DIRECTION_VARIATION))
    
    return weight, variation


def calculate_speed_weight(scat_speed_kt: float, model_speed_kt: float) -> Tuple[float, float]:
    """
    Calcula o peso baseado na variação de velocidade.
    
    Args:
        scat_speed_kt: Velocidade medida pelo scatterometer (knots)
        model_speed_kt: Velocidade prevista pelo modelo (knots)
    
    Returns:
        Tuple (peso, variação em knots)
        - peso: 1.0 se variação=0kt, 0.0 se variação>=30kt
        - variação: diferença de velocidade com sinal
    """
    variation = model_speed_kt - scat_speed_kt
    abs_variation = abs(variation)
    
    # Peso proporcional: 1 quando var=0, 0 quando |var|>=30
    weight = max(0.0, 1.0 - (abs_variation / MAX_SPEED_VARIATION))
    
    return weight, variation


def calculate_combined_initial_weight(
    scat_dir: float, 
    model_dir: float,
    scat_speed_kt: float, 
    model_speed_kt: float
) -> Tuple[float, float, float, float, float]:
    """
    Calcula o peso inicial combinado (direção + velocidade).
    
    Returns:
        Tuple (peso_combinado, peso_dir, var_dir, peso_vel, var_vel)
    """
    dir_weight, dir_variation = calculate_direction_weight(scat_dir, model_dir)
    speed_weight, speed_variation = calculate_speed_weight(scat_speed_kt, model_speed_kt)
    
    # Média simples dos dois pesos
    combined_weight = (dir_weight + speed_weight) / 2.0
    
    return combined_weight, dir_weight, dir_variation, speed_weight, speed_variation


def update_weight_with_comparison(
    initial_weight: float,
    scat_dir: float,
    model_dir: float,
    scat_speed_kt: float,
    model_speed_kt: float
) -> Tuple[float, float, float]:
    """
    Atualiza o peso usando a fórmula: sqrt(peso_inicial * peso_comparativo)
    
    Returns:
        Tuple (novo_peso, erro_dir, erro_vel)
    """
    dir_weight, dir_error = calculate_direction_weight(scat_dir, model_dir)
    speed_weight, speed_error = calculate_speed_weight(scat_speed_kt, model_speed_kt)
    
    # Peso comparativo
    comparative_weight = (dir_weight + speed_weight) / 2.0
    
    # Novo peso = sqrt(inicial * comparativo)
    # Garantir que o produto não seja negativo
    product = max(0.0, initial_weight * comparative_weight)
    new_weight = math.sqrt(product)
    
    return new_weight, dir_error, speed_error


# =============================================================================
# GERENCIADOR DE PESOS
# =============================================================================

class ModelWeightsManager:
    """
    Gerenciador de pesos dinâmicos para modelos meteorológicos.
    
    Uso:
        manager = ModelWeightsManager()
        
        # Verificar se há novo SCAT e resetar se necessário
        if manager.check_and_reset_if_new_scat():
            print("Novo ciclo iniciado!")
        
        # Atualizar peso de um modelo com nova comparação
        manager.update_model_weight(
            model_name="ecmwf_ifs025",
            model_dir=225.0,
            model_speed_kt=15.0
        )
        
        # Obter pesos normalizados para uso
        weights = manager.get_normalized_weights()
    """
    
    def __init__(
        self,
        weights_file: Path = WEIGHTS_FILE,
        scat_file: Path = SCAT_FILE,
        nav_lat: float = -22.50,
        nav_lon: float = -40.50,
        search_radius_nm: float = 50.0
    ):
        self.weights_file = Path(weights_file)
        self.scat_file = Path(scat_file)
        self.nav_lat = nav_lat
        self.nav_lon = nav_lon
        self.search_radius_deg = search_radius_nm / 60.0  # NM para graus
        
        self.state = self._load_state()
    
    def _load_state(self) -> WeightsState:
        """Carrega estado persistido ou cria novo."""
        if self.weights_file.exists():
            try:
                with open(self.weights_file, 'r') as f:
                    data = json.load(f)
                return WeightsState.from_dict(data)
            except Exception as e:
                print(f"⚠️ Erro carregando pesos: {e}. Criando novo estado.")
        
        return self._create_initial_state()
    
    def _create_initial_state(self) -> WeightsState:
        """Cria estado inicial com pesos iguais."""
        state = WeightsState()
        state.cycle_start = utcnow().isoformat()
        
        for model in WEATHER_MODELS:
            state.weights[model] = ModelWeight(
                model_name=model,
                initial_weight=1.0,
                current_weight=1.0,
                last_updated=utcnow().isoformat()
            )
        
        return state
    
    def _save_state(self):
        """Persiste estado atual."""
        self.weights_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.weights_file, 'w') as f:
            json.dump(self.state.to_dict(), f, indent=2)
    
    def _get_scat_file_hash(self) -> str:
        """Retorna hash do arquivo SCAT para detectar mudanças."""
        if not self.scat_file.exists():
            return ""
        
        # Usar timestamp de modificação + tamanho como "hash" simples
        stat = self.scat_file.stat()
        return f"{stat.st_mtime}_{stat.st_size}"
    
    def _load_scat_data(self) -> Optional[dict]:
        """Carrega dados do scatterometer."""
        if not self.scat_file.exists():
            return None
        
        try:
            with open(self.scat_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️ Erro carregando SCAT: {e}")
            return None
    
    def _extract_reference_wind(self, scat_data: dict) -> Tuple[float, float]:
        """
        Extrai vento de referência do SCAT (média dos pontos próximos ao navio).
        
        Returns:
            Tuple (velocidade_média_kt, direção_média)
        """
        winds = scat_data.get("winds", [])
        
        if not winds:
            return 0.0, 0.0
        
        # Filtrar pontos dentro do raio de busca
        nearby_winds = []
        for w in winds:
            lat = w.get("lat", 0)
            lon = w.get("lon", 0)
            
            # Distância simples (não geodésica, ok para áreas pequenas)
            dist = math.sqrt(
                (lat - self.nav_lat)**2 + 
                (lon - self.nav_lon)**2
            )
            
            if dist <= self.search_radius_deg:
                nearby_winds.append(w)
        
        if not nearby_winds:
            # Se não há pontos próximos, pegar os 10 mais próximos
            winds_with_dist = []
            for w in winds:
                lat = w.get("lat", 0)
                lon = w.get("lon", 0)
                dist = math.sqrt(
                    (lat - self.nav_lat)**2 + 
                    (lon - self.nav_lon)**2
                )
                winds_with_dist.append((dist, w))
            
            winds_with_dist.sort(key=lambda x: x[0])
            nearby_winds = [w for _, w in winds_with_dist[:10]]
        
        if not nearby_winds:
            return 0.0, 0.0
        
        # Calcular médias
        speeds = [w.get("speed_kt", 0) for w in nearby_winds]
        avg_speed = sum(speeds) / len(speeds)
        
        # Média circular para direção
        directions = [w.get("direction", 0) for w in nearby_winds]
        x_sum = sum(math.cos(math.radians(d)) for d in directions)
        y_sum = sum(math.sin(math.radians(d)) for d in directions)
        avg_dir = math.degrees(math.atan2(y_sum, x_sum))
        if avg_dir < 0:
            avg_dir += 360
        
        # IMPORTANTE: Subtrair 180° para calibração correta
        # O scatterometer mede "de onde vem o vento", precisamos ajustar
        avg_dir_calibrated = (avg_dir - 180.0) % 360.0
        
        return avg_speed, avg_dir_calibrated
    
    def check_and_reset_if_new_scat(self) -> bool:
        """
        Verifica se há novo arquivo SCAT e reseta os pesos se necessário.
        
        Returns:
            True se um novo ciclo foi iniciado, False caso contrário.
        """
        current_hash = self._get_scat_file_hash()
        
        if not current_hash:
            return False
        
        if current_hash == self.state.scat_file_hash:
            return False
        
        # Novo arquivo SCAT detectado - iniciar novo ciclo
        print("🔄 Novo arquivo SCAT detectado! Resetando pesos...")
        
        scat_data = self._load_scat_data()
        if not scat_data:
            return False
        
        # Extrair referência
        ref_speed, ref_dir = self._extract_reference_wind(scat_data)
        
        print(f"   📍 Referência SCAT: {ref_speed:.1f} kt @ {ref_dir:.0f}°")
        
        # Criar novo estado
        self.state = WeightsState()
        self.state.scat_timestamp = scat_data.get("timestamp", "")
        self.state.scat_file_hash = current_hash
        self.state.cycle_start = utcnow().isoformat()
        self.state.reference_speed_kt = ref_speed
        self.state.reference_direction = ref_dir
        
        # Inicializar pesos - os pesos iniciais serão calculados na primeira comparação
        for model in WEATHER_MODELS:
            self.state.weights[model] = ModelWeight(
                model_name=model,
                initial_weight=1.0,  # Será ajustado na primeira comparação
                current_weight=1.0,
                last_updated=utcnow().isoformat()
            )
        
        self._save_state()
        return True
    
    def calculate_initial_weights(self, model_forecasts: Dict[str, Tuple[float, float]]):
        """
        Calcula os pesos iniciais de todos os modelos baseado na comparação com o SCAT.
        
        Args:
            model_forecasts: Dict com {model_name: (direção, velocidade_kt)}
                            Previsões dos modelos para o horário do SCAT
        """
        ref_speed = self.state.reference_speed_kt
        ref_dir = self.state.reference_direction
        
        print(f"\n📊 Calculando pesos iniciais (ref: {ref_speed:.1f}kt @ {ref_dir:.0f}°):")
        
        for model_name, (model_dir, model_speed_kt) in model_forecasts.items():
            if model_name not in self.state.weights:
                continue
            
            # Calcular peso inicial
            combined, dir_w, dir_var, spd_w, spd_var = calculate_combined_initial_weight(
                ref_dir, model_dir,
                ref_speed, model_speed_kt
            )
            
            weight = self.state.weights[model_name]
            weight.initial_weight = combined
            weight.current_weight = combined
            weight.direction_error = dir_var
            weight.speed_error = spd_var
            weight.comparison_count = 1
            weight.last_updated = utcnow().isoformat()
            
            print(f"   {model_name}:")
            print(f"      Previsão: {model_speed_kt:.1f}kt @ {model_dir:.0f}°")
            print(f"      Δ Dir: {dir_var:+.1f}° (peso: {dir_w:.3f})")
            print(f"      Δ Vel: {spd_var:+.1f}kt (peso: {spd_w:.3f})")
            print(f"      Peso inicial: {combined:.3f}")
        
        self._save_state()
    
    def update_model_weight(
        self,
        model_name: str,
        model_dir: float,
        model_speed_kt: float,
        actual_dir: Optional[float] = None,
        actual_speed_kt: Optional[float] = None
    ):
        """
        Atualiza o peso de um modelo com nova comparação.
        
        Se actual_dir/speed não forem fornecidos, usa referência do SCAT.
        
        A fórmula é: peso_novo = sqrt(peso_inicial_ciclo * peso_comparativo)
        """
        if model_name not in self.state.weights:
            return
        
        # Usar valores atuais ou referência do SCAT
        ref_dir = actual_dir if actual_dir is not None else self.state.reference_direction
        ref_speed = actual_speed_kt if actual_speed_kt is not None else self.state.reference_speed_kt
        
        weight = self.state.weights[model_name]
        
        # Calcular novo peso
        new_weight, dir_error, speed_error = update_weight_with_comparison(
            weight.initial_weight,
            ref_dir, model_dir,
            ref_speed, model_speed_kt
        )
        
        # Atualizar estado
        weight.current_weight = new_weight
        weight.direction_error = (weight.direction_error * weight.comparison_count + dir_error) / (weight.comparison_count + 1)
        weight.speed_error = (weight.speed_error * weight.comparison_count + speed_error) / (weight.comparison_count + 1)
        weight.comparison_count += 1
        weight.last_updated = utcnow().isoformat()
        
        self._save_state()
    
    def get_normalized_weights(self) -> Dict[str, float]:
        """
        Retorna pesos normalizados (somam 1.0) para uso em cálculos.
        """
        weights = {}
        total = 0.0
        
        for model_name, weight in self.state.weights.items():
            w = max(0.001, weight.current_weight)  # Peso mínimo para evitar zero
            weights[model_name] = w
            total += w
        
        # Normalizar
        if total > 0:
            for model_name in weights:
                weights[model_name] /= total
        
        return weights
    
    def get_weight(self, model_name: str) -> float:
        """Retorna peso atual de um modelo (não normalizado)."""
        if model_name in self.state.weights:
            return self.state.weights[model_name].current_weight
        return 1.0
    
    def get_status(self) -> dict:
        """Retorna status completo para exibição."""
        normalized = self.get_normalized_weights()
        
        return {
            "scat_timestamp": self.state.scat_timestamp,
            "cycle_start": self.state.cycle_start,
            "reference": {
                "speed_kt": self.state.reference_speed_kt,
                "direction": self.state.reference_direction
            },
            "models": {
                model_name: {
                    "initial_weight": weight.initial_weight,
                    "current_weight": weight.current_weight,
                    "normalized_weight": normalized.get(model_name, 0),
                    "avg_dir_error": weight.direction_error,
                    "avg_speed_error": weight.speed_error,
                    "comparisons": weight.comparison_count
                }
                for model_name, weight in self.state.weights.items()
            }
        }
    
    def print_status(self):
        """Imprime status formatado."""
        status = self.get_status()
        
        print("\n" + "=" * 70)
        print("📊 MODEL WEIGHTS STATUS")
        print("=" * 70)
        print(f"SCAT Timestamp: {status['scat_timestamp']}")
        print(f"Ciclo iniciado: {status['cycle_start']}")
        print(f"Referência: {status['reference']['speed_kt']:.1f} kt @ {status['reference']['direction']:.0f}°")
        print("-" * 70)
        print(f"{'Modelo':<25} {'Inicial':>8} {'Atual':>8} {'Normal%':>8} {'ΔDir':>8} {'ΔVel':>8} {'N':>4}")
        print("-" * 70)
        
        for model_name, data in status['models'].items():
            print(f"{model_name:<25} {data['initial_weight']:>8.3f} {data['current_weight']:>8.3f} "
                  f"{data['normalized_weight']*100:>7.1f}% {data['avg_dir_error']:>+7.1f}° "
                  f"{data['avg_speed_error']:>+7.1f}kt {data['comparisons']:>4}")
        
        print("=" * 70)


# =============================================================================
# INTEGRAÇÃO COM APP.PY
# =============================================================================

def get_weighted_average(values: Dict[str, float], manager: ModelWeightsManager) -> float:
    """
    Calcula média ponderada usando os pesos do manager.
    
    Args:
        values: Dict com {model_name: valor}
        manager: Instância do ModelWeightsManager
    
    Returns:
        Média ponderada
    """
    weights = manager.get_normalized_weights()
    
    total = 0.0
    weight_sum = 0.0
    
    for model_name, value in values.items():
        if value is not None and model_name in weights:
            w = weights[model_name]
            total += value * w
            weight_sum += w
    
    return total / weight_sum if weight_sum > 0 else 0.0


def get_weighted_circular_average(directions: Dict[str, float], manager: ModelWeightsManager) -> float:
    """
    Calcula média circular ponderada para direções.
    
    Args:
        directions: Dict com {model_name: direção_em_graus}
        manager: Instância do ModelWeightsManager
    
    Returns:
        Direção média ponderada (0-360)
    """
    weights = manager.get_normalized_weights()
    
    x_sum = 0.0
    y_sum = 0.0
    
    for model_name, direction in directions.items():
        if direction is not None and model_name in weights:
            w = weights[model_name]
            x_sum += w * math.cos(math.radians(direction))
            y_sum += w * math.sin(math.radians(direction))
    
    avg_rad = math.atan2(y_sum, x_sum)
    avg_deg = math.degrees(avg_rad)
    
    if avg_deg < 0:
        avg_deg += 360
    
    return avg_deg


# =============================================================================
# EXEMPLO DE USO / TESTE
# =============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("TESTE DO SISTEMA DE PESOS DINÂMICOS")
    print("=" * 70)
    
    # Criar manager
    manager = ModelWeightsManager()
    
    # Simular verificação de novo SCAT
    if manager.check_and_reset_if_new_scat():
        print("\n✅ Novo ciclo iniciado!")
        
        # Simular previsões dos modelos para o horário do SCAT
        # (direção, velocidade_kt)
        model_forecasts = {
            "ecmwf_ifs025": (280.0, 14.0),
            "icon_seamless": (275.0, 15.5),
            "gfs_seamless": (290.0, 13.0),
            "meteofrance_seamless": (285.0, 14.5),
            "jma_seamless": (270.0, 16.0),
        }
        
        manager.calculate_initial_weights(model_forecasts)
    
    # Mostrar status
    manager.print_status()
    
    # Simular uma atualização
    print("\n🔄 Simulando atualização com nova comparação...")
    manager.update_model_weight(
        model_name="ecmwf_ifs025",
        model_dir=278.0,
        model_speed_kt=13.5
    )
    
    manager.print_status()
    
    # Exemplo de uso para médias ponderadas
    print("\n📈 Exemplo de médias ponderadas:")
    
    speeds = {
        "ecmwf_ifs025": 14.2,
        "icon_seamless": 15.1,
        "gfs_seamless": 13.8,
        "meteofrance_seamless": 14.6,
        "jma_seamless": 15.8,
    }
    
    avg_speed = get_weighted_average(speeds, manager)
    print(f"   Velocidade média ponderada: {avg_speed:.1f} kt")
    
    directions = {
        "ecmwf_ifs025": 278.0,
        "icon_seamless": 282.0,
        "gfs_seamless": 275.0,
        "meteofrance_seamless": 280.0,
        "jma_seamless": 270.0,
    }
    
    avg_dir = get_weighted_circular_average(directions, manager)
    print(f"   Direção média ponderada: {avg_dir:.0f}°")
