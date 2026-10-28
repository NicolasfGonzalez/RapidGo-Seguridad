import pytest

# Umbrales operativos definidos para la variante de Seguridad del Repartidor
SPEED_THRESHOLD_KMH = 45.0
BATTERY_THRESHOLD_PCT = 15

def evaluate_courier_risk(speed: float, battery_level: int) -> str:
    """Evalúa la telemetría del repartidor de forma aislada sin dependencias de Kafka."""
    if battery_level < 0 or battery_level > 100:
        raise ValueError("El nivel de batería debe estar en el rango de 0% a 100%")
    if speed < 0:
        raise ValueError("La velocidad no puede ser un valor negativo")
        
    if battery_level < BATTERY_THRESHOLD_PCT:
        return "CRITICAL_BATTERY"
    if speed > SPEED_THRESHOLD_KMH:
        return "SPEEDING_WARNING"
        
    return "NORMAL"

def test_normal_telemetry():
    """Caso normal: Repartidor dentro de parámetros operativos seguros."""
    assert evaluate_courier_risk(30.0, 80) == "NORMAL"

def test_speeding_warning_alert():
    """Caso límite/alerta: Repartidor supera el umbral máximo de velocidad (45 km/h)."""
    assert evaluate_courier_risk(60.0, 50) == "SPEEDING_WARNING"

def test_critical_battery_alert():
    """Caso límite/alerta: Repartidor por debajo del umbral crítico de batería (15%)."""
    assert evaluate_courier_risk(20.0, 10) == "CRITICAL_BATTERY"

def test_invalid_telemetry_raises_exception():
    """Caso inválido: Comprueba la excepción ante valores anómalos o corruptos."""
    with pytest.raises(ValueError):
        evaluate_courier_risk(-5.0, 50)
    with pytest.raises(ValueError):
        evaluate_courier_risk(30.0, 150)