import glob
import json
import os
import time
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Configuracion (rutas DENTRO del contenedor: volumen ./datalake:/opt/airflow/datalake)
# ---------------------------------------------------------------------------
DATALAKE_PATH = os.environ.get(
    "DATALAKE_PATH", "/opt/airflow/datalake/processed/security_telemetry"
)
CURATED_PATH = os.environ.get(
    "CURATED_PATH", "/opt/airflow/datalake/curated/security_reports"
)
INCIDENTES_PATH = os.environ.get(
    "INCIDENTES_PATH", "/opt/airflow/datalake/curated/incidents"
)


SPEED_LIMIT_KMH = 45.0
BATTERY_CRITICAL_PCT = 15
FRESHNESS_MAX_MIN = float(os.environ.get("FRESHNESS_MAX_MIN", "3"))
MAX_SPEED_PLAUSIBLE_KMH = 200          # por encima, el dato se considera invalido
ZONA_CENTRO = (4.6097, -74.0817)       # centro de la zona operativa (lat, lon)
ZONA_RADIO_KM = 1.2                    # fuera de este radio = desvio

# Contrato del Parquet de Processed
ESQUEMA_ESPERADO = {
    "courier_id": "is_string",
    "latitude": "is_floating",
    "longitude": "is_floating",
    "speed": "is_floating",
    "battery_level": "is_integer",
    "status": "is_string",
    "timestamp": "is_string",
    "event_time": "is_timestamp",
    "speed_alert": "is_boolean",
    "battery_alert": "is_boolean",
}
COLUMNAS_CLAVE = [
    "courier_id", "latitude", "longitude", "speed", "battery_level", "event_time",
]

TIPO_FALLO = {
    "validar_fuente": "transitorio: la fuente aun no tiene datos",
    "validar_esquema": "logico: contrato del evento roto",
    "verificar_frescura": "transitorio: pipeline detenido (producer o Spark)",
    "verificar_calidad_datos": "logico: dato invalido en el Data Lake",
}


# ---------------------------------------------------------------------------
# Registro automatico de incidentes (callbacks)
# ---------------------------------------------------------------------------
def _registrar_incidente(context, estado):
    try:
        ti = context["task_instance"]
        incidente = {
            "registrado_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "dag_id": ti.dag_id,
            "run_id": context["run_id"],
            "task_id": ti.task_id,
            "estado": estado,
            "tipo_fallo": TIPO_FALLO.get(ti.task_id, "no clasificado"),
            "error": str(context.get("exception"))[:1000],
        }
        os.makedirs(INCIDENTES_PATH, exist_ok=True)
        ruta = os.path.join(INCIDENTES_PATH, f"incidentes_{context['ds']}.jsonl")
        with open(ruta, "a", encoding="utf-8") as f:
            f.write(json.dumps(incidente, ensure_ascii=False) + "\n")
        print(f"Incidente registrado en {ruta}")
    except Exception as e:  # un callback nunca debe tumbar la tarea
        print(f"No se pudo registrar el incidente: {e}")


def _incidente_fallo(context):
    _registrar_incidente(context, "fallida")


def _incidente_reintento(context):
    _registrar_incidente(context, "reintentando")


default_args = {
    "owner": "rapidgo_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=5),
    "on_failure_callback": _incidente_fallo,
    "on_retry_callback": _incidente_reintento,
}


# ---------------------------------------------------------------------------
# Utilidades de lectura
# ---------------------------------------------------------------------------
def _archivos(ds, corte=None):
    archivos = glob.glob(os.path.join(DATALAKE_PATH, f"event_date={ds}", "*.parquet"))
    if corte is not None:
        archivos = [a for a in archivos if os.path.getmtime(a) <= corte]
    return sorted(archivos)


def _leer_snapshot(ds, corte, columnas):
    import pyarrow.parquet as pq

    archivos = _archivos(ds, corte)
    if not archivos:
        raise AirflowException(f"No hay Parquet en event_date={ds} hasta el corte {corte}")
    return pq.read_table(archivos, columns=columnas).to_pandas()


def _corte(ti):
    return ti.xcom_pull(task_ids="verificar_frescura")["corte_mtime"]


def _distancia_km(lat, lon, lat0, lon0):
    """Distancia haversine (km) de cada punto al centro de la zona."""
    import numpy as np

    p1, p2 = np.radians(lat), np.radians(lat0)
    dphi = p2 - p1
    dlmb = np.radians(lon0 - lon)
    a = np.sin(dphi / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dlmb / 2) ** 2
    return 2 * 6371.0088 * np.arcsin(np.sqrt(a))


def _filas_invalidas(df):
    """Mascara de filas que violan las reglas de calidad."""
    return (
        df[COLUMNAS_CLAVE].isnull().any(axis=1)
        | (df["speed"] < 0) | (df["speed"] > MAX_SPEED_PLAUSIBLE_KMH)
        | (df["battery_level"] < 0) | (df["battery_level"] > 100)
        | (df["latitude"].abs() > 90) | (df["longitude"].abs() > 180)
    )


# ---------------------------------------------------------------------------
# Tareas
# ---------------------------------------------------------------------------
def validar_fuente(ds, **kwargs):
    """Tarea 1: existe la particion del dia y tiene archivos Parquet."""
    particion = os.path.join(DATALAKE_PATH, f"event_date={ds}")
    print(f"Inspeccionando Data Lake: {particion}")
    if not os.path.isdir(particion):
        raise AirflowException(f"La particion {particion} no existe todavia.")
    archivos = _archivos(ds)
    if not archivos:
        raise AirflowException(f"La particion {particion} no contiene archivos Parquet.")
    print(f"Archivos Parquet en la particion: {len(archivos)}")
    return {"particion": particion, "archivos": len(archivos)}


def validar_esquema(ds, **kwargs):
    """Tarea 2: cada Parquet cumple el contrato (columnas y tipos). Fallo logico."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    errores = []
    archivos = _archivos(ds)
    for ruta in archivos:
        nombre = os.path.basename(ruta)
        try:
            esquema = pq.read_schema(ruta)
        except Exception as e:
            errores.append(f"{nombre}: ilegible ({type(e).__name__})")
            continue
        for columna, predicado in ESQUEMA_ESPERADO.items():
            if columna not in esquema.names:
                errores.append(f"{nombre}: falta la columna '{columna}'")
            elif not getattr(pa.types, predicado)(esquema.field(columna).type):
                errores.append(
                    f"{nombre}: '{columna}' tiene tipo {esquema.field(columna).type}"
                )
    if errores:
        # AirflowFailException = falla SIN reintentos: reintentar no repara un esquema roto
        raise AirflowFailException(
            f"Esquema invalido en {len(errores)} caso(s). Primeros: " + "; ".join(errores[:5])
        )
    print(f"Esquema valido en {len(archivos)} archivos.")


def verificar_frescura(ds, **kwargs):
    """Tarea 3: el pipeline sigue vivo (hay Parquet recientes). Fallo transitorio."""
    todos = glob.glob(os.path.join(DATALAKE_PATH, "event_date=*", "*.parquet"))
    if not todos:
        raise AirflowException("No hay ningun Parquet en Processed.")

    ultimo = max(os.path.getmtime(a) for a in todos)
    antiguedad = (time.time() - ultimo) / 60
    print(f"Ultimo Parquet escrito hace {antiguedad:.1f} min (maximo {FRESHNESS_MAX_MIN} min)")
    if antiguedad > FRESHNESS_MAX_MIN:
        raise AirflowException(
            f"Pipeline sin datos nuevos: el ultimo Parquet se escribio hace "
            f"{antiguedad:.1f} min (maximo permitido: {FRESHNESS_MAX_MIN} min). "
            f"Verifica que el producer y spark-consumer esten en ejecucion."
        )
    # Este corte define la foto de datos que analizan las tareas siguientes
    return {"corte_mtime": ultimo, "archivos": len(todos), "antiguedad_min": round(antiguedad, 1)}


def kpi_velocidad(ds, ti, **kwargs):
    """Tarea 4a (paralela): KPIs de velocidad."""
    df = _leer_snapshot(ds, _corte(ti), ["courier_id", "speed", "speed_alert"])
    por_repartidor = df.groupby("courier_id")["speed"].mean().round(2)
    kpi = {
        "eventos": int(len(df)),
        "repartidores": int(df["courier_id"].nunique()),
        "velocidad_promedio": round(float(df["speed"].mean()), 2),
        "velocidad_maxima": round(float(df["speed"].max()), 2),
        "alertas_velocidad": int(df["speed_alert"].fillna(False).astype(bool).sum()),
        "velocidad_promedio_por_repartidor": {k: float(v) for k, v in por_repartidor.items()},
    }
    print(f"KPIs de velocidad: {kpi}")
    return kpi


def kpi_bateria(ds, ti, **kwargs):
    """Tarea 4b (paralela): KPIs de bateria."""
    df = _leer_snapshot(ds, _corte(ti), ["courier_id", "battery_level", "battery_alert"])
    criticos = df[df["battery_alert"].fillna(False).astype(bool)]
    kpi = {
        "eventos": int(len(df)),
        "eventos_bateria_critica": int(len(criticos)),
        "repartidores_con_bateria_critica": sorted(criticos["courier_id"].unique().tolist()),
        "bateria_minima": int(df["battery_level"].min()),
        "bateria_promedio": round(float(df["battery_level"].mean()), 1),
    }
    print(f"KPIs de bateria: {kpi}")
    return kpi


def kpi_desvios(ds, ti, **kwargs):
    """Tarea 4c (paralela): desvios = eventos fuera de la zona operativa (geocerca)."""
    df = _leer_snapshot(ds, _corte(ti), ["courier_id", "latitude", "longitude"])
    distancia = _distancia_km(df["latitude"], df["longitude"], *ZONA_CENTRO)
    fuera = df[distancia > ZONA_RADIO_KM]
    kpi = {
        "eventos": int(len(df)),
        "desvios": int(len(fuera)),
        "repartidores_con_desvio": sorted(fuera["courier_id"].unique().tolist()),
        "distancia_maxima_km": round(float(distancia.max()), 2),
    }
    print(f"KPIs de desvios: {kpi}")
    return kpi


def verificar_calidad_datos(ds, ti, **kwargs):
    """Tarea 5: calidad de los datos reales y coherencia de los KPIs. Fallo logico."""
    import pandas as pd

    vel = ti.xcom_pull(task_ids="kpi_velocidad")
    bat = ti.xcom_pull(task_ids="kpi_bateria")
    des = ti.xcom_pull(task_ids="kpi_desvios")
    if not (vel and bat and des):
        raise AirflowFailException("Faltan resultados de las tareas de KPI.")

    corte = _corte(ti)
    df = _leer_snapshot(ds, corte, COLUMNAS_CLAVE)
    problemas = []

    # 1) Eventos validos
    n_invalidos = int(_filas_invalidas(df).sum())
    if n_invalidos:
        import pyarrow.parquet as pq

        culpables = [
            os.path.basename(r)
            for r in _archivos(ds, corte)
            if _filas_invalidas(pq.read_table(r, columns=COLUMNAS_CLAVE).to_pandas()).any()
        ]
        problemas.append(
            f"{n_invalidos} de {len(df)} eventos invalidos (nulos o fuera de rango). "
            f"Archivos con problemas: {culpables[:5]}"
        )

    # 2) Coherencia: las tres ramas analizaron exactamente la misma foto
    if not (vel["eventos"] == bat["eventos"] == des["eventos"] == len(df)):
        problemas.append(
            f"KPIs inconsistentes: velocidad={vel['eventos']}, bateria={bat['eventos']}, "
            f"desvios={des['eventos']}, lectura directa={len(df)}"
        )

    # 3) Las alertas no pueden superar el numero de eventos
    if vel["alertas_velocidad"] > vel["eventos"] or bat["eventos_bateria_critica"] > bat["eventos"]:
        problemas.append("Mas alertas que eventos")

    if problemas:
        raise AirflowFailException("Calidad fallida: " + " | ".join(problemas))
    print(f"Calidad OK: {len(df)} eventos validos y KPIs coherentes.")
    return {"eventos_validados": int(len(df))}


def generar_reporte_alertas(ds, ts_nodash, ti, **kwargs):
    """Tarea 6: reporte final con datos reales, persistido en la capa Curated."""
    fres = ti.xcom_pull(task_ids="verificar_frescura")
    vel = ti.xcom_pull(task_ids="kpi_velocidad")
    bat = ti.xcom_pull(task_ids="kpi_bateria")
    des = ti.xcom_pull(task_ids="kpi_desvios")
    cal = ti.xcom_pull(task_ids="verificar_calidad_datos")

    os.makedirs(CURATED_PATH, exist_ok=True)
    generado = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    datos_hasta = datetime.fromtimestamp(fres["corte_mtime"], timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    con_bateria = ", ".join(bat["repartidores_con_bateria_critica"]) or "ninguno"
    con_desvio = ", ".join(des["repartidores_con_desvio"]) or "ninguno"

    reporte = (
        "=== REPORTE DE SEGURIDAD DEL REPARTIDOR ===\n"
        f"Particion analizada: event_date={ds} (acumulado del dia, UTC)\n"
        f"Datos hasta: {datos_hasta} UTC | Generado: {generado} UTC\n"
        f"Eventos validados: {cal['eventos_validados']} | Repartidores: {vel['repartidores']}\n"
        f"Velocidad promedio / maxima: {vel['velocidad_promedio']} / {vel['velocidad_maxima']} km/h\n"
        f"Excesos de velocidad (>{SPEED_LIMIT_KMH} km/h): {vel['alertas_velocidad']}\n"
        f"Eventos de bateria critica (<{BATTERY_CRITICAL_PCT}%): {bat['eventos_bateria_critica']} "
        f"(repartidores: {con_bateria})\n"
        f"Desvios (>{ZONA_RADIO_KM} km del centro de zona): {des['desvios']} "
        f"(repartidores: {con_desvio}; maxima distancia {des['distancia_maxima_km']} km)\n"
        "Calidad de datos: aprobada\n"
    )

    ruta_txt = os.path.join(CURATED_PATH, f"reporte_{ts_nodash}.txt")
    ruta_json = os.path.join(CURATED_PATH, f"kpis_{ts_nodash}.json")
    with open(ruta_txt, "w", encoding="utf-8") as f:
        f.write(reporte)
    with open(ruta_json, "w", encoding="utf-8") as f:
        json.dump(
            {"ds": ds, "generado_utc": generado, "datos_hasta_utc": datos_hasta,
             "frescura": fres, "velocidad": vel, "bateria": bat, "desvios": des},
            f, indent=2, ensure_ascii=False,
        )
    print(reporte)
    print(f"Reporte guardado en {ruta_txt}")
    return ruta_txt


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
with DAG(
    "dag_seguridad_repartidores",
    default_args=default_args,
    description="Orquestacion batch y calidad para Seguridad del Repartidor - RapidGo",
    doc_md=__doc__,
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["rapidgo", "variante2", "seguridad", "semana3"],
) as dag:

    t_fuente = PythonOperator(task_id="validar_fuente", python_callable=validar_fuente)

    t_esquema = PythonOperator(task_id="validar_esquema", python_callable=validar_esquema)

    # Transitorio: damos 3 reintentos espaciados 2 min (ventana de ~6 min para recuperar)
    t_frescura = PythonOperator(
        task_id="verificar_frescura",
        python_callable=verificar_frescura,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    t_vel = PythonOperator(task_id="kpi_velocidad", python_callable=kpi_velocidad)
    t_bat = PythonOperator(task_id="kpi_bateria", python_callable=kpi_bateria)
    t_des = PythonOperator(task_id="kpi_desvios", python_callable=kpi_desvios)

    t_calidad = PythonOperator(
        task_id="verificar_calidad_datos", python_callable=verificar_calidad_datos
    )

    t_reporte = PythonOperator(
        task_id="generar_reporte_alertas", python_callable=generar_reporte_alertas
    )

    t_fuente >> t_esquema >> t_frescura >> [t_vel, t_bat, t_des] >> t_calidad >> t_reporte