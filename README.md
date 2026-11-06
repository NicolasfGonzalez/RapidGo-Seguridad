# RapidGo - Variante 2: Seguridad del Repartidor
## Integrantes del equipo:
- Vivian Karith Ávila Ávila
- Laura Valentina Ortiz Bustos
- Nicolás Felipe González Bello
## Curso: Línea de Profundización I - Big Data.
## Institución: Universidad de Cundinamarca.

# Semana 1
## 1. Contexto y Planteamiento del Problema
La variante de Seguridad del Repartidor (Variante 2) procesa en tiempo real los eventos de telemetría emitidos por los dispositivos móviles de la flota. Para evaluar correctamente los KPIs (velocidad promedio, nivel de batería, desvíos de ruta e inactividad) y disparar alertas de seguridad en Spark Structured Streaming.

## 2. Definición del Problema
RapidGo requiere monitorear la operación en tiempo real para mitigar riesgos asociados a la seguridad de los repartidores durante sus entregas. Esta variante captura la telemetría enviada por los dispositivos móviles de la flota para procesar eventos de ubicación, velocidad, estado de la batería y cumplimiento de rutas

### Objetivos del Sistema
- Prevenir accidentes detectando excesos de velocidad continuos. 
- Evitar la pérdida de trazabilidad rastreando el nivel de batería crítica y la señal GPS del dispositivo.
- Identificar desvíos e inactividad no justificada durante el cumplimiento de pedidos.

## 3. KPIs y Reglas de Alerta
### KPIs Métricos: 
1. **Velocidad Promedio (`speed_kmh`):** Medida continua en ventanas deslizantes de 5 minutos agrupadas por repartidor
2. **Porcentaje de Batería (`battery_percent`):** Carga restante del smartphone del conductor (0-100%).
3. **Desvío de Ruta (`route_deviation_m`):** Métrica cuantitativa en metros respecto al camino sugerido.
4. **Inactividad en Ruta (`idle_minutes`):** Minutos que el repartidor permanece detenido estando en estado `"in_rute"`.

### Reglas de Alerta
- **Exceso de Velocidad:** Se activa cuando `speed_kmh > 45.0` durante la entrega, este valor se justifica ya que las vías y el trafico propio del municipio de Facatativá, no se presta para una velocidad mayor priorizando la seguridad vial.
- **Batería Crítica / Pérdida de Señal:** Se activa si `battery_percent < 15%` **O** si `gps_signal_active == false`, esto con el fin de poder cuidar la integridad del repartidor y el pedido en todo momento.
- **Repartidor Desviado:** Se activa cuando `route_deviation_m > 500`, este valor para dejar un rango de flexibilidad para que el repartidor tome "atajos".

---

# Semana 2
## 1. Ingesta y Procesamiento en Tiempo Real
Durante la segunda iteración se materializó el flujo de datos (End-to-End) desde la generación de eventos hasta su almacenamiento:
- **Productor Kafka:** Se implementó un script en Python (`courier_producer.py`) que simula la telemetría de 3 repartidores, enviando datos al tópico `driver_security_events` usando `courier_id` como *Partition Key*.
- **Consumidor Spark Streaming:** Se desarrolló el motor de procesamiento continuo (`streaming_consumer.py`). Lee de Kafka, aplica las reglas operativas de batería (<15%) y velocidad (>45 km/h), calcula agregaciones por ventanas de tiempo (5 minutos) implementando *Watermarks* de 10 minutos para gestionar retrasos de red, y escribe el resultado.
- **Persistencia en Data Lake:** Con base en el **ADR-002**, se configuró Spark para persistir la data filtrada en formato columnar **Parquet** dentro de la capa `Processed` del Data Lake local, habilitando *Checkpointing* para tolerancia a fallos.

## 2. Calidad de Código e Integración Continua (CI)
Para garantizar la estabilidad del pipeline, se implementaron prácticas de DevOps:
- **Automatización con GitHub Actions:** Se configuró el archivo `ci.yml` que se dispara automáticamente ante *pushes* o *Pull Requests* hacia las ramas `main` y `QA`.
- **Pruebas Unitarias (Pytest):** La lógica de validación de las alertas operacionales fue separada en el archivo `test_security_rules.py`, permitiendo verificar límites máximos de velocidad, umbrales de batería, y rechazo de data corrupta. El pipeline CI ejecuta estas pruebas antes de aprobar los cambios.

---

# Semana 3
## 1. Orquestación con Apache Airflow
Se implementó el DAG `dag_seguridad_repartidores` (`airflow/dags/rapidgo_security_orchestration.py`), que se ejecuta cada hora (`@hourly`) y controla la calidad de los datos que Spark deja en el Data Lake antes de calcular los KPIs y generar el reporte de alertas.

| # | Task | Función |
|---|------|---------|
| 1 | `validar_fuente` | Verifica que exista la partición del día y que tenga archivos Parquet. |
| 2 | `validar_esquema` | Verifica que cada Parquet cumpla el contrato (columnas y tipos). |
| 3 | `verificar_frescura` | Verifica que el último Parquet no sea más antiguo que `FRESHNESS_MAX_MIN` (3 min por defecto). Devuelve `corte_mtime` por **XCom**. |
| 4 | `kpi_velocidad`, `kpi_bateria`, `kpi_desvios` | Se ejecutan en paralelo sobre la misma "foto" de datos (`mtime <= corte_mtime`). |
| 5 | `verificar_calidad_datos` | Valida nulos, rangos plausibles y coherencia entre las tres ramas de KPIs. |
| 6 | `generar_reporte_alertas` | Genera el reporte (`reporte_*.txt`) y los KPIs (`kpis_*.json`) en la capa `Curated`. |

**Flujo:** `validar_fuente → validar_esquema → verificar_frescura → [kpi_velocidad, kpi_bateria, kpi_desvios] → verificar_calidad_datos → generar_reporte_alertas`

## 2. Resiliencia y Fallo Controlado
Según el **ADR-003**, los fallos se clasifican en:
- **Transitorios** (`validar_fuente`, `verificar_frescura`): se reintentan (`verificar_frescura` con 3 reintentos cada 2 minutos).
- **Lógicos** (`validar_esquema`, `verificar_calidad_datos`): no se reintentan, porque repetir la tarea no corrige un contrato roto ni un dato inválido.
- **Registro de incidentes:** los callbacks de fallo y reintento escriben en `datalake/curated/incidents/incidentes_<fecha>.jsonl`.

**Fallo controlado demostrado:** se detuvo el contenedor `spark-consumer` mientras el producer seguía enviando eventos. `verificar_frescura` falló con el mensaje *"el ultimo Parquet se escribio hace 23.1 min (maximo permitido: 3.0 min)"*, las 5 tareas siguientes no se ejecutaron y, tras reiniciar el consumidor y hacer *Clear task*, el DAG se recuperó completo (intento 5 en `success`). La evidencia está en [`docs/checkpoints/week3.md`](docs/checkpoints/week3.md).

## 3. Integración Continua Ampliada
El workflow `ci.yml` ahora, además de validar sintaxis, ejecutar `pytest` y validar `docker compose config`, **importa el DAG con `DagBag`** y falla si hay errores de importación. Se fijó Python 3.11 para ser compatible con Airflow 2.8.1, y se agregó la prueba `test_priority_critical_battery_over_speeding` (la batería crítica tiene prioridad sobre el exceso de velocidad).

## 4. Cómo Ejecutar el Entorno Local
```bash
# 1. Levantar Kafka, Spark, PostgreSQL y Airflow
docker compose up -d

# 2. Iniciar el generador de eventos
python producer/courier_producer.py

# 3. Abrir Airflow en http://localhost:8080 (usuario: admin / contraseña: admin, solo entorno local)
#    y ejecutar el DAG dag_seguridad_repartidores

# 4. Ejecutar las pruebas
pytest -v
```

## 5. Documentación de la Semana
- Checkpoint: [`docs/checkpoints/week3.md`](docs/checkpoints/week3.md)
- Decisión de arquitectura: [`ADR-003`](docs/decisions/ADR-003-orchestration-and-failure-recovery.md)

---

## Estructura del Repositorio Actualizada
```text
rapidgo-variante2-seguridad/
├── .github/workflows/
│   └── ci.yml                    # CI: sintaxis, pytest, docker compose e importación del DAG
├── airflow/
│   ├── dags/
│   │   └── rapidgo_security_orchestration.py  # DAG dag_seguridad_repartidores (8 tasks)
│   └── logs/                     # Logs de Airflow (volumen compartido entre contenedores)
├── datalake/                     # Persistencia Parquet
│   ├── checkpoints/              # Estado y offsets de Spark Streaming
│   ├── processed/                # Data validada y persistida en Parquet
│   └── curated/                  # Reportes, KPIs e incidentes generados por Airflow
├── docs/                         # Documentación de arquitectura y reportes
│   ├── architecture.png          # Diagrama del pipeline
│   ├── checkpoints/
│   │   ├── Semana1.md            # Reporte Checkpoint Semana 1
│   │   ├── Semana2.md            # Reporte Checkpoint Semana 2
│   │   ├── week3.md              # Reporte Checkpoint Semana 3
│   │   └── img/week3/            # Evidencias de ejecución (capturas)
│   └── decisions/
│       ├── ADR-001-partitioning-kafka.md
│       ├── ADR-002-Columnar Storage and Fault Tolerance in Streaming.md
│       └── ADR-003-orchestration-and-failure-recovery.md
├── producer/
│   └── courier_producer.py       # Generador de telemetría hacia Kafka
├── samples/
│   └── event_example.json        # Contrato de datos de prueba
├── spark/
│   └── streaming_consumer.py     # Lógica Spark Streaming, Watermarking y Sink a Parquet
├── tests/
│   └── test_security_rules.py    # Pruebas automatizadas (Pytest) de reglas de seguridad
├── docker-compose.yml            # Kafka, Spark, PostgreSQL y Airflow (webserver, scheduler, init)
├── README.md                     # Documento principal
└── requirements.txt              # Librerías de Python
```