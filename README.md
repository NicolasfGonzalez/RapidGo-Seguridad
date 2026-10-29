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
- **Automatización con GitHub Actions:** Se configuró el archivo `CI.yml` que se dispara automáticamente ante *pushes* o *Pull Requests* hacia las ramas `main` y `QA`.
- **Pruebas Unitarias (Pytest):** La lógica de validación de las alertas operacionales fue separada en el archivo `test_security_rules.py`, permitiendo verificar límites máximos de velocidad, umbrales de batería, y rechazo de data corrupta. El pipeline CI ejecuta estas pruebas antes de aprobar los cambios.

---

## Estructura del Repositorio Actualizada
```text
rapidgo-variante2-seguridad/
├── .github/workflows/
│   └── CI.yml                    # Pipeline de Integración Continua (Acciones de validación)
├── airflow/dags/                 # DAGs de Orquestación (Por implementar)
├── datalake/                     # Persistencia Parquet
│   ├── checkpoints/              # Estado y offsets de Spark Streaming
│   └── processed/                # Data validada y persistida en Parquet
├── docs/                         # Documentación de arquitectura y reportes
│   ├── arquitectura.png          # Diagrama del pipeline
│   ├── checkpoints/
│   │   ├── semana1.md            # Reporte Checkpoint Semana 1
│   │   └── semana2.md            # Reporte Checkpoint Semana 2
│   └── decisions/
│       ├── ADR-001-partitioning-kafka.md # Decisión sobre Particiones y Keys
│       └── ADR-002-columnar-storage-and-fault-tolerance-in-streaming.md # Decisión sobre Parquet y Checkpointing
├── producer/
│   └── courier_producer.py       # Generador de telemetría hacia Kafka
├── samples/
│   └── evento_sample.json        # Contrato de datos de prueba
├── spark/
│   └── streaming_consumer.py     # Lógica Spark Streaming, Watermarking y Sink a Parquet
├── tests/
│   └── test_security_rules.py    # Pruebas automatizadas (Pytest) de KPIs
├── docker-compose.yml            # Entorno local comprimido (Kafka, Spark, etc)
├── README.md                     # Documento principal
└── requirements.txt              # Librerías de Python (pytest, kafka-python, pyspark)
```