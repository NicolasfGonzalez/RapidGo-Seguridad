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
* **Exceso de Velocidad:** Se activa cuando `speed_kmh > 45.0` durante la entrega, estae valor se justifica ya que las vías y el trafico propio del municipio de Facatativá, no se presta para una velocidad mayor priorizando la seguridad vial.
* **Batería Crítica / Pérdida de Señal:** Se activa si `battery_percent < 15%` **O** si `gps_signal_active == false`, esto con el fin de poder cuidar la integridad del repartidor y el pedido en todo momento.
* **Repartidor Desviado:** Se activa cuando `route_deviation_m > 500`, este valor para dejar un rango de flexibilidad para que el repartidor tome "atajos".
## 4. Estructura del Repositorio
```text
rapidgo-variante2-seguridad/
├── .github/workflows/        # Integración Continua
├── airflow/dags/             # DAGs de Orquestación
├── datalake/                 # Persistencia Parquet Raw/Processed/Curated
├── docs/                     # Documentación de arquitectura y reportes
│   ├── arquitectura.png      # Diagrama del pipeline
│   ├── checkpoints/
│   │   └── semana1.md        # Reporte Checkpoint Semana 1
│   └── decisions/
│       └── ADR-001-particionamiento-kafka.md # Registro técnico ADR-001
├── producer/                 # Productor Python de eventos
├── samples/
│   └── evento_sample.json    # Contrato de datos de prueba
├── spark/                    # Scripts de Spark Structured Streaming
├── tests/                    # Pruebas unitarias de esquemas y reglas
├── docker-compose.yml        # Entorno local comprimido
├── README.md                 # Documento principal
└── requirements.txt          # Librerías de Python
```
