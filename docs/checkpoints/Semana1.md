**1. Qué quedó funcionando esta semana**
- Repositorio de GitHub creado con los 3 colaboradores configurados (Karith Ávila, Nicolas Gonzalez, Laura Ortiz)
- Tablero GitHub Projects (Kanban) con columnas BACKLOG, READY, IN PROGRESS, REVIEW, DONE y más de 12 Issues creados e individualmente asignados.
- Estructura base de carpetas del proyecto inicializada.
- Contrato JSON oficial del evento de telemetría definido en `samples/event_example.json`.
- Arquitectura específica para la Variante 2 documentada y diagrama.

**2. Valor real u observación de ejecución** Se verificó la sintaxis del contrato JSON mediante validación formal del esquema. El evento incluye metadatos de GPS, métricas continuas de velocidad/batería y contextos cualitativos (desvíos e inactividad en metros/minutos), garantizando el consumo directo por parte de Spark Streaming.
  
**3. Decisión técnica más importante** Se redactó el ADR-001 documentando la decisión sobre key/particiones o decisión arquitectónica equivalente. Se definió utilizar `driver_id` como Partition Key y configurar 3 particiones en Apache Kafka. Esta decisión asegura la preservación del orden secuencial de los eventos por repartidor dentro de la misma partición, condición indispensable para calcular ventanas deslizantes de velocidad y aceleración sin desfasamiento temporal.

**4. Riesgo principal identificado**
- **Riesgo:** Desbalanceo de carga en las particiones de Kafka si un grupo reducido de repartidores genera un volumen desproporcionado de telemetría.
- **Mitigación:** Monitorear el volumen por partición y configurar un número de 3 particiones iniciales para distribuir uniformemente los identificadores mediante la función de hashing predeterminada de Kafka.

**5. Plan para la Semana 2 y Trazabilidad**
- Construir el Producer en Python para generación continua de telemetría.
- Configurar Spark Structured Streaming consumiendo de Kafka y escribiendo en la capa RAW/Processed del Data Lake en Parquet.
- Activar el flujo de CI en GitHub Actions para validación de sintaxis y pytest.
