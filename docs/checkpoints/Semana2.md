# Checkpoint de Seguimiento - Semana 2

**1. Qué quedó funcionando esta semana**
- Pipeline de Integración Continua (CI) en GitHub Actions (`CI.yml`) configurado para ramas `main` y `QA`, que automatiza la validación de sintaxis Python, instalación de dependencias, y comprobación estructural de `docker compose`.
- Suite de pruebas automatizadas con `pytest` (`test_security_rules.py`) aislando las reglas de negocio (velocidad > 45 km/h, batería < 15%) y manejando excepciones de telemetría inválida.
- *Producer* de Kafka en Python (`courier_producer.py`) finalizado. Genera telemetría iterativa de prueba para 3 repartidores utilizando el `courier_id` como *partition key* y enviando datos al tópico `driver_security_events`.
- *Consumer* en Spark Structured Streaming (`streaming_consumer.py`) implementado. Deserializa JSON, aplica reglas de alerta en tiempo real, procesa agregaciones por ventana temporal y materializa los datos en dos salidas (Sink): Consola y archivos Parquet.

**2. Valor real u observación de ejecución**
El flujo *End-to-End* es plenamente funcional. El CI garantiza la estabilidad del código en el repositorio, bloqueando integraciones que rompan las reglas de alerta. En tiempo de ejecución, el *Consumer* lee la telemetría sin pérdida de esquemas, evalúa instantáneamente las políticas de seguridad vial y de los equipos, y persiste la data procesada en Parquet de forma inmutable, preparando el terreno para capas de consumo analítico (BI) o bases de datos de servicio.

**3. Decisión técnica más importante**
La implementación del esquema arquitectónico documentado en el ADR-002: escribir en modo *append* hacia un formato columnar (Parquet) habilitando explícitamente el *Checkpointing* y *Watermarks*. Esta decisión transforma un simple script de lectura en un trabajo de streaming robusto, apto para producción, que tolera caídas del sistema y gestiona latencias de red sin corromper el cálculo de las ventanas de tiempo. Adicionalmente, el aislamiento de la lógica de evaluación en funciones puras testeables por `pytest` blinda la calidad del motor de reglas.

**4. Riesgo principal identificado**
- **Riesgo:** El problema de los "Archivos Pequeños" (*Small Files Problem*). Al escribir streaming continuo en modo *append* directo a disco, Spark generará un volumen alto de archivos Parquet muy pequeños, lo cual degradará el rendimiento de lectura del Data Lake en el futuro debido a la saturación de los metadatos del File System.
- **Mitigación:** En la siguiente fase se deberá ajustar el *trigger interval* en el método `writeStream` para generar lotes más grandes, o diseñar un proceso *batch* asíncrono (ej. una tarea en Airflow o un script cron programado) que periódicamente compacte los archivos pequeños de la carpeta `processed/security_telemetry` en archivos de mayor tamaño optimizados (aprox. 128 MB a 1 GB).

**5. Plan para la Semana 3 y Trazabilidad**
- Diseñar y desplegar la capa de servicio (Serving Layer): Enviar métricas procesadas a una base de datos relacional (ej. PostgreSQL) o NoSQL para habilitar consultas rápidas.
- Integrar una herramienta de visualización (ej. Grafana, Metabase o PowerBI) que lea las alertas de seguridad y muestre los repartidores en estado crítico en un dashboard en tiempo real.
- Modularizar y optimizar la infraestructura de Docker Compose para simular el ambiente de producción de forma integral.