# ADR-002: Estrategia de Persistencia Columnar y Tolerancia a Fallos en Streaming (Variante 2 - Seguridad del Repartidor)

- **Estado:** Aprobado
- **Fecha:** 29 de octubre de 2026
- **Autores:** Equipo 5 (Vivian Karith, Nicolas Gonzalez, Laura Ortiz)
- **Componente:** Procesamiento Streaming / Data Lake

## 1. Decisión Arquitectónica Tomada
Se define el uso del formato **Parquet** para la persistencia de datos en la capa *Processed* del Data Lake local, acompañado de un mecanismo de **Checkpointing** obligatorio en Spark Structured Streaming, y la implementación de **Watermarking de 10 minutos** para el manejo de datos atrasados (late data) en las agregaciones por ventanas.

## 2. Justificación Técnica en la Arquitectura del Pipeline

1. **Eficiencia de Almacenamiento y Lectura (Parquet):**
   Al escribir los eventos procesados (con alertas de batería y velocidad evaluadas) desde Spark hacia la ruta `data/processed/security_telemetry`, el formato columnar Parquet comprime los datos eficientemente y preserva de manera nativa el esquema (`StructType`) definido en la ingesta. Esto optimiza futuras consultas analíticas reduciendo el I/O en disco, ya que los motores de consulta solo leerán las columnas necesarias.

2. **Tolerancia a Fallos y Continuidad (Checkpointing):**
   El flujo de escritura en Parquet incluye la directiva `checkpointLocation`. Este mecanismo almacena metadatos del estado del motor de streaming y los *offsets* de Kafka procesados. Si el contenedor o proceso de Spark falla, al reiniciar la aplicación, Spark lee el checkpoint y reanuda el procesamiento exactamente donde lo dejó (semántica *Exactly-Once* a nivel de procesamiento), evitando la duplicación o pérdida de la telemetría de los repartidores.

3. **Manejo de Datos Atrasados (Watermarking):**
   El cálculo de velocidad promedio en ventanas deslizantes de 5 minutos requiere resiliencia frente a retrasos en la red móvil de los repartidores. El *Watermark* de 10 minutos sobre el `event_time` establece un umbral determinista: Spark mantendrá el estado de la ventana en memoria esperando datos rezagados hasta por 10 minutos antes de cerrarla definitivamente. Esto previene el desbordamiento de la memoria RAM (OutOfMemory) por retención indefinida de estados antiguos.

4. **Desacoplamiento de Reglas de Negocio:**
   La lógica de evaluación de riesgos (`evaluate_courier_risk`) se encapsuló y aisló del motor de Kafka/Spark, permitiendo su validación estricta mediante pruebas unitarias continuas integradas en el pipeline de GitHub Actions (CI).